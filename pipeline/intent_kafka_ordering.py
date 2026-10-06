"""
Cross-topic ordering for the Kafka intent consumer.

The problem. One actor's events are spread over four topics (checkout, atc, click, other).
Kafka orders records only inside one partition, and the session state machine is order
sensitive: an event that is applied after a later one has already moved the actor's cursor
can start a spurious session (a gap more negative than the 30 s tolerance). Sorting the
records of one poll does not fix that, because a lagging topic simply delivers its older
record in a later poll.

The order that matters. The old state machine ran inside /track and saw events in the order
/track handled them, not in pixel-time (occurred_at) order. /track stamps every record with
the time it sent it (Kafka CreateTime), so the record timestamp reproduces that arrival
order across topics. This module releases records in (timestamp, topic, partition) order.

The rule (a watermark, as in stream processors). The oldest buffered record H, timestamp T,
may be released only if every other assigned partition either
  * has a buffered record (its next record is no older than T), or
  * is caught up with the end of its log AND the clock has passed T + slack,
    because a record created before T could still be on its way to the log for up to
    `slack` (the producer's send deadline plus retries).
Otherwise a lagging partition could still deliver something older than H, so H waits.

Nothing is dropped or reordered beyond that: partitions keep their own Kafka order, and a
record that nevertheless arrives older than something already released (an append delay
longer than the slack) is counted and logged in `violations`, then applied as usual.

Memory is bounded: `over_capacity()` tells the consumer to pause the partitions that are
ahead, so only the lagging ones are fetched until the buffer drains.

Scope. The guarantee covers the partitions of ONE consumer. An actor's partitions in the
different topics must therefore all belong to the same consumer; the consumer enforces that
(see IntentConsumer._domain_ok).
"""

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

TopicPartitionKey = Tuple[str, int]

DEFAULT_SLACK_MS = 10_000
# librdkafka refreshes a partition's high watermark with every fetch response, at most
# fetch.wait.max.ms (500 ms) old; the margin keeps that staleness inside the guarantee.
WATERMARK_STALENESS_MARGIN_MS = 1_000


@dataclass
class Buffered:
    ts_ms: int
    key: TopicPartitionKey
    offset: int
    record: Any
    size: int


class OrderingBuffer:
    def __init__(
        self,
        slack_ms: int = DEFAULT_SLACK_MS,
        max_records: int = 2000,
        max_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self.slack_ms = slack_ms
        self.max_records = max_records
        self.max_bytes = max_bytes
        self._queues: Dict[TopicPartitionKey, Deque[Buffered]] = {}
        self._assigned: Set[TopicPartitionKey] = set()
        self._last_ts: Dict[TopicPartitionKey, int] = {}
        self.records = 0
        self.bytes = 0
        self.high_water_records = 0
        self.released_high_ts: Optional[int] = None
        self.violations = 0

    # ---------- assignment ----------

    def set_assignment(self, keys: Iterable[TopicPartitionKey]) -> None:
        self._assigned = set(keys)
        for key in [k for k in self._queues if k not in self._assigned]:
            self.drop(key)

    def clear(self) -> None:
        for key in list(self._queues):
            self.drop(key)
        self._assigned = set()
        self._last_ts.clear()

    def drop(self, key: TopicPartitionKey) -> None:
        """Forgets a partition's buffered records (they will be re-read after a seek or rebalance)."""
        for item in self._queues.pop(key, ()):
            self.records -= 1
            self.bytes -= item.size
        self._last_ts.pop(key, None)

    # ---------- input ----------

    def add(self, key: TopicPartitionKey, ts_ms: int, offset: int, record: Any, size: int) -> bool:
        """Buffers a record. Returns False if it is older than something already released."""
        late = self.released_high_ts is not None and ts_ms < self.released_high_ts
        if late:
            self.violations += 1
        # Timestamps inside a partition may wobble by a few ms; keep the partition's own order.
        ts = max(ts_ms, self._last_ts.get(key, ts_ms))
        self._last_ts[key] = ts
        self._queues.setdefault(key, deque()).append(Buffered(ts, key, offset, record, size))
        self.records += 1
        self.bytes += size
        self.high_water_records = max(self.high_water_records, self.records)
        return not late

    # ---------- capacity ----------

    def over_capacity(self) -> bool:
        return self.records >= self.max_records or self.bytes >= self.max_bytes

    def drained(self) -> bool:
        return self.records <= self.max_records // 2 and self.bytes <= self.max_bytes // 2

    def buffered_partitions(self) -> List[TopicPartitionKey]:
        return [key for key, queue in self._queues.items() if queue]

    # ---------- output ----------

    def release(self, limit: int, at_end: Callable[[TopicPartitionKey], bool], now_ms: int) -> List[Any]:
        """Records that are safe to apply now, in (timestamp, topic, partition, offset) order."""
        out: List[Any] = []
        caught_up: Dict[TopicPartitionKey, bool] = {}
        while len(out) < limit:
            heads = {key: queue[0] for key, queue in self._queues.items() if queue}
            if not heads:
                break
            key, head = min(heads.items(), key=lambda kv: (kv[1].ts_ms, kv[0], kv[1].offset))
            if not self._safe(head, heads, at_end, caught_up, now_ms):
                break
            self._queues[key].popleft()
            self.records -= 1
            self.bytes -= head.size
            self.released_high_ts = head.ts_ms if self.released_high_ts is None else max(self.released_high_ts, head.ts_ms)
            out.append(head.record)
        return out

    def _safe(self, head, heads, at_end, caught_up, now_ms) -> bool:
        for other in self._assigned:
            if other in heads:
                continue
            if other not in caught_up:
                caught_up[other] = bool(at_end(other))
            if not caught_up[other]:
                return False                      # a lagging partition may still hold something older
            if now_ms < head.ts_ms + self.slack_ms + WATERMARK_STALENESS_MARGIN_MS:
                return False                      # caught up, but an older record may still be in flight
        return True

    def waiting_on(self, at_end: Callable[[TopicPartitionKey], bool], now_ms: int) -> Dict[str, Any]:
        """Why nothing was released, for the stall log."""
        heads = {key: queue[0] for key, queue in self._queues.items() if queue}
        if not heads:
            return {}
        head = min(heads.values(), key=lambda b: (b.ts_ms, b.key, b.offset))
        lagging = sorted(f"{k[0]}[{k[1]}]" for k in self._assigned if k not in heads and not at_end(k))
        return {"oldest_ms": now_ms - head.ts_ms, "lagging": lagging, "buffered": self.records}
