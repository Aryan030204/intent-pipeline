"""
Kafka -> MySQL intent ingestion, run as batch cycles (not a polling loop).

A cycle runs when EITHER trigger fires (the worker owns the triggers, see workers/intent_kafka_worker.py):
  * batch completion: the events waiting across all topics reach INTENT_EVENTS_BATCH
                      (`pending_events()`: end offset - committed offset, read from offsets only);
  * schedule:         every INTENT_KAFKA_RUN_EVERY_MINUTES (default 25), whatever is waiting.

    cycle:  take the single-run lock
      -> for every partition: range = [committed offset, first offset newer than now - slack)
      -> read exactly those ranges, merged across partitions in /track send order (Kafka timestamp)
      -> validate each record (invalid -> DLQ, never blocks the partition)
      -> ONE MySQL transaction per brand per batch   (apply_batch, savepoint per message)
      -> COMMIT
      -> commit Kafka offsets, per partition, only up to the first unresolved record
      -> close the consumer and release the lock

The invariant: a Kafka offset is committed only after the MySQL COMMIT of every record at or
below it (or after the record was delivered to the DLQ). Nothing commits an offset first. A crash
between COMMIT and the offset commit replays records, which MySQL absorbs (UNIQUE event_id, ATC
primary key, cursor untouched by duplicates).

Ordering. An actor's events sit on up to four topics, which Kafka does not order against each
other, and the session state machine is order sensitive. /track stamps every record with its send
time (Kafka CreateTime); the old state machine saw events in the order /track handled them, which
is that order. A cycle therefore only takes records older than `now - slack` (slack covers a record
still on its way to the log, default 10 s), so everything it takes is already in the log, and merges
all partitions by (timestamp, topic, partition, offset): exact send order, no waiting heuristics. A
record that still appears older than something already applied (an append delay beyond the slack)
is applied in arrival order and counted: `order_violations`.

Failure classes
- Poison (InvalidMessage, unknown brand, or a row MySQL rejects as bad data): a validation failure
  goes to the DLQ at once. A MySQL data rejection is retried up to max_record_attempts times within
  the cycle, then goes to the DLQ. The DLQ send must be acknowledged before the offset moves.
- Retryable (connection loss, deadlock, lock timeout, failed COMMIT, unacknowledged DLQ send, any
  unclassified error): the brand's transaction rolls back, no offset moves past the records, and the
  cycle stops. The next trigger starts again from the committed offsets.
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Hashable, List, Optional, Set, Tuple

from pipeline.intent_kafka_contract import (
    INTENT_TOPICS,
    InvalidMessage,
    expected_topic,
    parse_record,
)
from pipeline.intent_kafka_ordering import DEFAULT_SLACK_MS, WATERMARK_STALENESS_MARGIN_MS, OrderingBuffer
from pipeline.intent_kafka_writer import BrandResult, WorkItem, apply_batch
from pipeline.intent_session_state import DEFAULT_SESSION_TIMEOUT_S
from pipeline.state import logger

DEFAULT_GROUP_ID = "intent-pipeline-workers"   # only the name offsets are committed under
DEFAULT_DLQ_TOPIC = "intent.dlq"
DLQ_FLUSH_TIMEOUT_S = 30.0
OFFSET_INVALID = -1001
MAX_POLL_RECORDS = 500


@dataclass
class ConsumerConfig:
    bootstrap_servers: str = "kafka-service:9092"
    group_id: str = DEFAULT_GROUP_ID
    topics: Tuple[str, ...] = INTENT_TOPICS
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    events_batch: int = 500                  # INTENT_EVENTS_BATCH: the trigger size and the records per MySQL transaction
    run_every_minutes: float = 25.0          # the schedule trigger
    check_interval_s: float = 30.0           # how often the batch trigger looks at the pending count
    max_record_attempts: int = 3
    session_timeout_s: int = DEFAULT_SESSION_TIMEOUT_S
    order_slack_s: float = DEFAULT_SLACK_MS / 1000
    order_buffer_max: int = 2000
    stall_timeout_s: float = 60.0            # a cycle that makes no progress for this long gives up

    def kafka_settings(self, client_id: str) -> Dict[str, Any]:
        """librdkafka settings. Partitions are assigned by hand (no group membership, no rebalancing);
        the group id only names where offsets are committed. Offsets are manual, and the prefetch
        queue is small so a slow MySQL cannot make memory grow."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "fetch.max.bytes": 8 * 1024 * 1024,
            "max.partition.fetch.bytes": 1024 * 1024,
            "queued.max.messages.kbytes": 8192,
            "queued.min.messages": max(100, min(self.events_batch, MAX_POLL_RECORDS) * 2),
            "enable.partition.eof": False,
        }


@dataclass
class CycleResult:
    reason: str
    status: str               # done | empty | stopped | aborted | skipped
    records: int = 0
    detail: str = ""
    seconds: float = 0.0
    counters: Dict[str, int] = field(default_factory=dict)


def _ref(msg) -> Tuple[str, int, int]:
    return (msg.topic(), msg.partition(), msg.offset())


class IntentIngestor:
    """`kafka_factory()` returns a new confluent_kafka.Consumer (never subscribed), `dlq` a Producer,
    `db.transaction(brand_index)` a context manager yielding a store and committing on exit,
    `brand_resolver(brand_id)` the brand's database index or None, `run_lock` an object with
    acquire() -> bool and release() that keeps two workers from running a cycle at once."""

    def __init__(
        self,
        kafka_factory: Callable[[], Any],
        dlq,
        db,
        brand_resolver: Callable[[str], Optional[int]],
        config: Optional[ConsumerConfig] = None,
        stop_event: Optional[threading.Event] = None,
        name: str = "intent-kafka",
        topic_partition_factory: Optional[Callable[..., Any]] = None,
        clock_ms: Optional[Callable[[], int]] = None,
        run_lock=None,
    ) -> None:
        self.kafka_factory = kafka_factory
        self.dlq = dlq
        self.db = db
        self.brand_resolver = brand_resolver
        self.cfg = config or ConsumerConfig()
        self.stop_event = stop_event or threading.Event()
        self.name = name
        self._tp = topic_partition_factory or _confluent_topic_partition
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.run_lock = run_lock
        self.kafka = None                      # the consumer of the cycle in progress
        self._monitor = None                   # offsets-only consumer for pending_events()
        self._attempts: Dict[Hashable, int] = {}
        self.stats: Dict[str, int] = defaultdict(int)
        # Slack is applied as the cycle's cutoff, not as a per-record wait, so the buffer itself waits for nothing
        # but a partition that has not been read yet. It lives across cycles to remember what was already applied.
        self.ordering = OrderingBuffer(0, self.cfg.order_buffer_max)
        self._backpressure = False
        self._paused_now: Set[Tuple[str, int]] = set()
        self.last_resolved: Set[Hashable] = set()

    # ---------- trigger 1: batch completion ----------

    def pending_events(self) -> int:
        """Events waiting across all topics: for every partition, end offset - the offset ingestion
        has committed (or the start of the log). Reads offsets only; consumes nothing."""
        try:
            if self._monitor is None:
                self._monitor = self.kafka_factory()
            return sum(max(0, end - start) for start, end in self._offset_bounds(self._monitor).values())
        except Exception:
            self._close_monitor()
            raise

    def _close_monitor(self) -> None:
        monitor, self._monitor = self._monitor, None
        if monitor is not None:
            try:
                monitor.close()
            except Exception:
                pass

    def _partitions(self, kafka) -> List[Tuple[str, int]]:
        meta = kafka.list_topics(timeout=10).topics
        keys = []
        for topic in self.cfg.topics:
            if topic not in meta or getattr(meta[topic], "error", None):
                raise RuntimeError(f"topic {topic} is not available")
            keys.extend((topic, p) for p in sorted(meta[topic].partitions))
        return keys

    def _offset_bounds(self, kafka, cutoff_ms: Optional[int] = None) -> Dict[Tuple[str, int], Tuple[int, int]]:
        """{partition: (start, end)} where start is the committed offset (or the log start) and end the
        log end, or with a cutoff the first offset whose timestamp is at or after it."""
        keys = self._partitions(kafka)
        committed = {(tp.topic, tp.partition): tp.offset for tp in
                     kafka.committed([self._tp(t, p, OFFSET_INVALID) for t, p in keys], timeout=15)}
        bounds = {}
        for key in keys:
            tp = self._tp(key[0], key[1], OFFSET_INVALID)
            low, high = kafka.get_watermark_offsets(tp, timeout=10, cached=False)
            start = committed.get(key, OFFSET_INVALID)
            start = low if start is None or start < 0 else max(start, low)
            end = high
            if cutoff_ms is not None and high > start:
                found = kafka.offsets_for_times([self._tp(key[0], key[1], cutoff_ms)], timeout=10)[0].offset
                if found is not None and found >= 0:
                    end = min(found, high)
            bounds[key] = (start, max(start, end))
        return bounds

    # ---------- one cycle (either trigger) ----------

    def run_cycle(self, reason: str) -> CycleResult:
        started = time.monotonic()
        if self.run_lock is not None and not self.run_lock.acquire():
            logger.warning(f"[intent-kafka] category=cycle_skipped reason={reason} another worker holds the run lock")
            return CycleResult(reason, "skipped", detail="another worker is running a cycle")
        before = dict(self.stats)
        kafka = None
        try:
            kafka = self.kafka = self.kafka_factory()
            cutoff = self.clock_ms() - int(self.cfg.order_slack_s * 1000) - WATERMARK_STALENESS_MARGIN_MS
            ranges = {k: v for k, v in self._offset_bounds(kafka, cutoff).items() if v[1] > v[0]}
            if not ranges:
                logger.info(f"[intent-kafka] category=cycle_empty trigger={reason} nothing older than the slack is waiting")
                return CycleResult(reason, "empty", seconds=time.monotonic() - started)
            total = sum(end - start for start, end in ranges.values())
            logger.info(
                f"[intent-kafka] category=cycle_started trigger={reason} records<={total} "
                f"ranges={ {f'{t}[{p}]': f'{s}-{e - 1}' for (t, p), (s, e) in sorted(ranges.items())} }"
            )
            status, detail = self._drain(kafka, ranges)
            counters = {k: self.stats[k] - before.get(k, 0) for k in self.stats if self.stats[k] != before.get(k, 0)}
            elapsed = time.monotonic() - started
            logger.info(f"[intent-kafka] category=cycle_finished trigger={reason} status={status} "
                        f"seconds={elapsed:.1f} counters={counters} {detail}")
            return CycleResult(reason, status, counters.get("records", 0), detail, elapsed, counters)
        except Exception as exc:
            logger.error(f"[intent-kafka] category=cycle_failed trigger={reason} {type(exc).__name__}: {exc}")
            return CycleResult(reason, "aborted", detail=f"{type(exc).__name__}: {exc}", seconds=time.monotonic() - started)
        finally:
            self.ordering.clear()
            self._backpressure = False
            self._paused_now = set()
            self.kafka = None
            if kafka is not None:
                try:
                    kafka.close()
                except Exception:
                    pass
            if self.run_lock is not None:
                self.run_lock.release()

    def _drain(self, kafka, ranges) -> Tuple[str, str]:
        kafka.assign([self._tp(t, p, start) for (t, p), (start, _end) in sorted(ranges.items())])
        self.ordering.clear()
        self.ordering.set_assignment(ranges)
        exhausted: Set[Tuple[str, int]] = set()
        last_progress = time.monotonic()
        while True:
            if self.stop_event.is_set():
                return "stopped", "stop requested; processed batches are committed, the rest waits for the next cycle"
            fetched = self._fetch(kafka, ranges, exhausted)
            self._apply_pauses(kafka, exhausted)
            released = self.ordering.release(self.cfg.events_batch, lambda key: key in exhausted, self.clock_ms())
            if released:
                if not self._process_with_retry(released):
                    return "aborted", "a batch could not be committed; offsets stay where they were"
            if fetched or released:
                last_progress = time.monotonic()
            if len(exhausted) == len(ranges) and self.ordering.records == 0:
                return "done", ""
            if time.monotonic() - last_progress > self.cfg.stall_timeout_s:
                return "aborted", f"no progress for {self.cfg.stall_timeout_s:.0f}s (exhausted {len(exhausted)}/{len(ranges)})"

    def _fetch(self, kafka, ranges, exhausted) -> int:
        messages = kafka.consume(num_messages=min(self.cfg.events_batch, MAX_POLL_RECORDS), timeout=1.0)
        taken = 0
        for msg in messages or []:
            error = msg.error()
            if error is not None:
                self.stats["poll_errors"] += 1
                if getattr(error, "fatal", lambda: False)():
                    raise RuntimeError(f"fatal Kafka error: {error}")
                logger.warning(f"[intent-kafka] category=kafka_poll_error name={self.name} error={error}")
                continue
            key = (msg.topic(), msg.partition())
            if key not in ranges or msg.offset() >= ranges[key][1]:
                exhausted.add(key)                      # beyond this cycle's range: left for the next one
                continue
            _kind, ts = msg.timestamp()
            ts_ms = ts if isinstance(ts, int) and ts > 0 else self.clock_ms()
            if not self.ordering.add(key, ts_ms, msg.offset(), msg, len(msg.value() or b"")):
                self.stats["order_violations"] += 1
                logger.error(
                    f"[intent-kafka] category=order_violation topic={key[0]} partition={key[1]} offset={msg.offset()} "
                    f"older than events already applied (append delay above the {self.cfg.order_slack_s}s slack); "
                    f"applied in arrival order"
                )
            taken += 1
            if msg.offset() + 1 >= ranges[key][1]:
                exhausted.add(key)
        for key, (_start, end) in ranges.items():       # control records leave no message to see
            if key not in exhausted:
                try:
                    position = kafka.position([self._tp(key[0], key[1], OFFSET_INVALID)])[0].offset
                except Exception:
                    continue
                if position is not None and position >= end:
                    exhausted.add(key)
        self.stats["ordering_buffer_high_water"] = self.ordering.high_water_records
        return taken

    def _apply_pauses(self, kafka, exhausted) -> None:
        """Stops fetching partitions that are fully read, and under memory pressure the ones that are
        ahead (recomputed every pass: a partition that has run dry must be resumed at once, it may be
        the one everything else is waiting for)."""
        if self.ordering.over_capacity():
            if not self._backpressure:
                logger.warning(f"[intent-kafka] category=ordering_backpressure buffered={self.ordering.records} "
                               f"pausing the partitions that are ahead")
            self._backpressure = True
        elif self._backpressure and self.ordering.drained():
            self._backpressure = False
        ahead = set(self.ordering.buffered_partitions()) if self._backpressure else set()
        wanted = ahead | set(exhausted)
        to_pause, to_resume = wanted - self._paused_now, self._paused_now - wanted
        if to_pause:
            kafka.pause([self._tp(t, p, OFFSET_INVALID) for t, p in sorted(to_pause)])
        if to_resume:
            kafka.resume([self._tp(t, p, OFFSET_INVALID) for t, p in sorted(to_resume)])
        self._paused_now = wanted

    def _process_with_retry(self, released: list) -> bool:
        """Applies a released batch. Rows MySQL rejects are retried straight away, up to
        max_record_attempts, then quarantined; any infrastructure failure stops the cycle."""
        resolved: Set[Hashable] = set()
        for _attempt in range(self.cfg.max_record_attempts + 1):
            watched = ("txn_failures", "dlq_failures", "offset_commit_failures")
            before = {k: self.stats[k] for k in watched}
            ok = self.process_batch(released, resolved)
            resolved = self.last_resolved          # later passes leave settled records alone
            if ok:
                return True
            if any(self.stats[k] > before[k] for k in watched):
                return False
        return False

    def close(self) -> None:
        self._close_monitor()
        close = getattr(self.db, "close", None)
        if close:
            close()

    # ---------- one batch ----------

    def process_batch(self, records: list, already_resolved: Optional[Set[Hashable]] = None) -> bool:
        """Returns True when every record was resolved (committed to MySQL or sent to the
        DLQ) and its offsets were committed. `already_resolved` are records an earlier pass of the
        same batch settled; they are not processed (or quarantined) again, but they still count
        when working out how far each partition's offset can move. After the call,
        `self.last_resolved` holds everything resolved so far."""
        started = time.monotonic()
        order: List[Hashable] = [_ref(m) for m in records]
        by_ref = {_ref(m): m for m in records}
        resolved: Set[Hashable] = set(already_resolved or ())
        dlq_entries: List[Tuple[Any, str, int]] = []  # (record, reason, attempts)

        work: Dict[int, List[WorkItem]] = defaultdict(list)
        brand_of_index: Dict[int, str] = {}
        for msg in records:
            ref = _ref(msg)
            if ref in resolved:
                continue
            try:
                message = parse_record(msg.value())
            except InvalidMessage as exc:
                dlq_entries.append((msg, f"invalid_message: {exc}", 0))
                continue
            if expected_topic(message.event_name) != msg.topic():
                logger.warning(
                    f"[intent-kafka] category=topic_mismatch event_id={message.event_id} "
                    f"event_name={message.event_name} topic={msg.topic()}"
                )
            brand_index = self.brand_resolver(message.brand_id)
            if brand_index is None:
                dlq_entries.append((msg, f"unknown_brand: {message.brand_id}", 0))
                continue
            brand_of_index[brand_index] = message.brand_id
            work[brand_index].append(WorkItem(ref, message))

        for brand_index, items in work.items():
            brand = brand_of_index[brand_index]
            try:
                with self.db.transaction(brand_index) as store:
                    result: BrandResult = apply_batch(store, items, self.cfg.session_timeout_s)
            except Exception as exc:  # transaction rolled back by the provider
                self.stats["txn_failures"] += 1
                logger.error(
                    f"[intent-kafka] category=mysql_transaction_failed name={self.name} brand={brand} "
                    f"records={len(items)} offsets_not_committed reason={type(exc).__name__}: {str(exc)[:300]}"
                )
                continue

            for item in items:
                if item.ref in result.failed:
                    attempts = self._attempts.get(item.ref, 0) + 1
                    self._attempts[item.ref] = attempts
                    if attempts >= self.cfg.max_record_attempts:
                        dlq_entries.append((by_ref[item.ref], f"rejected_by_mysql: {result.failed[item.ref]}", attempts))
                    else:
                        self.stats["record_retries"] += 1
                else:
                    resolved.add(item.ref)
                    self._attempts.pop(item.ref, None)
            for key, value in result.counts().items():
                self.stats[key] += value
            logger.info(
                f"[intent-kafka] category=batch_committed name={self.name} brand={brand} "
                f"records={len(items)} counts={result.counts()} partitions={self._describe(items)}"
            )

        for ref in self._send_to_dlq(dlq_entries):
            resolved.add(ref)
            self._attempts.pop(ref, None)

        self.last_resolved = resolved
        return self._commit_offsets(order, resolved, started)

    @staticmethod
    def _describe(items: List[WorkItem]) -> List[str]:
        spans: Dict[Tuple[str, int], List[int]] = defaultdict(list)
        for item in items:
            spans[item.ref[:2]].append(item.ref[2])
        return [f"{t}[{p}]:{min(o)}-{max(o)}" for (t, p), o in sorted(spans.items())]

    # ---------- DLQ ----------

    def _send_to_dlq(self, entries: List[Tuple[Any, str, int]]) -> Set[Hashable]:
        """Returns the refs whose DLQ delivery was acknowledged."""
        if not entries:
            return set()
        delivered: Set[Hashable] = set()
        failed: Dict[Hashable, str] = {}
        for msg, reason, attempts in entries:
            ref = _ref(msg)

            def on_delivery(err, _record, ref=ref):
                if err is None:
                    delivered.add(ref)
                else:
                    failed[ref] = str(err)

            try:
                self.dlq.produce(
                    self.cfg.dlq_topic,
                    key=msg.key(),
                    value=msg.value() if msg.value() is not None else b"",
                    headers=[
                        ("dlq_reason", reason.encode("utf-8")[:1000]),
                        ("dlq_source_topic", ref[0].encode("utf-8")),
                        ("dlq_source_partition", str(ref[1]).encode("utf-8")),
                        ("dlq_source_offset", str(ref[2]).encode("utf-8")),
                        ("dlq_attempts", str(attempts).encode("utf-8")),
                        ("dlq_failed_at", str(int(time.time())).encode("utf-8")),
                    ],
                    on_delivery=on_delivery,
                )
            except Exception as exc:
                failed[ref] = f"{type(exc).__name__}: {exc}"
        try:
            self.dlq.flush(DLQ_FLUSH_TIMEOUT_S)
        except Exception as exc:
            logger.error(f"[intent-kafka] category=dlq_flush_failed name={self.name} error={exc}")

        for msg, reason, attempts in entries:
            ref = _ref(msg)
            if ref in delivered:
                self.stats["dlq_sent"] += 1
                logger.error(
                    f"[intent-kafka] category=poison_quarantined name={self.name} topic={ref[0]} "
                    f"partition={ref[1]} offset={ref[2]} dlq={self.cfg.dlq_topic} attempts={attempts} reason={reason[:300]}"
                )
            else:
                self.stats["dlq_failures"] += 1
                logger.error(
                    f"[intent-kafka] category=dlq_delivery_failed name={self.name} topic={ref[0]} "
                    f"partition={ref[1]} offset={ref[2]} record_stays_unresolved error={failed.get(ref, 'not acknowledged')}"
                )
        return delivered

    # ---------- offsets ----------

    def _commit_offsets(self, order: List[Hashable], resolved: Set[Hashable], started: float) -> bool:
        """Commits, per partition, the offset after the last record of the leading run of resolved
        records. A partition with an unresolved record keeps its offset at that record."""
        per_partition: Dict[Tuple[str, int], List[int]] = defaultdict(list)
        for topic, partition, offset in order:
            per_partition[(topic, partition)].append(offset)

        to_commit = []
        blocked_any = False
        for (topic, partition), offsets in per_partition.items():
            offsets.sort()
            next_offset = offsets[0]
            for offset in offsets:
                if (topic, partition, offset) in resolved:
                    next_offset = offset + 1
                else:
                    blocked_any = True
                    break
            if next_offset > offsets[0]:
                to_commit.append(self._tp(topic, partition, next_offset))

        committed = True
        if to_commit:
            try:
                self.kafka.commit(offsets=to_commit, asynchronous=False)
                self.stats["offset_commits"] += 1
                logger.info(
                    f"[intent-kafka] category=offsets_committed name={self.name} "
                    f"offsets={[(p.topic, p.partition, p.offset) for p in to_commit]} "
                    f"elapsed_ms={(time.monotonic() - started) * 1000:.0f}"
                )
            except Exception as exc:
                # MySQL already committed; the records are read again next cycle and absorbed as duplicates.
                committed = False
                self.stats["offset_commit_failures"] += 1
                logger.error(
                    f"[intent-kafka] category=offset_commit_failed name={self.name} "
                    f"error={type(exc).__name__}: {exc} (records will be read again and deduplicated)"
                )
        self.stats["records"] += len(order)
        return committed and not blocked_any


def _confluent_topic_partition(topic: str, partition: int, offset: int = OFFSET_INVALID):
    from confluent_kafka import TopicPartition

    return TopicPartition(topic, partition, offset)
