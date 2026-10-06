"""
Kafka -> MySQL intent consumer: batching, per-brand transactions, manual offsets, DLQ.

    poll a bounded batch
      -> validate every record (invalid -> DLQ, never blocks the partition)
      -> group valid records by brand, merge partitions by occurred_at
      -> ONE MySQL transaction per brand           (apply_batch, savepoint per message)
      -> COMMIT
      -> commit Kafka offsets, per partition, only up to the first unresolved record

The invariant: a Kafka offset is committed only after the MySQL COMMIT of every record at
or below it (or after the record was delivered to the DLQ). Nothing here ever commits an
offset first. A crash between COMMIT and the offset commit replays records, which MySQL
absorbs (UNIQUE event_id, ATC primary key, cursor untouched by duplicates).

Ordering (pipeline/intent_kafka_ordering.py). Records are not applied as they are polled.
They are buffered and released in Kafka-timestamp order across all of this consumer's
partitions, behind a watermark, so a lagging topic cannot deliver an older event after the
actor's session state has moved on. Offsets only ever cover released-and-resolved records.
The guarantee is per consumer, so a consumer must own every partition an actor can land on:
ordering_domain "single" (one consumer owns everything) or "copartitioned" (topics with equal
partition counts, so partition i of every topic goes to the same consumer). A consumer whose
assignment violates the domain pauses instead of processing, and says so loudly.

Failure classes
- Poison (InvalidMessage, unknown brand, or a row MySQL rejects as bad data): a validation
  failure goes to the DLQ at once because retrying cannot help. A MySQL data rejection is
  retried up to max_record_attempts redeliveries, then goes to the DLQ. The DLQ send must
  be acknowledged before the offset moves past the record; if it is not, the record stays
  unresolved and the partition waits.
- Retryable (connection loss, deadlock, lock timeout, a failed COMMIT, any unclassified
  error): the brand's transaction rolls back, no offset moves for the partitions that
  brand's records came from, the consumer seeks those partitions back to the first
  unresolved offset and retries after a backoff. Healthy brands in the same poll still
  commit; their records are idempotent when the partition is read again.
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Hashable, List, Optional, Set, Tuple

from pipeline.intent_kafka_contract import (
    INTENT_TOPICS,
    InvalidMessage,
    expected_topic,
    parse_record,
)
from pipeline.intent_kafka_ordering import DEFAULT_SLACK_MS, OrderingBuffer
from pipeline.intent_kafka_writer import BrandResult, WorkItem, apply_batch
from pipeline.intent_session_state import DEFAULT_SESSION_TIMEOUT_S
from pipeline.state import logger

DEFAULT_GROUP_ID = "intent-pipeline-workers"
DEFAULT_DLQ_TOPIC = "intent.dlq"
DLQ_FLUSH_TIMEOUT_S = 30.0


@dataclass
class ConsumerConfig:
    bootstrap_servers: str = "kafka-service:9092"
    group_id: str = DEFAULT_GROUP_ID
    topics: Tuple[str, ...] = INTENT_TOPICS
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    batch_size: int = 200
    batch_wait_s: float = 1.0
    max_record_attempts: int = 3
    retry_backoff_s: float = 2.0
    retry_backoff_max_s: float = 30.0
    session_timeout_s: int = DEFAULT_SESSION_TIMEOUT_S
    stats_interval_s: float = 60.0
    ordering_domain: str = "single"          # "single" or "copartitioned"
    order_slack_s: float = DEFAULT_SLACK_MS / 1000
    order_buffer_max: int = 2000

    def kafka_settings(self, client_id: str) -> Dict[str, Any]:
        """librdkafka settings. Offsets are manual, and the prefetch queue is small so a
        slow MySQL cannot make memory grow: at most about queued.max.messages.kbytes of
        records sit in the client beyond the batch being processed."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
            "fetch.max.bytes": 8 * 1024 * 1024,
            "max.partition.fetch.bytes": 1024 * 1024,
            "queued.max.messages.kbytes": 8192,
            "queued.min.messages": max(100, self.batch_size * 2),
            "enable.partition.eof": False,
        }


class FatalConsumerError(RuntimeError):
    """The consumer cannot continue safely; the process should exit and be restarted."""


def _ref(msg) -> Tuple[str, int, int]:
    return (msg.topic(), msg.partition(), msg.offset())


class IntentConsumer:
    """One consumer (one thread). `kafka` is a confluent_kafka.Consumer, `dlq` a Producer,
    `db.transaction(brand_index)` a context manager yielding a store and committing on exit,
    `brand_resolver(brand_id)` the brand's database index or None."""

    def __init__(
        self,
        kafka,
        dlq,
        db,
        brand_resolver: Callable[[str], Optional[int]],
        config: Optional[ConsumerConfig] = None,
        stop_event: Optional[threading.Event] = None,
        name: str = "consumer-0",
        topic_partition_factory: Optional[Callable[[str, int, int], Any]] = None,
        clock_ms: Optional[Callable[[], int]] = None,
    ) -> None:
        self.kafka = kafka
        self.dlq = dlq
        self.db = db
        self.brand_resolver = brand_resolver
        self.cfg = config or ConsumerConfig()
        self.stop_event = stop_event or threading.Event()
        self.name = name
        self._tp = topic_partition_factory or _confluent_topic_partition
        self._attempts: Dict[Hashable, int] = {}
        self._backoff = self.cfg.retry_backoff_s
        self._last_stats = time.monotonic()
        self.stats: Dict[str, int] = defaultdict(int)
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.ordering = OrderingBuffer(int(self.cfg.order_slack_s * 1000), self.cfg.order_buffer_max)
        self._assignment_dirty = True
        self._domain_ok = False
        self._backpressure = False
        self._paused_domain: Set[Tuple[str, int]] = set()
        self._paused_now: Set[Tuple[str, int]] = set()
        self._last_domain_log = 0.0
        self._last_stall_log = 0.0

    # ---------- lifecycle ----------

    def run(self) -> None:
        logger.info(
            f"[intent-kafka] category=consumer_started name={self.name} group={self.cfg.group_id} "
            f"topics={list(self.cfg.topics)} batch_size={self.cfg.batch_size} "
            f"batch_wait_s={self.cfg.batch_wait_s} max_attempts={self.cfg.max_record_attempts}"
        )
        self.kafka.subscribe(
            list(self.cfg.topics),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        try:
            more_ready = False
            while not self.stop_event.is_set():
                self._buffer(self._poll(0.0 if more_ready else self.cfg.batch_wait_s))
                self._maybe_log_stats()
                ready = self._refresh_domain()
                self._apply_pauses()
                released = []
                if ready:
                    released = self.ordering.release(self.cfg.batch_size, self._at_end, self.clock_ms())
                more_ready = len(released) >= self.cfg.batch_size
                if not released:
                    self._log_stall()
                    continue
                if self.process_batch(released):
                    self._backoff = self.cfg.retry_backoff_s
                else:
                    self._wait_backoff()
        finally:
            self._shutdown()

    def stop(self) -> None:
        self.stop_event.set()

    def _shutdown(self) -> None:
        logger.info(f"[intent-kafka] category=shutdown name={self.name} closing consumer and DB connections")
        try:
            self.dlq.flush(DLQ_FLUSH_TIMEOUT_S)
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning(f"[intent-kafka] category=shutdown dlq flush failed: {exc}")
        try:
            self.kafka.close()
        except Exception as exc:
            logger.warning(f"[intent-kafka] category=shutdown consumer close failed: {exc}")
        close = getattr(self.db, "close", None)
        if close:
            close()
        logger.info(f"[intent-kafka] category=shutdown name={self.name} stopped stats={dict(self.stats)}")

    def _on_assign(self, _consumer, partitions) -> None:
        self._assignment_dirty = True
        self.ordering.set_assignment((p.topic, p.partition) for p in partitions)
        logger.info(
            f"[intent-kafka] category=partitions_assigned name={self.name} "
            f"partitions={[f'{p.topic}[{p.partition}]' for p in partitions]}"
        )

    def _on_revoke(self, _consumer, partitions) -> None:
        # Runs inside poll(), between batches: no batch is in flight, and every batch's
        # offsets were committed synchronously before the next poll, so nothing to flush.
        self._attempts.clear()
        self._reset_assignment_state()
        logger.info(
            f"[intent-kafka] category=partitions_revoked name={self.name} "
            f"partitions={[f'{p.topic}[{p.partition}]' for p in partitions]}"
        )

    def _on_lost(self, _consumer, partitions) -> None:
        self._attempts.clear()
        self._reset_assignment_state()
        logger.warning(f"[intent-kafka] category=partitions_lost name={self.name} count={len(partitions)}")

    def _poll(self, timeout: float) -> list:
        messages = self.kafka.consume(num_messages=self.cfg.batch_size, timeout=timeout)
        usable = []
        for msg in messages or []:
            error = msg.error()
            if error is None:
                usable.append(msg)
            elif getattr(error, "fatal", lambda: False)():
                raise FatalConsumerError(f"fatal Kafka error: {error}")
            else:
                self.stats["poll_errors"] += 1
                logger.warning(f"[intent-kafka] category=kafka_poll_error name={self.name} error={error}")
        return usable

    def _wait_backoff(self) -> None:
        delay = self._backoff
        self._backoff = min(self._backoff * 2, self.cfg.retry_backoff_max_s)
        logger.warning(f"[intent-kafka] category=retry_backoff name={self.name} sleeping_s={delay:.1f}")
        self.stop_event.wait(delay)

    # ---------- ordering: buffer, domain, backpressure ----------

    def _reset_assignment_state(self) -> None:
        # Eager rebalance: everything is revoked. Buffered records are uncommitted and will be
        # re-read by whoever owns the partitions next.
        self.ordering.clear()
        self._backpressure = False
        self._paused_domain.clear()
        self._paused_now.clear()
        self._assignment_dirty = True
        self._domain_ok = False

    def _buffer(self, messages: list) -> None:
        now = self.clock_ms()
        for msg in messages:
            _kind, ts = msg.timestamp()
            ts_ms = ts if isinstance(ts, int) and ts > 0 else now   # no timestamp: arrival time
            ok = self.ordering.add((msg.topic(), msg.partition()), ts_ms, msg.offset(), msg,
                                   len(msg.value() or b""))
            if not ok:
                self.stats["order_violations"] += 1
                logger.error(
                    f"[intent-kafka] category=order_violation name={self.name} topic={msg.topic()} "
                    f"partition={msg.partition()} offset={msg.offset()} record is older than events already "
                    f"applied (append delay above the {self.cfg.order_slack_s}s slack); applied in arrival order"
                )
        self.stats["ordering_buffer_high_water"] = self.ordering.high_water_records

    def _refresh_domain(self) -> bool:
        """True when this consumer owns every partition its actors can land on, so the
        ordering guarantee holds. Recomputed after every rebalance."""
        if not self._assignment_dirty:
            return self._domain_ok
        assigned = {(tp.topic, tp.partition) for tp in (self.kafka.assignment() or [])}
        if not assigned:
            self._domain_ok = False       # nothing assigned yet (or this member got nothing)
            return False
        self.ordering.set_assignment(assigned)
        try:
            meta = self.kafka.list_topics(timeout=10).topics
            counts = {t: len(meta[t].partitions) for t in self.cfg.topics}
        except Exception as exc:
            logger.warning(f"[intent-kafka] category=domain_check_failed name={self.name} error={exc}; will retry")
            return False
        every = {(t, p) for t, n in counts.items() for p in range(n)}
        if self.cfg.ordering_domain == "single":
            ok, expected = assigned == every, every
        else:
            equal = len(set(counts.values())) == 1
            held = {p for _t, p in assigned}
            expected = {(t, p) for t in counts for p in held}
            ok = equal and assigned == expected
        self._assignment_dirty = False
        self._domain_ok = ok
        self._paused_domain = set() if ok else set(assigned)
        if ok:
            # A new assignment must not inherit pause flags set under an earlier one (librdkafka does
            # not promise to clear them across a rebalance), or a consumer that has just become the
            # sole owner would sit on paused partitions and never process again.
            self.kafka.resume([self._tp(t, p, -1001) for t, p in sorted(assigned)])
            self._paused_now = set()
        if ok:
            logger.info(f"[intent-kafka] category=ordering_domain_ok name={self.name} mode={self.cfg.ordering_domain} "
                        f"partitions={len(assigned)}")
        else:
            self._log_domain_violation(assigned, expected, counts)
        return ok

    def _log_domain_violation(self, assigned, expected, counts) -> None:
        if time.monotonic() - self._last_domain_log < 30:
            return
        self._last_domain_log = time.monotonic()
        logger.error(
            f"[intent-kafka] category=ordering_domain_violated name={self.name} mode={self.cfg.ordering_domain} "
            f"assigned={len(assigned)} required={len(expected)} partition_counts={counts}. Not consuming: with "
            f"partitions split across consumers an actor's events from different topics cannot be ordered. "
            f"Run exactly one consumer (mode single) or give all topics the same partition count (copartitioned)."
        )

    def _apply_pauses(self) -> None:
        """Backpressure: while the buffer is over capacity, pause exactly the partitions that
        currently hold records (they are ahead) so that only the lagging ones are fetched. The set
        is recomputed on every pass: a partition that has just run dry must be resumed at once,
        because it may be the lagging partition everything else is waiting for."""
        if self.ordering.over_capacity():
            if not self._backpressure:
                logger.warning(
                    f"[intent-kafka] category=ordering_backpressure name={self.name} buffered={self.ordering.records} "
                    f"pausing the partitions that are ahead; fetching only the lagging ones"
                )
            self._backpressure = True
        elif self._backpressure and self.ordering.drained():
            self._backpressure = False
        ahead = set(self.ordering.buffered_partitions()) if self._backpressure else set()
        wanted = ahead | self._paused_domain
        to_pause, to_resume = wanted - self._paused_now, self._paused_now - wanted
        if to_pause:
            self.kafka.pause([self._tp(t, p, -1001) for t, p in sorted(to_pause)])
        if to_resume:
            self.kafka.resume([self._tp(t, p, -1001) for t, p in sorted(to_resume)])
        self._paused_now = wanted

    def _at_end(self, key: Tuple[str, int]) -> bool:
        """True when everything the broker holds for this partition has been fetched."""
        try:
            tp = self._tp(key[0], key[1], -1001)
            position = self.kafka.position([tp])[0].offset
            low, high = self.kafka.get_watermark_offsets(tp, timeout=2, cached=True)
        except Exception:
            return False
        if high is None or high < 0:
            return False                      # no fetch response yet, so nothing is known
        if position is None or position < 0:
            return high == low                # not started reading: an empty log is caught up
        return position >= high

    def _log_stall(self) -> None:
        if not self.ordering.records or time.monotonic() - self._last_stall_log < 30:
            return
        info = self.ordering.waiting_on(self._at_end, self.clock_ms())
        if info and info["oldest_ms"] > (self.ordering.slack_ms + 5000):
            self._last_stall_log = time.monotonic()
            logger.warning(f"[intent-kafka] category=ordering_wait name={self.name} {info}")

    # ---------- one batch ----------

    def process_batch(self, records: list) -> bool:
        """Returns True when every record was resolved (committed to MySQL or sent to the
        DLQ) and its offsets were committed."""
        started = time.monotonic()
        order: List[Hashable] = [_ref(m) for m in records]
        by_ref = {_ref(m): m for m in records}
        resolved: Set[Hashable] = set()
        dlq_entries: List[Tuple[Any, str, int]] = []  # (record, reason, attempts)

        work: Dict[int, List[WorkItem]] = defaultdict(list)
        brand_of_index: Dict[int, str] = {}
        for msg in records:
            ref = _ref(msg)
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

        failed_brands: Dict[int, str] = {}
        for brand_index, items in work.items():
            brand = brand_of_index[brand_index]
            try:
                with self.db.transaction(brand_index) as store:
                    result: BrandResult = apply_batch(store, items, self.cfg.session_timeout_s)
            except Exception as exc:  # transaction rolled back by the provider
                failed_brands[brand_index] = f"{type(exc).__name__}: {exc}"
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
        """Commits, per partition, the offset after the last record of the leading run of
        resolved records. A partition with an unresolved record is sought back to it."""
        per_partition: Dict[Tuple[str, int], List[int]] = defaultdict(list)
        for topic, partition, offset in order:
            per_partition[(topic, partition)].append(offset)

        to_commit = []
        to_seek = []
        for (topic, partition), offsets in per_partition.items():
            offsets.sort()
            next_offset = offsets[0]
            blocked = False
            for offset in offsets:
                if (topic, partition, offset) in resolved:
                    next_offset = offset + 1
                else:
                    blocked = True
                    break
            if next_offset > offsets[0]:
                to_commit.append(self._tp(topic, partition, next_offset))
            if blocked:
                to_seek.append(self._tp(topic, partition, next_offset))

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
                # MySQL already committed; the records are redelivered and absorbed as duplicates.
                committed = False
                self.stats["offset_commit_failures"] += 1
                logger.error(
                    f"[intent-kafka] category=offset_commit_failed name={self.name} "
                    f"error={type(exc).__name__}: {exc} (records will be redelivered and deduplicated)"
                )

        for tp in to_seek:
            self.ordering.drop((tp.topic, tp.partition))   # buffered records behind it are re-read
            try:
                self.kafka.seek(tp)
            except Exception as exc:
                # Without the seek the consumer would read past an unresolved record and the
                # next offset commit could skip it. Stop; a restart resumes at the committed offset.
                raise FatalConsumerError(
                    f"cannot seek {tp.topic}[{tp.partition}] back to {tp.offset}: {exc}"
                ) from exc
            logger.warning(
                f"[intent-kafka] category=kafka_retry name={self.name} topic={tp.topic} "
                f"partition={tp.partition} redeliver_from_offset={tp.offset}"
            )
        self.stats["records"] += len(order)
        return committed and not to_seek

    # ---------- observability ----------

    def _maybe_log_stats(self) -> None:
        if time.monotonic() - self._last_stats < self.cfg.stats_interval_s:
            return
        self._last_stats = time.monotonic()
        lag = {}
        try:
            assigned = self.kafka.assignment()
            if assigned:
                for tp in self.kafka.committed(assigned, timeout=5):
                    _low, high = self.kafka.get_watermark_offsets(tp, timeout=5, cached=False)
                    committed = tp.offset if tp.offset is not None and tp.offset >= 0 else 0
                    lag[f"{tp.topic}[{tp.partition}]"] = max(0, high - committed)
        except Exception as exc:
            lag = {"error": type(exc).__name__}
        logger.info(f"[intent-kafka] category=stats name={self.name} stats={dict(self.stats)} lag={lag}")


def _confluent_topic_partition(topic: str, partition: int, offset: int):
    from confluent_kafka import TopicPartition

    return TopicPartition(topic, partition, offset)
