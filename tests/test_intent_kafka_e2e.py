"""End to end: real Kafka -> real IntentConsumer -> real MySQL (skipped unless both are named
in the environment; see conftest.py). Verifies actual database rows and actual group offsets.

Each test creates its own topics (same partition counts as kafka-service/topics.conf) and its
own consumer group, so nothing is shared between tests."""

import contextlib
import json
import os
import threading
import time
import uuid

import pytest

from pipeline.intent_kafka_consumer import ConsumerConfig, IntentConsumer
from pipeline.intent_kafka_db import BrandConnections
from tests.conftest import requires_kafka
from tests.helpers import atc, encode, fixtures, stamp, value

pytestmark = requires_kafka

PARTITIONS = {"intent.checkout": 2, "intent.atc": 2, "intent.click": 3, "intent.other": 3}
WAIT_S = 90


def bootstrap():
    return os.environ["INTENT_TEST_KAFKA_BOOTSTRAP"]


class KafkaEnv:
    def __init__(self, counts=None):
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        self.counts = dict(counts or PARTITIONS)

        self.prefix = f"it{uuid.uuid4().hex[:6]}."
        self.group = f"it-group-{uuid.uuid4().hex[:8]}"
        admin = AdminClient({"bootstrap.servers": bootstrap()})
        topics = [NewTopic(self.prefix + name, n, 1) for name, n in self.counts.items()]
        topics.append(NewTopic(self.prefix + "intent.dlq", 1, 1))
        for future in admin.create_topics(topics).values():
            future.result(30)
        self.topics = tuple(self.prefix + name for name in self.counts)
        self.dlq_topic = self.prefix + "intent.dlq"
        # murmur2_random is the partitioner kafkajs (the /track producer) uses.
        self.producer = Producer({"bootstrap.servers": bootstrap(), "partitioner": "murmur2_random", "acks": "all"})
        self.delivered = []   # (topic, partition, offset, key) from delivery reports

    def topic_for(self, logical):
        return self.prefix + logical

    def send(self, logical_topic, key, payload, timestamp=None):
        body = payload if isinstance(payload, bytes) else encode(payload)
        extra = {} if timestamp is None else {"timestamp": int(timestamp)}   # Kafka CreateTime, as /track stamps it

        def done(err, msg):
            assert err is None, err
            self.delivered.append((msg.topic(), msg.partition(), msg.offset(), msg.key().decode()))

        self.producer.produce(self.topic_for(logical_topic), key=key.encode(), value=body, on_delivery=done, **extra)
        self.producer.flush(30)

    def send_fixture(self, name, **override):
        case = fixtures()[name]
        message = {**case["value"], **override}
        self.send(case["topic"], case["key"], message)

    def config(self, **overrides):
        settings = dict(
            bootstrap_servers=bootstrap(), group_id=self.group, topics=self.topics, dlq_topic=self.dlq_topic,
            batch_size=50, batch_wait_s=0.5, retry_backoff_s=0.2, retry_backoff_max_s=0.5, stats_interval_s=3600,
            order_slack_s=1.0)
        settings.update(overrides)
        return ConsumerConfig(**settings)

    def committed(self, logical_topic):
        from confluent_kafka import Consumer, TopicPartition

        topic = self.topic_for(logical_topic)
        probe = Consumer({"bootstrap.servers": bootstrap(), "group.id": self.group, "enable.auto.commit": False})
        try:
            parts = [TopicPartition(topic, p) for p in range(self.counts[logical_topic])]
            return {tp.partition: tp.offset for tp in probe.committed(parts, timeout=15)}
        finally:
            probe.close()

    def committed_total(self, logical_topic):
        """Sum of the group's committed offsets (partitions with no commit report -1001 and are ignored)."""
        return sum(v for v in self.committed(logical_topic).values() if v > 0)

    def read_all(self, topic, count, timeout=30):
        from confluent_kafka import Consumer

        reader = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"reader-{uuid.uuid4().hex[:6]}",
                           "auto.offset.reset": "earliest", "enable.auto.commit": False})
        reader.subscribe([topic])
        out, deadline = [], time.time() + timeout
        try:
            while len(out) < count and time.time() < deadline:
                msg = reader.poll(1.0)
                if msg is not None and msg.error() is None:
                    out.append(msg)
        finally:
            reader.close()
        return out


class FlakyDb:
    """Fails the first `fail_first` transactions AFTER their writes, before COMMIT, so the
    real MySQL rollback is what undoes them."""

    def __init__(self, inner, fail_first=0):
        self.inner, self.fail_first, self.failures = inner, fail_first, 0

    @contextlib.contextmanager
    def transaction(self, brand_index):
        with self.inner.transaction(brand_index) as store:
            yield store
            if self.failures < self.fail_first:
                self.failures += 1
                raise ConnectionError("injected failure before COMMIT")

    def close(self):
        self.inner.close()


class Running:
    """Real consumers on threads, stopped and joined by the context manager. Each consumer has
    its own stop event so a test can stop one and leave the others running."""

    def __init__(self, env, db, count=1, fail_first=0, autostart=True, **cfg):
        from confluent_kafka import Consumer, Producer

        self.env, self.db, self.autostart = env, db, autostart
        self.stops = [threading.Event() for _ in range(count)]
        self.dlq = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
        self.flaky = [FlakyDb(BrandConnections(connection_factory=db.factory), fail_first) for _ in range(count)]
        config = env.config(**cfg)
        self.consumers = [
            IntentConsumer(Consumer(config.kafka_settings(f"it-{n}")), self.dlq, self.flaky[n],
                           {"bbb_shop": 1}.get, config, self.stops[n], name=f"c{n}")
            for n in range(count)
        ]
        self.errors = []
        self.threads = [threading.Thread(target=self._run, args=(c,)) for c in self.consumers]

    @property
    def stop(self):
        return self.stops[0]

    def _run(self, consumer):
        try:
            consumer.run()
        except BaseException as exc:
            self.errors.append(exc)

    def start_one(self, n):
        self.threads[n].start()

    def stop_one(self, n):
        self.stops[n].set()
        self.threads[n].join(30)
        assert not self.threads[n].is_alive()

    def __enter__(self):
        if self.autostart:
            for n in range(len(self.threads)):
                self.start_one(n)
        return self

    def __exit__(self, *exc):
        for event in self.stops:
            event.set()
        for t in self.threads:
            if t.ident is not None:
                t.join(30)
        assert all(not t.is_alive() for t in self.threads), "consumer thread did not stop"
        assert self.errors == [], self.errors


def wait_for(predicate, what, timeout=WAIT_S):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.3)
    raise AssertionError(f"timed out waiting for {what}")


def count(db, table):
    return db.rows(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]


def cursor_seq(db, actor):
    (row,) = db.rows("SELECT events_seq FROM intent_actor_cursors WHERE actor_id = %s", (actor,))
    seq = row["events_seq"]
    seq = json.loads(seq) if isinstance(seq, (str, bytes)) else seq
    return [seq[str(i)]["event_id"] for i in range(1, len(seq) + 1)]


@pytest.fixture
def env():
    return KafkaEnv()


# ---------------- the four topics, end to end ----------------

def test_checkout_atc_click_and_other_events_reach_mysql_with_the_right_topic_and_session(env, clean_db):
    order = ["page_viewed", "checkout_started", "product_added_to_cart", "click_useful"]
    for name in order:
        env.send_fixture(name)

    # topic and partition: each message arrived on the topic the real producer routed it to
    by_topic = {t.removeprefix(env.prefix): [m for m in env.delivered if m[0] == t] for t in env.topics}
    assert [len(by_topic[t]) for t in ("intent.other", "intent.checkout", "intent.atc", "intent.click")] == [1, 1, 1, 1]
    assert {m[3] for m in env.delivered} == {"bbb_shop:cid-1"}   # one actor key

    with Running(env, clean_db):
        wait_for(lambda: count(clean_db, "behavioral_events") == 3 and count(clean_db, "click_events") == 1, "rows")

    events = {r["event_id"]: r for r in clean_db.rows("SELECT * FROM behavioral_events")}
    assert set(events) == {"sh-AAAA-0001", "sh-CO-1", "sh-ATC-1"}            # event_id exactly as sent
    assert events["sh-ATC-1"]["product_id"] == "Product:42" and events["sh-ATC-1"]["quantity"] == 2
    assert float(events["sh-CO-1"]["checkout_total"]) == 999.5
    (click,) = clean_db.rows("SELECT * FROM click_events")
    assert click["event_id"] == "sh-CLICK-1" and click["click_bucket"] == "useful_click" and click["click_tag"] == "BUTTON"
    assert click["occurred_at"].isoformat() == "2026-10-05T10:30:00"          # store-local wall clock, unshifted

    sessions = {r["session_id"] for r in list(events.values()) + [click]}
    assert len(sessions) == 1                                                  # one actor, one session
    (cursor,) = clean_db.rows("SELECT * FROM intent_actor_cursors")
    assert cursor["actor_id"] == "cid-1" and cursor["session_id"] in sessions
    assert len(cursor_seq(clean_db, "cid-1")) == 4
    assert clean_db.rows("SELECT * FROM intent_sessions") == []                # still open


def test_redelivered_duplicates_create_no_second_row_and_no_state_change(env, clean_db):
    names = ["checkout_started", "product_added_to_cart", "click_useful", "page_viewed"]
    for name in names:
        env.send_fixture(name)
    with Running(env, clean_db) as run:
        wait_for(lambda: count(clean_db, "behavioral_events") == 3 and count(clean_db, "click_events") == 1, "first pass")
        before = (clean_db.rows("SELECT * FROM behavioral_events ORDER BY id"), clean_db.rows("SELECT * FROM intent_actor_cursors"),
                  clean_db.rows("SELECT * FROM intent_atc_dedupe"))
        for name in names:                                  # the same four events delivered again
            env.send_fixture(name)
        wait_for(lambda: run.consumers[0].stats["duplicates"] + run.consumers[0].stats["atc_deduped"] >= 4, "duplicates seen")
    after = (clean_db.rows("SELECT * FROM behavioral_events ORDER BY id"), clean_db.rows("SELECT * FROM intent_actor_cursors"),
             clean_db.rows("SELECT * FROM intent_atc_dedupe"))
    assert after == before and count(clean_db, "click_events") == 1


def test_same_actor_same_partition_and_kafka_order_becomes_session_order(env, clean_db):
    n = 40
    for i in range(n):
        env.send("intent.other", "bbb_shop:cid-1", value(f"ord-{i:02d}", i))
    assert len({m[1] for m in env.delivered}) == 1                             # one key, one partition
    with Running(env, clean_db):
        wait_for(lambda: count(clean_db, "behavioral_events") == n, "all events")
    assert cursor_seq(clean_db, "cid-1") == [f"ord-{i:02d}" for i in range(n)]


def test_many_actors_are_spread_over_partitions_and_each_keeps_its_own_order(env, clean_db):
    actors = [f"user-{i}" for i in range(12)]
    for step in range(5):
        for actor in actors:
            env.send("intent.other", f"bbb_shop:{actor}", value(f"{actor}-{step}", step, actor=actor, client=actor))
    assert len({m[1] for m in env.delivered}) > 1                              # not all in one partition
    with Running(env, clean_db):
        wait_for(lambda: count(clean_db, "behavioral_events") == 60, "all events")
    for actor in actors:
        assert cursor_seq(clean_db, actor) == [f"{actor}-{s}" for s in range(5)]


def test_two_consumers_share_co_partitioned_topics_without_duplicating(clean_db):
    """Scaling out is allowed only when the four topics have the same partition count (copartitioned
    mode), so partition i of every topic goes to the same consumer."""
    env = KafkaEnv(counts={t: 3 for t in PARTITIONS})
    actors = [f"user-{i}" for i in range(16)]
    for step in range(4):
        for actor in actors:
            env.send("intent.other", f"bbb_shop:{actor}", value(f"{actor}-{step}", step, actor=actor, client=actor))
    with Running(env, clean_db, count=2, ordering_domain="copartitioned") as run:
        wait_for(lambda: count(clean_db, "behavioral_events") == 64, "all events")
        wait_for(lambda: sum(c.stats["applied"] for c in run.consumers) == 64, "applied counters")
        assert sum(c.stats["duplicates"] for c in run.consumers) == 0          # parallel, not duplicate
        assert all(c.stats["applied"] > 0 for c in run.consumers)              # both did real work
    assert count(clean_db, "behavioral_events") == 64
    for actor in actors:
        assert cursor_seq(clean_db, actor) == [f"{actor}-{s}" for s in range(4)]


# ---------------- failure, redelivery, offsets ----------------

def test_a_failed_mysql_transaction_commits_no_offset_and_the_record_is_redelivered(env, clean_db):
    env.send_fixture("page_viewed")
    # A 5 s backoff holds the consumer between the failure and the redelivery, so the state
    # right after the failure can be inspected without racing the retry.
    with Running(env, clean_db, fail_first=1, retry_backoff_s=5.0, retry_backoff_max_s=5.0) as run:
        wait_for(lambda: run.consumers[0].stats["txn_failures"] == 1, "the injected failure")
        assert count(clean_db, "behavioral_events") == 0           # the real MySQL rolled the writes back
        assert count(clean_db, "intent_actor_cursors") == 0
        assert env.committed_total("intent.other") == 0            # and Kafka was told nothing
        wait_for(lambda: count(clean_db, "behavioral_events") == 1, "redelivered record stored", timeout=60)
        wait_for(lambda: env.committed_total("intent.other") == 1, "offset committed after the success")
    assert run.flaky[0].failures == 1 and len(cursor_seq(clean_db, "cid-1")) == 1


def test_the_offset_is_committed_only_after_the_rows_are_visible_in_mysql(env, clean_db):
    env.send_fixture("page_viewed")
    with Running(env, clean_db):
        wait_for(lambda: env.committed_total("intent.other") >= 1, "offset committed")
        assert count(clean_db, "behavioral_events") == 1                       # committed offset implies committed row


def test_poison_records_go_to_the_dlq_and_healthy_records_around_them_are_stored(env, clean_db):
    env.send("intent.other", "k", value("good-1", 0, actor="u1", client="u1"))
    env.send("intent.other", "k", b"{ this is not json")
    env.send("intent.other", "k", value("ghost-1", 1, brand="ghost_shop"))
    env.send("intent.other", "k", {**value("bad-time", 2), "occurred_at": "2026-10-05T10:30:00+05:30"})
    env.send("intent.other", "k", value("good-2", 3, actor="u1", client="u1"))
    with Running(env, clean_db):
        wait_for(lambda: count(clean_db, "behavioral_events") == 2, "healthy rows")
        wait_for(lambda: env.committed_total("intent.other") >= 5, "offsets past the poison records")
    dlq = env.read_all(env.dlq_topic, 3)
    reasons = sorted(dict(m.headers())["dlq_reason"].decode().split(":")[0] for m in dlq)
    assert reasons == ["invalid_message", "invalid_message", "unknown_brand"]
    assert {m.value() for m in dlq} >= {b"{ this is not json"}
    headers = dict(dlq[0].headers())
    assert headers["dlq_source_topic"].decode().startswith(env.prefix) and "dlq_source_offset" in headers
    assert {r["event_id"] for r in clean_db.rows("SELECT event_id FROM behavioral_events")} == {"good-1", "good-2"}


def test_a_row_mysql_rejects_is_retried_then_quarantined_without_blocking_the_partition(env, clean_db):
    env.send("intent.other", "k", value("first", 0, actor="u1", client="u1"))
    env.send("intent.other", "k", value("toolong", 1, actor="u1", client="u1", name="product_viewed",
                                        raw={"product_id": "p", "product_title": "t" * 600}))
    env.send("intent.other", "k", value("last", 2, actor="u1", client="u1"))
    with Running(env, clean_db, max_record_attempts=3):
        wait_for(lambda: count(clean_db, "behavioral_events") == 2, "healthy rows")
        wait_for(lambda: env.committed_total("intent.other") >= 3, "partition moved past the bad row")
    (dlq,) = env.read_all(env.dlq_topic, 1)
    headers = {k: v.decode() for k, v in dlq.headers()}
    assert headers["dlq_attempts"] == "3" and headers["dlq_reason"].startswith("rejected_by_mysql")
    assert {r["event_id"] for r in clean_db.rows("SELECT event_id FROM behavioral_events")} == {"first", "last"}


# ---------------- shutdown ----------------

def test_graceful_shutdown_finishes_the_batch_commits_offsets_and_stops(env, clean_db):
    for i in range(20):
        env.send("intent.other", "bbb_shop:cid-1", value(f"s-{i:02d}", i))
    run = Running(env, clean_db)
    with run:
        wait_for(lambda: count(clean_db, "behavioral_events") >= 1, "first rows")
    # __exit__ set the stop event and joined: the thread ended and nothing raised
    stored = count(clean_db, "behavioral_events")
    committed = env.committed_total("intent.other")
    assert committed == stored                                  # every stored row's offset was committed, none beyond
    assert run.consumers[0].kafka is not None and run.flaky[0].inner._open == {}   # DB connections closed


# ---------------- startup validation against the real topic list ----------------

def test_startup_refuses_to_run_without_the_dlq_topic_and_accepts_it_when_present(env):
    from workers.intent_kafka_worker import check_topics

    config = env.config()
    # Topics exist, DLQ topic does not (the state of the real broker until topics.conf gains intent.dlq).
    from confluent_kafka.admin import AdminClient

    missing = ConsumerConfig(bootstrap_servers=bootstrap(), topics=env.topics, dlq_topic=env.prefix + "intent.dlq.absent")
    with pytest.raises(SystemExit, match="topics.conf"):
        check_topics(missing)
    check_topics(config)
