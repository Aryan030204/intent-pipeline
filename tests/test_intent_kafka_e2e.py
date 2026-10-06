"""End to end: real Kafka -> real ingest cycle -> real MySQL (skipped unless both are named in the
environment; see conftest.py). Verifies actual database rows, actual group offsets and the real
/track publisher's timestamps.

Each test creates its own topics (same partition counts as kafka-service/topics.conf) and its own
offsets group, so nothing is shared between tests. Kafka timestamps are set explicitly where a test
needs a record to be "sent" at a precise time, exactly as /track's producer stamps them at send time."""

import contextlib
import json
import os
import subprocess
import threading
import time
import uuid

import pytest

import workers.intent_kafka_worker as worker
from pipeline.intent_kafka_consumer import ConsumerConfig, IntentIngestor
from pipeline.intent_kafka_db import BrandConnections, RunLock
from tests.conftest import requires_kafka
from tests.helpers import atc, encode, fixtures, value

pytestmark = requires_kafka

PARTITIONS = {"intent.checkout": 2, "intent.atc": 2, "intent.click": 3, "intent.other": 3}
WAIT_S = 60
KEY = "bbb_shop:cid-1"


def bootstrap():
    return os.environ["INTENT_TEST_KAFKA_BOOTSTRAP"]


def now_ms():
    return int(time.time() * 1000)


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

    def send_fixture(self, name, timestamp=None, **override):
        case = fixtures()[name]
        self.send(case["topic"], case["key"], {**case["value"], **override}, timestamp)

    def config(self, **overrides):
        settings = dict(
            bootstrap_servers=bootstrap(), group_id=self.group, topics=self.topics, dlq_topic=self.dlq_topic,
            events_batch=50, order_slack_s=1.0, max_record_attempts=3, stall_timeout_s=30)
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
        """Sum of the committed offsets (partitions with no commit report -1001 and are ignored)."""
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
    """Fails the first `fail_first` transactions AFTER their writes, before COMMIT, so the real
    MySQL rollback is what undoes them. `after_commit` runs after each successful commit."""

    def __init__(self, inner, fail_first=0):
        self.inner, self.fail_first, self.failures, self.after_commit = inner, fail_first, 0, None

    @contextlib.contextmanager
    def transaction(self, brand_index):
        with self.inner.transaction(brand_index) as store:
            yield store
            if self.failures < self.fail_first:
                self.failures += 1
                raise ConnectionError("injected failure before COMMIT")
        if self.after_commit:
            self.after_commit()

    def close(self):
        self.inner.close()


class Rig:
    """The real ingestor: real Kafka consumers per cycle, real DLQ producer, real MySQL connections,
    real MySQL run lock."""

    def __init__(self, env, db, fail_first=0, **cfg):
        from confluent_kafka import Consumer, Producer

        self.env, self.db = env, db
        self.stop = threading.Event()
        self.config = env.config(**cfg)
        self.dlq = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
        self.flaky = FlakyDb(BrandConnections(connection_factory=db.factory), fail_first)
        self.ingestor = IntentIngestor(
            lambda: Consumer(self.config.kafka_settings("it")), self.dlq, self.flaky, {"bbb_shop": 1}.get,
            self.config, self.stop, run_lock=RunLock(db.factory, 1, f"it-lock-{env.group}"))
        self.controller = worker.CycleController(self.ingestor, self.config, self.stop)

    def cycle(self, reason="test"):
        return self.ingestor.run_cycle(reason)

    def close(self):
        self.ingestor.close()


@pytest.fixture
def env():
    return KafkaEnv()


def count(db, table):
    return db.rows(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]


def cursor_seq(db, actor):
    (row,) = db.rows("SELECT events_seq FROM intent_actor_cursors WHERE actor_id = %s", (actor,))
    seq = row["events_seq"]
    seq = json.loads(seq) if isinstance(seq, (str, bytes)) else seq
    return [seq[str(i)]["event_id"] for i in range(1, len(seq) + 1)]


def stored_sessions(db):
    return db.rows("SELECT session_id, event_count FROM intent_sessions ORDER BY id")


def wait_for(predicate, what, timeout=WAIT_S):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.3)
    raise AssertionError(f"timed out waiting for {what}")


OLD = lambda: now_ms() - 120_000       # records sent two minutes ago: already past any slack


# ---------------- the four topics, end to end ----------------

def test_checkout_atc_click_and_other_events_reach_mysql_with_the_right_topic_and_session(env, clean_db):
    for name in ["page_viewed", "checkout_started", "product_added_to_cart", "click_useful"]:
        env.send_fixture(name)
    by_topic = {t.removeprefix(env.prefix): [m for m in env.delivered if m[0] == t] for t in env.topics}
    assert [len(by_topic[t]) for t in ("intent.other", "intent.checkout", "intent.atc", "intent.click")] == [1, 1, 1, 1]
    assert {m[3] for m in env.delivered} == {"bbb_shop:cid-1"}   # one actor key

    rig = Rig(env, clean_db)
    try:
        time.sleep(2.5)                               # the records are now older than the 1 s slack + margin
        assert rig.ingestor.pending_events() == 4
        assert rig.cycle().status == "done"
        assert rig.ingestor.pending_events() == 0
    finally:
        rig.close()

    events = {r["event_id"]: r for r in clean_db.rows("SELECT * FROM behavioral_events")}
    assert set(events) == {"sh-AAAA-0001", "sh-CO-1", "sh-ATC-1"}            # event_id exactly as sent
    assert events["sh-ATC-1"]["product_id"] == "Product:42" and events["sh-ATC-1"]["quantity"] == 2
    assert float(events["sh-CO-1"]["checkout_total"]) == 999.5
    (click,) = clean_db.rows("SELECT * FROM click_events")
    assert click["event_id"] == "sh-CLICK-1" and click["click_bucket"] == "useful_click" and click["click_tag"] == "BUTTON"
    assert click["occurred_at"].isoformat() == "2026-10-05T10:30:00"          # store-local wall clock, unshifted
    sessions = {r["session_id"] for r in list(events.values()) + [click]}
    assert len(sessions) == 1 and len(cursor_seq(clean_db, "cid-1")) == 4     # one actor, one open session
    assert clean_db.rows("SELECT * FROM intent_sessions") == []
    assert (env.committed_total("intent.other"), env.committed_total("intent.checkout"),
            env.committed_total("intent.atc"), env.committed_total("intent.click")) == (1, 1, 1, 1)


def test_redelivered_duplicates_create_no_second_row_and_no_state_change(env, clean_db):
    names = ["checkout_started", "product_added_to_cart", "click_useful", "page_viewed"]
    for name in names:
        env.send_fixture(name, timestamp=OLD())
    rig = Rig(env, clean_db)
    try:
        assert rig.cycle().status == "done"
        before = (clean_db.rows("SELECT * FROM behavioral_events ORDER BY id"), clean_db.rows("SELECT * FROM intent_actor_cursors"),
                  clean_db.rows("SELECT * FROM intent_atc_dedupe"))
        for name in names:                                  # the same four events delivered again
            env.send_fixture(name, timestamp=OLD())
        assert rig.cycle().status == "done"
        assert rig.ingestor.stats["duplicates"] + rig.ingestor.stats["atc_deduped"] == 4
    finally:
        rig.close()
    after = (clean_db.rows("SELECT * FROM behavioral_events ORDER BY id"), clean_db.rows("SELECT * FROM intent_actor_cursors"),
             clean_db.rows("SELECT * FROM intent_atc_dedupe"))
    assert after == before and count(clean_db, "click_events") == 1


# ---------------- the two triggers ----------------

def test_the_batch_trigger_waits_for_the_total_across_all_topics_then_ingests(env, clean_db):
    rig = Rig(env, clean_db, events_batch=4)
    try:
        env.send("intent.other", KEY, value("o1", 0), timestamp=OLD())
        env.send("intent.click", KEY, value("c1", 1, click=True), timestamp=OLD())
        env.send("intent.atc", KEY, atc("a1", 2), timestamp=OLD())
        assert rig.ingestor.pending_events() == 3
        assert rig.controller.check_batch() is None                      # 3 events < 4: nothing runs
        assert count(clean_db, "behavioral_events") == 0
        env.send("intent.checkout", KEY, value("k1", 3, name="checkout_started"), timestamp=OLD())
        result = rig.controller.check_batch()                            # a different topic completes the batch
        assert result.status == "done" and result.reason == "batch"
        assert count(clean_db, "behavioral_events") == 3 and count(clean_db, "click_events") == 1
        assert rig.ingestor.pending_events() == 0
    finally:
        rig.close()


def test_the_schedule_trigger_ingests_what_is_waiting_even_below_the_batch_size(env, clean_db):
    env.send("intent.other", KEY, value("o1", 0), timestamp=OLD())
    rig = Rig(env, clean_db, events_batch=500)
    try:
        assert rig.controller.check_batch() is None and count(clean_db, "behavioral_events") == 0
        assert rig.controller.run("schedule").status == "done"
        assert count(clean_db, "behavioral_events") == 1
    finally:
        rig.close()


def test_the_scheduler_runs_both_triggers_on_their_own(env, clean_db):
    """The real scheduler: the batch check fires when the batch fills, the schedule picks up the remainder."""
    rig = Rig(env, clean_db, events_batch=3, run_every_minutes=0.1, check_interval_s=1)   # schedule every 6 s
    scheduler = worker.build_scheduler(rig.controller, rig.config)
    runner = threading.Thread(target=scheduler.start)
    runner.start()
    try:
        for i in range(3):                                                   # fills the batch
            env.send("intent.other", f"bbb_shop:u{i}", value(f"s{i}", i, actor=f"u{i}", client=f"u{i}"), timestamp=OLD())
        wait_for(lambda: count(clean_db, "behavioral_events") == 3, "the batch trigger", 30)
        env.send("intent.other", "bbb_shop:u9", value("lonely", 9, actor="u9", client="u9"), timestamp=OLD())
        wait_for(lambda: count(clean_db, "behavioral_events") == 4, "the schedule trigger", 30)   # below the batch: only the schedule
    finally:
        scheduler.shutdown(wait=False)
        runner.join(20)
        rig.close()
    assert not runner.is_alive()


# ---------------- cross-topic ordering ----------------

def test_a_topic_that_lags_is_waited_for_so_the_session_is_not_split(env, clean_db):
    """e1 and e3 are on `other`; e2 (a click, sent between them) reaches the click topic after a first
    cycle already ran. Pixel times 0 s, 20 s, 70 s: applying e2 after e3 would be 50 s 'behind' the cursor."""
    t0 = now_ms()
    env.send("intent.other", KEY, value("e1", 0), timestamp=t0)
    env.send("intent.other", KEY, value("e3", 70), timestamp=t0 + 2000)
    rig = Rig(env, clean_db, order_slack_s=8)
    try:
        time.sleep(3)
        assert rig.cycle().status == "empty"                                   # both are inside the slack: held back
        assert count(clean_db, "behavioral_events") == 0
        env.send("intent.click", KEY, value("e2", 20, click=True), timestamp=t0 + 1000)   # the lagging topic delivers
        time.sleep(max(0, (t0 + 2000 + 9500 - now_ms()) / 1000))                 # until e3 is older than slack + margin
        assert rig.cycle().status == "done"
        assert rig.ingestor.stats["order_violations"] == 0
    finally:
        rig.close()
    assert cursor_seq(clean_db, "cid-1") == ["e1", "e2", "e3"] and stored_sessions(clean_db) == []


def test_without_the_slack_a_record_appended_late_splits_the_session_and_is_reported(env, clean_db):
    """The control: the same events with no slack. This is what the slack prevents; the late event is
    still stored, never dropped, and the violation is counted."""
    t0 = now_ms()
    env.send("intent.other", KEY, value("e1", 0), timestamp=t0)
    env.send("intent.other", KEY, value("e3", 70), timestamp=t0 + 2000)
    rig = Rig(env, clean_db, order_slack_s=0)
    try:
        time.sleep(max(0, (t0 + 2000 + 1500 - now_ms()) / 1000))
        assert rig.cycle().status == "done" and count(clean_db, "behavioral_events") == 2
        env.send("intent.click", KEY, value("e2", 20, click=True), timestamp=t0 + 1000)
        assert rig.cycle().status == "done"
        assert rig.ingestor.stats["order_violations"] == 1
    finally:
        rig.close()
    assert count(clean_db, "click_events") == 1 and len(stored_sessions(clean_db)) == 1
    assert cursor_seq(clean_db, "cid-1") == ["e2"]


def test_a_backlog_is_applied_in_send_order_across_topics_and_memory_stays_bounded(env, clean_db):
    """60 events on two topics, all waiting in Kafka before the cycle: the cycle reads one topic far ahead
    of the other. The buffer limit is 10 records, so the partition that is ahead is paused."""
    t0 = now_ms() - 120_000
    for i in range(30):
        env.send("intent.other", KEY, value(f"o{i:02d}", i * 2), timestamp=t0 + i * 200)
    for i in range(30):
        env.send("intent.click", KEY, value(f"c{i:02d}", i * 2 + 1, click=True), timestamp=t0 + i * 200 + 100)
    rig = Rig(env, clean_db, order_buffer_max=10, events_batch=20)
    try:
        assert rig.cycle().status == "done"
        stats = rig.ingestor.stats
    finally:
        rig.close()
    expected = [x for i in range(30) for x in (f"o{i:02d}", f"c{i:02d}")]
    assert cursor_seq(clean_db, "cid-1") == expected                     # interleaved exactly as sent
    assert stored_sessions(clean_db) == [] and stats["order_violations"] == 0
    assert stats["ordering_buffer_high_water"] <= 10 + 20 < 60           # cap + one read, never "everything"


def test_events_on_all_four_topics_are_applied_in_send_order(env, clean_db):
    t0 = now_ms() - 120_000
    plan = [("intent.other", "page_viewed"), ("intent.checkout", "checkout_started"), ("intent.atc", "product_added_to_cart"),
            ("intent.click", "click")] * 3
    expected = []
    for i, (topic, name) in enumerate(plan):
        event_id = f"x{i:02d}"
        expected.append(event_id)
        body = (value(event_id, i, click=True) if name == "click" else
                value(event_id, i, name=name, raw={"product_id": f"Product:{i}"} if "cart" in name else None))
        env.send(topic, KEY, body, timestamp=t0 + i * 100)               # produced topic by topic, not in time order
    rig = Rig(env, clean_db)
    try:
        assert rig.cycle().status == "done"
        assert rig.ingestor.stats["order_violations"] == 0
    finally:
        rig.close()
    assert cursor_seq(clean_db, "cid-1") == expected and stored_sessions(clean_db) == []


def test_many_actors_across_topics_each_keep_their_own_send_order(env, clean_db):
    t0 = now_ms() - 120_000
    actors = [f"a{i}" for i in range(6)]
    expected = {a: [] for a in actors}
    n = 0
    for step in range(6):
        for actor in actors:
            topic = ["intent.other", "intent.click", "intent.atc", "intent.checkout"][(step + len(actor)) % 4]
            event_id = f"{actor}-{step}"
            body = (value(event_id, step, actor=actor, client=actor, click=True) if topic == "intent.click" else
                    value(event_id, step, actor=actor, client=actor,
                          name={"intent.atc": "product_added_to_cart", "intent.checkout": "checkout_started"}.get(topic, "page_viewed"),
                          raw={"product_id": f"Product:{actor}{step}"} if topic == "intent.atc" else None))
            env.send(topic, f"bbb_shop:{actor}", body, timestamp=t0 + n * 50)
            expected[actor].append(event_id)
            n += 1
    rig = Rig(env, clean_db)
    try:
        assert rig.cycle().status == "done"
    finally:
        rig.close()
    for actor in actors:
        assert cursor_seq(clean_db, actor) == expected[actor]
    assert stored_sessions(clean_db) == []


# ---------------- failure, redelivery, offsets ----------------

def test_a_failed_mysql_transaction_aborts_the_cycle_with_no_offset_and_the_next_cycle_finishes(env, clean_db):
    env.send("intent.other", KEY, value("e1", 0), timestamp=OLD())
    rig = Rig(env, clean_db, fail_first=1)
    try:
        assert rig.cycle().status == "aborted"
        assert count(clean_db, "behavioral_events") == 0 and count(clean_db, "intent_actor_cursors") == 0   # the real rollback
        assert env.committed_total("intent.other") == 0                                                    # and Kafka was told nothing
        assert rig.cycle().status == "done"
        assert count(clean_db, "behavioral_events") == 1 and env.committed_total("intent.other") == 1
    finally:
        rig.close()
    assert len(cursor_seq(clean_db, "cid-1")) == 1               # the rolled-back attempt left no trace


def test_the_offset_is_committed_only_after_the_rows_are_visible_in_mysql(env, clean_db):
    env.send("intent.other", KEY, value("e1", 0), timestamp=OLD())
    rig = Rig(env, clean_db)
    seen = {}
    rig.flaky.after_commit = lambda: seen.update(committed_before_offset=env.committed_total("intent.other"),
                                                 rows=count(clean_db, "behavioral_events"))
    try:
        assert rig.cycle().status == "done"
    finally:
        rig.close()
    assert seen == {"committed_before_offset": 0, "rows": 1}         # at MySQL COMMIT time the offset was not yet committed
    assert env.committed_total("intent.other") == 1


def test_poison_records_go_to_the_dlq_and_healthy_records_around_them_are_stored(env, clean_db):
    ts = OLD()
    env.send("intent.other", "k", value("good-1", 0, actor="u1", client="u1"), timestamp=ts)
    env.send("intent.other", "k", b"{ this is not json", timestamp=ts + 1)
    env.send("intent.other", "k", value("ghost-1", 1, brand="ghost_shop"), timestamp=ts + 2)
    env.send("intent.other", "k", {**value("bad-time", 2), "occurred_at": "2026-10-05T10:30:00+05:30"}, timestamp=ts + 3)
    env.send("intent.other", "k", value("good-2", 3, actor="u1", client="u1"), timestamp=ts + 4)
    rig = Rig(env, clean_db)
    try:
        assert rig.cycle().status == "done"
    finally:
        rig.close()
    assert env.committed_total("intent.other") == 5
    dlq = env.read_all(env.dlq_topic, 3)
    reasons = sorted(dict(m.headers())["dlq_reason"].decode().split(":")[0] for m in dlq)
    assert reasons == ["invalid_message", "invalid_message", "unknown_brand"]
    assert len(env.read_all(env.dlq_topic, 4, timeout=3)) == 3        # each quarantined exactly once
    headers = dict(dlq[0].headers())
    assert headers["dlq_source_topic"].decode().startswith(env.prefix) and "dlq_source_offset" in headers
    assert {r["event_id"] for r in clean_db.rows("SELECT event_id FROM behavioral_events")} == {"good-1", "good-2"}


def test_a_row_mysql_rejects_is_retried_then_quarantined_without_blocking_the_partition(env, clean_db):
    ts = OLD()
    env.send("intent.other", "k", value("first", 0, actor="u1", client="u1"), timestamp=ts)
    env.send("intent.other", "k", value("toolong", 1, actor="u1", client="u1", name="product_viewed",
                                        raw={"product_id": "p", "product_title": "t" * 600}), timestamp=ts + 1)
    env.send("intent.other", "k", value("last", 2, actor="u1", client="u1"), timestamp=ts + 2)
    rig = Rig(env, clean_db, max_record_attempts=3)
    try:
        assert rig.cycle().status == "done"
    finally:
        rig.close()
    assert env.committed_total("intent.other") == 3                   # the partition moved past the bad row
    (dlq,) = env.read_all(env.dlq_topic, 1)
    headers = {k: v.decode() for k, v in dlq.headers()}
    assert headers["dlq_attempts"] == "3" and headers["dlq_reason"].startswith("rejected_by_mysql")
    assert {r["event_id"] for r in clean_db.rows("SELECT event_id FROM behavioral_events")} == {"first", "last"}


# ---------------- stop, lock, startup ----------------

def test_stopping_mid_cycle_commits_only_whole_batches_and_the_next_cycle_finishes_without_duplicates(env, clean_db):
    ts = OLD()
    for i in range(30):
        env.send("intent.other", f"bbb_shop:u{i}", value(f"s{i:02d}", i, actor=f"u{i}", client=f"u{i}"), timestamp=ts + i)
    rig = Rig(env, clean_db, events_batch=10)
    rig.flaky.after_commit = rig.stop.set                             # SIGTERM arrives after the first MySQL commit
    try:
        first = rig.cycle()
        stored = count(clean_db, "behavioral_events")
        assert first.status == "stopped" and 0 < stored < 30
        assert env.committed_total("intent.other") == stored           # every stored row's offset was committed, none beyond
        rig.flaky.after_commit = None
        rig.stop.clear()
        assert rig.cycle().status == "done"
        assert rig.ingestor.stats["duplicates"] == 0
    finally:
        rig.close()
    assert count(clean_db, "behavioral_events") == 30 and env.committed_total("intent.other") == 30


def test_a_second_worker_skips_its_cycle_while_the_first_holds_the_run_lock(env, clean_db):
    env.send("intent.other", KEY, value("e1", 0), timestamp=OLD())
    rig = Rig(env, clean_db)
    other_worker = RunLock(clean_db.factory, 1, f"it-lock-{env.group}")
    assert other_worker.acquire()                                      # a real GET_LOCK on a real connection
    try:
        skipped = rig.cycle()
        assert skipped.status == "skipped" and count(clean_db, "behavioral_events") == 0
    finally:
        other_worker.release()
    try:
        assert rig.cycle().status == "done" and count(clean_db, "behavioral_events") == 1
    finally:
        rig.close()


def test_startup_refuses_to_run_without_the_dlq_topic_and_accepts_it_when_present(env):
    missing = ConsumerConfig(bootstrap_servers=bootstrap(), topics=env.topics, dlq_topic=env.prefix + "intent.dlq.absent")
    with pytest.raises(SystemExit, match="topics.conf"):
        worker.check_topics(missing)
    worker.check_topics(env.config())


# ---------------- the assumption the whole ordering design rests on ----------------

ALERTS_DIR = os.environ.get("INTENT_TEST_ALERTS_SERVICE_DIR", "")


@pytest.mark.skipif(not ALERTS_DIR, reason="INTENT_TEST_ALERTS_SERVICE_DIR not set")
def test_the_real_track_publisher_stamps_each_record_with_its_send_time(env):
    """Ordering by Kafka timestamp only reproduces /track's arrival order if the real producer sets
    CreateTime at send time. Publishes through the actual alerts-service publisher and reads it back."""
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    script = os.path.join(os.path.dirname(__file__), "fixtures", "produce_with_track_publisher.js")
    out = subprocess.run(["node", script, ALERTS_DIR, bootstrap(), env.topic_for("intent.other"), "bbb_shop:probe"],
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    sent = json.loads(out.stdout.strip().splitlines()[-1])
    (msg,) = env.read_all(env.topic_for("intent.other"), 1)
    kind, ts = msg.timestamp()
    assert kind == TIMESTAMP_CREATE_TIME
    assert abs(ts - sent["sentAt"]) < 3000                              # stamped when it was sent
