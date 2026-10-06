"""Ingest cycles: both triggers, the cutoff and ranges, cross-topic ordering, failures, stop, single-run lock."""

import threading

import workers.intent_kafka_worker as worker
from pipeline.intent_kafka_consumer import ConsumerConfig, IntentIngestor
from tests.helpers import BRANDS, FakeBroker, FakeDb, FakeDlq, atc, seq_ids, value

OTHER, ATC, CLICK, CHECKOUT = "intent.other", "intent.atc", "intent.click", "intent.checkout"
T = 1_700_000_000_000          # an arbitrary "send time" in ms
LONG_AGO = 3_600_000


class FakeLock:
    def __init__(self, free=True):
        self.free, self.acquired, self.released = free, 0, 0

    def acquire(self):
        self.acquired += 1
        return self.free

    def release(self):
        self.released += 1


class Rig:
    def __init__(self, events_batch=500, slack=10, clock=T + LONG_AGO, lock=None, **cfg):
        self.log = []
        self.broker = FakeBroker(log=self.log)
        self.dlq, self.db = FakeDlq(self.log), FakeDb(self.log)
        self.now = clock
        self.lock = lock
        self.stop = threading.Event()
        self.config = ConsumerConfig(events_batch=events_batch, order_slack_s=slack, **cfg)
        self.ingestor = IntentIngestor(self.broker.factory, self.dlq, self.db, BRANDS.get, self.config, self.stop,
                                       clock_ms=lambda: self.now, run_lock=lock)
        self.controller = worker.CycleController(self.ingestor, self.config, self.stop)

    def put(self, topic, partition, body, ts=T):
        return self.broker.put(topic, partition, body, ts=ts)

    @property
    def store(self):
        return self.db.stores[1]

    def committed(self):
        return dict(self.broker.committed)


# ---------------- trigger 1: batch completion ----------------

def test_pending_events_is_the_sum_of_end_offset_minus_committed_across_all_topics():
    rig = Rig()
    for i in range(3):
        rig.put(OTHER, 0, value(f"o{i}", i))
    rig.put(CLICK, 2, value("c0", 5, click=True))
    rig.put(ATC, 1, atc("a0", 6))
    rig.put(CHECKOUT, 0, value("k0", 7, name="checkout_started"))
    assert rig.ingestor.pending_events() == 6
    rig.broker.committed[(OTHER, 0)] = 2
    assert rig.ingestor.pending_events() == 4                   # 6 minus the 2 already ingested
    assert [c.assigned for c in rig.broker.created] == [[]]     # it read offsets and consumed nothing


def test_the_batch_trigger_fires_when_the_total_across_topics_reaches_the_batch_size_not_before():
    rig = Rig(events_batch=4)
    rig.put(OTHER, 0, value("o0", 0)); rig.put(CLICK, 0, value("c0", 1, click=True)); rig.put(ATC, 0, atc("a0", 2))
    assert rig.controller.check_batch() is None                 # 3 < 4: nothing runs
    assert rig.store.events == {} and len(rig.broker.created) == 1   # only the offsets-only monitor exists
    rig.put(CHECKOUT, 1, value("k0", 3, name="checkout_started"))   # a different topic completes the batch
    result = rig.controller.check_batch()
    assert result.status == "done" and result.reason == "batch"
    assert set(rig.store.events) == {"o0", "a0", "k0"} and set(rig.store.clicks) == {"c0"}
    assert rig.ingestor.pending_events() == 0


def test_more_than_the_batch_size_also_triggers_and_is_ingested_in_chunks_of_the_batch_size():
    rig = Rig(events_batch=3)
    for i in range(7):
        rig.put(OTHER, 0, value(f"o{i}", i, actor=f"u{i}", client=f"u{i}"))
    assert rig.controller.check_batch().status == "done"
    assert len(rig.store.events) == 7
    assert len([e for e in rig.log if e[0] == "mysql_commit"]) == 3  # 3 + 3 + 1 records per transaction


def test_pending_events_that_are_still_inside_the_slack_trigger_a_cycle_that_leaves_them_for_later():
    rig = Rig(events_batch=2, slack=10, clock=T + 5_000)
    rig.put(OTHER, 0, value("o0", 0), ts=T)
    rig.put(OTHER, 0, value("o1", 1), ts=T + 1_000)
    result = rig.controller.check_batch()
    assert result.status == "empty" and rig.store.events == {} and rig.committed() == {}
    rig.now = T + 60_000                                         # the slack has passed
    assert rig.controller.check_batch().status == "done" and len(rig.store.events) == 2


# ---------------- trigger 2: the schedule ----------------

def test_the_schedule_runs_a_cycle_even_when_far_fewer_events_than_the_batch_are_waiting():
    rig = Rig(events_batch=500)
    rig.put(OTHER, 0, value("o0", 0))
    result = rig.controller.run("schedule")
    assert result.status == "done" and result.reason == "schedule" and set(rig.store.events) == {"o0"}


def test_the_scheduler_has_both_triggers_with_the_configured_intervals_and_a_run_at_startup():
    rig = Rig()
    scheduler = worker.build_scheduler(rig.controller, ConsumerConfig(run_every_minutes=25, check_interval_s=30))
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert set(jobs) == {"intent_kafka_schedule", "intent_kafka_batch_check"}
    assert jobs["intent_kafka_schedule"].trigger.interval.total_seconds() == 25 * 60
    assert jobs["intent_kafka_batch_check"].trigger.interval.total_seconds() == 30
    assert jobs["intent_kafka_schedule"].next_run_time is not None          # first run at startup
    assert jobs["intent_kafka_schedule"].max_instances == jobs["intent_kafka_batch_check"].max_instances == 1


def test_defaults_are_25_minutes_and_a_batch_of_500(monkeypatch):
    for name in ("INTENT_EVENTS_BATCH", "INTENT_KAFKA_RUN_EVERY_MINUTES", "INTENT_EVENTS_CHECK_INTERVAL_S"):
        monkeypatch.delenv(name, raising=False)          # a developer's .env may set them
    config = worker.config_from_env()
    assert config.run_every_minutes == 25 and config.events_batch == 500


# ---------------- cycles never overlap ----------------

def test_a_trigger_that_fires_while_a_cycle_is_running_is_skipped_not_queued():
    rig = Rig(events_batch=1)
    rig.put(OTHER, 0, value("o0", 0))
    assert rig.controller._busy.acquire(blocking=False)         # a cycle is "running"
    try:
        assert rig.controller.check_batch() is None and rig.controller.run("schedule") is None
        assert rig.store.events == {}
    finally:
        rig.controller._busy.release()
    assert rig.controller.run("schedule").status == "done"


def test_a_second_worker_skips_its_cycle_when_the_run_lock_is_held():
    lock = FakeLock(free=False)
    rig = Rig(lock=lock)
    rig.put(OTHER, 0, value("o0", 0))
    result = rig.ingestor.run_cycle("schedule")
    assert result.status == "skipped" and rig.store.events == {} and rig.broker.created == []
    assert lock.acquired == 1 and lock.released == 0            # it never took the lock, so it must not release it


def test_the_run_lock_is_released_after_a_normal_cycle_and_after_a_failed_one():
    lock = FakeLock()
    rig = Rig(lock=lock)
    rig.put(OTHER, 0, value("o0", 0))
    rig.ingestor.run_cycle("schedule")
    rig.db.fail_brand[1] = ConnectionError("down")
    rig.put(OTHER, 0, value("o1", 1, actor="u2", client="u2"))
    assert rig.ingestor.run_cycle("schedule").status == "aborted"
    assert lock.acquired == 2 and lock.released == 2


# ---------------- ranges and the slack cutoff ----------------

def test_a_cycle_only_takes_records_older_than_the_slack_and_resumes_from_the_committed_offset():
    rig = Rig(slack=10, clock=T + 60_000)
    rig.put(OTHER, 0, value("old", 0), ts=T)                    # 60 s old
    rig.put(OTHER, 0, value("fresh", 5, actor="u2", client="u2"), ts=T + 57_000)   # 3 s old: inside the slack
    first = rig.ingestor.run_cycle("schedule")
    assert set(rig.store.events) == {"old"} and rig.committed() == {(OTHER, 0): 1} and first.status == "done"
    rig.now = T + 80_000
    second = rig.ingestor.run_cycle("schedule")
    assert set(rig.store.events) == {"old", "fresh"} and rig.committed() == {(OTHER, 0): 2}
    assert rig.store.cursors["cid-1"] is not None and second.status == "done"
    assert rig.ingestor.stats["duplicates"] == 0                # the second cycle did not re-read the first record


def test_an_empty_cycle_assigns_nothing_and_still_closes_its_consumer():
    rig = Rig()
    result = rig.ingestor.run_cycle("schedule")
    assert result.status == "empty" and rig.broker.created[0].closed and rig.broker.created[0].assigned == []


# ---------------- cross-topic ordering ----------------

def test_a_cycle_applies_events_of_different_topics_in_send_order_and_the_session_is_not_split():
    rig = Rig()
    rig.put(OTHER, 0, value("e1", 0), ts=T)
    rig.put(OTHER, 0, value("e3", 70), ts=T + 2_000)
    rig.put(CLICK, 0, value("e2", 20, click=True), ts=T + 1_000)   # sits on another topic, sent between them
    assert rig.ingestor.run_cycle("schedule").status == "done"
    assert seq_ids(rig.store, "cid-1") == ["e1", "e2", "e3"]
    assert rig.store.sessions == []


def test_a_topic_whose_record_is_appended_late_is_waited_for_by_the_slack():
    """e2 (sent between e1 and e3) reaches the click topic after a first cycle already ran."""
    rig = Rig(slack=10, clock=T + 4_000)
    rig.put(OTHER, 0, value("e1", 0), ts=T)
    rig.put(OTHER, 0, value("e3", 70), ts=T + 2_000)
    assert rig.ingestor.run_cycle("schedule").status == "empty"      # both are still inside the slack: nothing applied
    rig.put(CLICK, 0, value("e2", 20, click=True), ts=T + 1_000)     # the lagging topic delivers
    rig.now = T + 20_000
    assert rig.ingestor.run_cycle("schedule").status == "done"
    assert seq_ids(rig.store, "cid-1") == ["e1", "e2", "e3"] and rig.store.sessions == []
    assert rig.ingestor.stats["order_violations"] == 0


def test_without_the_slack_a_late_appended_record_splits_the_session_and_is_reported():
    """The control: the same events with no slack. This is what the slack prevents."""
    rig = Rig(slack=0, clock=T + 4_000)
    rig.put(OTHER, 0, value("e1", 0), ts=T)
    rig.put(OTHER, 0, value("e3", 70), ts=T + 2_000)
    assert rig.ingestor.run_cycle("schedule").status == "done"       # applies e1 and e3 straight away
    rig.put(CLICK, 0, value("e2", 20, click=True), ts=T + 1_000)
    rig.now = T + 20_000
    rig.ingestor.run_cycle("schedule")
    assert rig.ingestor.stats["order_violations"] == 1                  # detected, and the event is still stored
    assert "e2" in rig.store.clicks and len(rig.store.sessions) == 1    # e2 is 50 s behind e3: a spurious split
    assert seq_ids(rig.store, "cid-1") == ["e2"]


def test_a_lagging_partition_is_waited_for_inside_a_cycle_and_the_buffer_pauses_the_ones_ahead():
    rig = Rig(order_buffer_max=5)
    for i in range(12):
        rig.put(OTHER, 0, value(f"o{i:02d}", i * 2), ts=T + i * 200)
    for i in range(4):
        rig.put(CLICK, 0, value(f"c{i}", i * 2 + 1, click=True), ts=T + i * 200 + 100)
    rig.broker.delay[(CLICK, 0)] = 3                              # the click topic delivers three reads late
    assert rig.ingestor.run_cycle("schedule").status == "done"
    expected = []
    for i in range(12):
        expected.append(f"o{i:02d}")
        if i < 4:
            expected.append(f"c{i}")
    assert seq_ids(rig.store, "cid-1") == expected                # exact send order despite the lag
    consumer = rig.broker.created[0]
    assert any((OTHER, 0) in call for call in consumer.pause_calls)    # the partition that was ahead was paused
    assert rig.store.sessions == []


def test_a_partition_that_runs_dry_while_the_buffer_is_full_is_resumed_not_left_paused():
    """Regression for a stall found on a real broker: the paused set was fixed when the buffer filled, so
    a partition that then ran dry stayed paused although everything was waiting for it to fetch more."""
    rig = Rig(order_buffer_max=6, stall_timeout_s=0.5)
    rig.broker.read_limit = 6                                      # a read returns less than the backlog
    for i in range(11):
        rig.put(OTHER, 0, value(f"o{i:02d}", i), ts=T + i)          # other[0]: 11 records, all older than the click ones
    for i in range(5):
        rig.put(CLICK, 0, value(f"c{i}", 20 + i, click=True), ts=T + 5_000 + i)
    result = rig.ingestor.run_cycle("schedule")
    assert result.status == "done", result.detail                  # a stuck partition would end as 'aborted' (no progress)
    assert len(rig.store.events) == 11 and len(rig.store.clicks) == 5
    assert seq_ids(rig.store, "cid-1") == [f"o{i:02d}" for i in range(11)] + [f"c{i}" for i in range(5)]


# ---------------- failures ----------------

def test_a_mysql_failure_aborts_the_cycle_with_no_offset_and_the_next_cycle_finishes_the_job():
    rig = Rig()
    rig.put(OTHER, 0, value("e1", 0)); rig.put(OTHER, 0, value("e2", 5))
    rig.db.fail_brand[1] = ConnectionError("lost connection")
    assert rig.ingestor.run_cycle("schedule").status == "aborted"
    assert rig.committed() == {} and rig.store.events == {}
    del rig.db.fail_brand[1]
    assert rig.ingestor.run_cycle("schedule").status == "done"
    assert rig.committed() == {(OTHER, 0): 2} and seq_ids(rig.store, "cid-1") == ["e1", "e2"]
    assert rig.ingestor.stats["duplicates"] == 0


def test_a_failed_offset_commit_means_the_next_cycle_reads_the_records_again_and_deduplicates_them():
    rig = Rig()
    rig.put(OTHER, 0, value("e1", 0)); rig.put(OTHER, 0, atc("a1", 5))
    rig.broker.fail_commit = True
    assert rig.ingestor.run_cycle("schedule").status == "aborted"
    assert "e1" in rig.store.events and rig.committed() == {}
    rig.broker.fail_commit = False
    assert rig.ingestor.run_cycle("schedule").status == "done"
    assert len(rig.store.events) == 2 and rig.committed() == {(OTHER, 0): 2}
    assert rig.ingestor.stats["duplicates"] + rig.ingestor.stats["atc_deduped"] == 2   # nothing applied twice
    assert seq_ids(rig.store, "cid-1") == ["e1", "a1"]


def test_poison_records_are_quarantined_inside_the_cycle_and_the_cycle_completes():
    class DataTooLong(Exception):
        errno = 1406

    rig = Rig(max_record_attempts=3)
    rig.db.stores[1].fail_on["toolong"] = DataTooLong("Data too long for column")
    rig.put(OTHER, 0, value("first", 0, actor="u1", client="u1"))
    rig.put(OTHER, 0, b"{ not json")
    rig.put(OTHER, 0, value("ghost", 1, brand="ghost_shop"))
    rig.put(OTHER, 0, value("toolong", 2, actor="u1", client="u1"))
    rig.put(OTHER, 0, value("last", 3, actor="u1", client="u1"))
    result = rig.ingestor.run_cycle("schedule")
    assert result.status == "done" and rig.committed() == {(OTHER, 0): 5}
    assert set(rig.store.events) == {"first", "last"}
    reasons = sorted(s["headers"]["dlq_reason"].split(":")[0] for s in rig.dlq.sent)
    assert reasons == ["invalid_message", "rejected_by_mysql", "unknown_brand"]
    assert [s["headers"]["dlq_attempts"] for s in rig.dlq.sent if "rejected" in s["headers"]["dlq_reason"]] == ["3"]
    assert len(rig.dlq.sent) == 3                              # each poison record is quarantined once, however many passes ran


def test_a_dlq_send_that_is_not_acknowledged_aborts_the_cycle_without_skipping_the_record():
    rig = Rig()
    rig.dlq.fail = True
    rig.put(OTHER, 0, b"garbage")
    rig.put(OTHER, 0, value("e1", 1))
    assert rig.ingestor.run_cycle("schedule").status == "aborted"
    assert rig.committed() == {}                                 # the poison record is not skipped, nothing is lost


def test_a_kafka_failure_aborts_the_cycle_and_releases_the_lock():
    lock = FakeLock()
    rig = Rig(lock=lock)
    rig.ingestor.kafka_factory = lambda: (_ for _ in ()).throw(RuntimeError("broker unreachable"))
    assert rig.ingestor.run_cycle("schedule").status == "aborted"
    assert lock.released == 1


# ---------------- stop and close ----------------

def test_stopping_mid_cycle_commits_only_the_batches_already_processed_and_the_rest_waits_for_the_next_cycle():
    rig = Rig(events_batch=2)
    for i in range(6):
        rig.put(OTHER, 0, value(f"e{i}", i, actor=f"u{i}", client=f"u{i}"))
    rig.db.on_commit = rig.stop.set                              # SIGTERM arrives after the first MySQL commit
    result = rig.ingestor.run_cycle("schedule")
    assert result.status == "stopped" and rig.committed() == {(OTHER, 0): 2}
    assert len(rig.store.events) == 2
    rig.db.on_commit = None
    rig.stop.clear()
    assert rig.ingestor.run_cycle("schedule").status == "done"
    assert len(rig.store.events) == 6 and rig.committed() == {(OTHER, 0): 6}
    assert rig.ingestor.stats["duplicates"] == 0                 # nothing applied twice, nothing lost


def test_close_releases_the_offsets_monitor_and_the_database_connections():
    rig = Rig()
    rig.ingestor.pending_events()
    monitor = rig.broker.created[0]
    rig.ingestor.close()
    assert monitor.closed and rig.db.closed


def test_a_stop_request_prevents_new_cycles_from_starting():
    rig = Rig(events_batch=1)
    rig.put(OTHER, 0, value("e1", 0))
    rig.stop.set()
    assert rig.controller.run("schedule") is None and rig.controller.check_batch() is None
    assert rig.store.events == {}
