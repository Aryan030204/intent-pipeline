import threading

import pytest

from pipeline.intent_kafka_consumer import (
    ConsumerConfig,
    FatalConsumerError,
    IntentConsumer,
)
from tests.helpers import (
    BRANDS,
    FakeDb,
    FakeDlq,
    FakeError,
    FakeKafka,
    FakeMsg,
    atc,
    item,
    record,
    seq_ids,
    value,
)

OTHER, ATC, CLICK = "intent.other", "intent.atc", "intent.click"


class DataTooLong(Exception):
    errno = 1406


def build(batches=(), **cfg):
    log = []
    kafka, dlq, db = FakeKafka(list(batches), log), FakeDlq(log), FakeDb(log)
    config = ConsumerConfig(retry_backoff_s=0.001, retry_backoff_max_s=0.002, batch_wait_s=0, **cfg)
    consumer = IntentConsumer(kafka, dlq, db, BRANDS.get, config, name="test")
    return consumer, kafka, dlq, db, log


def positions(log, kind):
    return [i for i, entry in enumerate(log) if entry[0] == kind]


# ---------------- the central invariant ----------------

def test_mysql_commit_happens_before_the_offset_commit_and_the_offset_is_last_plus_one():
    consumer, kafka, _, db, log = build()
    ok = consumer.process_batch([record(OTHER, 0, 10, value("e1", 0)), record(OTHER, 0, 11, value("e2", 5))])
    assert ok
    assert positions(log, "mysql_commit")[0] < positions(log, "offset_commit")[0]
    assert kafka.commits == [{(OTHER, 0): 12}]
    assert set(db.stores[1].events) == {"e1", "e2"}


def test_no_offset_is_ever_committed_without_a_preceding_mysql_commit():
    consumer, kafka, _, db, log = build()
    db.fail_brand[1] = ConnectionError("mysql down")
    consumer.process_batch([record(OTHER, 0, 0, value("e1", 0))])
    assert positions(log, "offset_commit") == []


def test_offsets_are_committed_per_partition():
    consumer, kafka, *_ = build()
    consumer.process_batch([
        record(OTHER, 0, 5, value("a", 0, actor="u1")), record(OTHER, 2, 40, value("b", 1, actor="u2")),
        record(ATC, 1, 7, atc("c", 2, actor="u3")), record(OTHER, 0, 6, value("d", 3, actor="u1")),
    ])
    assert kafka.commits == [{(OTHER, 0): 7, (OTHER, 2): 41, (ATC, 1): 8}]


# ---------------- retryable failures ----------------

def test_a_mysql_failure_rolls_back_commits_no_offset_and_seeks_back_for_redelivery():
    consumer, kafka, dlq, db, log = build()
    db.fail_brand[1] = ConnectionError("lost connection")
    ok = consumer.process_batch([record(OTHER, 0, 10, value("e1", 0)), record(OTHER, 0, 11, value("e2", 5))])
    assert ok is False
    assert kafka.commits == [] and kafka.seeks == [(OTHER, 0, 10)]
    assert db.stores[1].events == {} and dlq.sent == []   # nothing written, nothing quarantined


def test_redelivery_after_a_failure_succeeds_without_duplicating_anything():
    consumer, kafka, _, db, _ = build()
    batch = [record(OTHER, 0, 10, value("e1", 0)), record(OTHER, 0, 11, value("e2", 5))]
    db.fail_brand[1] = RuntimeError("deadlock")
    assert consumer.process_batch(batch) is False
    del db.fail_brand[1]
    assert consumer.process_batch(batch) is True
    assert kafka.commits == [{(OTHER, 0): 12}]
    assert seq_ids(db.stores[1], "cid-1") == ["e1", "e2"]


def test_a_failed_mysql_commit_is_treated_as_unresolved():
    consumer, kafka, _, db, log = build()
    db.fail_commit[1] = ConnectionError("commit outcome unknown")
    assert consumer.process_batch([record(OTHER, 0, 3, value("e1", 0))]) is False
    assert kafka.commits == [] and kafka.seeks == [(OTHER, 0, 3)]


def test_a_failure_in_one_brand_does_not_stop_the_other_brands_data_or_offsets():
    consumer, kafka, _, db, _ = build()
    db.fail_brand[2] = ConnectionError("pts database down")
    ok = consumer.process_batch([
        record(OTHER, 0, 0, value("b0", 0, brand="bbb_shop")),      # healthy, leading run
        record(OTHER, 0, 1, value("p1", 1, brand="pts_shop")),      # fails
        record(OTHER, 0, 2, value("b2", 2, brand="bbb_shop")),      # healthy but behind the failure
        record(OTHER, 1, 0, value("b3", 3, actor="u2")),           # a partition untouched by the failure
    ])
    assert ok is False
    assert {"b0", "b2", "b3"} <= set(db.stores[1].events)           # bbb's transaction committed
    assert kafka.commits == [{(OTHER, 0): 1, (OTHER, 1): 1}]       # partition 0 only up to the failure
    assert kafka.seeks == [(OTHER, 0, 1)]


def test_after_the_blocked_record_succeeds_the_replayed_records_are_deduplicated():
    consumer, kafka, _, db, _ = build()
    db.fail_brand[2] = ConnectionError("down")
    batch = [record(OTHER, 0, 0, value("p1", 0, brand="pts_shop", actor="p")), record(OTHER, 0, 1, value("b1", 1))]
    consumer.process_batch(batch)
    del db.fail_brand[2]
    replay = [record(OTHER, 0, 0, value("p1", 0, brand="pts_shop", actor="p")), record(OTHER, 0, 1, value("b1", 1))]
    assert consumer.process_batch(replay) is True
    assert list(db.stores[1].events) == ["b1"] and list(db.stores[2].events) == ["p1"]
    assert seq_ids(db.stores[1], "cid-1") == ["b1"]                 # replay did not append a second step
    assert kafka.commits[-1] == {(OTHER, 0): 2}


def test_offset_commit_failure_after_the_mysql_commit_is_safe_to_redeliver():
    consumer, kafka, _, db, _ = build()
    kafka.fail_commit = True
    batch = [record(OTHER, 0, 0, value("e1", 0))]
    assert consumer.process_batch(batch) is False
    assert "e1" in db.stores[1].events and kafka.seeks == []
    kafka.fail_commit = False
    assert consumer.process_batch(batch) is True                    # redelivery
    assert len(db.stores[1].events) == 1 and kafka.commits == [{(OTHER, 0): 1}]


def test_unresolved_failure_requires_a_backoff_and_a_seek_failure_is_fatal():
    consumer, kafka, _, db, _ = build()
    db.fail_brand[1] = ConnectionError("down")
    kafka.fail_seek = True
    with pytest.raises(FatalConsumerError, match="seek"):
        consumer.process_batch([record(OTHER, 0, 0, value("e1", 0))])


# ---------------- poison records ----------------

def test_an_invalid_record_goes_to_the_dlq_before_its_offset_is_committed_and_neighbours_are_saved():
    consumer, kafka, dlq, db, log = build()
    ok = consumer.process_batch([
        record(OTHER, 0, 0, value("good1", 0)),
        record(OTHER, 0, 1, b"{ not json"),
        record(OTHER, 0, 2, value("good2", 5)),
    ])
    assert ok is True
    assert set(db.stores[1].events) == {"good1", "good2"}
    (sent,) = dlq.sent
    assert sent["topic"] == "intent.dlq" and sent["value"] == b"{ not json"
    assert sent["headers"]["dlq_source_topic"] == OTHER and sent["headers"]["dlq_source_offset"] == "1"
    assert "invalid_message" in sent["headers"]["dlq_reason"]
    assert positions(log, "dlq")[0] < positions(log, "offset_commit")[0]
    assert kafka.commits == [{(OTHER, 0): 3}]                       # not blocked behind the poison record


def test_an_unknown_brand_is_quarantined_not_written_and_not_lost():
    consumer, kafka, dlq, db, _ = build()
    ok = consumer.process_batch([record(OTHER, 0, 0, value("g1", 0, brand="ghost_shop")), record(OTHER, 0, 1, value("e1", 1))])
    assert ok is True
    assert "g1" not in db.stores[1].events and "g1" not in db.stores[2].events
    assert "unknown_brand: ghost_shop" in dlq.sent[0]["headers"]["dlq_reason"]
    assert "e1" in db.stores[1].events and kafka.commits == [{(OTHER, 0): 2}]


def test_a_row_mysql_rejects_is_retried_then_quarantined_after_the_attempt_limit():
    consumer, kafka, dlq, db, _ = build(max_record_attempts=3)
    db.stores[1].fail_on["bad"] = DataTooLong("Data too long for column")
    batch = lambda: [record(OTHER, 0, 0, value("ok", 0)), record(OTHER, 0, 1, value("bad", 5)), record(OTHER, 0, 2, value("ok2", 9))]

    assert consumer.process_batch(batch()) is False                 # attempt 1: blocked at offset 1
    assert kafka.commits == [{(OTHER, 0): 1}] and kafka.seeks == [(OTHER, 0, 1)] and dlq.sent == []
    assert consumer.process_batch([record(OTHER, 0, 1, value("bad", 5)), record(OTHER, 0, 2, value("ok2", 9))]) is False
    assert dlq.sent == []                                           # attempt 2: still retrying
    assert consumer.process_batch([record(OTHER, 0, 1, value("bad", 5)), record(OTHER, 0, 2, value("ok2", 9))]) is True
    assert len(dlq.sent) == 1 and dlq.sent[0]["headers"]["dlq_attempts"] == "3"
    assert kafka.commits[-1] == {(OTHER, 0): 3}                     # the partition moved on
    assert {"ok", "ok2"} <= set(db.stores[1].events) and "bad" not in db.stores[1].events


def test_healthy_records_are_not_dragged_into_the_dlq_with_a_poison_record():
    consumer, _, dlq, db, _ = build(max_record_attempts=1)
    db.stores[1].fail_on["bad"] = DataTooLong("x")
    consumer.process_batch([record(OTHER, 0, i, value(e, i)) for i, e in enumerate(["a", "bad", "c"])])
    assert [s["headers"]["dlq_source_offset"] for s in dlq.sent] == ["1"]
    assert {"a", "c"} <= set(db.stores[1].events)


@pytest.mark.parametrize("failure", [True, "raise"])
def test_if_the_dlq_send_is_not_acknowledged_the_record_stays_unresolved(failure):
    consumer, kafka, dlq, db, _ = build()
    dlq.fail = failure
    ok = consumer.process_batch([record(OTHER, 0, 0, b"garbage"), record(OTHER, 0, 1, value("e1", 0))])
    assert ok is False
    assert kafka.commits == [] and kafka.seeks == [(OTHER, 0, 0)]   # nothing skipped, nothing lost


# ---------------- ordering ----------------

def test_records_of_one_partition_are_applied_in_kafka_order_not_sorted():
    consumer, _, _, db, _ = build()
    consumer.process_batch([record(OTHER, 0, 0, value("later", 50)), record(OTHER, 0, 1, value("earlier", 40))])
    assert seq_ids(db.stores[1], "cid-1") == ["later", "earlier"]


def test_a_topic_that_does_not_match_the_event_name_is_still_processed():
    consumer, kafka, _, db, _ = build()
    consumer.process_batch([record(ATC, 0, 0, value("e1", 0))])     # page_viewed on the atc topic
    assert "e1" in db.stores[1].events and kafka.commits == [{(ATC, 0): 1}]


# ---------------- run loop: shutdown and errors ----------------

def test_graceful_shutdown_finishes_the_batch_commits_offsets_and_closes_everything():
    consumer, kafka, dlq, db, log = build([[record(OTHER, 0, 0, value("e1", 0))], [record(OTHER, 0, 1, value("e2", 5))]])
    kafka.on_empty = consumer.stop
    original = kafka.consume

    def consume_then_request_stop(num_messages, timeout):
        batch = original(num_messages, timeout)
        if batch and batch[0].offset() == 0:
            consumer.stop()   # SIGTERM arrives while the first batch is being handled
        return batch

    kafka.consume = consume_then_request_stop
    consumer.run()
    assert kafka.commits == [{(OTHER, 0): 1}]       # first batch fully handled; second never started
    assert set(db.stores[1].events) == {"e1"}
    assert kafka.closed and db.closed


def test_run_processes_batches_until_stopped():
    consumer, kafka, _, db, _ = build([[record(OTHER, 0, 0, value("e1", 0))], [record(OTHER, 0, 1, value("e2", 5))]])
    kafka.on_empty = consumer.stop
    consumer.run()
    assert kafka.commits == [{(OTHER, 0): 1}, {(OTHER, 0): 2}] and kafka.closed and db.closed


def test_run_backs_off_and_retries_after_a_failure():
    consumer, kafka, _, db, _ = build([[record(OTHER, 0, 0, value("e1", 0))]])
    db.fail_brand[1] = ConnectionError("down")
    calls = {"n": 0}

    def redeliver_then_heal():
        calls["n"] += 1
        if calls["n"] == 1:
            del db.fail_brand[1]
            kafka.batches.append([record(OTHER, 0, 0, value("e1", 0))])
        else:
            consumer.stop()

    kafka.on_empty = redeliver_then_heal
    consumer.run()
    assert kafka.commits == [{(OTHER, 0): 1}] and "e1" in db.stores[1].events


def test_a_fatal_kafka_error_stops_the_consumer_and_a_transient_one_does_not():
    consumer, kafka, *_ = build([[FakeMsg(OTHER, 0, 0, b"", error=FakeError("broker transport failure"))]])
    kafka.on_empty = consumer.stop
    consumer.run()   # transient: logged, loop continues, stops cleanly
    assert consumer.stats["poll_errors"] == 1

    consumer, kafka, *_ = build([[FakeMsg(OTHER, 0, 0, b"", error=FakeError("fatal", fatal=True))]])
    with pytest.raises(FatalConsumerError):
        consumer.run()
    assert kafka.closed


def test_consumer_settings_use_manual_offsets_and_a_bounded_prefetch():
    settings = ConsumerConfig(batch_size=200).kafka_settings("host-1")
    assert settings["enable.auto.commit"] is False and settings["enable.auto.offset.store"] is False
    assert settings["group.id"] == "intent-pipeline-workers"
    assert settings["queued.max.messages.kbytes"] <= 8192 and settings["queued.min.messages"] <= 1000
    assert settings["bootstrap.servers"] == "kafka-service:9092"
