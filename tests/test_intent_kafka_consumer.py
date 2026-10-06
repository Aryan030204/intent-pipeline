"""One batch: MySQL transaction, COMMIT, then Kafka offsets; failures and poison records."""

import pytest

from pipeline.intent_kafka_consumer import ConsumerConfig, IntentIngestor
from tests.helpers import BRANDS, FakeBroker, FakeDb, FakeDlq, atc, record, seq_ids, value

OTHER, ATC, CLICK = "intent.other", "intent.atc", "intent.click"


class DataTooLong(Exception):
    errno = 1406


def build(**cfg):
    log = []
    broker = FakeBroker(log=log)
    dlq, db = FakeDlq(log), FakeDb(log)
    config = ConsumerConfig(**cfg)
    ingestor = IntentIngestor(broker.factory, dlq, db, BRANDS.get, config, clock_ms=lambda: 10 ** 12)
    kafka = ingestor.kafka = broker.factory()          # process_batch commits through the cycle's consumer
    return ingestor, kafka, dlq, db, log, broker


def positions(log, kind):
    return [i for i, entry in enumerate(log) if entry[0] == kind]


# ---------------- the central invariant ----------------

def test_mysql_commit_happens_before_the_offset_commit_and_the_offset_is_last_plus_one():
    ingestor, kafka, _, db, log, _ = build()
    assert ingestor.process_batch([record(OTHER, 0, 10, value("e1", 0)), record(OTHER, 0, 11, value("e2", 5))])
    assert positions(log, "mysql_commit")[0] < positions(log, "offset_commit")[0]
    assert kafka.commits == [{(OTHER, 0): 12}]
    assert set(db.stores[1].events) == {"e1", "e2"}


def test_no_offset_is_ever_committed_without_a_preceding_mysql_commit():
    ingestor, kafka, _, db, log, _ = build()
    db.fail_brand[1] = ConnectionError("mysql down")
    ingestor.process_batch([record(OTHER, 0, 0, value("e1", 0))])
    assert positions(log, "offset_commit") == []


def test_offsets_are_committed_per_partition():
    ingestor, kafka, *_ = build()
    ingestor.process_batch([
        record(OTHER, 0, 5, value("a", 0, actor="u1")), record(OTHER, 2, 40, value("b", 1, actor="u2")),
        record(ATC, 1, 7, atc("c", 2, actor="u3")), record(OTHER, 0, 6, value("d", 3, actor="u1")),
    ])
    assert kafka.commits == [{(OTHER, 0): 7, (OTHER, 2): 41, (ATC, 1): 8}]


# ---------------- retryable failures ----------------

def test_a_mysql_failure_rolls_back_and_commits_no_offset():
    ingestor, kafka, dlq, db, log, _ = build()
    db.fail_brand[1] = ConnectionError("lost connection")
    ok = ingestor.process_batch([record(OTHER, 0, 10, value("e1", 0)), record(OTHER, 0, 11, value("e2", 5))])
    assert ok is False
    assert kafka.commits == []
    assert db.stores[1].events == {} and dlq.sent == []   # nothing written, nothing quarantined
    assert ingestor.stats["txn_failures"] == 1


def test_reading_the_same_records_again_after_a_failure_succeeds_without_duplicating_anything():
    ingestor, kafka, _, db, _, _ = build()
    batch = [record(OTHER, 0, 10, value("e1", 0)), record(OTHER, 0, 11, value("e2", 5))]
    db.fail_brand[1] = RuntimeError("deadlock")
    assert ingestor.process_batch(batch) is False
    del db.fail_brand[1]
    assert ingestor.process_batch(batch) is True
    assert kafka.commits == [{(OTHER, 0): 12}]
    assert seq_ids(db.stores[1], "cid-1") == ["e1", "e2"]


def test_a_failed_mysql_commit_is_treated_as_unresolved():
    ingestor, kafka, _, db, _, _ = build()
    db.fail_commit[1] = ConnectionError("commit outcome unknown")
    assert ingestor.process_batch([record(OTHER, 0, 3, value("e1", 0))]) is False
    assert kafka.commits == []


def test_a_failure_in_one_brand_does_not_stop_the_other_brands_data_or_offsets():
    ingestor, kafka, _, db, _, _ = build()
    db.fail_brand[2] = ConnectionError("pts database down")
    ok = ingestor.process_batch([
        record(OTHER, 0, 0, value("b0", 0, brand="bbb_shop")),      # healthy, leading run
        record(OTHER, 0, 1, value("p1", 1, brand="pts_shop")),      # fails
        record(OTHER, 0, 2, value("b2", 2, brand="bbb_shop")),      # healthy but behind the failure
        record(OTHER, 1, 0, value("b3", 3, actor="u2")),           # a partition untouched by the failure
    ])
    assert ok is False
    assert {"b0", "b2", "b3"} <= set(db.stores[1].events)           # bbb's transaction committed
    assert kafka.commits == [{(OTHER, 0): 1, (OTHER, 1): 1}]       # partition 0 only up to the failure


def test_after_the_blocked_record_succeeds_the_replayed_records_are_deduplicated():
    ingestor, kafka, _, db, _, _ = build()
    db.fail_brand[2] = ConnectionError("down")
    batch = lambda: [record(OTHER, 0, 0, value("p1", 0, brand="pts_shop", actor="p")), record(OTHER, 0, 1, value("b1", 1))]
    ingestor.process_batch(batch())
    del db.fail_brand[2]
    assert ingestor.process_batch(batch()) is True
    assert list(db.stores[1].events) == ["b1"] and list(db.stores[2].events) == ["p1"]
    assert seq_ids(db.stores[1], "cid-1") == ["b1"]                 # the replay did not append a second step
    assert kafka.commits[-1] == {(OTHER, 0): 2}


def test_offset_commit_failure_after_the_mysql_commit_is_safe_to_read_again():
    ingestor, kafka, _, db, _, broker = build()
    broker.fail_commit = True
    batch = lambda: [record(OTHER, 0, 0, value("e1", 0))]
    assert ingestor.process_batch(batch()) is False
    assert "e1" in db.stores[1].events
    broker.fail_commit = False
    assert ingestor.process_batch(batch()) is True                  # the next cycle reads it again
    assert len(db.stores[1].events) == 1 and kafka.commits == [{(OTHER, 0): 1}]
    assert ingestor.stats["offset_commit_failures"] == 1


# ---------------- poison records ----------------

def test_an_invalid_record_goes_to_the_dlq_before_its_offset_is_committed_and_neighbours_are_saved():
    ingestor, kafka, dlq, db, log, _ = build()
    ok = ingestor.process_batch([
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
    ingestor, kafka, dlq, db, _, _ = build()
    ok = ingestor.process_batch([record(OTHER, 0, 0, value("g1", 0, brand="ghost_shop")), record(OTHER, 0, 1, value("e1", 1))])
    assert ok is True
    assert "g1" not in db.stores[1].events and "g1" not in db.stores[2].events
    assert "unknown_brand: ghost_shop" in dlq.sent[0]["headers"]["dlq_reason"]
    assert "e1" in db.stores[1].events and kafka.commits == [{(OTHER, 0): 2}]


def test_a_row_mysql_rejects_is_retried_then_quarantined_after_the_attempt_limit():
    ingestor, kafka, dlq, db, _, _ = build(max_record_attempts=3)
    db.stores[1].fail_on["bad"] = DataTooLong("Data too long for column")
    batch = lambda: [record(OTHER, 0, 0, value("ok", 0)), record(OTHER, 0, 1, value("bad", 5)), record(OTHER, 0, 2, value("ok2", 9))]

    assert ingestor.process_batch(batch()) is False                 # attempt 1: blocked at offset 1
    assert kafka.commits == [{(OTHER, 0): 1}] and dlq.sent == []
    assert ingestor.process_batch(batch()) is False                 # attempt 2: still retrying
    assert dlq.sent == []
    assert ingestor.process_batch(batch()) is True                  # attempt 3: quarantined, the partition moves on
    assert len(dlq.sent) == 1 and dlq.sent[0]["headers"]["dlq_attempts"] == "3"
    assert kafka.commits[-1] == {(OTHER, 0): 3}
    assert {"ok", "ok2"} <= set(db.stores[1].events) and "bad" not in db.stores[1].events


def test_healthy_records_are_not_dragged_into_the_dlq_with_a_poison_record():
    ingestor, _, dlq, db, _, _ = build(max_record_attempts=1)
    db.stores[1].fail_on["bad"] = DataTooLong("x")
    ingestor.process_batch([record(OTHER, 0, i, value(e, i)) for i, e in enumerate(["a", "bad", "c"])])
    assert [s["headers"]["dlq_source_offset"] for s in dlq.sent] == ["1"]
    assert {"a", "c"} <= set(db.stores[1].events)


@pytest.mark.parametrize("failure", [True, "raise"])
def test_if_the_dlq_send_is_not_acknowledged_the_record_stays_unresolved(failure):
    ingestor, kafka, dlq, db, _, _ = build()
    dlq.fail = failure
    ok = ingestor.process_batch([record(OTHER, 0, 0, b"garbage"), record(OTHER, 0, 1, value("e1", 0))])
    assert ok is False
    assert kafka.commits == [] and ingestor.stats["dlq_failures"] == 1   # nothing skipped, nothing lost


# ---------------- ordering inside a batch ----------------

def test_records_are_applied_in_the_order_given_not_sorted_by_occurred_at():
    ingestor, _, _, db, _, _ = build()
    ingestor.process_batch([record(OTHER, 0, 0, value("later", 50)), record(OTHER, 0, 1, value("earlier", 40))])
    assert seq_ids(db.stores[1], "cid-1") == ["later", "earlier"]


def test_a_topic_that_does_not_match_the_event_name_is_still_processed():
    ingestor, kafka, _, db, _, _ = build()
    ingestor.process_batch([record(ATC, 0, 0, value("e1", 0))])     # page_viewed on the atc topic
    assert "e1" in db.stores[1].events and kafka.commits == [{(ATC, 0): 1}]


def test_consumer_settings_use_manual_offsets_a_bounded_prefetch_and_no_group_membership_settings():
    settings = ConsumerConfig(events_batch=200).kafka_settings("host-1")
    assert settings["enable.auto.commit"] is False and settings["enable.auto.offset.store"] is False
    assert settings["group.id"] == "intent-pipeline-workers"        # only names where offsets are committed
    assert settings["queued.max.messages.kbytes"] <= 8192 and settings["queued.min.messages"] <= 1000
    assert settings["bootstrap.servers"] == "kafka-service:9092"
    assert "session.timeout.ms" not in settings and "max.poll.interval.ms" not in settings
