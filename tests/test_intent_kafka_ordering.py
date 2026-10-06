"""Cross-topic ordering: the buffer that merges partitions in send order and waits for lagging ones.
The cycle-level behaviour (cutoff, lag, late appends) is in test_intent_kafka_cycle.py."""

import pytest

from pipeline.intent_kafka_ordering import WATERMARK_STALENESS_MARGIN_MS, OrderingBuffer
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch
from tests.helpers import items, seq_ids, value

OTHER, CLICK, ATC, CHECKOUT = "intent.other", "intent.click", "intent.atc", "intent.checkout"
SLACK = 10_000
DONE = SLACK + WATERMARK_STALENESS_MARGIN_MS


def buffer(partitions, **kw):
    b = OrderingBuffer(slack_ms=SLACK, **kw)
    b.set_assignment(partitions)
    return b


def at_end_of(*caught_up):
    return lambda key: key in caught_up


# ---------------- the buffer ----------------

def test_records_are_released_in_timestamp_order_across_partitions_and_each_partition_keeps_its_order():
    p0, p1 = (OTHER, 0), (CLICK, 0)
    b = buffer([p0, p1])
    for offset, ts in enumerate([100, 300, 200]):          # partition order wobbles: 200 comes after 300
        b.add(p0, ts, offset, f"a{offset}", 1)
    for offset, ts in enumerate([150, 250]):
        b.add(p1, ts, offset, f"b{offset}", 1)
    out = b.release(10, at_end_of(p0, p1), now_ms=10 ** 9)
    assert out == ["a0", "b0", "b1", "a1", "a2"]            # a2 follows a1 although its timestamp is lower


def test_a_lagging_partition_holds_the_oldest_record_back():
    other, click = (OTHER, 0), (CLICK, 0)
    b = buffer([other, click])
    b.add(other, 1000, 0, "e1", 1)
    b.add(other, 3000, 1, "e3", 1)
    assert b.release(10, at_end_of(other), now_ms=10 ** 9) == []          # click is not caught up: wait
    b.add(click, 2000, 0, "e2", 1)                                        # the lagging topic finally delivers
    assert b.release(10, at_end_of(other, click), now_ms=10 ** 9) == ["e1", "e2", "e3"]


def test_a_caught_up_partition_is_trusted_only_after_the_slack_has_passed():
    other, click = (OTHER, 0), (CLICK, 0)
    b = buffer([other, click])
    b.add(other, 1000, 0, "e1", 1)
    caught_up = at_end_of(other, click)
    assert b.release(10, caught_up, now_ms=1000 + DONE - 1) == []         # an older record may still be in flight
    assert b.release(10, caught_up, now_ms=1000 + DONE) == ["e1"]


def test_a_partition_with_a_record_older_than_the_head_needs_no_wait():
    other, click = (OTHER, 0), (CLICK, 0)
    b = buffer([other, click])
    b.add(click, 500, 0, "c", 1)
    b.add(other, 1000, 0, "e", 1)
    # click's record is the oldest head; other has a record too, so nothing older can still come from it
    assert b.release(1, at_end_of(), now_ms=0) == ["c"]


def test_ties_are_broken_by_topic_partition_and_offset_deterministically():
    keys = [(OTHER, 1), (ATC, 0), (OTHER, 0)]
    b = buffer(keys)
    for i, key in enumerate(keys):
        b.add(key, 5000, 0, str(key), 1)
    assert b.release(10, at_end_of(*keys), 10 ** 9) == [str((ATC, 0)), str((OTHER, 0)), str((OTHER, 1))]


def test_an_older_record_after_a_newer_one_was_released_is_counted_not_dropped():
    p = (OTHER, 0)
    b = buffer([p, (CLICK, 0)])
    b.add(p, 5000, 0, "new", 1)
    assert b.release(1, at_end_of(p, (CLICK, 0)), 10 ** 9) == ["new"]
    assert b.add((CLICK, 0), 4000, 0, "old", 1) is False and b.violations == 1
    assert b.release(1, at_end_of(p, (CLICK, 0)), 10 ** 9) == ["old"]      # still delivered


def test_capacity_is_bounded_and_reports_the_partitions_that_are_ahead():
    p0, p1 = (OTHER, 0), (CLICK, 0)
    b = buffer([p0, p1], max_records=4)
    for i in range(4):
        b.add(p0, 1000 + i, i, i, 10)
    assert b.over_capacity() and b.buffered_partitions() == [p0]            # p1 (empty, lagging) is not paused
    b.add(p1, 900, 0, "x", 10)
    assert b.release(10, at_end_of(), 0) == ["x"]                         # p0 is ahead of p1's record, p1 then lags
    assert b.over_capacity()                                              # still 4 buffered: nothing more is safe yet
    assert len(b.release(10, at_end_of(p0, p1), 10 ** 9)) == 4            # once p1 is caught up and the slack passed
    assert b.drained()
    b = buffer([p0], max_bytes=100)
    b.add(p0, 1, 0, "big", 100)
    assert b.over_capacity()


def test_dropping_a_partition_forgets_its_records_and_losing_the_assignment_clears_everything():
    p0, p1 = (OTHER, 0), (CLICK, 0)
    b = buffer([p0, p1])
    b.add(p0, 1, 0, "a", 5)
    b.add(p1, 2, 0, "b", 5)
    b.drop(p0)
    assert (b.records, b.bytes) == (1, 5)
    b.set_assignment([p0])                     # p1 revoked
    assert (b.records, b.bytes) == (0, 0)


def test_why_the_order_matters_applying_a_late_topics_event_last_splits_the_session():
    """The failure being prevented, shown directly on the state machine."""
    arrival_order = [value("e1", 0), value("e2", 20, click=True), value("e3", 70)]
    correct = InMemoryIntentStore()
    apply_batch(correct, items(*arrival_order))
    assert correct.sessions == [] and seq_ids(correct, "cid-1") == ["e1", "e2", "e3"]

    click_applied_last = InMemoryIntentStore()
    apply_batch(click_applied_last, items(arrival_order[0], arrival_order[2], arrival_order[1]))
    assert len(click_applied_last.sessions) == 1        # e2 is 50 s behind e3: a spurious session split
