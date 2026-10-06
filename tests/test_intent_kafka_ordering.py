"""Cross-topic ordering: the watermark buffer on its own, then inside the consumer loop."""

import pytest

from pipeline.intent_kafka_consumer import ConsumerConfig, IntentConsumer
from pipeline.intent_kafka_ordering import WATERMARK_STALENESS_MARGIN_MS, OrderingBuffer
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch
from tests.helpers import BRANDS, FakeDb, FakeDlq, FakeKafka, items, record, seq_ids, value

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


# ---------------- inside the consumer loop ----------------

def consumer_for(batches, clock=lambda: 10 ** 12, counts=None, assigned=None, **cfg):
    log = []
    kafka = FakeKafka(batches, log, counts=counts, assigned=assigned)
    db = FakeDb(log)
    config = ConsumerConfig(retry_backoff_s=0.001, retry_backoff_max_s=0.002, batch_wait_s=0, **cfg)
    consumer = IntentConsumer(kafka, FakeDlq(log), db, BRANDS.get, config, name="t", clock_ms=clock)
    kafka.on_empty = consumer.stop
    return consumer, kafka, db


def test_why_the_order_matters_applying_a_late_topics_event_last_splits_the_session():
    """The failure being prevented, shown directly on the state machine."""
    arrival_order = [value("e1", 0), value("e2", 20, click=True), value("e3", 70)]
    correct = InMemoryIntentStore()
    apply_batch(correct, items(*arrival_order))
    assert correct.sessions == [] and seq_ids(correct, "cid-1") == ["e1", "e2", "e3"]

    click_applied_last = InMemoryIntentStore()
    apply_batch(click_applied_last, items(arrival_order[0], arrival_order[2], arrival_order[1]))
    assert len(click_applied_last.sessions) == 1        # e2 is 50 s behind e3: a spurious session split


def test_a_lagging_topic_is_waited_for_so_events_are_applied_in_arrival_order():
    consumer, kafka, db = consumer_for([
        [record(OTHER, 0, 0, value("e1", 0), ts=1000), record(OTHER, 0, 1, value("e3", 70), ts=3000)],
        [record(CLICK, 0, 0, value("e2", 20, click=True), ts=2000)],      # the click topic is one poll behind
    ])
    consumer.run()
    store = db.stores[1]
    assert store.sessions == []                                          # no spurious split
    assert seq_ids(store, "cid-1") == ["e1", "e2", "e3"]                 # arrival order across topics
    assert kafka.commits[-1] == {(OTHER, 0): 2, (CLICK, 0): 1}


def test_nothing_is_applied_while_a_topic_is_behind_and_everything_follows_once_it_catches_up():
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0), ts=1000)], [], [],
                                        [record(CLICK, 0, 0, value("e2", 5, click=True), ts=2000)]])
    seen = []
    original = kafka.consume

    def watch(num_messages, timeout):
        seen.append(len(db.stores[1].events))
        return original(num_messages, timeout)

    kafka.consume = watch
    consumer.run()
    assert seen[:4] == [0, 0, 0, 0]                  # e1 held back while click had a record still to read
    assert set(db.stores[1].events) == {"e1"} and set(db.stores[1].clicks) == {"e2"}


def test_slack_delays_the_newest_events_until_a_late_append_could_no_longer_change_the_order():
    now = {"t": 1000 + 5_000}
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0), ts=1000)]], clock=lambda: now["t"],
                                       order_slack_s=10)
    calls = {"n": 0}

    def tick():
        calls["n"] += 1
        if calls["n"] == 3:
            now["t"] = 1000 + DONE
        if calls["n"] == 6:
            consumer.stop()

    kafka.on_empty = tick
    consumer.run()
    assert set(db.stores[1].events) == {"e1"}
    assert calls["n"] >= 3                           # it waited through the early polls first


def test_a_record_older_than_one_already_applied_is_applied_and_counted_not_dropped():
    """An append delay longer than the slack: the click record reaches the log only after
    the newer record was released. The guarantee is exceeded, so it is counted and logged,
    but the event is never dropped."""
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("late-first", 0), ts=1000)]])
    calls = {"n": 0}

    def appended_late():
        calls["n"] += 1
        if calls["n"] == 1:
            kafka.batches.append([record(CLICK, 0, 0, value("on-time", 5, click=True), ts=500)])
        elif calls["n"] == 3:
            consumer.stop()

    kafka.on_empty = appended_late
    consumer.run()
    assert consumer.stats["order_violations"] == 1
    assert "late-first" in db.stores[1].events and "on-time" in db.stores[1].clicks


def test_a_failed_batch_forgets_the_buffered_records_behind_the_failure():
    consumer, kafka, db = consumer_for([])
    db.fail_brand[1] = ConnectionError("down")
    batch = [record(OTHER, 0, 0, value("e1", 0)), record(OTHER, 0, 1, value("e2", 5))]
    consumer.ordering.set_assignment([(OTHER, 0)])
    consumer.ordering.add((OTHER, 0), 1000, 2, record(OTHER, 0, 2, value("e3", 9)), 10)   # buffered, not yet released
    assert consumer.process_batch(batch) is False
    assert kafka.seeks == [(OTHER, 0, 0)] and consumer.ordering.records == 0


def test_the_buffer_stays_bounded_by_pausing_partitions_that_are_ahead_then_resuming():
    ahead = [record(OTHER, 0, i, value(f"o{i}", i), ts=1000 + i) for i in range(6)]
    consumer, kafka, db = consumer_for([ahead, [], [record(CLICK, 0, 0, value("c0", 0, click=True), ts=900)]],
                                       order_buffer_max=4)
    consumer.run()
    assert kafka.pause_calls[0] == [(OTHER, 0)]                 # only the partition that is ahead is paused
    assert kafka.resume_calls                                   # resumed once the buffer drained
    assert len(db.stores[1].events) == 6 and len(db.stores[1].clicks) == 1


# ---------------- the ordering domain ----------------

ONE_SIDE = [(CLICK, 0), (CLICK, 1), (CLICK, 2), (OTHER, 0), (OTHER, 1), (OTHER, 2)]


def test_a_consumer_that_does_not_own_every_partition_pauses_instead_of_processing():
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0))]], assigned=ONE_SIDE)
    consumer.run()
    assert db.stores[1].events == {} and kafka.commits == []             # the other consumer holds ATC/checkout
    assert set(kafka.pause_calls[0]) == set(ONE_SIDE)                    # and nothing is fetched meanwhile


def test_a_consumer_that_owns_everything_processes_in_single_mode():
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0))]])
    consumer.run()
    assert "e1" in db.stores[1].events and kafka.pause_calls == []


def test_copartitioned_mode_lets_a_consumer_own_whole_partition_indexes_of_every_topic():
    equal = {CHECKOUT: 3, ATC: 3, CLICK: 3, OTHER: 3}
    mine = [(t, p) for t in equal for p in (0, 1)]                      # partitions 0 and 1 of all four topics
    consumer, kafka, db = consumer_for([[record(OTHER, 1, 0, value("e1", 0))]], counts=equal, assigned=mine,
                                       ordering_domain="copartitioned")
    consumer.run()
    assert "e1" in db.stores[1].events and kafka.pause_calls == []


def test_copartitioned_mode_refuses_a_split_that_separates_topics_of_the_same_index():
    equal = {CHECKOUT: 3, ATC: 3, CLICK: 3, OTHER: 3}
    split = [(CLICK, 0), (OTHER, 0), (ATC, 0)]                          # checkout[0] is somewhere else
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0))]], counts=equal, assigned=split,
                                       ordering_domain="copartitioned")
    consumer.run()
    assert db.stores[1].events == {}


def test_copartitioned_mode_is_refused_when_the_partition_counts_differ():
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0))]], ordering_domain="copartitioned")
    consumer.run()                                  # 2/2/3/3 are not co-partitioned
    assert db.stores[1].events == {}


def test_losing_the_assignment_clears_the_buffer_so_a_new_owner_rereads_it():
    consumer, kafka, db = consumer_for([])
    consumer.ordering.set_assignment([(OTHER, 0)])
    consumer.ordering.add((OTHER, 0), 1, 0, record(OTHER, 0, 0, value("e1", 0)), 10)
    consumer._on_revoke(None, [])
    assert consumer.ordering.records == 0 and consumer._domain_ok is False


def test_a_partition_that_runs_dry_while_the_buffer_is_full_is_resumed_not_left_paused():
    """Regression for a stall found on a real broker: when the buffer filled, every partition holding
    records was paused once, including one that then drained and became the lagging partition the rest
    were waiting for. The paused set must follow the buffer, not be fixed when the limit is hit."""
    first = [record(OTHER, 0, i, value(f"o{i}", i), ts=1000 + i) for i in range(5)] + \
            [record(CLICK, 0, i, value(f"c{i}", 10 + i, click=True), ts=2000 + i) for i in range(5)]
    later = [record(OTHER, 0, 5, value("o5", 30), ts=2500)]        # other[0] still has one more to deliver
    consumer, kafka, db = consumer_for([first, [], later], order_buffer_max=6)
    paused_when_the_lagging_partition_was_fetched = []
    original = kafka.consume

    def watch(num_messages, timeout):
        if kafka.batches and kafka.batches[0] is later:
            paused_when_the_lagging_partition_was_fetched.append(set(kafka.paused))
        return original(num_messages, timeout)

    kafka.consume = watch
    consumer.run()
    assert paused_when_the_lagging_partition_was_fetched, "the scenario did not reach the lagging fetch"
    assert (OTHER, 0) not in paused_when_the_lagging_partition_was_fetched[0]     # it had run dry, so it was resumed
    assert len(db.stores[1].events) == 6 and len(db.stores[1].clicks) == 5        # and everything was applied


def test_a_new_assignment_is_explicitly_resumed_so_stale_pause_flags_cannot_survive_a_rebalance():
    consumer, kafka, db = consumer_for([[record(OTHER, 0, 0, value("e1", 0))]])
    consumer.run()
    assert kafka.resume_calls and set(kafka.resume_calls[0]) == {(t, p) for t, n in kafka.counts.items() for p in range(n)}
