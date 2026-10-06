import pytest

from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch, is_data_error
from pipeline.intent_kafka_contract import InvalidMessage
from tests.helpers import atc, fixtures, item, items, seq_ids, stamp, value

TIMEOUT = 1800


def run(store, *messages, timeout=TIMEOUT):
    return apply_batch(store, items(*messages), timeout)


class DataTooLong(Exception):
    errno = 1406


class Deadlock(Exception):
    errno = 1213


# ---------------- sequencing, rollover ----------------

def test_events_of_one_actor_form_one_session_with_a_contiguous_sequence():
    store = InMemoryIntentStore()
    result = run(store, value("e1", 0), value("e2", 30), value("e3", 90))
    assert result.applied == 3 and result.sessions_closed == 0
    assert seq_ids(store, "cid-1") == ["e1", "e2", "e3"]
    assert list(store.cursors["cid-1"].events_seq) == ["1", "2", "3"]
    assert {store.events[e][5] for e in ("e1", "e2", "e3")} == {store.cursors["cid-1"].session_id}
    assert store.cursors["cid-1"].last_event_id == "e3"
    assert store.sessions == []  # open sessions are not intent_sessions rows


def test_rollover_writes_the_closed_session_row_and_restarts_the_sequence():
    store = InMemoryIntentStore()
    run(store, value("e1", 0), value("e2", 100, name="product_viewed"))
    first_session = store.cursors["cid-1"].session_id
    result = run(store, value("e3", 100 + TIMEOUT + 1))

    assert result.sessions_closed == 1
    assert store.cursors["cid-1"].session_id != first_session
    assert seq_ids(store, "cid-1") == ["e3"]
    (row,) = store.sessions
    # intent_sessions column order: session_id, actor_id, client_id, visitor_id, session_start,
    # session_end, session_time_spent_ms, event_count, page_view, product_view, ...
    assert row[0] == first_session and row[1] == "cid-1"
    assert row[4].isoformat() == "2026-10-05T10:30:00" and row[5].isoformat() == "2026-10-05T10:31:40"
    assert row[6] == 100_000 and row[7] == 2 and row[8] == 1 and row[9] == 1


def test_a_session_row_counts_clicks_by_bucket():
    store = InMemoryIntentStore()
    run(store, value("c1", 0, click=True, bucket="useful_click"), value("c2", 5, click=True, bucket="dead_click"),
        value("c3", 10, click=True, bucket="dead_click"), value("late", TIMEOUT + 100))
    (row,) = store.sessions
    click_count, useful, dead = row[10], row[11], row[12]
    assert (click_count, useful, dead) == (3, 1, 2)
    assert len(store.clicks) == 3 and "late" in store.events


# ---------------- out-of-order and late events (old behaviour) ----------------

def test_event_within_the_tolerance_before_last_stays_in_session_and_does_not_move_last_back():
    store = InMemoryIntentStore()
    run(store, value("e1", 100))
    run(store, value("e0", 80))  # 20 s earlier than the cursor's last event
    cursor = store.cursors["cid-1"]
    assert seq_ids(store, "cid-1") == ["e1", "e0"] and cursor.last_event_id == "e1"
    assert store.sessions == []


def test_event_more_than_30_seconds_late_starts_a_new_session_like_the_old_pipeline():
    store = InMemoryIntentStore()
    run(store, value("e1", 0), value("e2", 300))
    first = store.cursors["cid-1"].session_id
    result = run(store, value("late", 100))  # 200 s before the last event
    assert result.sessions_closed == 1 and store.cursors["cid-1"].session_id != first
    assert store.sessions[0][0] == first and store.sessions[0][7] == 2  # the closed session kept its 2 events
    assert "late" in store.events  # a late event is stored, never discarded


# ---------------- duplicates ----------------

def test_redelivering_a_batch_changes_nothing():
    store = InMemoryIntentStore()
    batch = [value("e1", 0), value("e2", 20), atc("a1", 30), value("c1", 40, click=True)]
    run(store, *batch)
    before = (dict(store.events), dict(store.clicks), set(store.atc), dict(store.cursors), list(store.sessions))
    again = run(store, *batch)
    assert again.applied == 0 and again.duplicates + again.atc_deduped == 4
    assert (dict(store.events), dict(store.clicks), set(store.atc), dict(store.cursors), list(store.sessions)) == before


def test_a_duplicate_event_after_a_rollover_does_not_close_a_session_twice():
    store = InMemoryIntentStore()
    run(store, value("e1", 0))
    late = value("e2", TIMEOUT + 10)
    run(store, late)
    run(store, late)
    assert len(store.sessions) == 1


def test_event_id_is_stored_exactly_as_sent():
    store = InMemoryIntentStore()
    run(store, value("sh-0B94-aBc", 0))
    assert "sh-0B94-aBc" in store.events


# ---------------- ATC dedupe ----------------

def test_atc_for_the_same_product_in_a_session_is_stored_once_and_leaves_the_cursor_alone():
    store = InMemoryIntentStore()
    run(store, value("e1", 0), atc("a1", 10))
    cursor_before = store.cursors["cid-1"]
    result = run(store, atc("a2", 20))
    assert result.atc_deduped == 1 and "a2" not in store.events
    assert store.cursors["cid-1"] == cursor_before


def test_atc_for_a_different_product_is_stored():
    store = InMemoryIntentStore()
    run(store, atc("a1", 0, product="Product:1"), atc("a2", 5, product="Product:2"))
    assert {"a1", "a2"} <= set(store.events)


def test_atc_for_the_same_product_in_a_new_session_is_stored_again():
    store = InMemoryIntentStore()
    run(store, atc("a1", 0), atc("a2", TIMEOUT + 60))
    assert {"a1", "a2"} <= set(store.events)


def test_synthetic_product_ids_never_dedupe_because_each_event_has_its_own():
    store = InMemoryIntentStore()
    run(store, atc("a1", 0, product="SYNTH:aaa"), atc("a2", 5, product="SYNTH:bbb"))
    assert {"a1", "a2"} <= set(store.events)


def test_the_add_to_cart_alias_is_stored_without_dedupe():
    store = InMemoryIntentStore()
    alias = lambda eid, at: value(eid, at, name="add_to_cart", raw={"product_id": "Product:42"})
    run(store, alias("x1", 0), alias("x2", 5))
    assert {"x1", "x2"} <= set(store.events) and store.atc == set()


# ---------------- identity ----------------

def test_events_without_any_actor_are_their_own_sessions_with_no_cursor():
    store = InMemoryIntentStore()
    run(store, value("n1", 0, actor=None, client=None), value("n2", 5, actor=None, client=None))
    assert store.cursors == {} and store.sessions == []
    assert store.events["n1"][5] != store.events["n2"][5]   # different session_id each
    assert store.events["n1"][2] is None                     # actor_id column NULL


def test_client_id_is_the_identity_when_actor_id_is_missing_and_visitor_id_never_is():
    store = InMemoryIntentStore()
    run(store, value("a", 0, actor=None, client="cid-9", visitor="vis-X"))
    assert list(store.cursors) == ["cid-9"]
    assert store.events["a"][4] == "vis-X"  # stored as information only


def test_two_actors_in_one_batch_get_independent_sessions():
    store = InMemoryIntentStore()
    run(store, value("a1", 0, actor="u1"), value("b1", 1, actor="u2"), value("a2", 2, actor="u1"))
    assert seq_ids(store, "u1") == ["a1", "a2"] and seq_ids(store, "u2") == ["b1"]
    assert store.cursors["u1"].session_id != store.cursors["u2"].session_id


# ---------------- real producer fixtures end to end through the writer ----------------

def test_every_real_producer_message_is_persisted_in_the_right_table():
    store = InMemoryIntentStore()
    messages = [case["value"] for case in fixtures().values() if case["value"]["actor_id"] == "cid-1"]
    result = apply_batch(store, [item(m, ("t", 0, i)) for i, m in enumerate(messages)], TIMEOUT)
    assert result.failed == {}
    clicks = {m["event_id"] for m in messages if m["type"] == "click"}
    assert set(store.clicks) == clicks
    assert set(store.events) == {m["event_id"] for m in messages} - clicks


# ---------------- locking order ----------------

def test_actors_are_locked_in_sorted_order_whatever_the_arrival_order():
    store = InMemoryIntentStore()
    run(store, value("d", 0, actor="delta"), value("a", 1, actor="alpha"), value("c", 2, actor="charlie"),
        value("b", 3, actor="bravo"))
    assert store.locked_actors == ["alpha", "bravo", "charlie", "delta"]


# ---------------- isolation and error classification ----------------

def test_a_row_mysql_rejects_is_rolled_back_alone_and_its_neighbours_commit():
    store = InMemoryIntentStore()
    store.fail_on["bad"] = DataTooLong("Data too long for column")
    result = run(store, value("ok1", 0), value("bad", 10), value("ok2", 20))
    assert set(store.events) == {"ok1", "ok2"}
    assert list(result.failed) == [("intent.other", 0, 1)]
    assert seq_ids(store, "cid-1") == ["ok1", "ok2"]


def test_a_rejected_row_of_one_actor_does_not_affect_another_actor():
    store = InMemoryIntentStore()
    store.fail_on["bad"] = DataTooLong("x")
    result = run(store, value("bad", 0, actor="u1"), value("good", 1, actor="u2"))
    assert "good" in store.events and "bad" not in store.events and "u1" not in store.cursors
    assert len(result.failed) == 1


def test_infrastructure_errors_propagate_so_the_whole_transaction_is_retried():
    for error in (Deadlock("deadlock"), ConnectionError("lost"), RuntimeError("anything unclassified")):
        store = InMemoryIntentStore()
        store.fail_on["e1"] = error
        with pytest.raises(type(error)):
            run(store, value("e1", 0))


def test_error_classification():
    assert is_data_error(InvalidMessage("x")) and is_data_error(DataTooLong("x"))
    assert not is_data_error(Deadlock("x")) and not is_data_error(TypeError("a bug"))
    assert not is_data_error(ConnectionError("x"))


def test_a_rejected_closing_session_row_rolls_back_the_event_that_triggered_it():
    class RejectSession(InMemoryIntentStore):
        def upsert_session(self, row):
            raise DataTooLong("session row rejected")

    store = RejectSession()
    run(store, value("e1", 0))
    result = run(store, value("e2", TIMEOUT + 5))
    assert "e2" not in store.events and len(result.failed) == 1
    assert seq_ids(store, "cid-1") == ["e1"]   # the cursor still describes the old session
