import itertools
import json
import pathlib
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from pipeline.intent_session_state import (
    CONTINUE,
    CONTINUE_LATE,
    NEW_SESSION,
    ORPHAN,
    apply_event,
    atc_product_id,
    decide_timing,
)
from pipeline.intent_sqs_contract import MalformedMessage, parse_message
from pipeline.intent_sqs_store import (
    InMemoryIntentStore,
    MySqlIntentStore,
    SchemaMissing,
    reset_schema_verification,
    verify_state_schema,
)
from pipeline.intent_sqs_writer import apply_batch, apply_messages
from tests.fake_db import FakeConnection, FakeDbCursor, FatalDbError, SchemaState

ROOT = pathlib.Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"
TIMEOUT = 1800
BASE = datetime(2026, 10, 4, 6, 0, 0)


def at(seconds: float, base: datetime = BASE) -> str:
    return (base + timedelta(seconds=seconds)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def event(event_id, when, actor="actor-1", name="page_viewed", raw=None, client=None, click=False,
          bucket=None, base=BASE, brand="bbb_shop"):
    msg = {
        "schema_version": 1,
        "type": "click" if click else "event",
        "message_key": event_id,
        "brand_id": brand,
        "event_id": event_id,
        "event_name": "click" if click else name,
        "actor_id": actor,
        "client_id": client,
        "visitor_id": "visitor-x",
        "session_id": None,
        "occurred_at": at(when, base),
        "url": "https://shop.example/products/item",
        "referrer": None,
        "user_agent": "Mozilla/5.0 test",
        "session_start": None,
        "session_end": None,
        "session_time_spent": None,
        "raw": raw,
    }
    if click:
        msg["click"] = {"tag_name": "button", "element_id": "buy"}
        msg["signals"] = {}
        msg["click_bucket"] = bucket or "useful_click"
    return msg


def parse(msgs):
    return [parse_message(json.dumps(m)) for m in msgs]


def run(store, msgs, timeout=TIMEOUT):
    return apply_messages(store, parse(msgs), timeout)


def cur(last, start=None, actor="a"):
    start = start or last
    return {
        "actor_id": actor, "session_id": "s-1", "session_start": start, "last_event_at": last,
        "last_event_id": "e-last", "events_seq": {"1": {"event_name": "page_viewed", "event_id": "e-last"}},
    }


# ---- pure timing rules ----

def test_no_cursor_is_new_session():
    assert decide_timing(None, datetime(2026, 1, 1), TIMEOUT) == NEW_SESSION


def test_gap_equal_to_timeout_continues_and_above_starts_new():
    last = datetime(2026, 1, 1, 0, 0)
    assert decide_timing(cur(last), last + timedelta(seconds=TIMEOUT), TIMEOUT) == CONTINUE
    assert decide_timing(cur(last), last + timedelta(seconds=TIMEOUT, microseconds=1), TIMEOUT) == NEW_SESSION


def test_event_within_30s_before_last_continues_without_moving_last_back():
    last = datetime(2026, 1, 1, 0, 10)
    nxt, closed, sid = apply_event(cur(last, datetime(2026, 1, 1)),
                                   {"event_id": "e2", "event_name": "page_viewed"},
                                   datetime(2026, 1, 1, 0, 9, 45), TIMEOUT, "a")
    assert closed is None and sid == "s-1"
    assert nxt["last_event_at"] == last


def test_event_late_by_more_than_30s_but_inside_session_is_continue_late():
    cursor = cur(datetime(2026, 1, 1, 0, 10), datetime(2026, 1, 1, 0, 0))
    assert decide_timing(cursor, datetime(2026, 1, 1, 0, 5), TIMEOUT) == CONTINUE_LATE


def test_late_event_inside_session_appends_without_split_or_moving_last_back():
    last = datetime(2026, 1, 1, 0, 10)
    cursor = cur(last, datetime(2026, 1, 1, 0, 0))
    nxt, closed, sid = apply_event(cursor, {"event_id": "late", "event_name": "page_viewed"},
                                   datetime(2026, 1, 1, 0, 5), TIMEOUT, "a")
    assert closed is None and sid == "s-1"
    assert nxt["last_event_at"] == last
    assert list(nxt["events_seq"].keys()) == ["1", "2"]


def test_event_from_before_session_start_is_orphan_and_changes_nothing():
    cursor = cur(datetime(2026, 1, 1, 0, 10), datetime(2026, 1, 1, 0, 0))
    nxt, closed, sid = apply_event(cursor, {"event_id": "old", "event_name": "page_viewed"},
                                   datetime(2025, 12, 31, 23, 59), TIMEOUT, "a")
    assert decide_timing(cursor, datetime(2025, 12, 31, 23, 59), TIMEOUT) == ORPHAN
    assert closed is None
    assert nxt == cursor
    assert sid != "s-1"


# ---- ordering policy (SQS standard reordering) ----

def test_ordered_A_B_C_is_one_session_with_last_C():
    store = InMemoryIntentStore()
    run(store, [event("A", 0), event("B", 60), event("C", 120)])
    assert store.cursors["actor-1"]["last_event_id"] == "C"
    assert [v["event_id"] for v in store.cursors["actor-1"]["events_seq"].values()] == ["A", "B", "C"]
    assert store.sessions == []


def test_A_C_B_stays_one_session_last_stays_C():
    store = InMemoryIntentStore()
    run(store, [event("A", 0)])
    run(store, [event("C", 120)])
    run(store, [event("B", 60)])
    sid = store.cursors["actor-1"]["session_id"]
    assert store.cursors["actor-1"]["last_event_id"] == "C"
    assert [v["event_id"] for v in store.cursors["actor-1"]["events_seq"].values()] == ["A", "C", "B"]
    assert store.sessions == []
    assert all(store.events[e][5] == sid for e in ("A", "B", "C"))


def test_late_by_under_30s_does_not_split():
    store = InMemoryIntentStore()
    run(store, [event("A", 100)])
    run(store, [event("B", 80)])
    assert store.sessions == []
    assert store.events["B"][5] == store.events["A"][5]


def test_late_by_over_30s_inside_session_does_not_split():
    store = InMemoryIntentStore()
    run(store, [event("A", 0)])
    run(store, [event("C", 300)])
    run(store, [event("B", 100)])
    assert store.sessions == []
    assert store.events["B"][5] == store.events["A"][5]


def test_event_from_previous_session_arriving_late_is_orphan():
    store = InMemoryIntentStore()
    run(store, [event("A", 0), event("B", 60)])
    run(store, [event("D", TIMEOUT + 500)])
    current = store.cursors["actor-1"]["session_id"]
    closed_before = len(store.sessions)
    run(store, [event("OLD", 10)])
    assert len(store.sessions) == closed_before
    assert store.cursors["actor-1"]["session_id"] == current
    assert store.events["OLD"][5] != current


def test_arrival_order_does_not_change_final_state():
    batch = [event("A", 0), event("B", 40), event("C", 90), event("D", 200)]
    first, second = InMemoryIntentStore(), InMemoryIntentStore()
    run(first, batch)
    run(second, list(reversed(batch)))
    assert first.cursors["actor-1"]["events_seq"] == second.cursors["actor-1"]["events_seq"]
    assert first.cursors["actor-1"]["last_event_id"] == second.cursors["actor-1"]["last_event_id"] == "D"


# ---- flows: rollover, duplicates, ATC, actorless ----

def test_rollover_closes_previous_session_with_end_and_duration():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0), event("e-2", 100)])
    first = store.cursors["actor-1"]["session_id"]
    run(store, [event("e-3", 100 + TIMEOUT + 10)])
    assert store.cursors["actor-1"]["session_id"] != first
    closed = store.sessions[0]
    assert closed[0] == first
    assert closed[5] == (BASE + timedelta(seconds=100)).replace(tzinfo=timezone.utc)
    assert closed[6] == 100_000


def test_events_seq_resets_per_session_and_closed_sequence_is_kept():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0), event("e-2", 10)])
    run(store, [event("e-3", 10 + TIMEOUT + 5)])
    assert list(store.cursors["actor-1"]["events_seq"].keys()) == ["1"]
    assert list(json.loads(store.sessions[0][16]).keys()) == ["1", "2"]


def test_atc_is_deduped_per_session_and_does_not_move_cursor():
    store = InMemoryIntentStore()
    atc = lambda eid, when: event(eid, when, name="product_added_to_cart", raw={"product_id": "555"})
    counts = run(store, [atc("a-1", 0), atc("a-2", 30)])
    assert "a-1" in store.events and "a-2" not in store.events
    assert counts["atc_deduped"] == 1
    assert list(store.cursors["actor-1"]["events_seq"].keys()) == ["1"]


def test_atc_same_product_in_new_session_is_recorded_again():
    store = InMemoryIntentStore()
    atc = lambda eid, when: event(eid, when, name="product_added_to_cart", raw={"product_id": "555"})
    run(store, [atc("a-1", 0), atc("a-2", TIMEOUT + 60)])
    assert "a-1" in store.events and "a-2" in store.events


def test_duplicate_event_delivery_does_not_mutate_state():
    store = InMemoryIntentStore()
    msgs = [event("e-1", 0), event("e-2", 20)]
    run(store, msgs)
    before = store.cursors["actor-1"]
    counts = run(store, msgs)
    assert counts["events"] == 0 and counts["duplicates"] == 2
    assert len(store.events) == 2
    assert store.cursors["actor-1"] == before


def test_duplicate_click_delivery_creates_no_second_click_row():
    store = InMemoryIntentStore()
    run(store, [event("c-1", 0, click=True)])
    counts = run(store, [event("c-1", 0, click=True)])
    assert counts["clicks"] == 0 and counts["duplicates"] == 1
    assert len(store.clicks) == 1


def test_duplicate_after_rollover_does_not_close_twice():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0)])
    late = [event("e-2", TIMEOUT + 10)]
    run(store, late)
    run(store, late)
    assert len(store.sessions) == 1


def test_actorless_events_are_one_off_sessions_without_cursor():
    store = InMemoryIntentStore()
    run(store, [event("x-1", 0, actor=None), event("x-2", 5, actor=None)])
    assert len(store.events) == 2 and store.cursors == {} and store.sessions == []
    assert len({store.events[k][5] for k in ("x-1", "x-2")}) == 2


def test_client_id_is_used_when_actor_id_is_missing():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0, actor=None, client="client-9")])
    assert "client-9" in store.cursors
    assert store.events["e-1"][2] == "client-9"


def test_visitor_id_is_never_identity():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0, actor=None)])
    assert "visitor-x" not in store.cursors


def test_messages_applied_in_occurred_at_order_within_batch():
    store = InMemoryIntentStore()
    run(store, [event("e-late", 50), event("e-early", 0)])
    seq = store.cursors["actor-1"]["events_seq"]
    assert seq["1"]["event_id"] == "e-early" and seq["2"]["event_id"] == "e-late"


# ---- actor locking and deterministic lock order ----

def test_actor_lock_order_is_sorted_regardless_of_batch_order():
    actors = ["delta", "alpha", "charlie", "bravo"]
    for perm in itertools.permutations(actors):
        store = InMemoryIntentStore()
        run(store, [event(f"e-{a}", i * 10, actor=a) for i, a in enumerate(perm)])
        assert store.locked_actors == sorted(actors), perm


def test_two_consumers_with_opposite_batch_orders_lock_in_same_sequence():
    a, b = InMemoryIntentStore(), InMemoryIntentStore()
    run(a, [event("z", 0, actor="zed"), event("m", 10, actor="mike"), event("a", 20, actor="amy")])
    run(b, [event("a", 20, actor="amy"), event("m", 10, actor="mike"), event("z", 0, actor="zed")])
    assert a.locked_actors == b.locked_actors == ["amy", "mike", "zed"]


def test_mysql_store_takes_named_lock_before_row_lock():
    cur_ = FakeDbCursor()
    store = MySqlIntentStore(cur_)
    store.lock_actor("a")
    assert store.get_cursor_for_update("a") is None
    get_lock = next(i for i, s in enumerate(cur_.sql) if "GET_LOCK" in s)
    row_lock = next(i for i, s in enumerate(cur_.sql) if "FOR UPDATE" in s)
    assert get_lock < row_lock
    store.unlock_all()
    assert any("RELEASE_LOCK" in s for s in cur_.sql)


# ---- poison isolation ----

class _FailingInsertStore(InMemoryIntentStore):
    def __init__(self, bad_event_id, exc):
        super().__init__()
        self.bad_event_id = bad_event_id
        self.exc = exc

    def insert_event(self, kind, row):
        if row[0] == self.bad_event_id:
            raise self.exc
        return super().insert_event(kind, row)


class DataTooLong(Exception):
    errno = 1406


def test_poison_message_in_actor_group_rolls_back_alone_and_neighbours_commit():
    store = _FailingInsertStore("m2", DataTooLong("data too long"))
    counts = run(store, [event("m1", 0), event("m2", 30), event("m3", 60)])
    assert "m1" in store.events and "m3" in store.events and "m2" not in store.events
    assert [m["event_id"] for m in counts["failed_messages"]] == ["m2"]


def test_poison_actor_does_not_block_healthy_actor():
    store = InMemoryIntentStore()
    store.fail_on_actor.add("bad")
    counts = run(store, [event("g1", 0, actor="good"), event("b1", 0, actor="bad"),
                         event("g2", 30, actor="good")])
    assert "g1" in store.events and "g2" in store.events and "b1" not in store.events
    assert [m["event_id"] for m in counts["failed_messages"]] == ["b1"]


def test_fatal_mysql_error_is_not_isolated():
    store = _FailingInsertStore("m1", FatalDbError("connection lost"))
    with pytest.raises(FatalDbError):
        run(store, [event("m1", 0)])


# ---- contract: timestamps, timezone ----

def _body(**overrides):
    base = {
        "schema_version": 1, "type": "event", "message_key": "k", "brand_id": "bbb_shop",
        "event_id": "e", "event_name": "page_viewed", "actor_id": "a",
        "occurred_at": "2026-10-04T00:30:00.000Z",
    }
    base.update(overrides)
    return json.dumps(base)


def test_store_local_wall_clock_is_stored_unchanged():
    msg = parse_message(_body(occurred_at="2026-10-04T00:30:00.000Z"))
    store = InMemoryIntentStore()
    apply_messages(store, [msg], TIMEOUT)
    assert store.events["e"][-1] == datetime(2026, 10, 4, 0, 30, tzinfo=timezone.utc)


def test_microseconds_are_preserved():
    msg = parse_message(_body(occurred_at="2026-10-04T00:30:00.123456Z"))
    store = InMemoryIntentStore()
    apply_messages(store, [msg], TIMEOUT)
    assert store.events["e"][-1].microsecond == 123456


def test_explicit_offset_is_rejected_to_avoid_double_conversion():
    with pytest.raises(MalformedMessage):
        parse_message(_body(occurred_at="2026-10-04T00:30:00.000+05:30"))


def test_session_rollover_at_midnight_keeps_store_local_business_date():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0, base=datetime(2026, 10, 3, 23, 50)),
                event("e-2", 0, base=datetime(2026, 10, 4, 0, 1))], timeout=60)
    closed = store.sessions[0]
    assert closed[4].date().isoformat() == "2026-10-03"


def test_two_brands_with_different_zones_pass_the_same_wall_clock_through():
    a, b = InMemoryIntentStore(), InMemoryIntentStore()
    run(a, [event("e-1", 0, base=datetime(2026, 10, 4, 0, 30))])
    run(b, [event("e-1", 0, base=datetime(2026, 10, 4, 0, 30), brand="pts_shop")])
    assert a.events["e-1"][-1] == b.events["e-1"][-1] == datetime(2026, 10, 4, 0, 30, tzinfo=timezone.utc)


def test_existing_v1_fixtures_still_parse():
    for name in ("event_v1.json", "click_v1.json", "session_snapshot_v1.json"):
        parse_message(json.dumps(json.loads((FIXTURES / name).read_text(encoding="utf-8"))))


# ---- ATC product id ----

@pytest.mark.parametrize("raw_product_id, expected", [
    ("gid://shopify/Product/9", "Product:9"),
    ("Product:9", "Product:9"),
    ("9000000000001", "9000000000001"),
    ("SYNTH:abcdef0123456789", None),
    ("FALLBACK:x", None),
    ("", None),
])
def test_atc_product_id_matches_old_producer_semantics(raw_product_id, expected):
    assert atc_product_id({"event_name": "product_added_to_cart", "raw": {"product_id": raw_product_id}}) == expected


def test_gid_and_normalized_atc_dedupe_as_same_product():
    store = InMemoryIntentStore()
    run(store, [
        event("a-1", 0, name="product_added_to_cart", raw={"product_id": "gid://shopify/Product/9"}),
        event("a-2", 30, name="product_added_to_cart", raw={"product_id": "Product:9"}),
    ])
    assert "a-1" in store.events and "a-2" not in store.events


# ---- schema verification (read-only, once per database) ----

def _verify(state):
    reset_schema_verification()
    verify_state_schema(FakeDbCursor(state))


def test_verifier_passes_on_correct_schema():
    _verify(SchemaState())


def test_verifier_fails_on_missing_table():
    with pytest.raises(SchemaMissing, match="intent_atc_dedupe"):
        _verify(SchemaState(missing_tables=("intent_atc_dedupe",)))


def test_verifier_fails_on_missing_column_used_by_inserts():
    with pytest.raises(SchemaMissing, match="behavioral_events.event_name"):
        _verify(SchemaState(missing_columns=("behavioral_events.event_name",)))


def test_verifier_fails_on_missing_unique_key_needed_for_dedupe():
    with pytest.raises(SchemaMissing, match="behavioral_events has no unique key"):
        _verify(SchemaState(missing_unique=("behavioral_events",)))


def test_verifier_fails_on_incorrect_atc_primary_key():
    with pytest.raises(SchemaMissing, match="intent_atc_dedupe primary key"):
        _verify(SchemaState(pk_override={"intent_atc_dedupe": ("session_id",)}))


def test_verifier_fails_on_incorrect_cursor_primary_key():
    with pytest.raises(SchemaMissing, match="intent_actor_cursors primary key"):
        _verify(SchemaState(pk_override={"intent_actor_cursors": ("actor_id", "session_id")}))


def test_verifier_fails_on_wrong_source_updated_at_type():
    with pytest.raises(SchemaMissing, match=r"expected datetime\(6\)"):
        _verify(SchemaState(source_type="datetime"))


def test_schema_is_verified_once_per_database_not_per_batch():
    reset_schema_verification()
    cur_ = FakeDbCursor()
    conn = FakeConnection()
    apply_batch(cur_, conn, parse([event("e-1", 0)]))
    first = sum("information_schema" in q for q in cur_.sql)
    apply_batch(cur_, conn, parse([event("e-2", 10)]))
    apply_batch(cur_, conn, parse([event("e-3", 20)]))
    assert first > 0
    assert sum("information_schema" in q for q in cur_.sql) == first


def test_apply_batch_issues_no_ddl():
    reset_schema_verification()
    cur_ = FakeDbCursor()
    apply_batch(cur_, FakeConnection(), parse([event("e-1", 0), event("c-1", 5, click=True)]))
    assert not [q for q in cur_.sql if "CREATE TABLE" in q.upper() or "ALTER TABLE" in q.upper()]


def test_missing_migration_writes_nothing():
    reset_schema_verification()
    cur_ = FakeDbCursor(SchemaState(missing_tables=("intent_atc_dedupe",)))
    conn = FakeConnection()
    with pytest.raises(SchemaMissing):
        apply_batch(cur_, conn, parse([event("e-1", 0)]))
    assert not any(q.lstrip().upper().startswith("INSERT") for q in cur_.sql)
    assert conn.commits == 0


def test_outage_aborts_batch_without_commit():
    reset_schema_verification()
    store = _FailingInsertStore("e-1", FatalDbError("mysql down"))
    with pytest.raises(FatalDbError):
        run(store, [event("e-1", 0)])


# ---- static guarantees ----

def test_state_modules_do_not_reference_mongo_or_outbox():
    banned = ("pymongo", "MongoClient", "intent_outbox", "intent_sessions.events",
              "intent_sessions.click_events", "intent_sessions.actor_cursors",
              "intent_sessions.session_history", "intent_sessions.intent_outbox", "slug_cache", "SlugCache")
    for rel in ("pipeline/intent_session_state.py", "pipeline/intent_sqs_store.py",
                "pipeline/intent_sqs_writer.py", "workers/intent_sqs_worker.py"):
        source = (ROOT / rel).read_text(encoding="utf-8")
        for term in banned:
            assert term not in source, f"{rel} references {term}"


def test_worker_import_path_does_not_load_pymongo():
    code = ("import sys, pipeline.intent_sqs_writer, workers.intent_sqs_worker\n"
            "print('PYMONGO=' + str('pymongo' in sys.modules))")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=str(ROOT))
    assert "PYMONGO=False" in out.stdout, out.stdout + out.stderr


# ---- ID length caps (VARCHAR(100)) ----

@pytest.mark.parametrize("field", ["event_id", "actor_id", "client_id"])
def test_id_of_100_chars_is_accepted(field):
    parse_message(_body(**{field: "x" * 100}))


@pytest.mark.parametrize("field", ["event_id", "actor_id", "client_id"])
def test_id_of_101_chars_is_rejected(field):
    with pytest.raises(MalformedMessage):
        parse_message(_body(**{field: "x" * 101}))
