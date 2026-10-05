import json
import pathlib
from datetime import datetime, timedelta, timezone

import pytest

from pipeline.intent_session_state import (
    CONTINUE,
    NEW_SESSION,
    apply_event,
    atc_product_id,
    decide_timing,
)
from pipeline.intent_sqs_contract import parse_message
from pipeline.intent_sqs_store import InMemoryIntentStore, MySqlIntentStore
from pipeline.intent_sqs_writer import apply_messages

ROOT = pathlib.Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"
TIMEOUT = 1800
BASE = datetime(2026, 10, 4, 6, 0, 0)


def at(seconds: float) -> str:
    return (BASE + timedelta(seconds=seconds)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def event(event_id, when, actor="actor-1", name="page_viewed", raw=None, client=None, click=False, bucket=None):
    msg = {
        "schema_version": 1,
        "type": "click" if click else "event",
        "message_key": event_id,
        "brand_id": "bbb_shop",
        "event_id": event_id,
        "event_name": "click" if click else name,
        "actor_id": actor,
        "client_id": client,
        "visitor_id": "visitor-x",
        "session_id": None,
        "occurred_at": at(when),
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


def closed_session_rows(store):
    return store.sessions


def session_row_field(row, index):
    return row[index]


# ---- pure rules ----

def test_decide_timing_no_cursor_is_new_session():
    assert decide_timing(None, datetime(2026, 1, 1), TIMEOUT) == NEW_SESSION


def test_decide_timing_within_timeout_continues():
    cur = {"last_event_at": datetime(2026, 1, 1, 0, 0, 0)}
    assert decide_timing(cur, datetime(2026, 1, 1, 0, 29, 0), TIMEOUT) == CONTINUE


def test_decide_timing_gap_above_timeout_starts_new_session():
    cur = {"last_event_at": datetime(2026, 1, 1, 0, 0, 0)}
    assert decide_timing(cur, datetime(2026, 1, 1, 0, 30, 1), TIMEOUT) == NEW_SESSION


def test_decide_timing_out_of_order_within_tolerance_continues_and_keeps_last():
    cur = {
        "actor_id": "a", "session_id": "s", "session_start": datetime(2026, 1, 1),
        "last_event_at": datetime(2026, 1, 1, 0, 10), "last_event_id": "e1",
        "events_seq": {"1": {"event_name": "page_viewed", "event_id": "e1"}},
    }
    msg = {"event_id": "e2", "event_name": "page_viewed"}
    nxt, closed, sid = apply_event(cur, msg, datetime(2026, 1, 1, 0, 9, 45), TIMEOUT, "a")
    assert closed is None and sid == "s"
    assert nxt["last_event_at"] == datetime(2026, 1, 1, 0, 10)


def test_decide_timing_more_than_30s_before_last_event_starts_new_session():
    cur = {"last_event_at": datetime(2026, 1, 1, 0, 10)}
    assert decide_timing(cur, datetime(2026, 1, 1, 0, 9, 29), TIMEOUT) == NEW_SESSION


def test_atc_product_id_rejects_synthetic_and_missing_ids():
    assert atc_product_id({"event_name": "product_added_to_cart", "raw": {"product_id": "123"}}) == "123"
    assert atc_product_id({"event_name": "product_added_to_cart", "raw": {"product_id": "SYNTH:x"}}) is None
    assert atc_product_id({"event_name": "product_added_to_cart", "raw": {"product_id": "FALLBACK:x"}}) is None
    assert atc_product_id({"event_name": "product_added_to_cart", "raw": {}}) is None
    assert atc_product_id({"event_name": "page_viewed", "raw": {"product_id": "123"}}) is None


# ---- flows through the in-memory store (duplicates, rollover, ATC) ----

def test_direct_event_message_is_consumed_and_gets_a_derived_session_id():  # 1, 3
    store = InMemoryIntentStore()
    counts = run(store, [event("e-1", 0)])
    assert counts["events"] == 1
    row = store.events["e-1"]
    assert row[5] is not None and len(row[5]) == 36  # session_id is a uuid, not null
    assert store.cursors["actor-1"]["session_id"] == row[5]


def test_direct_click_message_is_consumed(): # 2
    store = InMemoryIntentStore()
    counts = run(store, [event("c-1", 0, click=True)])
    assert counts["clicks"] == 1
    assert "c-1" in store.clicks


def test_existing_session_continues_and_events_seq_is_contiguous(): # 5, 9
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0), event("e-2", 60), event("e-3", 120, name="product_viewed", raw={"product_id": "9"})])
    seq = store.cursors["actor-1"]["events_seq"]
    assert list(seq.keys()) == ["1", "2", "3"]
    assert seq["3"]["event_name"] == "product_viewed"
    assert store.sessions == []  # open session is not written to intent_sessions


def test_rollover_closes_previous_session_with_end_and_duration(): # 6, 7, 8
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0), event("e-2", 100)])
    first_session = store.cursors["actor-1"]["session_id"]
    run(store, [event("e-3", 100 + TIMEOUT + 10)])  # gap > timeout

    assert store.cursors["actor-1"]["session_id"] != first_session
    assert len(store.sessions) == 1
    closed = store.sessions[0]
    assert closed[0] == first_session
    assert closed[5] == (BASE + timedelta(seconds=100)).replace(tzinfo=timezone.utc)  # session_end
    assert closed[6] == 100_000  # session_time_spent_ms = 100 s, timeout gap excluded


def test_events_seq_resets_for_new_session_and_closed_session_keeps_old_sequence(): # 9
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0), event("e-2", 10)])
    run(store, [event("e-3", 10 + TIMEOUT + 5)])
    assert list(store.cursors["actor-1"]["events_seq"].keys()) == ["1"]
    closed_seq = json.loads(store.sessions[0][16])
    assert list(closed_seq.keys()) == ["1", "2"]


def test_atc_is_deduped_per_session_on_the_consumer(): # 10
    store = InMemoryIntentStore()
    atc = lambda eid, when: event(eid, when, name="product_added_to_cart", raw={"product_id": "555"})
    counts = run(store, [atc("a-1", 0), atc("a-2", 30)])
    assert "a-1" in store.events and "a-2" not in store.events
    assert counts["atc_deduped"] == 1
    assert store.cursors["actor-1"]["events_seq"].keys() == {"1"}  # deduped ATC did not move the cursor


def test_atc_same_product_in_new_session_is_recorded_again(): # per-session dedupe semantics
    store = InMemoryIntentStore()
    atc = lambda eid, when: event(eid, when, name="product_added_to_cart", raw={"product_id": "555"})
    run(store, [atc("a-1", 0), atc("a-2", TIMEOUT + 60)])
    assert "a-1" in store.events and "a-2" in store.events


def test_duplicate_sqs_delivery_of_event_creates_no_second_row_or_session(): # 11
    store = InMemoryIntentStore()
    msgs = [event("e-1", 0), event("e-2", 20)]
    run(store, msgs)
    before_cursor = dict(store.cursors["actor-1"])
    counts = run(store, msgs)  # redelivery
    assert counts["events"] == 0 and counts["duplicates"] == 2
    assert len(store.events) == 2
    assert store.cursors["actor-1"]["session_id"] == before_cursor["session_id"]
    assert store.cursors["actor-1"]["events_seq"] == before_cursor["events_seq"]


def test_duplicate_click_delivery_creates_no_second_click_row(): # 12
    store = InMemoryIntentStore()
    run(store, [event("c-1", 0, click=True)])
    counts = run(store, [event("c-1", 0, click=True)])
    assert counts["clicks"] == 0 and counts["duplicates"] == 1
    assert len(store.clicks) == 1


def test_duplicate_delivery_after_rollover_does_not_close_session_twice(): # 11 (rollover case)
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0)])
    late = [event("e-2", TIMEOUT + 10)]
    run(store, late)
    run(store, late)
    assert len(store.sessions) == 1


def test_actorless_events_are_one_off_sessions_without_cursor_or_close():
    store = InMemoryIntentStore()
    run(store, [event("x-1", 0, actor=None, client=None), event("x-2", 5, actor=None, client=None)])
    assert len(store.events) == 2
    assert store.cursors == {}
    assert store.sessions == []
    sids = {store.events[k][5] for k in ("x-1", "x-2")}
    assert len(sids) == 2  # each one-off event gets its own session id


def test_client_id_is_used_when_actor_id_missing():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0, actor=None, client="client-9")])
    assert "client-9" in store.cursors
    assert store.events["e-1"][2] == "client-9"  # actor_id column carries the actor key


def test_visitor_id_is_never_the_actor_identity():
    store = InMemoryIntentStore()
    run(store, [event("e-1", 0, actor=None, client=None)])
    assert "visitor-x" not in store.cursors


def test_messages_are_applied_in_occurred_at_order_within_a_batch():
    store = InMemoryIntentStore()
    run(store, [event("e-late", 50), event("e-early", 0)])
    seq = store.cursors["actor-1"]["events_seq"]
    assert seq["1"]["event_id"] == "e-early"
    assert seq["2"]["event_id"] == "e-late"


# ---- contract compatibility and no Mongo / outbox dependency ----

def test_existing_v1_fixtures_still_parse(): # 17
    for name in ("event_v1.json", "click_v1.json", "session_snapshot_v1.json"):
        parse_message(json.dumps(json.loads((FIXTURES / name).read_text(encoding="utf-8"))))


def test_mysql_store_locks_the_cursor_row_for_update(): # 13/14 support
    class Cur:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append(sql)

        def fetchone(self):
            return None

    cur = Cur()
    assert MySqlIntentStore(cur).get_cursor_for_update("a") is None
    assert "FOR UPDATE" in cur.sql[-1]


def test_state_modules_do_not_reference_mongo_or_outbox(): # 15, 16
    for rel in (
        "pipeline/intent_session_state.py",
        "pipeline/intent_sqs_store.py",
        "pipeline/intent_sqs_writer.py",
        "workers/intent_sqs_worker.py",
    ):
        source = (ROOT / rel).read_text(encoding="utf-8")
        for banned in (
            "pymongo", "MongoClient", "intent_outbox",
            "intent_sessions.events", "intent_sessions.click_events", "intent_sessions.actor_cursors",
            "intent_sessions.session_history", "intent_sessions.intent_outbox", "slug_cache", "SlugCache",
        ):
            assert banned not in source, f"{rel} references {banned}"


class SchemaOkCursor:
    """Answers the read-only schema checks as if the migration had been applied.
    Overrides let a test describe a specific wrong schema."""
    rowcount = 0
    pk_rows = [("intent_actor_cursors", "actor_id"), ("intent_atc_dedupe", "session_id"), ("intent_atc_dedupe", "product_id")]
    source_type = [("datetime(6)",)]
    tables = [("intent_actor_cursors",), ("intent_atc_dedupe",)]

    def __init__(self):
        self.sql = []
        self._last = ""

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self._last = sql

    def fetchone(self):
        return ("testdb",) if "SELECT DATABASE()" in self._last else None

    def fetchall(self):
        if "information_schema.tables" in self._last:
            return list(self.tables)
        if "information_schema.statistics" in self._last:
            return list(self.pk_rows)
        if "information_schema.columns" in self._last:
            return list(self.source_type)
        return []

    def executemany(self, sql, rows):
        pass


def test_failed_transaction_propagates_so_the_worker_keeps_the_message(): # 13
    from pipeline.intent_sqs_store import reset_schema_verification
    from pipeline.intent_sqs_writer import apply_batch

    reset_schema_verification()

    class Boom(SchemaOkCursor):
        def execute(self, sql, params=None):
            if "INSERT INTO behavioral_events" in sql:
                raise RuntimeError("mysql down")
            super().execute(sql, params)

    class Conn:
        commits = 0

        def commit(self):
            Conn.commits += 1

    with pytest.raises(RuntimeError):
        apply_batch(Boom(), Conn(), parse([event("e-1", 0)]))
    assert Conn.commits == 0


# ---- correction 2: no per-batch DDL; schema verified once per database ----

def test_apply_batch_issues_no_ddl(): # correction 2
    from pipeline.intent_sqs_store import reset_schema_verification
    from pipeline.intent_sqs_writer import apply_batch

    reset_schema_verification()
    cur = SchemaOkCursor()

    class Conn:
        def commit(self):
            pass

    apply_batch(cur, Conn(), parse([event("e-1", 0), event("c-1", 5, click=True)]))
    apply_batch(cur, Conn(), parse([event("e-2", 10)]))
    ddl = [q for q in cur.sql if "CREATE TABLE" in q.upper() or "ALTER TABLE" in q.upper()]
    assert ddl == []


def test_schema_is_verified_once_per_database_not_per_batch(): # correction 2
    from pipeline.intent_sqs_store import reset_schema_verification
    from pipeline.intent_sqs_writer import apply_batch

    reset_schema_verification()
    cur = SchemaOkCursor()

    class Conn:
        def commit(self):
            pass

    apply_batch(cur, Conn(), parse([event("e-1", 0)]))
    checks_after_first = sum("information_schema" in q for q in cur.sql)
    apply_batch(cur, Conn(), parse([event("e-2", 10)]))
    apply_batch(cur, Conn(), parse([event("e-3", 20)]))
    checks_after_all = sum("information_schema" in q for q in cur.sql)
    assert checks_after_first == 3  # tables, primary keys, column type: read-only, once
    assert checks_after_all == checks_after_first


def test_missing_migration_refuses_to_write(): # correction 2
    from pipeline.intent_sqs_store import SchemaMissing, reset_schema_verification
    from pipeline.intent_sqs_writer import apply_batch

    reset_schema_verification()

    class NoTables(SchemaOkCursor):
        def fetchall(self):
            if "information_schema.tables" in self._last:
                return [("intent_actor_cursors",)]  # intent_atc_dedupe absent
            return super().fetchall()

    cur = NoTables()

    class Conn:
        commits = 0

        def commit(self):
            Conn.commits += 1

    with pytest.raises(SchemaMissing):
        apply_batch(cur, Conn(), parse([event("e-1", 0)]))
    assert not any("INSERT" in q for q in cur.sql)
    assert Conn.commits == 0


# ---- correction 1: deterministic actor lock order ----

def _multi_actor_messages(order):
    return [event(f"e-{a}", i * 10, actor=a) for i, a in enumerate(order)]


def test_actor_lock_order_is_sorted_regardless_of_batch_order(): # correction 1
    import itertools

    actors = ["delta", "alpha", "charlie", "bravo"]
    for perm in itertools.permutations(actors):
        store = InMemoryIntentStore()
        apply_messages(store, parse(_multi_actor_messages(perm)), TIMEOUT)
        assert store.locked_actors == sorted(actors), perm


def test_two_consumers_with_opposite_batch_orders_lock_in_the_same_sequence(): # correction 1
    a_first = InMemoryIntentStore()
    b_first = InMemoryIntentStore()
    apply_messages(a_first, parse(_multi_actor_messages(["zed", "mike", "amy"])), TIMEOUT)
    apply_messages(b_first, parse(_multi_actor_messages(["amy", "mike", "zed"])), TIMEOUT)
    assert a_first.locked_actors == b_first.locked_actors == ["amy", "mike", "zed"]


def test_threaded_consumers_with_overlapping_actors_do_not_deadlock(): # correction 1
    import threading

    shared_locks = {a: threading.Lock() for a in ["amy", "mike", "zed"]}

    class HoldingStore(InMemoryIntentStore):
        def __init__(self):
            super().__init__()
            self.held = []

        def get_cursor_for_update(self, actor_id):
            shared_locks[actor_id].acquire(timeout=5)
            self.held.append(actor_id)
            return super().get_cursor_for_update(actor_id)

        def release(self):
            for a in reversed(self.held):
                shared_locks[a].release()

    done = []

    def consume(order, label):
        store = HoldingStore()
        try:
            apply_messages(store, parse(_multi_actor_messages(order)), TIMEOUT)
            done.append(label)
        finally:
            store.release()

    t1 = threading.Thread(target=consume, args=(["zed", "amy", "mike"], "one"))
    t2 = threading.Thread(target=consume, args=(["mike", "zed", "amy"], "two"))
    t1.start()
    t2.start()
    t1.join(10)
    t2.join(10)
    assert not t1.is_alive() and not t2.is_alive(), "consumers deadlocked"
    assert sorted(done) == ["one", "two"]


# ---- correction 3: ATC product-id semantics ----

@pytest.mark.parametrize(
    "raw_product_id, expected",
    [
        ("gid://shopify/Product/9", "Product:9"),   # producer normalizeShopifyId output for a GID
        ("Product:9", "Product:9"),                 # already normalized: idempotent
        ("9000000000001", "9000000000001"),          # plain numeric id stays as-is
        ("SYNTH:abcdef0123456789", None),            # synthesized: never dedupes (old rule)
        ("FALLBACK:x", None),                        # fallback: never dedupes (old rule)
        ("", None),
    ],
)
def test_atc_product_id_matches_old_producer_semantics(raw_product_id, expected): # correction 3
    from pipeline.intent_session_state import atc_product_id

    msg = {"event_name": "product_added_to_cart", "raw": {"product_id": raw_product_id}}
    assert atc_product_id(msg) == expected


def test_gid_and_normalized_atc_are_deduped_as_the_same_product(): # correction 3 regression
    store = InMemoryIntentStore()
    run(store, [
        event("a-1", 0, name="product_added_to_cart", raw={"product_id": "gid://shopify/Product/9"}),
        event("a-2", 30, name="product_added_to_cart", raw={"product_id": "Product:9"}),
    ])
    assert "a-1" in store.events and "a-2" not in store.events


# ---- verifier: correct / missing / incorrect primary key / wrong type ----

def _verify_with(cursor_cls):
    from pipeline.intent_sqs_store import reset_schema_verification, verify_state_schema

    reset_schema_verification()
    verify_state_schema(cursor_cls())


def test_verifier_passes_on_correct_schema(): # verifier: correct schema
    _verify_with(SchemaOkCursor)


def test_verifier_fails_on_missing_table(): # verifier: missing table
    from pipeline.intent_sqs_store import SchemaMissing

    class Missing(SchemaOkCursor):
        tables = [("intent_actor_cursors",)]

    with pytest.raises(SchemaMissing, match="intent_atc_dedupe"):
        _verify_with(Missing)


def test_verifier_fails_on_incorrect_atc_primary_key(): # verifier: incorrect PK
    from pipeline.intent_sqs_store import SchemaMissing

    class BadAtcKey(SchemaOkCursor):
        pk_rows = [("intent_actor_cursors", "actor_id"), ("intent_atc_dedupe", "session_id")]

    with pytest.raises(SchemaMissing, match="intent_atc_dedupe primary key"):
        _verify_with(BadAtcKey)


def test_verifier_fails_on_incorrect_cursor_primary_key(): # verifier: incorrect PK
    from pipeline.intent_sqs_store import SchemaMissing

    class BadCursorKey(SchemaOkCursor):
        pk_rows = [
            ("intent_actor_cursors", "actor_id"), ("intent_actor_cursors", "session_id"),
            ("intent_atc_dedupe", "session_id"), ("intent_atc_dedupe", "product_id"),
        ]

    with pytest.raises(SchemaMissing, match="intent_actor_cursors primary key"):
        _verify_with(BadCursorKey)


def test_verifier_fails_on_wrong_source_updated_at_type(): # verifier: wrong type
    from pipeline.intent_sqs_store import SchemaMissing

    class WrongType(SchemaOkCursor):
        source_type = [("datetime",)]

    with pytest.raises(SchemaMissing, match=r"expected datetime\(6\)"):
        _verify_with(WrongType)
