"""Real-MySQL tests (skipped unless INTENT_TEST_MYSQL_HOST is set; see conftest.py).

These exist because fakes cannot show how mysql-connector behaves: rowcount for duplicate
inserts, unread results on an unbuffered cursor, savepoint rollback, lock waits, JSON and
DATETIME round trips."""

import json
import threading

import pytest

from pipeline.intent_kafka_db import BrandConnections
from pipeline.intent_kafka_store import (
    MySqlIntentStore,
    SchemaMissing,
    apply_migration,
    assert_rowcount_semantics,
    verify_schema,
)
from pipeline.intent_kafka_writer import apply_batch, is_data_error
from tests.conftest import requires_mysql
from tests.helpers import atc, items, stamp, value

pytestmark = requires_mysql
TIMEOUT = 1800


def transact(db, *messages):
    """One brand transaction exactly as the consumer runs it (buffered cursor by default)."""
    connections = BrandConnections(connection_factory=db.factory)
    try:
        with connections.transaction(1) as store:
            return apply_batch(store, items(*messages), TIMEOUT)
    finally:
        connections.close()


def table(db, name):
    return db.rows(f"SELECT * FROM {name} ORDER BY 1")


def _seq(cursor_row):
    seq = cursor_row["events_seq"]
    return json.loads(seq) if isinstance(seq, (str, bytes)) else seq


# ---------------- migration and schema verification ----------------

def test_migration_is_idempotent_and_the_schema_verifies(mysql_db):
    connection = mysql_db.connect()
    try:
        cursor = connection.cursor(buffered=True)
        apply_migration(cursor, connection)
        apply_migration(cursor, connection)   # second run: no error, nothing changes
        verify_schema(cursor)
    finally:
        connection.close()


def test_state_tables_have_the_expected_columns_types_and_primary_keys(mysql_db):
    columns = {
        (r["table_name"], r["column_name"]): (r["column_type"], r["is_nullable"])
        for r in mysql_db.rows(
            "SELECT table_name AS table_name, column_name AS column_name, column_type AS column_type, is_nullable AS is_nullable FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name IN ('intent_actor_cursors', 'intent_atc_dedupe')"
        )
    }
    assert columns[("intent_actor_cursors", "actor_id")] == ("varchar(100)", "NO")
    assert columns[("intent_actor_cursors", "session_start")] == ("datetime(6)", "NO")
    assert columns[("intent_actor_cursors", "last_event_at")] == ("datetime(6)", "NO")
    assert columns[("intent_actor_cursors", "events_seq")][0] == "json"
    assert columns[("intent_atc_dedupe", "product_id")] == ("varchar(100)", "NO")
    pks = mysql_db.rows(
        "SELECT table_name AS table_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS cols "
        "FROM information_schema.statistics WHERE table_schema = DATABASE() AND index_name = 'PRIMARY' "
        "AND table_name IN ('intent_actor_cursors', 'intent_atc_dedupe') GROUP BY table_name"
    )
    assert {r["table_name"]: r["cols"] for r in pks} == {
        "intent_actor_cursors": "actor_id", "intent_atc_dedupe": "session_id,product_id"}


def test_existing_intent_tables_keep_their_unique_keys(mysql_db):
    keys = {
        (r["table_name"], r["column_name"])
        for r in mysql_db.rows(
            "SELECT table_name AS table_name, column_name AS column_name FROM information_schema.statistics WHERE table_schema = DATABASE() "
            "AND non_unique = 0 AND index_name <> 'PRIMARY'"
        )
    }
    assert {("behavioral_events", "event_id"), ("click_events", "event_id"), ("intent_sessions", "session_id")} <= keys


def test_verifier_names_a_missing_state_table(mysql_db):
    mysql_db.execute("CREATE TABLE verify_probe (id INT)")  # unrelated table in the same database
    mysql_db.execute("DROP TABLE intent_atc_dedupe")
    try:
        connection = mysql_db.connect()
        try:
            with pytest.raises(SchemaMissing, match="intent_atc_dedupe"):
                verify_schema(connection.cursor(buffered=True))
        finally:
            connection.close()
    finally:
        mysql_db.execute("DROP TABLE verify_probe")
        connection = mysql_db.connect()
        apply_migration(connection.cursor(buffered=True), connection)
        connection.close()


def test_verifier_rejects_a_missing_unique_key_and_a_wrong_primary_key(mysql_db):
    mysql_db.execute("ALTER TABLE behavioral_events DROP INDEX uq_event_id")
    connection = mysql_db.connect()
    try:
        with pytest.raises(SchemaMissing, match="no unique key"):
            verify_schema(connection.cursor(buffered=True))
    finally:
        connection.close()
        mysql_db.execute("ALTER TABLE behavioral_events ADD UNIQUE KEY uq_event_id (event_id)")

    mysql_db.execute("ALTER TABLE intent_atc_dedupe DROP PRIMARY KEY, ADD PRIMARY KEY (session_id)")
    connection = mysql_db.connect()
    try:
        with pytest.raises(SchemaMissing, match="primary key"):
            verify_schema(connection.cursor(buffered=True))
    finally:
        connection.close()
        mysql_db.execute("ALTER TABLE intent_atc_dedupe DROP PRIMARY KEY, ADD PRIMARY KEY (session_id, product_id)")


def test_verifier_rejects_a_missing_column(mysql_db):
    mysql_db.execute("ALTER TABLE click_events DROP COLUMN click_bucket")
    connection = mysql_db.connect()
    try:
        with pytest.raises(SchemaMissing, match="click_events.click_bucket"):
            verify_schema(connection.cursor(buffered=True))
    finally:
        connection.close()
        mysql_db.execute("ALTER TABLE click_events ADD COLUMN click_bucket ENUM('useful_click','dead_click') NULL")


# ---------------- driver behaviour the code relies on ----------------

def test_duplicate_insert_reports_zero_rows_and_new_insert_reports_one(clean_db):
    connection = clean_db.connect()
    try:
        assert_rowcount_semantics(connection)
        store = MySqlIntentStore(connection.cursor(dictionary=True, buffered=True))
        row = ("sh-1",) + (None,) * 19
        row = list(row)
        row[1], row[19] = "page_viewed", stamp(0)[:-1].replace("T", " ")
        assert store.insert_event("event", tuple(row)) is True
        assert store.insert_event("event", tuple(row)) is False     # driver reports 0 for the no-op duplicate
        assert store.claim_atc("s1", "Product:1") is True
        assert store.claim_atc("s1", "Product:1") is False
        assert store.claim_atc("s2", "Product:1") is True
        connection.rollback()
    finally:
        connection.close()


def test_a_connection_with_found_rows_is_refused_because_duplicates_would_look_new(mysql_db):
    from mysql.connector.constants import ClientFlag

    connection = mysql_db.connect(client_flags=[ClientFlag.FOUND_ROWS])
    try:
        with pytest.raises(SchemaMissing, match="FOUND_ROWS"):
            assert_rowcount_semantics(connection)
    finally:
        connection.close()


def test_unread_result_regression_every_select_is_consumed_even_on_an_unbuffered_cursor(clean_db):
    """The production bug was 'InternalError: Unread result found'. An unbuffered cursor is the
    strictest case: a SELECT left unread makes the next statement raise. A whole multi-actor
    batch (cursor reads FOR UPDATE, ATC claims, inserts, rollover) must run on one."""
    import mysql.connector

    connection = clean_db.connect()
    try:
        unbuffered = connection.cursor(dictionary=True)
        # The sensitivity check: prove this cursor type really does raise on an unread SELECT.
        unbuffered.execute("SELECT 1 AS one UNION SELECT 2")
        with pytest.raises(mysql.connector.errors.InternalError, match="Unread result"):
            unbuffered.execute("SELECT 3")
        unbuffered.fetchall()
        connection.rollback()

        store = MySqlIntentStore(connection.cursor(dictionary=True))
        result = apply_batch(store, items(
            value("a1", 0, actor="u1"), value("b1", 1, actor="u2"), atc("a2", 2, actor="u1"),
            value("a3", TIMEOUT + 60, actor="u1"), value("c1", 3, click=True, actor="u3"),
        ), TIMEOUT)
        connection.commit()
    finally:
        connection.close()
    assert result.applied == 5 and result.sessions_closed == 1 and result.failed == {}


# ---------------- transactions ----------------

def test_a_committed_batch_persists_events_clicks_cursor_and_closed_session(clean_db):
    result = transact(clean_db, value("e1", 0), value("c1", 5, click=True), value("e2", TIMEOUT + 10))
    assert result.applied == 3 and result.sessions_closed == 1
    assert [r["event_id"] for r in table(clean_db, "behavioral_events")] == ["e1", "e2"]
    assert [r["event_id"] for r in table(clean_db, "click_events")] == ["c1"]
    (cursor,) = table(clean_db, "intent_actor_cursors")
    assert cursor["actor_id"] == "cid-1" and cursor["last_event_id"] == "e2"
    (session,) = table(clean_db, "intent_sessions")
    assert session["actor_id"] == "cid-1" and session["event_count"] == 2 and session["click_count"] == 1
    assert session["session_time_spent_ms"] == 5000
    assert session["session_start"].isoformat() == "2026-10-05T10:30:00"
    assert session["session_end"].isoformat() == "2026-10-05T10:30:05"


def test_timestamps_are_stored_as_the_exact_store_local_wall_clock(clean_db):
    transact(clean_db, {**value("e1"), "occurred_at": "2026-10-05T10:30:00.123Z"})
    (row,) = table(clean_db, "behavioral_events")
    assert row["occurred_at"].isoformat() == "2026-10-05T10:30:00.123000"   # no timezone shift, microseconds kept
    (cursor,) = table(clean_db, "intent_actor_cursors")
    assert cursor["session_start"].isoformat() == "2026-10-05T10:30:00.123000"


def test_the_event_row_carries_the_extracted_fields(clean_db):
    transact(clean_db, atc("atc-1", 0, product="Product:42"))
    (row,) = table(clean_db, "behavioral_events")
    assert row["event_id"] == "atc-1" and row["event_name"] == "product_added_to_cart"
    assert row["product_id"] == "Product:42" and row["quantity"] == 1
    assert row["actor_id"] == "cid-1" and row["client_id"] == "cid-1" and row["visitor_id"] == "vis-1"
    assert row["session_id"] == table(clean_db, "intent_actor_cursors")[0]["session_id"]


def test_an_exception_rolls_the_whole_transaction_back(clean_db):
    connections = BrandConnections(connection_factory=clean_db.factory)
    try:
        with pytest.raises(RuntimeError, match="boom"):
            with connections.transaction(1) as store:
                apply_batch(store, items(value("e1", 0), value("e2", 5, actor="u2")), TIMEOUT)
                raise RuntimeError("boom")
    finally:
        connections.close()
    for name in ("behavioral_events", "intent_actor_cursors"):
        assert table(clean_db, name) == []


def test_redelivering_committed_events_changes_nothing(clean_db):
    batch = [value("e1", 0), value("e2", 20), atc("a1", 30), value("c1", 40, click=True)]
    transact(clean_db, *batch)
    snapshot = {t: table(clean_db, t) for t in ("behavioral_events", "click_events", "intent_actor_cursors", "intent_atc_dedupe")}
    again = transact(clean_db, *batch)
    assert again.applied == 0
    assert {t: table(clean_db, t) for t in snapshot} == snapshot


def test_atc_dedupe_is_enforced_by_the_database(clean_db):
    transact(clean_db, atc("a1", 0))
    result = transact(clean_db, atc("a2", 10))   # same product, same session
    assert result.atc_deduped == 1
    assert [r["event_id"] for r in table(clean_db, "behavioral_events")] == ["a1"]
    assert len(table(clean_db, "intent_atc_dedupe")) == 1


def test_a_row_mysql_rejects_is_isolated_by_a_savepoint_and_neighbours_commit(clean_db):
    """product_title is VARCHAR(500); 600 characters is a real strict-mode data error (1406)."""
    bad = value("bad", 5, name="product_viewed", raw={"product_id": "p", "product_title": "x" * 600})
    result = transact(clean_db, value("ok1", 0), bad, value("ok2", 9))
    assert [r["event_id"] for r in table(clean_db, "behavioral_events")] == ["ok1", "ok2"]
    assert len(result.failed) == 1
    (cursor,) = table(clean_db, "intent_actor_cursors")
    assert list(_seq(cursor)) == ["1", "2"]


def test_driver_error_classification_on_real_errors(clean_db):
    import mysql.connector

    connection = clean_db.connect()
    try:
        cursor = connection.cursor(buffered=True)
        with pytest.raises(mysql.connector.Error) as too_long:
            cursor.execute("INSERT INTO behavioral_events (event_id, event_name, occurred_at, product_title) VALUES ('x','n',NOW(),%s)", ("y" * 600,))
        assert is_data_error(too_long.value)
        with pytest.raises(mysql.connector.Error) as missing:
            cursor.execute("SELECT * FROM table_that_does_not_exist")
        assert not is_data_error(missing.value)   # a schema problem must block and alert, not quarantine
    finally:
        connection.close()


# ---------------- connection handling ----------------

def test_the_connection_is_reused_across_batches_and_recovers_after_it_is_killed(clean_db):
    connections = BrandConnections(connection_factory=clean_db.factory)
    try:
        def connection_id():
            with connections.transaction(1) as store:
                store.cursor.execute("SELECT CONNECTION_ID() AS id")
                return store.cursor.fetchall()[0]["id"]

        first = connection_id()
        assert connection_id() == first                      # reused, no reconnect per batch
        clean_db.execute(f"KILL {first}")
        second = connection_id()                             # ping fails, a new connection is opened
        assert second != first
    finally:
        connections.close()


def test_a_commit_failure_leaves_no_rows_and_the_provider_recovers(clean_db):
    connections = BrandConnections(connection_factory=clean_db.factory)
    try:
        with pytest.raises(Exception):
            with connections.transaction(1) as store:
                apply_batch(store, items(value("e1", 0)), TIMEOUT)
                store.cursor.execute("SELECT CONNECTION_ID() AS id")
                clean_db.execute(f"KILL {store.cursor.fetchall()[0]['id']}")   # dies before COMMIT
        assert table(clean_db, "behavioral_events") == []
        with connections.transaction(1) as store:
            apply_batch(store, items(value("e1", 0)), TIMEOUT)
        assert [r["event_id"] for r in table(clean_db, "behavioral_events")] == ["e1"]
    finally:
        connections.close()


# ---------------- concurrency: two consumers on one actor ----------------

def test_concurrent_consumers_on_one_actor_lose_no_event_and_keep_one_cursor(clean_db):
    """Models a rebalance overlap: two connections process batches for the same new actor at
    once. FOR UPDATE serialises them; a deadlock on the first-event race is a retryable error
    that the consumer would retry, so the test retries it the same way."""
    errors = []

    def worker(offset):
        connections = BrandConnections(connection_factory=clean_db.factory)
        try:
            for n in range(10):
                event = value(f"t{offset}-{n}", n * 2 + offset, actor="shared")   # all within the 30 s tolerance: one session
                for attempt in range(5):
                    try:
                        with connections.transaction(1) as store:
                            apply_batch(store, items(event), 10 ** 9)
                        break
                    except Exception as exc:  # deadlock / lock wait: the consumer retries these
                        if getattr(exc, "errno", None) not in (1213, 1205) or attempt == 4:
                            errors.append(exc)
                            return
        finally:
            connections.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    (cursor,) = table(clean_db, "intent_actor_cursors")
    seq = _seq(cursor)
    ids = [step["event_id"] for step in seq.values()]
    assert len(ids) == 20 and len(set(ids)) == 20                       # nothing lost, nothing twice
    assert len(table(clean_db, "behavioral_events")) == 20
    assert sorted(seq, key=int) == [str(i) for i in range(1, 21)]       # contiguous steps


# ---------------- first-event races (found by running the real worker with two consumers) ----------------

def _race(db, rounds, actors_for_round):
    """Runs `rounds` rounds. In each round every thread applies one event for its actor at the same
    instant (a Barrier), through its own connection, with NO retry: any error is recorded."""
    errors = []
    names = actors_for_round(0)
    barrier = threading.Barrier(len(names))

    def run(slot):
        connections = BrandConnections(connection_factory=db.factory)
        try:
            for r in range(rounds):
                actor = actors_for_round(r)[slot]
                barrier.wait(timeout=30)
                try:
                    with connections.transaction(1) as store:
                        apply_batch(store, items(value(f"r{r}-s{slot}", slot, actor=actor, client=actor)), 10 ** 9)
                except Exception as exc:
                    errors.append((r, slot, getattr(exc, "errno", None), str(exc)[:80]))
        finally:
            connections.close()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(len(names))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def test_consumers_meeting_different_new_actors_at_once_never_deadlock(clean_db):
    """The production-shaped case: two consumers, different partitions, a new visitor each, at the
    same moment. SELECT ... FOR UPDATE on a missing cursor row takes a gap lock under REPEATABLE
    READ, and the following INSERTs then deadlock each other (error 1213)."""
    errors = _race(clean_db, rounds=25, actors_for_round=lambda r: [f"new-{r}-a", f"new-{r}-b", f"new-{r}-c"])
    assert errors == []
    assert len(table(clean_db, "intent_actor_cursors")) == 75 and len(table(clean_db, "behavioral_events")) == 75


def test_consumers_meeting_the_same_new_actor_at_once_serialise_without_losing_an_event(clean_db):
    """A rebalance overlap on a brand-new actor: both read 'no cursor'. The second must wait for
    the first and then continue from its state, not overwrite it."""
    errors = _race(clean_db, rounds=20, actors_for_round=lambda r: [f"same-{r}", f"same-{r}"])
    assert errors == []
    cursors = table(clean_db, "intent_actor_cursors")
    assert len(cursors) == 20
    for cursor in cursors:
        assert len(_seq(cursor)) == 2                       # both events are in the one open session
    assert len(table(clean_db, "behavioral_events")) == 40
    assert len({r["session_id"] for r in table(clean_db, "behavioral_events")}) == 20   # one session per actor


def test_a_claimed_placeholder_row_is_not_a_cursor_and_becomes_one_on_the_first_event(clean_db):
    connection = clean_db.connect()
    try:
        store = MySqlIntentStore(connection.cursor(dictionary=True, buffered=True))
        assert store.get_cursor_for_update("fresh") is None       # claims the row, reads it back as "no cursor"
        connection.commit()
    finally:
        connection.close()
    (placeholder,) = table(clean_db, "intent_actor_cursors")
    assert placeholder["actor_id"] == "fresh" and placeholder["session_id"] == ""

    result = transact(clean_db, value("e1", 0, actor="fresh", client="fresh"))
    assert result.applied == 1 and result.sessions_closed == 0    # no phantom 1970 session was closed
    (cursor,) = table(clean_db, "intent_actor_cursors")
    assert cursor["session_id"] != "" and cursor["last_event_id"] == "e1"
    assert table(clean_db, "intent_sessions") == []


def test_a_duplicate_only_batch_leaves_no_session_state_behind(clean_db):
    transact(clean_db, value("e1", 0))
    transact(clean_db, value("e1", 0))                            # redelivery of the same event
    (cursor,) = table(clean_db, "intent_actor_cursors")
    assert list(_seq(cursor)) == ["1"] and cursor["last_event_id"] == "e1"


# ---------------- timezone semantics against the real rollups ----------------

def test_session_times_land_in_the_day_bucket_the_rollups_and_the_event_rows_use(clean_db):
    """The consumer stores the store-local wall clock unchanged. The rollups bound intent_sessions.session_start
    with IST datetimes, so the day a session is counted on must equal the day of its own events
    (DATE(occurred_at), also store-local). Actor B's session starts at 00:10 store-local on 6 Oct, which is
    18:40 UTC on 5 Oct: it must be counted on the 6th."""
    from datetime import date

    from pipeline.rollups import _ensure_all_rollup_tables, _rollup_intent_daily_summary

    hour = 3600
    a_start, b_start = 13 * hour + 20 * 60, 13 * hour + 40 * 60   # 23:50 on 5 Oct and 00:10 on 6 Oct (BASE is 10:30)
    transact(clean_db,
             value("a1", a_start, actor="A", client="A"), value("a2", a_start + 60, actor="A", client="A"),
             value("b1", b_start, actor="B", client="B"), value("b2", b_start + 60, actor="B", client="B"))
    # a later event of each actor, beyond the 30 minute timeout, closes the sessions
    transact(clean_db, value("a3", a_start + 3 * hour, actor="A", client="A"), value("b3", b_start + 3 * hour, actor="B", client="B"))

    days = {r["event_id"]: r["day"] for r in clean_db.rows("SELECT event_id, DATE(occurred_at) AS day FROM behavioral_events")}
    assert days["a1"] == date(2026, 10, 5) and days["b1"] == date(2026, 10, 6)

    sessions = {r["actor_id"]: r for r in clean_db.rows("SELECT actor_id, session_start, session_end FROM intent_sessions")}
    assert sessions["A"]["session_start"].isoformat() == "2026-10-05T23:50:00"
    assert sessions["B"]["session_start"].isoformat() == "2026-10-06T00:10:00"

    connection = clean_db.connect()
    try:
        cursor = connection.cursor(dictionary=True, buffered=True)
        _ensure_all_rollup_tables(cursor, connection)
        _rollup_intent_daily_summary(cursor, connection, [date(2026, 10, 5), date(2026, 10, 6)])
        summary = {r["summary_date"]: r["sessions"] for r in
                   (lambda c: (c.execute("SELECT summary_date, sessions FROM intent_daily_summary") or c.fetchall()))(cursor)}
    finally:
        connection.close()
    assert summary == {date(2026, 10, 5): 1, date(2026, 10, 6): 1}   # one session per store-local day, none shifted
