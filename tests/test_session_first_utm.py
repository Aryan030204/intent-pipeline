"""intent_sessions.first_utm_source: recorded at session close, optional column, backfill script."""
import importlib.util
import os
from datetime import datetime

from pipeline import intent_kafka_store as store_mod
from pipeline.intent_kafka_store import (
    FIRST_UTM_COLUMN,
    SESSION_UPSERT_SQL,
    SESSION_UPSERT_SQL_NO_FIRST_UTM,
    MySqlIntentStore,
    first_utm_column_present,
)
from pipeline.intent_kafka_writer import _closed_session_row
from pipeline.intent_session_state import ActorCursor, closed_session_doc, first_utm_source, seq_entry
from pipeline.intent_events import _INTENT_SESSIONS_UPSERT_COLUMNS

T0 = datetime(2026, 10, 5, 10, 30, 0)


class Msg:
    """The few IntentMessage fields seq_entry reads."""

    def __init__(self, event_name="page_viewed", event_id="e1", url=None):
        self.event_name = event_name
        self.event_id = event_id
        self.click_bucket = None
        self.client_id = "c1"
        self.visitor_id = "v1"
        self.url = url


def cursor_with(events_seq):
    return ActorCursor("actor-1", "sess-1", T0, T0, "e1", events_seq)


# ---- recording the source ---------------------------------------------------------------------------------------
def test_a_step_entry_carries_the_utm_source_of_its_page_url():
    assert seq_entry(Msg(url="https://shop.example/?utm_source=Google&utm_medium=cpc"))["utm_source"] == "Google"
    assert seq_entry(Msg(url="https://shop.example/products/a"))["utm_source"] is None
    assert seq_entry(Msg(url=None))["utm_source"] is None


def test_the_first_event_with_a_source_wins_even_if_later_steps_have_other_sources():
    seq = {
        "1": {"event_name": "page_viewed", "utm_source": None},
        "2": {"event_name": "page_viewed", "utm_source": "facebook"},
        "3": {"event_name": "click", "utm_source": "google"},
    }
    assert first_utm_source(seq) == "facebook"


def test_steps_are_read_in_numeric_order_and_blank_or_missing_values_are_skipped():
    seq = {"10": {"utm_source": "late"}, "2": {"utm_source": "  "}, "9": {"utm_source": "early"}, "x": {"utm_source": "bad"}}
    assert first_utm_source(seq) == "early"
    assert first_utm_source({}) is None
    assert first_utm_source(None) is None
    assert first_utm_source({"1": {"event_name": "click"}}) is None  # a session recorded before utm_source existed


def test_a_long_source_is_cut_to_the_column_width():
    assert len(first_utm_source({"1": {"utm_source": "x" * 400}})) == 255


# ---- the closed-session row -------------------------------------------------------------------------------------
def test_the_closed_session_row_ends_with_the_first_source_after_the_columns():
    doc = closed_session_doc(cursor_with({
        "1": {"event_name": "page_viewed", "event_id": "e1", "utm_source": None},
        "2": {"event_name": "product_viewed", "event_id": "e2", "utm_source": "google"},
    }))
    row = _closed_session_row(doc)

    assert len(row) == len(_INTENT_SESSIONS_UPSERT_COLUMNS) + 1
    assert row[-1] == "google"
    assert row[0] == "sess-1"          # still the session id first, the columns in the same order as before


def test_a_session_without_any_source_has_none_as_its_last_value():
    row = _closed_session_row(closed_session_doc(cursor_with({"1": {"event_name": "page_viewed", "event_id": "e1"}})))
    assert row[-1] is None and len(row) == len(_INTENT_SESSIONS_UPSERT_COLUMNS) + 1


# ---- the store ----------------------------------------------------------------------------------------------------
class RecordingCursor:
    def __init__(self, rows=None):
        self.calls = []
        self.rows = rows or []

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self.rows


def a_row(first="google"):
    return tuple(f"v{i}" for i in range(len(_INTENT_SESSIONS_UPSERT_COLUMNS))) + (first,)


def test_the_statement_names_the_column_and_keeps_an_existing_value_when_the_new_one_is_null():
    assert "first_utm_source" in SESSION_UPSERT_SQL.split("VALUES")[0]
    assert "first_utm_source = COALESCE(VALUES(first_utm_source), first_utm_source)" in SESSION_UPSERT_SQL
    assert "first_utm_source" not in SESSION_UPSERT_SQL_NO_FIRST_UTM


def test_a_database_with_the_column_gets_the_whole_row():
    cursor = RecordingCursor()
    MySqlIntentStore(cursor, first_utm=True).upsert_session(a_row())
    sql, params = cursor.calls[0]
    assert sql.count("%s") == len(_INTENT_SESSIONS_UPSERT_COLUMNS) + 1
    assert params == a_row()


def test_a_database_without_the_column_is_written_without_the_last_value():
    cursor = RecordingCursor()
    MySqlIntentStore(cursor, first_utm=False).upsert_session(a_row())
    sql, params = cursor.calls[0]
    assert "first_utm_source" not in sql
    assert params == a_row()[:-1]
    assert sql.count("%s") == len(params)


def test_bulk_writes_follow_the_same_rule():
    for first_utm in (True, False):
        cursor = RecordingCursor()
        MySqlIntentStore(cursor, first_utm=first_utm).upsert_sessions([a_row(), a_row("meta")])
        sql, params = cursor.calls[0]
        width = len(_INTENT_SESSIONS_UPSERT_COLUMNS) + (1 if first_utm else 0)
        assert len(params) == 2 * width
        assert ("first_utm_source" in sql) is first_utm


def test_the_column_check_reads_information_schema():
    assert first_utm_column_present(RecordingCursor(rows=[{"n": 1}])) is True
    assert first_utm_column_present(RecordingCursor(rows=[{"n": 0}])) is False


def test_the_required_columns_do_not_include_it_so_a_pending_migration_never_stops_ingestion():
    assert FIRST_UTM_COLUMN not in store_mod.REQUIRED_COLUMNS["intent_sessions"]


# ---- the backfill script -----------------------------------------------------------------------------------------
def load_script():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "backfill_first_utm.py")
    spec = importlib.util.spec_from_file_location("backfill_first_utm", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ScriptCursor:
    """intent_sessions rows: (id, session_id, first_utm); behavioral_events rows: (session_id, utm_source) in time order."""

    def __init__(self, sessions, events):
        self.sessions, self.events = sessions, events
        self.updates = []
        self._rows = []

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        if sql.startswith("SELECT MIN(id)"):
            ids = [s[0] for s in self.sessions]
            self._rows = [{"lo": min(ids) if ids else None, "hi": max(ids) if ids else None}]
        elif sql.startswith("SELECT session_id FROM intent_sessions"):
            low, high = params[0], params[1]
            self._rows = [{"session_id": s[1]} for s in self.sessions if low <= s[0] <= high and s[2] is None]
        elif sql.startswith("SELECT session_id, utm_source FROM behavioral_events"):
            wanted = set(params)
            self._rows = [{"session_id": e[0], "utm_source": e[1]} for e in self.events if e[0] in wanted]
        elif sql.startswith("UPDATE intent_sessions"):
            self.updates.append(params)
            pairs = list(zip(params[0::2], params[1::2]))
            for session_id, value in pairs:
                for i, s in enumerate(self.sessions):
                    if s[1] == session_id and s[2] is None:
                        self.sessions[i] = (s[0], s[1], value)
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return self._rows


class Conn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def test_the_backfill_fills_only_sessions_without_a_source_with_their_first_event_source():
    script = load_script()
    cursor = ScriptCursor(
        sessions=[(1, "s1", None), (2, "s2", None), (3, "s3", "already"), (4, "s4", None)],
        events=[("s1", "google"), ("s1", "meta"), ("s2", "meta"), ("s3", "other")],
    )
    conn = Conn()
    stats = script.backfill(cursor, conn, datetime(2026, 10, 1), datetime(2026, 10, 2), batch=2, sleep_s=0, dry_run=False, max_rows=None)

    assert {s[1]: s[2] for s in cursor.sessions} == {"s1": "google", "s2": "meta", "s3": "already", "s4": None}
    assert stats["updated"] == 2 and stats["sessions"] == 3   # s4 has no events with a source: stays NULL
    assert conn.commits == 1   # only the batch that had something to write is committed


def test_a_dry_run_writes_nothing_and_max_rows_stops_early():
    script = load_script()
    sessions = [(1, "s1", None), (2, "s2", None), (3, "s3", None)]
    events = [("s1", "a"), ("s2", "b"), ("s3", "c")]

    cursor = ScriptCursor(list(sessions), events)
    dry = script.backfill(cursor, Conn(), datetime(2026, 10, 1), datetime(2026, 10, 2), batch=10, sleep_s=0, dry_run=True, max_rows=None)
    assert cursor.updates == [] and dry["updated"] == 3

    cursor = ScriptCursor(list(sessions), events)
    limited = script.backfill(cursor, Conn(), datetime(2026, 10, 1), datetime(2026, 10, 2), batch=1, sleep_s=0, dry_run=False, max_rows=2)
    assert limited["updated"] == 2


def test_the_script_refuses_bad_arguments():
    script = load_script()
    for argv in (["--brand", "BBB", "--from", "2026-10-05", "--to", "2026-10-01"], ["--brand", "BBB", "--from", "2026-10-01", "--to", "2026-10-02", "--batch", "0"]):
        try:
            script.parse_args(argv)
        except SystemExit:
            continue
        raise AssertionError(f"accepted {argv}")
