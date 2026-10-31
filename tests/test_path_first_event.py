"""behavioral_path_daily.first_event: set up in place, backfilled in batches, written by the rollup."""
from datetime import date, datetime, timedelta

from pipeline import rollups
from pipeline.state import IST

DAY = date(2026, 10, 29)


class Conn:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class Cursor:
    """Scripted: answers SHOW COLUMNS / SHOW INDEX from `has_*`, UPDATE row counts from `update_counts`, session rows
    for the path stage from `sessions`, and records every statement."""

    def __init__(self, has_column=True, has_index=True, update_counts=(0,), sessions=(), fail_on=None):
        self.has_column, self.has_index = has_column, has_index
        self.update_counts = list(update_counts)
        self.sessions = list(sessions)
        self.fail_on = fail_on
        self.calls = []
        self.rowcount = 0
        self._rows = []
        self._one = None

    def execute(self, sql, params=None):
        text = " ".join(sql.split())
        self.calls.append((text, params))
        if self.fail_on and self.fail_on in text:
            raise RuntimeError("boom")
        self._rows, self._one = [], None
        if text.startswith("SHOW COLUMNS"):
            self._one = {"Field": "first_event"} if self.has_column else None
        elif text.startswith("SHOW INDEX"):
            self._one = {"Key_name": "idx_first_event_date"} if self.has_index else None
        elif text.startswith("UPDATE behavioral_path_daily"):
            self.rowcount = self.update_counts.pop(0) if self.update_counts else 0
        elif "FROM intent_sessions" in text:
            self._rows = self.sessions
        self.last = text

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._rows

    def executemany(self, sql, rows):
        self.calls.append((" ".join(sql.split()), list(rows)))
        self.rowcount = len(rows)


def statements(cursor, prefix):
    return [c for c in cursor.calls if c[0].startswith(prefix)]


# ---- setup of an existing table ------------------------------------------------------------------------------------
def test_the_column_and_index_are_added_when_missing_and_left_alone_when_present():
    cursor, conn = Cursor(has_column=False, has_index=False), Conn()
    rollups._ensure_path_first_event(cursor, conn)
    alters = [c[0] for c in statements(cursor, "ALTER TABLE")]
    assert any("ADD COLUMN first_event VARCHAR(100) NULL" in a for a in alters)
    assert any("ADD KEY idx_first_event_date (first_event, summary_date)" in a for a in alters)

    cursor = Cursor(has_column=True, has_index=True)
    rollups._ensure_path_first_event(cursor, Conn())
    assert statements(cursor, "ALTER TABLE") == []


def test_old_rows_are_filled_in_batches_until_none_are_left():
    batch = rollups.PATH_FIRST_EVENT_BACKFILL_BATCH
    cursor, conn = Cursor(update_counts=[batch, batch, 120]), Conn()
    filled = rollups._backfill_path_first_event(cursor, conn)

    updates = statements(cursor, "UPDATE behavioral_path_daily")
    assert len(updates) == 3 and filled == 2 * batch + 120
    assert "WHERE first_event IS NULL LIMIT" in updates[0][0]
    assert "LEFT(SUBSTRING_INDEX(sequence, '>', 1), %s)" in updates[0][0]
    assert conn.commits >= 3   # each batch is committed on its own


def test_the_backfill_stops_after_the_maximum_number_of_batches():
    batch = rollups.PATH_FIRST_EVENT_BACKFILL_BATCH
    cursor = Cursor(update_counts=[batch] * 1000)
    rollups._backfill_path_first_event(cursor, Conn())
    assert len(statements(cursor, "UPDATE behavioral_path_daily")) == rollups.PATH_FIRST_EVENT_BACKFILL_MAX_BATCHES


def test_a_failure_in_the_setup_never_stops_the_rollup():
    cursor, conn = Cursor(has_column=False, fail_on="ADD COLUMN"), Conn()
    rollups._ensure_path_first_event(cursor, conn)   # does not raise
    assert conn.rollbacks == 1


def test_it_is_part_of_the_table_setup():
    cursor = Cursor()
    rollups._ensure_path_first_event(cursor, Conn())
    assert any(c[0].startswith("SHOW COLUMNS") for c in cursor.calls)


# ---- the first event of a path -------------------------------------------------------------------------------------
def test_the_first_event_is_the_text_before_the_first_arrow():
    assert rollups._first_event_of("page_viewed>click>product_viewed") == "page_viewed"
    assert rollups._first_event_of("click") == "click"
    assert rollups._first_event_of("(empty)") == "(empty)"
    assert rollups._first_event_of("") == ""
    assert len(rollups._first_event_of("x" * 300 + ">y")) == rollups.PATH_FIRST_EVENT_MAX_LEN


# ---- the rollup writes it ------------------------------------------------------------------------------------------
def path_session(i, names):
    seq = {str(n + 1): {name: f"e{i}-{n}"} for n, name in enumerate(names)}
    start = datetime.combine(DAY, datetime.min.time(), tzinfo=IST) + timedelta(minutes=i)
    return {"id": i, "session_start": start, "session_id": f"s{i}", "actor_id": f"a{i}", "event_sequence": seq,
            "add_to_cart_count": 0, "checkout_started_count": 0}


def run_stage(has_column):
    sessions = [path_session(1, ["page_viewed", "click"]), path_session(2, ["product_viewed", "click"])]
    cursor = Cursor(has_column=has_column, sessions=sessions)
    rollups._rollup_behavioral_path_daily(cursor, Conn(), [DAY])
    inserts = statements(cursor, "INSERT INTO behavioral_path_daily")
    return cursor, inserts


def test_with_the_column_every_path_row_carries_its_first_event():
    _, inserts = run_stage(has_column=True)
    sql, rows = inserts[0]
    assert "first_event" in sql and "first_event = VALUES(first_event)" in sql
    assert sorted(r[-1] for r in rows) == ["page_viewed", "product_viewed"]
    assert sql.count("%s") == len(rows[0])


def test_without_the_column_the_rollup_writes_what_it_always_wrote():
    _, inserts = run_stage(has_column=False)
    sql, rows = inserts[0]
    assert "first_event" not in sql
    assert all(len(r) == 9 for r in rows) and sql.count("%s") == 9
