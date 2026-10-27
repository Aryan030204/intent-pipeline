"""scripts/backfill_utm.py: fills utm_* from the stored url, gently, and only where they are empty."""

from datetime import datetime

import pytest

from scripts import backfill_utm as bf

A = "https://shop.com/?utm_source=google&utm_medium=cpc"
B = "https://shop.com/p?utm_campaign=diwali"
EMPTY_VALUES = "https://shop.com/?utm_source=&utm_medium="     # mentions utm_ but carries nothing to store
START, END = datetime(2026, 10, 1), datetime(2026, 10, 28)


class Cursor:
    """A table of (id, url) rows with all utm columns empty; understands the script's three statements."""

    def __init__(self, rows, id_bounds="auto"):
        self.rows = dict(rows)
        self.updates, self.selects = [], []
        self._out = []
        self.bounds = id_bounds

    def execute(self, sql, params=None):
        if sql.startswith("SELECT MIN(id)"):
            ids = sorted(self.rows)
            if self.bounds == "auto":
                self._out = [{"lo": ids[0] if ids else None, "hi": ids[-1] if ids else None}]
            else:
                self._out = [{"lo": self.bounds[0], "hi": self.bounds[1]}]
        elif sql.startswith("SELECT id, url"):
            low, high, pattern = params
            self.selects.append((low, high))
            assert pattern == "%utm\\_%"
            self._out = [{"id": i, "url": u} for i, u in sorted(self.rows.items()) if low <= i <= high and "utm_" in u]
        elif sql.startswith("UPDATE"):
            n = len(bf.UTM_COLUMNS) + 1
            flat = list(params)
            batch = [tuple(flat[i:i + n]) for i in range(0, len(flat), n)]
            self.updates.append((sql, batch))
            for row_id, *_values in batch:      # the real UPDATE sets the columns, so the row stops matching
                self.rows.pop(row_id, None)
            self._out = []

    def fetchall(self):
        return self._out


class Conn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def run(cursor, **kwargs):
    args = dict(batch=2, sleep_s=0.0, dry_run=False, max_rows=None)
    args.update(kwargs)
    conn = Conn()
    sleeps = []
    stats = bf.backfill_table(cursor, conn, "behavioral_events", START, END, args["batch"], args["sleep_s"],
                              args["dry_run"], args["max_rows"], sleep=sleeps.append)
    return stats, conn, sleeps


def test_rows_with_utm_values_are_updated_with_the_extracted_values():
    cursor = Cursor({1: A, 2: "https://shop.com/plain", 3: B})
    stats, conn, _ = run(cursor, batch=10)
    assert stats["updated"] == 2
    (sql, batch), = cursor.updates
    assert batch == [(1, "google", "cpc", None, None, None), (3, None, None, "diwali", None, None)]
    assert sql.startswith("UPDATE behavioral_events t JOIN (SELECT %s AS id, %s AS utm_source")
    assert "ON v.id = t.id SET t.utm_source = v.utm_source" in sql and "t.utm_content = v.utm_content" in sql
    assert conn.commits == 1


def test_a_row_that_mentions_utm_but_carries_no_value_is_left_alone():
    cursor = Cursor({1: EMPTY_VALUES, 2: A})
    stats, _conn, _ = run(cursor, batch=10)
    assert stats["candidates"] == 2 and stats["updated"] == 1
    assert [row[0] for row in cursor.updates[0][1]] == [2]


def test_the_id_range_is_walked_in_batches_with_a_pause_between_them():
    cursor = Cursor({i: A for i in range(10, 17)})                   # ids 10..16
    stats, conn, sleeps = run(cursor, batch=3, sleep_s=0.25)
    assert cursor.selects == [(10, 12), (13, 15), (16, 16)]
    assert stats["updated"] == 7 and conn.commits == 3
    assert sleeps == [0.25, 0.25]                                      # between batches, not after the last


def test_running_it_again_finds_nothing_left_to_do():
    cursor = Cursor({1: A, 2: B})
    run(cursor, batch=10)
    assert cursor.rows == {}                                           # the fake drops a row once it is filled in
    stats, conn, _ = run(cursor, batch=10)
    assert stats["updated"] == 0 and conn.commits == 0 and len(cursor.updates) == 1


def test_a_dry_run_counts_but_writes_nothing():
    cursor = Cursor({1: A, 2: B})
    stats, conn, _ = run(cursor, batch=10, dry_run=True)
    assert stats["updated"] == 2 and cursor.updates == [] and conn.commits == 0


def test_max_rows_stops_the_run():
    cursor = Cursor({i: A for i in range(1, 11)})
    stats, _conn, _ = run(cursor, batch=4, max_rows=5)
    assert stats["updated"] == 5
    assert sum(len(batch) for _sql, batch in cursor.updates) == 5


def test_no_rows_in_the_date_range_is_a_quiet_no_op():
    stats, conn, _ = run(Cursor({}))
    assert stats == {"scanned_windows": 0, "candidates": 0, "updated": 0} and conn.commits == 0


def test_the_candidate_query_only_looks_at_rows_that_are_still_empty_and_mention_utm():
    seen = []

    class Spy(Cursor):
        def execute(self, sql, params=None):
            seen.append(sql)
            super().execute(sql, params)

    run(Spy({1: A}), batch=5)
    select = next(s for s in seen if s.startswith("SELECT id, url"))
    for column in bf.UTM_COLUMNS:
        assert f"{column} IS NULL" in select
    assert "url LIKE %s" in select and "id BETWEEN %s AND %s" in select
    assert any(s.startswith("SELECT MIN(id)") and "occurred_at >= %s" in s for s in seen)   # range via idx_occurred_at


def test_argument_validation():
    ok = ["--brand", "BBB", "--from", "2026-10-01", "--to", "2026-10-27"]
    args = bf.parse_args(ok)
    assert (args.batch, args.sleep, args.dry_run, args.tables) == (2000, 0.5, False, "events,clicks")
    assert bf.parse_args(ok + ["--dry-run", "--tables", "events", "--batch", "500"]).tables == "events"
    for bad in (ok[:2] + ["--from", "2026-10-28", "--to", "2026-10-27"], ok + ["--batch", "0"],
                ok + ["--tables", "orders"], ["--brand", "BBB"]):
        with pytest.raises(SystemExit):
            bf.parse_args(bad)


def test_a_numeric_brand_is_taken_as_the_index():
    assert bf.resolve_brand_index("2") == 2
