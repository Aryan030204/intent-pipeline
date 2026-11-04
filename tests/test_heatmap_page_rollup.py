"""heatmap_page_daily: the page-views-per-page-and-device rollup behind the dashboard's Heatmaps list, with the
sessions at each Behaviour intent level (the High / Medium / Low pill of every row)."""
from datetime import date, timedelta

from pipeline import rollups

DAY = date(2026, 10, 29)


class Conn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


class Cursor:
    """Returns the scripted rows for the page-views SELECT and records everything else. `missing_columns` are
    columns SHOW COLUMNS does not find; `fail_join` makes the SELECT that joins intent_sessions raise."""

    def __init__(self, rows, missing_columns=(), fail_join=False):
        self.rows = rows
        self.calls = []
        self.rowcount = 0
        self.missing_columns = set(missing_columns)
        self.fail_join = fail_join
        self._column = None

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        if self.fail_join and "LEFT JOIN intent_sessions" in flat:
            raise RuntimeError("query execution was interrupted")
        self.calls.append((flat, params))
        self._column = params[0] if flat.startswith("SHOW COLUMNS") else None

    def fetchone(self):
        return None if self._column in self.missing_columns else (self._column,)

    def fetchall(self):
        return self.rows

    def executemany(self, sql, rows):
        self.calls.append((" ".join(sql.split()), list(rows)))
        self.rowcount = len(rows)


def run(rows, dates=(DAY,), **cursor_options):
    cursor = Cursor(rows, **cursor_options)
    result = rollups._rollup_heatmap_page_daily(cursor, Conn(), list(dates))
    return result, cursor


def selects(cursor):
    return [c for c in cursor.calls if c[0].startswith("SELECT")]


def inserts(cursor):
    return [c for c in cursor.calls if c[0].startswith("INSERT INTO heatmap_page_daily")]


def row(path, device, views, sessions, d=DAY, high=0, medium=0, low=0):
    return {"d": d, "raw_path": path, "device_type": device, "page_views": views, "sessions": sessions,
            "high_sessions": high, "medium_sessions": medium, "low_sessions": low}


def test_it_is_one_of_the_rollup_stages_and_its_table_is_created_with_the_others(monkeypatch):
    assert "heatmap_page_daily" in [name for name, _ in rollups._ROLLUP_STAGES]
    for name in ("intent_daily_summary", "page_behavior_daily", "click_behavior_daily",
                 "product_behavior_daily", "product_intent_daily", "behavioral_path_daily"):
        monkeypatch.setattr(rollups, f"_ensure_{name}_table", lambda cursor, connection: None)
    cursor, conn = Cursor([]), Conn()
    rollups._ensure_all_rollup_tables(cursor, conn)
    created = [c[0] for c in cursor.calls if "CREATE TABLE IF NOT EXISTS heatmap_page_daily" in c[0]]
    assert len(created) == 1
    assert "PRIMARY KEY (summary_date, page_path, device_type)" in created[0]


def test_only_page_viewed_events_are_counted_from_the_real_events_table():
    _, cursor = run([])
    select = cursor.calls[0][0]
    assert "event_name = 'page_viewed'" in select
    assert "FROM behavioral_events" in select
    assert "tmp_rollup" not in select  # the temporary copy has no user_agent column
    assert "%%iPad%%" in select  # LIKE patterns are escaped for the driver


def test_paths_are_normalised_like_the_click_rollup_and_equal_pages_are_added_up():
    rows = [
        row("https://shop.example/Products/Kit/", "desktop", 10, 6),
        row("https://shop.example/products/kit", "desktop", 5, 3),
        row("/pages/about?x=1", "mobile", 2, 2),
    ]
    result, cursor = run(rows)

    written = {r[1:3]: r[3:5] for r in inserts(cursor)[0][1]}
    assert written[("/products/kit", "desktop")] == (15, 9)
    assert ("/pages/about", "mobile") in written
    assert result["generated"] == 2 and result["scanned"] == 17


def test_the_same_path_on_two_devices_stays_two_rows():
    _, cursor = run([row("/", "desktop", 4, 4), row("/", "mobile", 7, 7)])
    devices = sorted(r[2] for r in inserts(cursor)[0][1])
    assert devices == ["desktop", "mobile"]


def test_a_page_with_no_usable_path_is_left_out():
    _, cursor = run([row("", "desktop", 3, 3), row("/", "desktop", 1, 1)])
    paths = [r[1] for r in inserts(cursor)[0][1]]
    assert paths == ["/"]


def test_rows_outside_the_window_are_ignored():
    _, cursor = run([row("/", "desktop", 1, 1, d=date(2026, 9, 1))])
    assert inserts(cursor) == []


def test_each_day_replaces_its_stale_rows():
    _, cursor = run([row("/", "desktop", 1, 1)])
    deletes = [c for c in cursor.calls if c[0].startswith("DELETE FROM heatmap_page_daily")]
    assert len(deletes) == 1
    assert "(page_path, device_type) NOT IN" in deletes[0][0]
    assert deletes[0][1] == [DAY, "/", "desktop"]


def test_it_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("INTENT_HEATMAP_ROLLUP", "off")
    result, cursor = run([row("/", "desktop", 1, 1)])
    assert cursor.calls == [] and result["generated"] == 0


# ---------------- intent levels (the pill) ----------------

def test_the_select_joins_the_closed_sessions_and_counts_each_level():
    _, cursor = run([])
    select = selects(cursor)[0][0]
    assert "FROM behavioral_events e" in select and "LEFT JOIN intent_sessions s ON s.session_id = e.session_id" in select
    for level in ("high", "medium", "low"):
        assert f"= '{level}' THEN e.session_id END) AS {level}_sessions" in select
    assert "e.event_name = 'page_viewed'" in select and "e.user_agent LIKE '%%iPad%%'" in select
    assert "tmp_rollup" not in select


def test_the_level_rule_is_the_dashboards_behaviour_rule():
    flat = " ".join(rollups._BEHAVIOR_LEVEL_EXPR.split())
    assert "WHEN s.session_id IS NULL OR s.intent_exclusion_reason = 'bot' THEN 'unclassified'" in flat
    assert "WHEN s.session_time_spent_ms < 5000 OR (s.click_count = 0 AND s.scroll_count = 0) THEN 'low'" in flat
    assert "WHEN s.click_count > 0 AND (s.add_to_cart_count > 0 OR s.checkout_started_count > 0) THEN 'high'" in flat
    assert "ELSE 'medium'" in flat


def test_the_levels_of_paths_that_normalise_to_one_page_are_added_up():
    rows = [
        row("https://shop.example/Products/Kit/", "desktop", 10, 6, high=1, medium=2, low=3),
        row("https://shop.example/products/kit", "desktop", 5, 3, high=1, medium=0, low=2),
    ]
    _, cursor = run(rows)
    (written,) = inserts(cursor)[0][1]
    assert written == (DAY, "/products/kit", "desktop", 15, 9, 2, 2, 5)


def test_sessions_that_are_still_open_or_bots_are_in_no_level():
    # the query leaves them out of all three counts, so a page can have more sessions than classified ones
    _, cursor = run([row("/", "desktop", 10, 8, high=1, medium=2, low=3)])
    (written,) = inserts(cursor)[0][1]
    assert written[4] == 8 and sum(written[5:8]) == 6


def test_the_insert_writes_and_overwrites_the_three_level_columns():
    _, cursor = run([row("/", "desktop", 1, 1, low=1)])
    sql = inserts(cursor)[0][0]
    for column in ("high_sessions", "medium_sessions", "low_sessions"):
        assert column in sql and f"{column} = VALUES({column})" in sql


def test_the_recent_days_are_recomputed_with_the_window():
    _, cursor = run([row("/", "desktop", 1, 1, low=1), row("/", "desktop", 4, 4, d=DAY - timedelta(days=1), high=2)])
    start, end = selects(cursor)[0][1]
    assert start.date() == DAY - timedelta(days=1) and end.date() == DAY + timedelta(days=1)
    assert sorted(r[0] for r in inserts(cursor)[0][1]) == [DAY - timedelta(days=1), DAY]


def test_the_lookback_can_be_set(monkeypatch):
    monkeypatch.setattr(rollups, "HEATMAP_INTENT_LOOKBACK_DAYS", 3)
    _, cursor = run([])
    assert selects(cursor)[0][1][0].date() == DAY - timedelta(days=2)


def test_with_the_intent_part_off_it_is_the_page_views_only_stage_again(monkeypatch):
    monkeypatch.setenv("INTENT_HEATMAP_INTENT", "off")
    _, cursor = run([row("/", "desktop", 4, 3, high=9)])
    select = selects(cursor)[0][0]
    assert "LEFT JOIN" not in select and "intent_sessions" not in select
    assert selects(cursor)[0][1][0].date() == DAY            # no extra days either
    sql, rows_written = inserts(cursor)[0]
    assert "high_sessions" not in sql and rows_written == [(DAY, "/", "desktop", 4, 3)]


def test_when_the_joined_read_fails_the_page_counts_still_refresh_and_no_level_is_written():
    result, cursor = run([row("/", "desktop", 4, 3)], fail_join=True)
    sql, rows_written = inserts(cursor)[0]
    assert "high_sessions" not in sql and rows_written == [(DAY, "/", "desktop", 4, 3)]
    assert result["generated"] == 1


def test_the_table_is_created_with_the_level_columns_and_older_tables_get_them(monkeypatch):
    for name in ("intent_daily_summary", "page_behavior_daily", "click_behavior_daily",
                 "product_behavior_daily", "product_intent_daily", "behavioral_path_daily"):
        monkeypatch.setattr(rollups, f"_ensure_{name}_table", lambda cursor, connection: None)
    cursor = Cursor([], missing_columns={"high_sessions", "medium_sessions", "low_sessions"})
    rollups._ensure_all_rollup_tables(cursor, Conn())
    created = [c[0] for c in cursor.calls if "CREATE TABLE IF NOT EXISTS heatmap_page_daily" in c[0]]
    assert all(f"{c} INT UNSIGNED NOT NULL DEFAULT 0" in created[0] for c in ("high_sessions", "medium_sessions", "low_sessions"))
    alters = [c[0] for c in cursor.calls if c[0].startswith("ALTER TABLE heatmap_page_daily ADD COLUMN")]
    assert [a.split("ADD COLUMN ")[1].split(" ")[0] for a in alters] == ["high_sessions", "medium_sessions", "low_sessions"]


def test_a_table_that_already_has_the_columns_is_left_alone(monkeypatch):
    for name in ("intent_daily_summary", "page_behavior_daily", "click_behavior_daily",
                 "product_behavior_daily", "product_intent_daily", "behavioral_path_daily"):
        monkeypatch.setattr(rollups, f"_ensure_{name}_table", lambda cursor, connection: None)
    cursor = Cursor([])
    rollups._ensure_all_rollup_tables(cursor, Conn())
    assert not [c for c in cursor.calls if c[0].startswith("ALTER TABLE heatmap_page_daily")]
