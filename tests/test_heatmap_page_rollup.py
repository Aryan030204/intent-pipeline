"""heatmap_page_daily: the page-views-per-page-and-device rollup behind the dashboard's Heatmaps list."""
from datetime import date

from pipeline import rollups

DAY = date(2026, 10, 29)


class Conn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


class Cursor:
    """Returns the scripted rows for the page-views SELECT and records everything else."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self.rows

    def executemany(self, sql, rows):
        self.calls.append((" ".join(sql.split()), list(rows)))
        self.rowcount = len(rows)


def run(rows, dates=(DAY,)):
    cursor = Cursor(rows)
    result = rollups._rollup_heatmap_page_daily(cursor, Conn(), list(dates))
    return result, cursor


def inserts(cursor):
    return [c for c in cursor.calls if c[0].startswith("INSERT INTO heatmap_page_daily")]


def row(path, device, views, sessions, d=DAY):
    return {"d": d, "raw_path": path, "device_type": device, "page_views": views, "sessions": sessions}


def test_it_is_one_of_the_rollup_stages_and_its_table_is_created_with_the_others(monkeypatch):
    assert "heatmap_page_daily" in [name for name, _ in rollups._ROLLUP_STAGES]
    for name in ("intent_daily_summary", "page_behavior_daily", "click_behavior_daily",
                 "product_behavior_daily", "behavioral_path_daily"):
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
