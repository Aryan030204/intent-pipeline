"""Scoring (phase 2) and rollup (phase 3) query optimisations, tested with fake cursors.

None of this SQL could be run against a real MySQL when it was written, so the guarantees tested here are
the ones that protect production: the statements have the intended shape, every new query falls back to the
original on a SQL rejection, connection-level errors still propagate, and the rollup queries only read columns
the temporary copies contain. Real-MySQL checks are listed in the final report."""

import re
from datetime import date, datetime

import pytest
from mysql.connector import errors as mysql_errors

import intent_engine.repository as repo
import pipeline.db as db
import pipeline.rollups as rollups
from intent_engine.session_scorer import _build_enrichment

SYNTAX = lambda: mysql_errors.ProgrammingError(msg="You have an error in your SQL syntax", errno=1064)
CONNECTION_LOST = lambda: mysql_errors.OperationalError(msg="Lost connection", errno=2013)
NOW = datetime(2026, 10, 6, 12, 0, 0)


class ScriptCursor:
    """Records statements; `fail_on` maps a substring of the SQL to an exception to raise (once per statement text);
    `rows_for` maps a substring to the rows fetchall returns."""

    def __init__(self, fail_on=None, rows_for=None):
        self.calls, self.many_calls = [], []
        self.fail_on, self.rows_for, self._rows, self.rowcount = dict(fail_on or {}), dict(rows_for or {}), [], 0

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self._rows = []
        for needle, error in list(self.fail_on.items()):
            if needle in sql:
                if callable(error):
                    raise error()
                raise error
        for needle, rows in self.rows_for.items():
            if needle in sql:
                self._rows = [dict(r) for r in rows(params)] if callable(rows) else [dict(r) for r in rows]

    def executemany(self, sql, rows):
        self.many_calls.append((sql, list(rows)))

    def fetchall(self):
        return self._rows


class Conn:
    def __init__(self):
        self.commits = self.rollbacks = 0
        self.autocommit_history = []

    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1

    @property
    def autocommit(self): return self.autocommit_history[-1] if self.autocommit_history else False

    @autocommit.setter
    def autocommit(self, value): self.autocommit_history.append(value)


# ======================= phase 2: scoring =======================

SCORED = lambda n: [(70.0 + i % 10, "high", "high", "pdp", "scored", NOW, "v1", f"s{i}") for i in range(n)]
EXCLUDED = lambda n: [("bot", NOW, "v1", f"x{i}") for i in range(n)]


def test_scored_sessions_are_updated_with_one_statement_per_chunk_not_one_per_session(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    cursor, conn = ScriptCursor(), Conn()
    assert repo.bulk_update_scored_sessions(cursor, conn, SCORED(1200)) == 1200
    assert len(cursor.calls) == 3 and cursor.many_calls == []       # 500 + 500 + 200, no per-row executemany
    sql, params = cursor.calls[0]
    assert sql.startswith("UPDATE intent_sessions s JOIN (SELECT %s AS predictive_score") and "%s AS session_id" in sql
    assert sql.count("UNION ALL") == 499 and "ON v.session_id = s.session_id" in sql
    assert "s.updated_at = s.updated_at" in sql                       # keeps the scoring watermark untouched
    assert len(params) == 500 * 8 and conn.commits == 3


def test_the_scored_update_overwrites_the_same_columns_as_the_original():
    cursor = ScriptCursor()
    repo.bulk_update_scored_sessions(cursor, Conn(), SCORED(2))
    sql = cursor.calls[0][0]
    original = ScriptCursor()
    repo._legacy_bulk_update_scored_sessions(original, Conn(), SCORED(2))
    original_set = set(re.findall(r"(\w+) = %s", original.many_calls[0][0]))
    batched_set = set(re.findall(r"s\.(\w+) = v\.", sql))
    assert original_set - {"session_id"} == batched_set


def test_the_excluded_update_nulls_the_scores_and_marks_the_session_excluded():
    cursor = ScriptCursor()
    assert repo.bulk_update_excluded_sessions(cursor, Conn(), EXCLUDED(3)) == 3
    sql, params = cursor.calls[0]
    for part in ("s.predictive_score = NULL", "s.intent_status = 'excluded'", "s.intent_exclusion_reason = v.exclusion_reason",
                 "s.updated_at = s.updated_at"):
        assert part in sql
    assert len(params) == 3 * 4


def test_a_sql_rejection_falls_back_to_the_original_per_session_update(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    cursor, conn = ScriptCursor(fail_on={"UNION ALL": SYNTAX}), Conn()
    assert repo.bulk_update_scored_sessions(cursor, conn, SCORED(4)) == 4
    assert len(cursor.many_calls) == 1 and len(cursor.many_calls[0][1]) == 4     # the original executemany ran
    assert conn.rollbacks == 1


def test_connection_errors_are_not_mistaken_for_sql_rejections(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    cursor = ScriptCursor(fail_on={"UNION ALL": CONNECTION_LOST})
    with pytest.raises(mysql_errors.OperationalError):
        repo.bulk_update_scored_sessions(cursor, Conn(), SCORED(2))
    assert cursor.many_calls == []                                    # no silent retry of a half-failed write


def test_legacy_mode_forces_the_original_updates(monkeypatch):
    monkeypatch.setenv("INTENT_SCORING_MODE", "legacy")
    cursor = ScriptCursor()
    repo.bulk_update_scored_sessions(cursor, Conn(), SCORED(3))
    repo.bulk_update_excluded_sessions(cursor, Conn(), EXCLUDED(2))
    assert cursor.calls == [] and len(cursor.many_calls) == 2


# ---- enrichment ----

PAGE_VIEWS = [  # what behavioral_events holds: (session, time, url, user agent)
    ("s1", datetime(2026, 10, 6, 10, 0, 5), "https://shop.example/collections/all?x=1", "Mozilla/5.0"),
    ("s1", datetime(2026, 10, 6, 10, 0, 1), "https://shop.example/products/blue-shirt?utm=a#top", "Mozilla/5.0"),
    ("s1", datetime(2026, 10, 6, 10, 2, 0), "https://shop.example/products/blue-shirt", "Mozilla/5.0"),
    ("s2", datetime(2026, 10, 6, 11, 0, 0), "https://shop.example/", None),
    ("s2", datetime(2026, 10, 6, 11, 5, 0), "https://shop.example/checkout", "Mozilla/5.0"),
    ("s3", datetime(2026, 10, 6, 12, 0, 0), "https://shop.example/pages/about-us", "Googlebot/2.1"),
]


def strip(url):
    return url.split("?", 1)[0].split("#", 1)[0] if url else url


def per_event_rows(params):
    return [{"session_id": s, "occurred_at": t, "url": u, "user_agent": ua} for s, t, u, ua in PAGE_VIEWS if s in params]


def grouped_rows(params):
    """What the grouped SQL returns: one row per (session, stripped path), MIN time, MAX non-null user agent."""
    groups = {}
    for s, t, u, ua in PAGE_VIEWS:
        if s not in params:
            continue
        g = groups.setdefault((s, strip(u)), {"session_id": s, "page_path": strip(u), "occurred_at": t, "user_agent": None})
        g["occurred_at"] = min(g["occurred_at"], t)
        if ua and (g["user_agent"] is None or ua > g["user_agent"]):
            g["user_agent"] = ua
    return list(groups.values())


def enrichment_with(cursor):
    return {k: (v.entry_page_type, v.page_types_seen, v.user_agent) for k, v in _build_enrichment(cursor, ["s1", "s2", "s3"]).items()}


def test_the_grouped_enrichment_query_gives_the_scorer_exactly_what_the_original_gave(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    grouped = ScriptCursor(rows_for={"GROUP BY session_id, page_path": grouped_rows})
    original = ScriptCursor(rows_for={"event_name = 'page_viewed'": per_event_rows})
    monkeypatch.setenv("INTENT_SCORING_MODE", "legacy")
    expected = enrichment_with(original)
    monkeypatch.delenv("INTENT_SCORING_MODE")
    assert enrichment_with(grouped) == expected
    assert expected["s1"][0] == "pdp" and expected["s2"][0] == "home" and expected["s1"][1] == {"pdp", "collection"}


def test_enrichment_sends_session_ids_in_bounded_lists_and_returns_the_old_column_names(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    cursor = ScriptCursor(rows_for={"GROUP BY session_id, page_path": lambda p: [
        {"session_id": p[0], "page_path": "https://x/", "occurred_at": NOW, "user_agent": "ua"}]})
    rows = repo.fetch_page_viewed_enrichment(cursor, [f"s{i}" for i in range(2500)])
    assert len(cursor.calls) == 3 and [len(c[1]) for c in cursor.calls] == [1000, 1000, 500]
    assert set(rows[0]) == {"session_id", "url", "occurred_at", "user_agent"}      # the keys _build_enrichment reads
    assert "GROUP BY session_id, page_path" in cursor.calls[0][0] and "MIN(occurred_at)" in cursor.calls[0][0]


def test_a_rejected_enrichment_query_falls_back_to_the_original(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    cursor = ScriptCursor(fail_on={"GROUP BY session_id, page_path": SYNTAX}, rows_for={"event_name = 'page_viewed'": per_event_rows})
    rows = repo.fetch_page_viewed_enrichment(cursor, ["s1", "s2"])
    assert len(rows) == 5 and any("GROUP BY" not in c[0] for c in cursor.calls)


def test_a_connection_error_during_enrichment_propagates(monkeypatch):
    monkeypatch.delenv("INTENT_SCORING_MODE", raising=False)
    with pytest.raises(mysql_errors.OperationalError):
        repo.fetch_page_viewed_enrichment(ScriptCursor(fail_on={"GROUP BY": CONNECTION_LOST}), ["s1"])


def test_click_user_agents_and_actor_sessions_use_bounded_lists_and_keep_their_order():
    cursor = ScriptCursor(rows_for={"FROM click_events": lambda p: [{"session_id": x, "user_agent": "ua"} for x in p]})
    found = repo.fetch_click_user_agents(cursor, [f"s{i}" for i in range(1500)])
    assert len(found) == 1500 and len(cursor.calls) == 2

    actors = sorted(f"a{i:04d}" for i in range(2500))
    cursor = ScriptCursor(rows_for={"FROM intent_sessions": lambda p: [
        {"actor_id": a, "session_id": f"{a}-2", "predictive_score": 2, "intent_bucket": "high"} for a in p]})
    rows = repo.fetch_scored_sessions_for_actors(cursor, actors)
    assert len(cursor.calls) == 3 and [r["actor_id"] for r in rows] == actors     # ascending, so recompute_actors still sees best-first


# ======================= phase 3: rollups =======================

WINDOW = [date(2026, 10, 5), date(2026, 10, 6)]


@pytest.fixture(autouse=True)
def fresh_rollup_state(monkeypatch):
    monkeypatch.setattr(rollups, "_temp_unavailable", False)
    monkeypatch.setattr(rollups, "_schema_ensured_brands", set())
    monkeypatch.delenv("INTENT_ROLLUP_SOURCE", raising=False)


def test_a_temporary_copy_is_made_once_outside_a_transaction_with_the_window_bounds():
    cursor, conn = ScriptCursor(), Conn()
    source = rollups._prepare_source(cursor, conn, WINDOW)
    assert source == rollups._Source("tmp_rollup_events", "tmp_rollup_clicks", True)
    sqls = [c[0] for c in cursor.calls]
    assert [s.split()[0] + " " + s.split()[1] for s in sqls] == ["DROP TEMPORARY", "DROP TEMPORARY", "CREATE TEMPORARY", "CREATE TEMPORARY"]
    assert "FROM behavioral_events WHERE occurred_at >= %s AND occurred_at < %s" in sqls[2]
    assert "FROM click_events WHERE occurred_at >= %s AND occurred_at < %s" in sqls[3]
    assert cursor.calls[2][1] == rollups._window_bounds(WINDOW)
    assert conn.autocommit_history == [True, False]                   # MySQL with GTIDs refuses temporary DDL in a transaction


def test_direct_mode_reads_the_real_tables_without_touching_the_database(monkeypatch):
    monkeypatch.setenv("INTENT_ROLLUP_SOURCE", "direct")
    cursor = ScriptCursor()
    assert rollups._prepare_source(cursor, Conn(), WINDOW) is rollups._DIRECT and cursor.calls == []


def test_a_privilege_refusal_switches_the_process_to_direct_reads_for_good():
    privilege = mysql_errors.ProgrammingError(msg="CREATE TEMPORARY TABLES command denied", errno=1044)
    cursor = ScriptCursor(fail_on={"CREATE TEMPORARY": privilege})
    assert rollups._prepare_source(cursor, Conn(), WINDOW) is rollups._DIRECT
    assert rollups._temp_unavailable is True
    again = ScriptCursor()
    assert rollups._prepare_source(again, Conn(), WINDOW) is rollups._DIRECT and again.calls == []   # not tried again


def test_any_other_failure_uses_direct_reads_for_this_run_only():
    cursor = ScriptCursor(fail_on={"CREATE TEMPORARY": CONNECTION_LOST})
    assert rollups._prepare_source(cursor, Conn(), WINDOW) is rollups._DIRECT
    assert rollups._temp_unavailable is False
    assert any(c[0].startswith("DROP TEMPORARY") for c in cursor.calls[-2:])        # half-made tables are cleaned up


def run_event_stages(source):
    cursor, conn = ScriptCursor(), Conn()
    for stage in (rollups._rollup_page_behavior_daily, rollups._rollup_click_behavior_daily, rollups._rollup_product_behavior_daily):
        stage(cursor, conn, WINDOW, source)
    return [c[0] for c in cursor.calls if "FROM" in c[0] and "SELECT" in c[0].upper()]


def test_the_event_rollups_read_the_temporary_copies_when_given_them():
    selects = run_event_stages(rollups._Source("tmp_rollup_events", "tmp_rollup_clicks", True))
    assert len(selects) == 5
    assert all("FROM tmp_rollup_" in s for s in selects)
    assert not any("FROM behavioral_events" in s or "FROM click_events" in s for s in selects)


def test_the_event_rollups_read_the_real_tables_by_default_exactly_as_before():
    for source in (None, rollups._DIRECT):
        selects = run_event_stages(source)
        assert len(selects) == 5 and not any("tmp_rollup_" in s for s in selects)


def test_the_rollup_queries_only_use_columns_the_temporary_copies_contain():
    kept_events = {"occurred_at", "url", "actor_id", "session_id", "event_name", "scroll_percent", "product_id", "product_title"}
    kept_clicks = {"occurred_at", "url", "click_tag", "click_element_id", "click_element_name", "click_element_type",
                   "click_href", "click_element_value", "click_bucket"}
    all_events = kept_events | {"event_id", "client_id", "visitor_id", "referrer", "user_agent", "variant_id", "variant_title",
                                "quantity", "price", "currency", "checkout_total", "ingested_at"}
    all_clicks = kept_clicks | {"event_id", "event_name", "actor_id", "client_id", "visitor_id", "session_id", "referrer",
                                "user_agent", "click_x", "click_y", "url_changed", "cart_changed", "ui_changed",
                                "meaningful_scroll", "product_id", "variant_id", "ingested_at"}
    for sql in run_event_stages(rollups._Source("tmp_rollup_events", "tmp_rollup_clicks", True)):
        words = set(re.findall(r"\b[a-z_]+\b", sql.split("FROM", 1)[0] + " " + sql.split("FROM", 1)[1]))
        if "tmp_rollup_events" in sql:
            assert not (words & (all_events - kept_events)), (sql, words & (all_events - kept_events))
        else:
            assert not (words & (all_clicks - kept_clicks)), (sql, words & (all_clicks - kept_clicks))


def test_a_stage_that_fails_on_the_temporary_copy_is_retried_on_the_real_tables():
    calls = []

    def stage(cursor, connection, dates, source):
        calls.append(source.temp)
        if source.temp:
            raise RuntimeError("temporary table problem")

    temp = rollups._Source("a", "b", True)
    assert rollups._run_stage("page", stage, ScriptCursor(), Conn(), WINDOW, temp, "BBB") is True
    assert calls == [True, False]


def test_a_stage_that_fails_everywhere_is_reported_and_a_direct_failure_is_not_retried():
    calls = []

    def stage(cursor, connection, dates, source):
        calls.append(source.temp)
        raise RuntimeError("boom")

    assert rollups._run_stage("page", stage, ScriptCursor(), Conn(), WINDOW, rollups._Source("a", "b", True), "BBB") is False
    assert calls == [True, False]
    calls.clear()
    assert rollups._run_stage("page", stage, ScriptCursor(), Conn(), WINDOW, rollups._DIRECT, "BBB") is False
    assert calls == [False]


def test_a_run_creates_the_tables_once_per_brand_and_always_drops_the_temporary_copies(monkeypatch):
    ensured, seen = [], []
    monkeypatch.setattr(rollups, "_ensure_all_rollup_tables", lambda c, conn: ensured.append(1))
    monkeypatch.setattr(rollups, "_determine_affected_dates", lambda c, now: (WINDOW, None))
    monkeypatch.setattr(rollups, "update_pipeline_metadata_timestamp", lambda *a, **k: None)
    monkeypatch.setattr(rollups, "_ROLLUP_STAGES", (("probe", lambda c, conn, dates, source: seen.append(source.temp)),))
    cursor, conn = ScriptCursor(), Conn()
    rollups.run_rollups_for_brand(1, "BBB", cursor, conn)
    rollups.run_rollups_for_brand(1, "BBB", cursor, conn)
    rollups.run_rollups_for_brand(2, "PTS", cursor, conn)
    assert len(ensured) == 2                                          # brand 1 once, brand 2 once, not once per run
    assert seen == [True, True, True]
    drops = [c[0] for c in cursor.calls if c[0].startswith("DROP TEMPORARY")]
    assert len(drops) == 3 * 4                                        # per run: two drops before the copy, two after the stages


def test_a_failed_schema_check_is_not_remembered(monkeypatch):
    attempts = []

    def flaky(c, conn):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("ddl failed")

    monkeypatch.setattr(rollups, "_ensure_all_rollup_tables", flaky)
    monkeypatch.setattr(rollups, "_determine_affected_dates", lambda c, now: (WINDOW, None))
    monkeypatch.setattr(rollups, "update_pipeline_metadata_timestamp", lambda *a, **k: None)
    monkeypatch.setattr(rollups, "_ROLLUP_STAGES", ())
    with pytest.raises(RuntimeError):
        rollups.run_rollups_for_brand(5, "X", ScriptCursor(), Conn())
    rollups.run_rollups_for_brand(5, "X", ScriptCursor(), Conn())
    assert len(attempts) == 2


# ---- the watermark upsert ----

def test_the_watermark_is_upserted_without_a_table_check_first():
    cursor, conn = ScriptCursor(), Conn()
    db.update_pipeline_metadata_timestamp(cursor, conn, "k", NOW)
    assert len(cursor.calls) == 1 and "INSERT INTO pipeline_metadata" in cursor.calls[0][0] and conn.commits == 1


def test_the_metadata_table_is_created_only_when_it_is_missing():
    missing = mysql_errors.ProgrammingError(msg="Table 'pipeline_metadata' doesn't exist", errno=1146)
    cursor, conn = ScriptCursor(fail_on={"INSERT INTO pipeline_metadata": None}), Conn()
    state = {"first": True}
    real_execute = cursor.execute

    def execute(sql, params=None):
        if "INSERT INTO pipeline_metadata" in sql and state["first"]:
            state["first"] = False
            cursor.calls.append((sql, params))
            raise missing
        cursor.fail_on.clear()
        return real_execute(sql, params)

    cursor.execute = execute
    db.update_pipeline_metadata_timestamp(cursor, conn, "k", NOW)
    kinds = [c[0].strip().split()[0] for c in cursor.calls]
    assert kinds == ["INSERT", "CREATE", "INSERT"]                    # tried, created the table, tried again


def test_other_errors_while_saving_the_watermark_propagate():
    with pytest.raises(mysql_errors.OperationalError):
        db.update_pipeline_metadata_timestamp(ScriptCursor(fail_on={"INSERT": CONNECTION_LOST}), Conn(), "k", NOW)


# ---- the schedule ----

def test_the_rollup_schedule_is_configurable_and_defaults_to_every_fifteen_minutes():
    import os

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    background = open(os.path.join(root, "aws_background.py"), encoding="utf-8").read()
    orchestration = open(os.path.join(root, "pipeline", "orchestration.py"), encoding="utf-8").read()
    assert 'os.environ.get("ROLLUP_CRON_MINUTE", "").strip() or "0,15,30,45"' in background
    assert "minute=ROLLUP_CRON_MINUTE" in background and "minute=aws_background.ROLLUP_CRON_MINUTE" in orchestration
    assert '"0,15,30,45"' not in orchestration
