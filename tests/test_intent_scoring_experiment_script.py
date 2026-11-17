"""
scripts/intent_scoring_experiment.py: the loader pages by keyset with SELECTs only, the read-only guard refuses anything
else, and the experiment never imports or calls the production scoring writers.
"""

import ast
import contextlib
import os
import subprocess
import sys
from datetime import date, datetime

import pytest

from intent_engine import experiment as ex
from intent_engine import replay as rp
from scripts import intent_scoring_experiment as script

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
START, END = datetime(2026, 11, 13), datetime(2026, 11, 14)
SESSION_COLS = {"session_id", "actor_id", "session_start", "session_end", "session_time_spent_ms", "event_count",
                "page_view_count", "product_view_count", "click_count", "useful_click_count", "dead_click_count",
                "add_to_cart_count", "checkout_started_count", "scroll_count", "id", "event_sequence",
                "predictive_score", "intent_status", "intent_bucket", "intent_exclusion_reason"}


def session_row(i, duration="default", **kw):
    row = {"session_id": f"s{i}", "actor_id": f"a{i}", "id": i, "session_start": datetime(2026, 11, 13, 9, 0, i % 60),
           "session_end": None, "session_time_spent_ms": 90000 if duration == "default" else duration, "event_count": 8,
           "page_view_count": 3, "product_view_count": 2, "click_count": 5, "useful_click_count": 4,
           "dead_click_count": 1, "add_to_cart_count": 0, "checkout_started_count": 0, "scroll_count": 2,
           "event_sequence": None, "predictive_score": None, "intent_status": "pending", "intent_bucket": None,
           "intent_exclusion_reason": None}
    row.update(kw)
    return row


class FakeDb:
    """A cursor that serves the experiment's reads and records every statement."""

    def __init__(self, rows, ro_value=1, clarity=None, fail_set=False):
        self.rows, self.sql, self.params = list(rows), [], []
        self.ro_value, self.clarity, self.fail_set = ro_value, clarity, fail_set
        self._result = []

    def execute(self, sql, params=()):
        self.sql.append(sql)
        self.params.append(params)
        text = " ".join(sql.split())
        if text.startswith("SET SESSION TRANSACTION READ ONLY"):
            if self.fail_set:
                raise RuntimeError("not allowed")
            self._result = []
        elif "@@transaction_read_only" in text:
            self._result = [{"ro": self.ro_value}]
        elif "information_schema.columns" in text:
            self._result = [{"name": c} for c in SESSION_COLS]
        elif text.startswith("SELECT") and "FROM intent_sessions" in text:
            limit = params[-1]
            self._result, self.rows = self.rows[:limit], self.rows[limit:]
        elif "FROM daily_user_intent_summary" in text:
            if self.clarity == "missing":
                raise RuntimeError("1146 table missing")
            self._result = [self.clarity] if self.clarity else []
        else:
            self._result = []

    def fetchall(self):
        return self._result


@pytest.fixture
def no_enrich(monkeypatch):
    seen = []

    def fake(cursor, rows):
        seen.append([r["session_id"] for r in rows])
        return {r["session_id"]: rp.Enrichment("pdp", {"pdp", "home"}, "Mozilla/5.0", {"/products/a"}) for r in rows}

    monkeypatch.setattr(script, "enrich_page", fake)
    monkeypatch.setattr(script, "load_behavioral_extras", lambda cursor, ids: {i: {"max_scroll_percent": 50.0} for i in ids})
    return seen


# ---------------- the read-only guard ----------------

@pytest.mark.parametrize("sql", [
    "INSERT INTO intent_sessions (session_id) VALUES ('x')", "UPDATE intent_sessions SET intent_bucket = 'high'",
    "DELETE FROM intent_sessions", "ALTER TABLE intent_sessions ADD COLUMN x INT", "CREATE TABLE t (a INT)",
    "DROP TABLE intent_sessions", "TRUNCATE intent_sessions", "REPLACE INTO intent_thresholds_daily VALUES (1)",
    "SET SESSION TRANSACTION READ WRITE", "CALL something()", "LOCK TABLES intent_sessions WRITE",
    "SELECT 1; DROP TABLE intent_sessions", "SELECT * FROM intent_sessions FOR UPDATE",
    "SELECT GET_LOCK('intent-scoring:1', 0)", "SELECT 1 INTO OUTFILE '/tmp/x'", "  /* hi */ UPDATE t SET a = 1", "", None,
])
def test_the_guard_refuses_everything_but_one_read(sql):
    raw = FakeDb([])
    cursor = script.ReadOnlyCursor(raw)
    with pytest.raises(script.ReadOnlyViolation):
        cursor.execute(sql)
    assert raw.sql == []                              # nothing reached the driver


@pytest.mark.parametrize("sql", ["SELECT 1", "  select a from t where b = %s;", "SHOW COLUMNS FROM intent_sessions",
                                 "SELECT x\nFROM y\nWHERE z = 1"])
def test_the_guard_lets_reads_through(sql):
    raw = FakeDb([])
    script.ReadOnlyCursor(raw).execute(sql, ("v",))
    assert raw.sql == [sql]


def test_the_guard_exposes_no_other_cursor_method():
    cursor = script.ReadOnlyCursor(FakeDb([]))
    for name in ("executemany", "callproc", "nextset", "stored_results"):
        with pytest.raises(script.ReadOnlyViolation):
            getattr(cursor, name)
    assert cursor.fetchall() == []


def test_the_session_is_set_read_only_and_verified():
    raw = FakeDb([])
    script.enforce_read_only_session(raw)
    assert raw.sql[0] == "SET SESSION TRANSACTION READ ONLY" and "@@transaction_read_only" in raw.sql[1]


def test_a_session_that_is_not_read_only_stops_the_run():
    with pytest.raises(script.ReadOnlyViolation):
        script.enforce_read_only_session(FakeDb([], ro_value=0))
    with pytest.raises(script.ReadOnlyViolation):
        script.enforce_read_only_session(FakeDb([], fail_set=True))


# ---------------- loading ----------------

def test_loading_pages_by_keyset_with_selects_only_and_never_uses_updated_at(no_enrich):
    db = FakeDb([session_row(i) for i in range(5)])
    pauses = []
    sessions = script.load_sessions(script.ReadOnlyCursor(db), "BBB", START, END, chunk=2, sleep_s=0.1, sleep=pauses.append)
    assert [s.session_id for s in sessions] == [f"s{i}" for i in range(5)]
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in db.sql)
    assert not any("updated_at" in sql for sql in db.sql)
    pages = [sql for sql in db.sql if "FROM intent_sessions" in sql]
    assert len(pages) == 3 and len(pauses) == 2
    assert "session_start >= %s AND session_start < %s" in pages[0] and "session_start > %s OR" in pages[1]
    assert all(isinstance(s, ex.ExperimentSession) and s.extras["max_scroll_percent"] == 50.0 for s in sessions)


def test_the_enrichment_and_extras_are_read_once_per_page_not_per_session_or_version(no_enrich):
    db = FakeDb([session_row(i) for i in range(5)])
    script.load_sessions(script.ReadOnlyCursor(db), "BBB", START, END, chunk=2, sleep_s=0)
    assert len(no_enrich) == 3 and [len(p) for p in no_enrich] == [2, 2, 1]


def test_pending_sessions_are_loaded_but_not_enriched(no_enrich):
    db = FakeDb([session_row(1), session_row(2, duration=None)])
    sessions = script.load_sessions(script.ReadOnlyCursor(db), "BBB", START, END, chunk=10, sleep_s=0)
    assert no_enrich == [["s1"]]
    assert [s.row["session_time_spent_ms"] is None for s in sessions] == [False, True]
    run = ex.run_experiment(sessions, ["V1"])
    assert {r.session_id: r.status for r in run.results["V1"]} == {"s1": "scored", "s2": "pending"}


def test_max_sessions_stops_the_loading(no_enrich):
    db = FakeDb([session_row(i) for i in range(10)])
    sessions = script.load_sessions(script.ReadOnlyCursor(db), "BBB", START, END, chunk=4, sleep_s=0, max_sessions=6)
    assert len(sessions) == 6


def test_only_the_columns_the_database_has_are_selected(no_enrich):
    assert "first_utm_source" not in script.session_columns(SESSION_COLS)
    assert "first_utm_source" in script.session_columns(SESSION_COLS | {"first_utm_source"})
    assert "predictive_score" not in script.session_columns({"session_id"})


def test_a_failing_extras_loader_does_not_stop_the_load(monkeypatch):
    monkeypatch.setattr(script, "enrich_page", lambda cursor, rows: {})

    def broken(cursor, ids):
        raise RuntimeError("no such column")

    sessions = script.load_sessions(script.ReadOnlyCursor(FakeDb([session_row(1)])), "BBB", START, END, chunk=5, sleep_s=0,
                                    extra_loaders=[broken])
    assert len(sessions) == 1 and dict(sessions[0].extras) == {}


def test_clarity_row_is_returned_or_none():
    row = {"low_intent_pct": 40, "medium_intent_pct": 45, "high_intent_pct": 15, "total_sessions": 100}
    assert script.load_clarity_row(script.ReadOnlyCursor(FakeDb([], clarity=row)), date(2026, 11, 13)) == row
    assert script.load_clarity_row(script.ReadOnlyCursor(FakeDb([], clarity=None)), date(2026, 11, 13)) is None
    assert script.load_clarity_row(script.ReadOnlyCursor(FakeDb([], clarity="missing")), date(2026, 11, 13)) is None


# ---------------- a whole run ----------------

def test_a_whole_brand_run_reads_only_writes_files_and_reports(tmp_path, no_enrich, capsys):
    rows = [session_row(i, predictive_score=None, intent_status="scored") for i in range(6)]
    db = FakeDb(rows, clarity={"low_intent_pct": 40.0, "medium_intent_pct": 45.0, "high_intent_pct": 15.0,
                               "total_sessions": 6})

    @contextlib.contextmanager
    def factory(index):
        yield db

    args = script.parse_args(["--brand", "BBB", "--date", "2026-11-13", "--out-dir", str(tmp_path), "--sleep", "0"])
    summary = script.run_brand("BBB", 1, args, factory)
    assert summary["sessions_loaded"] == 6 and summary["versions_run"] == ["V1", "V2", "V3", "V4", "V5", "V1C", "V6"]
    statements = [" ".join(s.split()) for s in db.sql]
    assert statements[0] == "SET SESSION TRANSACTION READ ONLY"
    assert all(s.upper().startswith(("SELECT", "SET SESSION TRANSACTION READ ONLY")) for s in statements)
    for name in ("session_scores.csv", "session_scores_long.csv", "summary.json", "report.txt"):
        assert (tmp_path / "BBB" / "2026-11-13" / name).is_file()
    assert "V1 parity" in (tmp_path / "BBB" / "2026-11-13" / "report.txt").read_text(encoding="utf-8")
    assert "Experiment complete." in capsys.readouterr().out


def test_a_day_without_sessions_returns_none(tmp_path):
    db = FakeDb([])

    @contextlib.contextmanager
    def factory(index):
        yield db

    args = script.parse_args(["--brand", "BBB", "--date", "2026-11-13", "--out-dir", str(tmp_path)])
    assert script.run_brand("BBB", 1, args, factory) is None
    assert not list(tmp_path.iterdir())


def test_a_session_not_verified_read_only_never_gets_to_read(tmp_path):
    db = FakeDb([session_row(1)], ro_value=0)

    @contextlib.contextmanager
    def factory(index):
        yield db

    args = script.parse_args(["--brand", "BBB", "--date", "2026-11-13", "--out-dir", str(tmp_path)])
    with pytest.raises(script.ReadOnlyViolation):
        script.run_brand("BBB", 1, args, factory)
    assert not any("FROM intent_sessions" in sql for sql in db.sql)


# ---------------- arguments ----------------

def test_defaults_and_version_selection():
    args = script.parse_args(["--brand", "BBB", "--date", "2026-11-13"])
    assert args.date == date(2026, 11, 13) and args.threshold_mode == "common"
    assert args.version_names == ["V1", "V2", "V3", "V4", "V5", "V1C", "V6"] and args.requested_versions == ["V1", "V2", "V3", "V4", "V5", "V1C", "V6"]
    assert args.out_dir == "experiment_results"
    assert script.parse_args(["--brand", "ALL", "--date", "2026-11-13", "--versions", "v1"]).requested_versions == ["V1"]


@pytest.mark.parametrize("extra", [["--versions", "V9"], ["--versions", "v1,v9"], ["--threshold-mode", "weird"],
                                   ["--chunk", "0"]])
def test_bad_arguments_are_refused(extra):
    with pytest.raises(SystemExit):
        script.parse_args(["--brand", "BBB", "--date", "2026-11-13", *extra])


def test_the_date_must_be_a_date():
    with pytest.raises(SystemExit):
        script.parse_args(["--brand", "BBB", "--date", "13-11-2026"])


# ---------------- nothing from production is imported or called ----------------

FORBIDDEN = {"runner", "_ensure_scoring_schema", "_brand_lock", "bulk_update_scored_sessions",
             "bulk_update_excluded_sessions", "bulk_upsert_actors", "upsert_daily_thresholds",
             "bulk_reassign_buckets_for_date", "recompute_actors", "calibrate_date_for_brand", "get_db_cursor",
             "run_intent_scoring_pipeline", "run_intent_calibration_pipeline", "score_sessions", "score_window_for_brand"}


@pytest.mark.parametrize("relative", ["intent_engine/experiment.py", "scripts/intent_scoring_experiment.py"])
def test_the_experiment_code_never_names_a_production_writer(relative):
    with open(os.path.join(ROOT, relative), encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(alias.name.split(".")[-1] for alias in node.names)
            if isinstance(node, ast.ImportFrom) and node.module:
                names.update(node.module.split("."))
    assert not (names & FORBIDDEN), names & FORBIDDEN


def test_importing_the_experiment_does_not_load_the_runner_or_the_writers():
    code = (
        "import sys, types\n"
        "try:\n    import pymongo\nexcept ImportError:\n"
        "    m = types.ModuleType('pymongo'); m.MongoClient = object; sys.modules['pymongo'] = m\n"
        f"sys.path.insert(0, {ROOT!r})\n"
        "import intent_engine.experiment, scripts.intent_scoring_experiment\n"
        "loaded = [m for m in ('intent_engine.runner', 'intent_engine.session_scorer', 'intent_engine.actor_scorer',"
        " 'aws_background', 'workers.intent_scoring_worker') if m in sys.modules]\n"
        "print('LOADED', loaded)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-800:]
    assert "LOADED []" in out.stdout


# ---------------- one invocation, one load, three versions ----------------

def _run(tmp_path, versions, rows=None, factory_db=None):
    db = factory_db or FakeDb(rows if rows is not None else [session_row(i) for i in range(7)])

    @contextlib.contextmanager
    def factory(index):
        yield db

    args = script.parse_args(["--brand", "BBB", "--date", "2026-11-13", "--out-dir", str(tmp_path), "--sleep", "0",
                              "--chunk", "3", *(["--versions", versions] if versions else [])])
    return script.run_brand("BBB", 1, args, factory), db


def test_the_console_shows_the_progress_in_order(tmp_path, no_enrich, capsys):
    _run(tmp_path, None)
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert lines[:10] == ["Loading sessions...", "Loaded 7 sessions.", "Running V1...", "Running V2...", "Running V3...",
                         "Running V4...", "Running V5...", "Running V1C...", "Running V6...", "Experiment complete."]


def test_the_sessions_are_loaded_once_whatever_the_number_of_versions(tmp_path, no_enrich, monkeypatch):
    calls = []
    real = script.load_sessions
    monkeypatch.setattr(script, "load_sessions", lambda *a, **k: (calls.append(1), real(*a, **k))[1])
    _, db_all = _run(tmp_path / "a", "v1,v2,v3")
    _, db_one = _run(tmp_path / "b", "v1")
    assert len(calls) == 2                                     # one load per run, never one per version
    assert len(db_all.sql) == len(db_one.sql)                  # the same statements whether one version or three
    pages = [s for s in db_all.sql if s.lstrip().startswith("SELECT") and "FROM intent_sessions" in s]
    assert len(pages) == 3                                     # 7 sessions in pages of 3: read once


@pytest.mark.parametrize("versions,expected", [("v1,v2", ["V1", "V2"]), ("v2,v3", ["V2", "V3"]),
                                               ("v1,v2,v3", ["V1", "V2", "V3"]), ("v3", ["V3"]),
                                               ("v4,v5", ["V4", "V5"]), ("v1,v2,v3,v4,v5,v1c,v6", ["V1", "V2", "V3", "V4", "V5", "V1C", "V6"])])
def test_the_selected_versions_are_the_ones_run_and_written(tmp_path, no_enrich, versions, expected):
    summary, _ = _run(tmp_path, versions)
    assert summary["versions_run"] == expected
    header = (tmp_path / "BBB" / "2026-11-13" / "session_scores.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
    for name in ("V1", "V2", "V3", "V4", "V5", "V1C", "V6"):
        assert (f"{name.lower()}_score" in header) == (name in expected)


def test_every_version_scored_the_same_loaded_sessions(tmp_path, no_enrich):
    summary, _ = _run(tmp_path, "v1,v2,v3")
    assert {v["population"]["total"] for v in summary["versions"].values()} == {7}
    assert set(summary["transitions"]) == {"V1->V2", "V1->V3", "V2->V3"}


def test_all_reads_stay_read_only_with_three_versions(tmp_path, no_enrich):
    _, db = _run(tmp_path, "v1,v2,v3")
    assert all(" ".join(s.split()).upper().startswith(("SELECT", "SET SESSION TRANSACTION READ ONLY")) for s in db.sql)
    assert not any(word in " ".join(db.sql).upper() for word in ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "CREATE ", "DROP "))
