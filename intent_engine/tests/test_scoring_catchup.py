"""
Startup catch-up: when the scoring worker starts it finds the last session already processed
in the database and scores the pending sessions closed after it, in chunks, before the normal
windowed pass and the cron take over.
"""

import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import aws_background  # noqa: F401  (must load before runner)
from intent_engine import repository, runner
from intent_engine.config import SCORING_DEFAULT_LOOKBACK, SCORING_OVERLAP

IST = ZoneInfo("Asia/Kolkata")
IST_NOW = datetime(2026, 10, 15, 11, 0, 0, tzinfo=IST)          # 05:30 UTC
DB_NOW = datetime(2026, 10, 15, 5, 30, 0)                        # the UTC database clock
LAST_PROCESSED = datetime(2026, 10, 7, 2, 25, 15)


class _Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return IST_NOW if tz is not None else IST_NOW.replace(tzinfo=None)


def _session(n, updated):
    return {"session_id": f"s{n:04d}", "actor_id": f"a{n}", "updated_at": updated}


def _wire(monkeypatch, last_processed, pending, chunk=3):
    """Replaces the database touch points. `pending` is the full ordered list of pending sessions."""
    calls = {"fetch": [], "score": [], "recompute": []}
    PRIMARY, READER = "primary", "reader"

    @contextmanager
    def primary(_i):
        yield PRIMARY, object()

    @contextmanager
    def reader(_i):
        yield READER

    def fake_fetch(cursor, start, end, limit, after=None):
        calls["fetch"].append({"cursor": cursor, "start": start, "end": end, "limit": limit, "after": after})
        rows = [s for s in pending if after is None or (s["updated_at"], s["session_id"]) > after]
        return rows[:limit]

    def fake_score(cursor, connection, sessions, now, read_cursor=None, label=""):
        calls["score"].append({"cursor": cursor, "read": read_cursor, "ids": [s["session_id"] for s in sessions]})
        return {s["actor_id"] for s in sessions}

    monkeypatch.setattr(runner, "datetime", _Frozen)
    monkeypatch.setattr(runner, "get_db_cursor", primary)
    monkeypatch.setattr(runner, "get_db_read_cursor", reader)
    monkeypatch.setattr(runner, "_ensure_scoring_schema", lambda *a, **k: None)
    monkeypatch.setattr(runner, "read_db_now", lambda c: DB_NOW)
    monkeypatch.setattr(runner, "fetch_last_processed_updated_at", lambda c: last_processed)
    monkeypatch.setattr(runner, "fetch_pending_sessions", fake_fetch)
    monkeypatch.setattr(runner, "score_sessions", fake_score)
    monkeypatch.setattr(runner, "recompute_actors", lambda c, conn, actors, now: calls["recompute"].append((c, set(actors))))
    monkeypatch.setenv("INTENT_SCORING_CATCHUP_CHUNK", str(chunk))
    return calls, PRIMARY, READER


def test_it_starts_at_the_last_processed_session_and_scores_every_pending_one_in_chunks(monkeypatch):
    pending = [_session(i, datetime(2026, 10, 8, 0, i)) for i in range(7)]
    calls, PRIMARY, READER = _wire(monkeypatch, LAST_PROCESSED, pending, chunk=3)

    total = runner._catch_up_brand(2, "BBB")

    assert total == 7
    assert [c["ids"] for c in calls["score"]] == [["s0000", "s0001", "s0002"], ["s0003", "s0004", "s0005"], ["s0006"]]
    first = calls["fetch"][0]
    assert first["start"] == LAST_PROCESSED - SCORING_OVERLAP     # from the last processed session
    assert first["end"] == DB_NOW                                  # to now, in the database clock
    assert first["after"] is None
    # each chunk resumes after the last row of the previous one
    assert calls["fetch"][1]["after"] == (pending[2]["updated_at"], "s0002")
    assert calls["fetch"][2]["after"] == (pending[5]["updated_at"], "s0005")


def test_reads_use_the_replica_cursor_and_writes_the_primary(monkeypatch):
    pending = [_session(i, datetime(2026, 10, 8, 0, i)) for i in range(2)]
    calls, PRIMARY, READER = _wire(monkeypatch, LAST_PROCESSED, pending)
    runner._catch_up_brand(2, "BBB")
    assert all(f["cursor"] == READER for f in calls["fetch"])
    assert all(s["cursor"] == PRIMARY and s["read"] == READER for s in calls["score"])
    assert [c for c, _ in calls["recompute"]] == [PRIMARY]


def test_actors_are_recomputed_after_each_chunk(monkeypatch):
    pending = [_session(i, datetime(2026, 10, 8, 0, i)) for i in range(5)]
    calls, _, _ = _wire(monkeypatch, LAST_PROCESSED, pending, chunk=2)
    runner._catch_up_brand(2, "BBB")
    assert [sorted(a) for _, a in calls["recompute"]] == [["a0", "a1"], ["a2", "a3"], ["a4"]]


def test_with_nothing_pending_it_scores_nothing(monkeypatch):
    calls, _, _ = _wire(monkeypatch, LAST_PROCESSED, [])
    assert runner._catch_up_brand(2, "BBB") == 0
    assert calls["score"] == [] and calls["recompute"] == []


def test_a_database_that_never_scored_falls_back_to_the_default_lookback(monkeypatch):
    calls, _, _ = _wire(monkeypatch, None, [])
    runner._catch_up_brand(2, "BBB")
    assert calls["fetch"][0]["start"] == DB_NOW - SCORING_DEFAULT_LOOKBACK


def test_a_chunk_that_fills_exactly_is_followed_by_one_more_look(monkeypatch):
    pending = [_session(i, datetime(2026, 10, 8, 0, i)) for i in range(4)]
    calls, _, _ = _wire(monkeypatch, LAST_PROCESSED, pending, chunk=2)
    assert runner._catch_up_brand(2, "BBB") == 4
    assert len(calls["fetch"]) == 3      # 2, 2, then an empty look that ends the loop


# --- the SQL ---------------------------------------------------------------------------------

class _Cur:
    def __init__(self, rows=None):
        self.rows, self.sql, self.params = rows or [], None, None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


def test_pending_query_selects_only_pending_closed_sessions_oldest_first():
    cur = _Cur()
    start, end = datetime(2026, 10, 7), datetime(2026, 10, 15)
    repository.fetch_pending_sessions(cur, start, end, 500)
    assert "intent_status = 'pending'" in cur.sql
    assert "session_time_spent_ms IS NOT NULL" in cur.sql
    assert "ORDER BY updated_at, session_id LIMIT %s" in cur.sql
    assert "updated_at > %s" not in cur.sql
    assert cur.params == (start, end, 500)


def test_pending_query_resumes_after_the_previous_chunk():
    cur = _Cur()
    after = (datetime(2026, 10, 8, 1, 2, 3), "s0042")
    repository.fetch_pending_sessions(cur, datetime(2026, 10, 7), datetime(2026, 10, 15), 100, after)
    assert "(updated_at > %s OR (updated_at = %s AND session_id > %s))" in cur.sql
    assert cur.params[2:5] == (after[0], after[0], "s0042")


def test_last_processed_looks_at_scored_and_excluded_sessions_only():
    cur = _Cur([{"last_updated": LAST_PROCESSED}])
    assert repository.fetch_last_processed_updated_at(cur) == LAST_PROCESSED
    assert "intent_status IN ('scored', 'excluded')" in cur.sql
    assert repository.fetch_last_processed_updated_at(_Cur([{"last_updated": None}])) is None
    assert repository.fetch_last_processed_updated_at(_Cur([])) is None


# --- the worker's startup order ---------------------------------------------------------------

def _run_worker_startup(monkeypatch, catchup):
    sys.path.insert(0, "workers")
    import importlib
    worker = importlib.import_module("workers.intent_scoring_worker")
    order = []

    class FakeScheduler:
        def __init__(self, *a, **k):
            pass

        def add_job(self, fn, trigger, **k):
            order.append(f"cron:{k.get('id')}")

        def start(self):
            order.append("scheduler_started")

    monkeypatch.setattr(worker, "run_intent_scoring_catchup", catchup(order))
    monkeypatch.setattr(worker, "run_intent_scoring_pipeline", lambda: order.append("scoring"))
    monkeypatch.setattr(worker, "run_intent_calibration_pipeline", lambda: order.append("calibration"))
    import apscheduler.schedulers.blocking as blocking
    monkeypatch.setattr(blocking, "BlockingScheduler", FakeScheduler)
    worker._run_scheduled()
    return order


def test_worker_defers_the_startup_runs_and_registers_the_cron(monkeypatch):
    # Nothing heavy runs inline any more: the catch-up/scoring/calibration are one delayed job, and
    # the recurring schedule is registered and started straight away.
    order = _run_worker_startup(monkeypatch, lambda o: (lambda: o.append("catchup")))
    assert order == ["cron:intent_scoring_startup", "cron:intent_scoring_job", "cron:intent_calibration_job", "scheduler_started"]


def test_the_delayed_startup_job_runs_the_catchup_first_then_the_normal_pass(monkeypatch):
    order = []
    sys.path.insert(0, "workers")
    import importlib
    worker = importlib.import_module("workers.intent_scoring_worker")
    monkeypatch.setattr(worker, "run_intent_scoring_catchup", lambda: order.append("catchup"))
    monkeypatch.setattr(worker, "run_intent_scoring_pipeline", lambda: order.append("scoring"))
    monkeypatch.setattr(worker, "run_intent_calibration_pipeline", lambda: order.append("calibration"))
    worker._startup_runs()
    assert order == ["catchup", "scoring", "calibration"]


def test_a_failing_catchup_never_blocks_the_normal_run_or_the_cron(monkeypatch):
    order = []

    def failing():
        order.append("catchup")
        raise RuntimeError("database unavailable")

    sys.path.insert(0, "workers")
    import importlib
    worker = importlib.import_module("workers.intent_scoring_worker")
    monkeypatch.setattr(worker, "run_intent_scoring_catchup", failing)
    monkeypatch.setattr(worker, "run_intent_scoring_pipeline", lambda: order.append("scoring"))
    monkeypatch.setattr(worker, "run_intent_calibration_pipeline", lambda: order.append("calibration"))
    worker._startup_runs()
    assert order == ["catchup", "scoring", "calibration"]


def test_catchup_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("INTENT_SCORING_STARTUP_CATCHUP", "false")
    monkeypatch.setattr(runner.aws_background, "initialize_brand_configs", lambda: pytest.fail("must not start"))
    runner.run_intent_scoring_catchup()
