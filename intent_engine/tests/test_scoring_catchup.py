"""
The pending-session drain (startup catch-up and its recurring job): newest first, by status, bounded in time,
under a per-brand lock, with a failing session unable to block the rest, and with metrics.
"""

import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import aws_background  # noqa: F401  (must load before runner)
from intent_engine import repository, runner
from intent_engine.config import SCORING_PENDING_LOOKBACK_DAYS

IST = ZoneInfo("Asia/Kolkata")
IST_NOW = datetime(2026, 10, 15, 11, 0, 0, tzinfo=IST)


class _Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return IST_NOW if tz is not None else IST_NOW.replace(tzinfo=None)


def _session(n):
    """Session n, newest = highest n (so the drain must hand them out n high to low)."""
    return {"session_id": f"s{n:04d}", "actor_id": f"a{n}", "id": 1000 + n, "session_start": datetime(2026, 10, 14, 0, n)}


class _LockCursor:
    """The primary cursor: answers GET_LOCK / RELEASE_LOCK."""

    def __init__(self, lock_free=True):
        self.lock_free, self.statements, self._rows = lock_free, [], []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        self._rows = [{"got": 1 if self.lock_free else 0}] if "GET_LOCK" in sql else [{"released": 1}]

    def fetchall(self):
        return self._rows


def _wire(monkeypatch, pending, chunk=3, failed=(), lock_free=True, index_present=True):
    """Replaces the database touch points. `pending` is the list of pending sessions, newest first."""
    calls = {"fetch": [], "score": [], "recompute": [], "count": []}
    PRIMARY, READER = _LockCursor(lock_free), "reader"

    @contextmanager
    def primary(_i):
        yield PRIMARY, object()

    @contextmanager
    def reader(_i):
        yield READER

    def fake_fetch(cursor, since, limit, before=None):
        calls["fetch"].append({"cursor": cursor, "since": since, "limit": limit, "before": before})
        rows = [s for s in pending if before is None or (s["session_start"], s["id"]) < before]
        return rows[:limit]

    def fake_score(cursor, connection, sessions, now, read_cursor=None, label="", stats=None):
        ids = [s["session_id"] for s in sessions]
        calls["score"].append({"cursor": cursor, "read": read_cursor, "ids": ids})
        bad = [i for i in ids if i in failed]
        if stats is not None:
            stats["scored"] = len(ids) - len(bad)
            stats["excluded"] = 0
            stats["failed"] = len(bad)
        return {s["actor_id"] for s in sessions if s["session_id"] not in failed}, bad

    monkeypatch.setattr(runner, "datetime", _Frozen)
    monkeypatch.setattr(runner, "get_db_cursor", primary)
    monkeypatch.setattr(runner, "get_db_read_cursor", reader)
    monkeypatch.setattr(runner, "_ensure_scoring_schema", lambda *a, **k: None)
    monkeypatch.setattr(runner, "fetch_pending_sessions_by_start", fake_fetch)
    monkeypatch.setattr(runner, "score_sessions_isolated", fake_score)
    monkeypatch.setattr(runner, "updated_at_index_present", lambda c: index_present)
    monkeypatch.setattr(runner, "count_pending_sessions", lambda c, since, cap: (calls["count"].append(cap), 777)[1])
    monkeypatch.setattr(runner, "recompute_actors", lambda c, conn, actors, now: calls["recompute"].append((c, set(actors))))
    monkeypatch.setenv("INTENT_SCORING_CATCHUP_CHUNK", str(chunk))
    monkeypatch.delenv("INTENT_SCORING_CATCHUP_BUDGET_S", raising=False)
    monkeypatch.delenv("INTENT_SCORING_PENDING_LOOKBACK_DAYS", raising=False)
    return calls, PRIMARY, READER


def test_it_scores_the_newest_pending_sessions_first_in_chunks_by_session_start_and_id(monkeypatch):
    pending = [_session(i) for i in range(6, -1, -1)]               # s0006 ... s0000
    calls, PRIMARY, READER = _wire(monkeypatch, pending, chunk=3)

    total = runner._catch_up_brand(2, "BBB")

    assert total == 7
    assert [c["ids"] for c in calls["score"]] == [["s0006", "s0005", "s0004"], ["s0003", "s0002", "s0001"], ["s0000"]]
    assert calls["fetch"][0]["before"] is None
    # each chunk resumes before the last row of the previous one, on the (session_start, id) key
    assert calls["fetch"][1]["before"] == (pending[2]["session_start"], pending[2]["id"])
    assert calls["fetch"][2]["before"] == (pending[5]["session_start"], pending[5]["id"])


def test_the_lookback_is_a_number_of_days_before_now_in_the_store_clock(monkeypatch):
    calls, _, _ = _wire(monkeypatch, [])
    runner._catch_up_brand(2, "BBB")
    assert calls["fetch"][0]["since"] == (IST_NOW - timedelta(days=SCORING_PENDING_LOOKBACK_DAYS)).replace(tzinfo=None)
    calls["fetch"].clear()
    runner._catch_up_brand(2, "BBB", lookback_days=3)                # the recurring drain looks back 3 days
    assert calls["fetch"][0]["since"] == (IST_NOW - timedelta(days=3)).replace(tzinfo=None)
    monkeypatch.setenv("INTENT_SCORING_PENDING_LOOKBACK_DAYS", "10")
    calls["fetch"].clear()
    runner._catch_up_brand(2, "BBB")
    assert calls["fetch"][0]["since"] == (IST_NOW - timedelta(days=10)).replace(tzinfo=None)


def test_it_no_longer_depends_on_the_last_scored_session(monkeypatch):
    # An older pending session is still found even though newer sessions were already scored: the drain
    # asks for pending sessions by status, not for "everything after the last processed one".
    assert not hasattr(runner, "fetch_last_processed_updated_at")
    pending = [_session(1)]
    calls, _, _ = _wire(monkeypatch, pending)
    assert runner._catch_up_brand(2, "BBB") == 1


def test_reads_use_the_replica_cursor_and_writes_the_primary(monkeypatch):
    calls, PRIMARY, READER = _wire(monkeypatch, [_session(i) for i in (1, 0)])
    runner._catch_up_brand(2, "BBB")
    assert all(f["cursor"] == READER for f in calls["fetch"])
    assert all(s["cursor"] is PRIMARY and s["read"] == READER for s in calls["score"])
    assert [c for c, _ in calls["recompute"]] == [PRIMARY]


def test_actors_are_recomputed_after_each_chunk(monkeypatch):
    calls, _, _ = _wire(monkeypatch, [_session(i) for i in range(4, -1, -1)], chunk=2)
    runner._catch_up_brand(2, "BBB")
    assert [sorted(a) for _, a in calls["recompute"]] == [["a3", "a4"], ["a1", "a2"], ["a0"]]


def test_with_nothing_pending_it_scores_nothing(monkeypatch):
    calls, _, _ = _wire(monkeypatch, [])
    assert runner._catch_up_brand(2, "BBB") == 0
    assert calls["score"] == [] and calls["recompute"] == []


def test_a_chunk_that_fills_exactly_is_followed_by_one_more_look(monkeypatch):
    calls, _, _ = _wire(monkeypatch, [_session(i) for i in range(3, -1, -1)], chunk=2)
    assert runner._catch_up_brand(2, "BBB") == 4
    assert len(calls["fetch"]) == 3      # 2, 2, then an empty look that ends the loop


# --- a failing session must not block the rest ---------------------------------------------------

def test_failed_sessions_are_counted_and_the_drain_carries_on(monkeypatch, caplog):
    calls, _, _ = _wire(monkeypatch, [_session(i) for i in range(5, -1, -1)], chunk=3, failed={"s0004"})
    with caplog.at_level(logging.INFO):
        total = runner._catch_up_brand(2, "BBB")
    assert total == 6 and len(calls["score"]) == 2                  # both chunks were processed
    # the actors of the failed session are not recomputed from a session that was not scored
    assert "a4" not in {a for _, actors in calls["recompute"] for a in actors}
    summary = next(r.getMessage() for r in caplog.records if "scoring_drain_summary" in r.getMessage())
    assert "sessions_failed=1" in summary and "sessions_scored=5" in summary and "sessions_pending=1" in summary


# --- metrics ---------------------------------------------------------------------------------------

def test_every_batch_and_every_run_logs_the_scoring_metrics(monkeypatch, caplog):
    _wire(monkeypatch, [_session(i) for i in range(3, -1, -1)], chunk=2)
    with caplog.at_level(logging.INFO):
        runner._catch_up_brand(2, "BBB")
    messages = [r.getMessage() for r in caplog.records]
    batch = next(m for m in messages if "category=scoring_batch" in m)
    for key in ("scoring_batch_size=2", "scoring_duration_ms=", "sessions_scored=2", "sessions_failed=0", "sessions_per_second="):
        assert key in batch, key
    summary = next(m for m in messages if "scoring_drain_summary" in m)
    for key in ("stopped=drained", "sessions_processed=4", "sessions_pending=0", "sessions_per_second=", "elapsed_s="):
        assert key in summary, key


# --- the time budget -------------------------------------------------------------------------------

def test_the_budget_stops_the_run_and_reports_the_backlog_that_is_left(monkeypatch, caplog):
    pending = [_session(i) for i in range(9, -1, -1)]
    calls, _, _ = _wire(monkeypatch, pending, chunk=3)
    monkeypatch.setenv("INTENT_SCORING_CATCHUP_BUDGET_S", "5")
    clock = iter([0.0, 0.0, 1.0, 2.0, 3.0, 9.0, 9.0, 9.5, 10.0, 10.0, 10.0, 10.0])      # budget (5 s) is over after the 2nd chunk
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(clock, 10.0))
    with caplog.at_level(logging.INFO):
        runner._catch_up_brand(2, "BBB")
    assert len(calls["score"]) < 4                                   # it stopped before draining all four chunks
    assert calls["count"] == [runner.SCORING_PENDING_COUNT_CAP]      # one bounded count of what is left
    summary = next(r.getMessage() for r in caplog.records if "scoring_drain_summary" in r.getMessage())
    assert "stopped=budget" in summary and "sessions_pending=777" in summary


def test_no_budget_means_no_time_limit_and_no_count(monkeypatch):
    calls, _, _ = _wire(monkeypatch, [_session(i) for i in range(5, -1, -1)], chunk=2)
    monkeypatch.setenv("INTENT_SCORING_CATCHUP_BUDGET_S", "0")
    assert runner._catch_up_brand(2, "BBB") == 6 and calls["count"] == []


# --- the per-brand lock ----------------------------------------------------------------------------

def test_a_brand_whose_scoring_lock_is_held_is_skipped(monkeypatch):
    calls, PRIMARY, _ = _wire(monkeypatch, [_session(1)], lock_free=False)
    assert runner._catch_up_brand(2, "BBB") == 0
    assert calls["fetch"] == [] and calls["score"] == []
    assert not any("RELEASE_LOCK" in s for s, _ in PRIMARY.statements)   # it never held it


def test_the_lock_is_taken_first_and_released_last(monkeypatch):
    calls, PRIMARY, _ = _wire(monkeypatch, [_session(1)])
    runner._catch_up_brand(2, "BBB")
    assert "GET_LOCK(%s, 0)" in PRIMARY.statements[0][0]            # a zero wait: never queues behind another run
    assert PRIMARY.statements[0][1] == ("intent-scoring:2",)
    assert "RELEASE_LOCK" in PRIMARY.statements[-1][0]


def test_the_windowed_pass_skips_a_brand_whose_lock_is_held_and_leaves_the_watermark_alone(monkeypatch):
    ran = []
    PRIMARY = _LockCursor(lock_free=False)

    @contextmanager
    def primary(_i):
        yield PRIMARY, object()

    monkeypatch.setattr(runner, "get_db_cursor", primary)
    monkeypatch.setattr(runner, "_ensure_scoring_schema", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_run_scoring_window", lambda *a, **k: ran.append(1))
    runner._run_scoring_for_brand(2, "BBB")
    assert ran == []


def test_the_windowed_pass_runs_when_the_lock_is_free(monkeypatch):
    ran = []
    PRIMARY = _LockCursor(lock_free=True)

    @contextmanager
    def primary(_i):
        yield PRIMARY, object()

    monkeypatch.setattr(runner, "get_db_cursor", primary)
    monkeypatch.setattr(runner, "_ensure_scoring_schema", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_run_scoring_window", lambda *a, **k: ran.append(1))
    runner._run_scoring_for_brand(2, "BBB")
    assert ran == [1] and "RELEASE_LOCK" in PRIMARY.statements[-1][0]


def test_two_runs_cannot_score_one_brand_at_the_same_time(monkeypatch):
    # one shared lock: the first holder gets it, the second does not, the lock frees when the first is done
    state = {"held": False}

    class Shared(_LockCursor):
        def execute(self, sql, params=None):
            self.statements.append((sql, params))
            if "GET_LOCK" in sql:
                got = 0 if state["held"] else 1
                state["held"] = state["held"] or got == 1
                self._rows = [{"got": got}]
            else:
                state["held"] = False
                self._rows = [{"released": 1}]

    shared = Shared()
    with runner._brand_lock(shared, 2) as first:
        with runner._brand_lock(shared, 2) as second:
            assert (first, second) == (True, False)
    with runner._brand_lock(shared, 2) as again:
        assert again is True


def test_a_database_without_the_lock_service_still_scores(monkeypatch):
    class NoLocks:
        def execute(self, sql, params=None):
            raise RuntimeError("GET_LOCK not allowed")

    with runner._brand_lock(NoLocks(), 2) as acquired:
        assert acquired is True


# --- the missing updated_at index ------------------------------------------------------------------

def test_a_missing_updated_at_index_is_reported_with_the_statement_to_run(monkeypatch, caplog):
    _wire(monkeypatch, [], index_present=False)
    with caplog.at_level(logging.WARNING):
        runner._catch_up_brand(2, "BBB")
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "scoring_index_missing" in text and "ADD KEY idx_sessions_updated_at (updated_at)" in text


# --- the SQL ---------------------------------------------------------------------------------------

class _Cur:
    def __init__(self, rows=None):
        self.rows, self.sql, self.params = rows or [], None, None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


def test_the_pending_query_selects_only_pending_closed_sessions_newest_first_on_the_existing_index():
    cur = _Cur()
    since = datetime(2026, 10, 7)
    repository.fetch_pending_sessions_by_start(cur, since, 500)
    assert "intent_status = 'pending'" in cur.sql                      # an already-scored session is never selected again
    assert "session_time_spent_ms IS NOT NULL" in cur.sql
    assert "ORDER BY session_start DESC, id DESC LIMIT %s" in cur.sql  # (session_start, id) is idx_session_start
    assert "updated_at" not in cur.sql                                 # needs no updated_at index
    assert cur.params == (since, 500)


def test_the_pending_query_resumes_before_the_previous_chunk():
    cur = _Cur()
    before = (datetime(2026, 10, 8, 1, 2, 3), 4242)
    repository.fetch_pending_sessions_by_start(cur, datetime(2026, 10, 7), 100, before)
    assert "(session_start < %s OR (session_start = %s AND id < %s))" in cur.sql
    assert cur.params[1:4] == (before[0], before[0], 4242)


def test_the_pending_count_is_bounded():
    cur = _Cur([{"n": 123}])
    assert repository.count_pending_sessions(cur, datetime(2026, 10, 7), 5000) == 123
    assert "LIMIT %s" in cur.sql and cur.params[-1] == 5000
    assert repository.count_pending_sessions(_Cur([]), datetime(2026, 10, 7)) == 0


def test_the_index_check_looks_for_an_index_starting_with_updated_at():
    cur = _Cur([{"present": 1}])
    assert repository.updated_at_index_present(cur) is True
    assert "column_name = 'updated_at'" in cur.sql and "seq_in_index = 1" in cur.sql
    assert repository.updated_at_index_present(_Cur([])) is False


def test_last_processed_looks_at_scored_and_excluded_sessions_only():
    last = datetime(2026, 10, 7, 2, 25, 15)
    cur = _Cur([{"last_updated": last}])
    assert repository.fetch_last_processed_updated_at(cur) == last
    assert "intent_status IN ('scored', 'excluded')" in cur.sql
    assert repository.fetch_last_processed_updated_at(_Cur([{"last_updated": None}])) is None
    assert repository.fetch_last_processed_updated_at(_Cur([])) is None


# --- the worker's schedule and startup order ---------------------------------------------------------

def _worker():
    sys.path.insert(0, "workers")
    import importlib
    return importlib.import_module("workers.intent_scoring_worker")


def _run_worker_startup(monkeypatch):
    worker = _worker()
    order = []

    class FakeScheduler:
        def __init__(self, *a, **k):
            pass

        def add_job(self, fn, trigger, **k):
            order.append(f"cron:{k.get('id')}")

        def start(self):
            order.append("scheduler_started")

    import apscheduler.schedulers.blocking as blocking
    monkeypatch.setattr(blocking, "BlockingScheduler", FakeScheduler)
    monkeypatch.setattr(worker, "_run_migrations", lambda: None)
    worker._run_scheduled()
    return order


def test_worker_defers_the_startup_runs_and_registers_the_cron_and_the_recurring_drain(monkeypatch):
    # Nothing heavy runs inline: the catch-up/scoring/calibration are one delayed job, and the recurring
    # schedule (scoring, the pending drain, calibration) is registered and started straight away.
    order = _run_worker_startup(monkeypatch)
    assert order == ["cron:intent_scoring_startup", "cron:intent_scoring_job", "cron:intent_scoring_drain",
                     "cron:intent_calibration_job", "scheduler_started"]


def test_the_drain_runs_twice_an_hour_away_from_the_other_ticks():
    worker = _worker()
    assert worker.DRAIN_CRON_MINUTES == "12,42"
    assert worker.SCORING_CRON_MINUTE not in (12, 42) and worker.CALIBRATION_CRON_MINUTE not in (12, 42)


def test_the_drain_job_runs_the_drain_under_the_shared_lock(monkeypatch):
    worker = _worker()
    ran = []
    monkeypatch.setattr(worker, "run_intent_scoring_drain", lambda: ran.append("drain"))
    worker._drain_job()
    assert ran == ["drain"]
    # and is skipped, not queued, when another heavy pass holds the lock
    assert worker._heavy_lock.acquire(blocking=False)
    try:
        worker._drain_job()
    finally:
        worker._heavy_lock.release()
    assert ran == ["drain"]


def test_the_delayed_startup_job_runs_the_catchup_first_then_the_normal_pass(monkeypatch):
    order = []
    worker = _worker()
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

    worker = _worker()
    monkeypatch.setattr(worker, "run_intent_scoring_catchup", failing)
    monkeypatch.setattr(worker, "run_intent_scoring_pipeline", lambda: order.append("scoring"))
    monkeypatch.setattr(worker, "run_intent_calibration_pipeline", lambda: order.append("calibration"))
    worker._startup_runs()
    assert order == ["catchup", "scoring", "calibration"]


def test_catchup_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("INTENT_SCORING_STARTUP_CATCHUP", "false")
    monkeypatch.setattr(runner.aws_background, "initialize_brand_configs", lambda: pytest.fail("must not start"))
    runner.run_intent_scoring_catchup()


def test_the_drain_uses_the_short_lookback(monkeypatch):
    seen = []
    monkeypatch.setattr(runner, "run_intent_scoring_catchup", lambda lookback_days=None: seen.append(lookback_days))
    monkeypatch.delenv("INTENT_SCORING_DRAIN_LOOKBACK_DAYS", raising=False)
    runner.run_intent_scoring_drain()
    monkeypatch.setenv("INTENT_SCORING_DRAIN_LOOKBACK_DAYS", "5")
    runner.run_intent_scoring_drain()
    assert seen == [3, 5]
