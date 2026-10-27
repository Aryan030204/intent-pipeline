"""Startup / schedule behaviour of the intent scoring worker (no database, no real scheduler)."""
import threading
from datetime import datetime, timedelta
from unittest import mock

import pytest

pytest.importorskip("apscheduler")

from workers import intent_scoring_worker as w  # noqa: E402


def _schedule(monkeypatch, delay=210):
    scheduler = mock.Mock()
    monkeypatch.setattr("apscheduler.schedulers.blocking.BlockingScheduler", lambda **kw: scheduler)
    monkeypatch.setattr(w, "_run_migrations", lambda: None)
    monkeypatch.setattr(w, "STARTUP_DELAY_S", delay)
    for name in ("run_intent_scoring_catchup", "run_intent_scoring_pipeline", "run_intent_calibration_pipeline"):
        monkeypatch.setattr(w, name, mock.Mock(name=name))
    w._run_scheduled()
    return scheduler


def test_nothing_heavy_runs_inline_at_startup(monkeypatch):
    scheduler = _schedule(monkeypatch)

    w.run_intent_scoring_catchup.assert_not_called()
    w.run_intent_scoring_pipeline.assert_not_called()
    w.run_intent_calibration_pipeline.assert_not_called()
    scheduler.start.assert_called_once()


def test_startup_runs_are_one_delayed_job(monkeypatch):
    before = datetime.now(w._IST)
    scheduler = _schedule(monkeypatch, delay=210)

    jobs = {c.kwargs["id"]: c for c in scheduler.add_job.call_args_list}
    startup = jobs["intent_scoring_startup"]
    assert startup.args[1] == "date"
    assert startup.kwargs["run_date"] >= before + timedelta(seconds=209)
    assert startup.kwargs["max_instances"] == 1


def test_recurring_schedule_is_off_the_shared_ticks(monkeypatch):
    scheduler = _schedule(monkeypatch)
    jobs = {c.kwargs["id"]: c.kwargs for c in scheduler.add_job.call_args_list}

    assert jobs["intent_scoring_job"]["hour"] == "*/2"
    assert jobs["intent_scoring_job"]["minute"] == 27  # was :00
    assert jobs["intent_calibration_job"]["hour"] == 2
    assert jobs["intent_calibration_job"]["minute"] == 10  # was 02:00
    for kwargs in jobs.values():
        assert kwargs.get("max_instances") == 1


def test_startup_job_runs_catchup_then_scoring_then_calibration(monkeypatch):
    order = []
    monkeypatch.setattr(w, "run_intent_scoring_catchup", lambda: order.append("catchup"))
    monkeypatch.setattr(w, "run_intent_scoring_pipeline", lambda: order.append("scoring"))
    monkeypatch.setattr(w, "run_intent_calibration_pipeline", lambda: order.append("calibration"))

    w._startup_runs()

    assert order == ["catchup", "scoring", "calibration"]


def test_one_failing_step_does_not_stop_the_next(monkeypatch):
    order = []

    def boom():
        raise RuntimeError("x")

    monkeypatch.setattr(w, "run_intent_scoring_catchup", boom)
    monkeypatch.setattr(w, "run_intent_scoring_pipeline", lambda: order.append("scoring"))
    monkeypatch.setattr(w, "run_intent_calibration_pipeline", lambda: order.append("calibration"))

    w._startup_runs()

    assert order == ["scoring", "calibration"]
    assert not w._heavy_lock.locked()


def test_scheduled_run_is_skipped_while_another_heavy_pass_runs(monkeypatch):
    started, release, runs = threading.Event(), threading.Event(), []

    def slow():
        runs.append("slow")
        started.set()
        release.wait(3)

    monkeypatch.setattr(w, "run_intent_scoring_pipeline", slow)
    t = threading.Thread(target=w._scoring_job)
    t.start()
    assert started.wait(2)

    other = mock.Mock()
    monkeypatch.setattr(w, "run_intent_calibration_pipeline", other)
    w._calibration_job()  # skipped, does not wait
    release.set()
    t.join(3)

    other.assert_not_called()
    assert runs == ["slow"]
