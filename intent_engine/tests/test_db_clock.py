"""
The scoring window is built from the IST clock but compared with intent_sessions.updated_at,
which the database stamps in its own clock (UTC on RDS). Regression tests for that mismatch:
before the fix, the window sat 5h30m in the future and every run found "no closed eligible
sessions".
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import aws_background  # noqa: F401  (must load before runner; the real worker imports it first)
from intent_engine import runner
from intent_engine.db_clock import db_clock_offset, read_db_now, to_db_clock

IST = ZoneInfo("Asia/Kolkata")


def test_offset_for_a_utc_database_and_an_ist_clock_is_minus_five_thirty():
    ist_now = datetime(2026, 10, 15, 4, 0, 1, 488159, tzinfo=IST)
    db_now = datetime(2026, 10, 14, 22, 30, 1, 500000)  # same instant, UTC wall clock
    assert db_clock_offset(db_now, ist_now) == timedelta(hours=-5, minutes=-30)


def test_offset_is_zero_when_the_database_runs_in_ist():
    ist_now = datetime(2026, 10, 15, 4, 0, 1, tzinfo=IST)
    assert db_clock_offset(datetime(2026, 10, 15, 4, 0, 2), ist_now) == timedelta(0)


def test_offset_rounds_away_the_few_seconds_between_the_two_clock_reads():
    ist_now = datetime(2026, 10, 15, 4, 0, 0, tzinfo=IST)
    assert db_clock_offset(datetime(2026, 10, 14, 22, 30, 3), ist_now) == timedelta(hours=-5, minutes=-30)
    assert db_clock_offset(datetime(2026, 10, 14, 22, 29, 58), ist_now) == timedelta(hours=-5, minutes=-30)


def test_to_db_clock_shifts_an_ist_instant_to_the_utc_digits():
    offset = timedelta(hours=-5, minutes=-30)
    assert to_db_clock(datetime(2026, 10, 15, 1, 0, 3, tzinfo=IST), offset) == datetime(2026, 10, 14, 19, 30, 3)


def test_the_unfixed_window_never_covers_a_just_updated_session_but_the_fixed_one_does():
    """The production case: run at 04:00 IST, a session stamped 22:20 UTC (10 minutes before)."""
    ist_now = datetime(2026, 10, 15, 4, 0, 1, tzinfo=IST)
    watermark = datetime(2026, 10, 15, 2, 0, 1, tzinfo=IST)  # previous run, 2 hours earlier
    window_start = watermark - timedelta(hours=1)             # SCORING_OVERLAP
    session_updated_at = datetime(2026, 10, 14, 22, 20, 0)    # database UTC clock

    old_start, old_end = window_start.replace(tzinfo=None), ist_now.replace(tzinfo=None)
    assert not (old_start <= session_updated_at < old_end)    # the bug: window is in the future

    offset = db_clock_offset(datetime(2026, 10, 14, 22, 30, 1), ist_now)
    new_start, new_end = to_db_clock(window_start, offset), to_db_clock(ist_now, offset)
    assert new_start <= session_updated_at < new_end


def test_read_db_now_supports_dict_and_tuple_cursors():
    class Cur:
        def __init__(self, row):
            self.row = row

        def execute(self, sql, params=None):
            assert "NOW()" in sql

        def fetchone(self):
            return self.row

    moment = datetime(2026, 10, 15, 0, 25, 39)
    assert read_db_now(Cur({"db_now": moment})) == moment
    assert read_db_now(Cur((moment,))) == moment
    assert read_db_now(Cur(None)) is None


def test_runner_passes_the_window_in_the_database_clock(monkeypatch):
    """Drives _run_scoring_for_brand with a UTC database and checks the window handed to the scorer."""
    ist_now = datetime(2026, 10, 15, 4, 0, 1, tzinfo=IST)
    db_now = ist_now.astimezone(timezone.utc).replace(tzinfo=None)
    watermark = ist_now - timedelta(hours=2)
    seen = {}

    class FakeCursor:
        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return {"db_now": db_now}

    @contextmanager
    def fake_db(_brand_index):
        yield FakeCursor(), object()

    @contextmanager
    def fake_reader(_brand_index):
        yield FakeCursor()

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return ist_now if tz is not None else ist_now.replace(tzinfo=None)

    def fake_score(cursor, connection, window_start, window_end, now, read_cursor=None):
        seen.update(start=window_start, end=window_end, now=now)
        return set()

    monkeypatch.setattr(runner, "get_db_cursor", fake_db)
    monkeypatch.setattr(runner, "get_db_read_cursor", fake_reader)
    monkeypatch.setattr(runner, "_ensure_scoring_schema", lambda *a, **k: None)
    monkeypatch.setattr(runner, "datetime", FrozenDatetime)
    monkeypatch.setattr(runner, "get_pipeline_metadata_timestamp", lambda *a, **k: watermark)
    monkeypatch.setattr(runner, "update_pipeline_metadata_timestamp", lambda *a, **k: seen.setdefault("watermark_saved", a[-1]))
    monkeypatch.setattr(runner, "score_window_for_brand", fake_score)
    monkeypatch.setattr(runner, "recompute_actors", lambda *a, **k: None)

    runner._run_scoring_for_brand(2, "BBB")

    # window = [watermark - 1h, now] = 01:00:01..04:00:01 IST = 19:30:01..22:30:01 UTC
    assert seen["start"] == datetime(2026, 10, 14, 19, 30, 1)
    assert seen["end"] == datetime(2026, 10, 14, 22, 30, 1)
    # everything else still uses the IST clock: scored_at and the saved watermark
    assert seen["now"] == ist_now
    assert seen["watermark_saved"] == ist_now
