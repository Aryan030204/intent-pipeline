"""I/O reductions: rollups skip a brand with nothing new, and a committed offset past the log end no longer stalls."""

from datetime import date

import pytest

from pipeline import rollups
from pipeline.intent_kafka_consumer import ConsumerConfig, IntentIngestor

from tests.helpers import FakeBroker, FakeDb
from tests.test_intent_phase2_phase3 import Conn, ScriptCursor

DAY = [date(2026, 10, 17)]
OLD_MARK = object()  # any non-None watermark


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("INTENT_ROLLUP_SKIP_UNCHANGED", raising=False)
    rollups._last_run.clear()
    monkeypatch.setattr(rollups, "_ensure_all_rollup_tables", lambda c, conn: None)
    monkeypatch.setattr(rollups, "_determine_affected_dates", lambda c, now: (DAY, OLD_MARK))
    monkeypatch.setattr(rollups, "update_pipeline_metadata_timestamp", lambda *a, **k: marks.append(1))
    marks.clear()
    yield
    rollups._last_run.clear()


marks = []


def _cursor(ids=(100, 50, 20)):
    """A cursor whose change-signature query reports the given newest ids (events, clicks, sessions)."""
    state = {"ids": ids}
    cursor = ScriptCursor(rows_for={"SELECT (SELECT MAX(id)": lambda p: [dict(zip(("events", "clicks", "sessions"), state["ids"]))]})
    cursor.state = state
    return cursor


def _run(cursor, stages, brand=1):
    ran = []
    rollups._ROLLUP_STAGES = (("probe", lambda c, conn, dates, source: ran.append(1)),) if stages else ()
    rollups.run_rollups_for_brand(brand, "BBB", cursor, Conn())
    return ran


@pytest.fixture(autouse=True)
def _restore_stages():
    original = rollups._ROLLUP_STAGES
    yield
    rollups._ROLLUP_STAGES = original


def test_second_run_with_nothing_new_is_skipped_but_the_watermark_still_advances():
    cursor = _cursor()
    assert _run(cursor, True) == [1]
    marks.clear()
    assert _run(cursor, True) == []                  # nothing new: no stage ran
    assert marks == [1]                              # watermark advanced


def test_a_new_event_click_or_session_forces_a_full_run():
    for position in range(3):
        rollups._last_run.clear()
        cursor = _cursor()
        _run(cursor, True)
        ids = list(cursor.state["ids"])
        ids[position] += 1
        cursor.state["ids"] = tuple(ids)
        assert _run(cursor, True) == [1], position


def test_a_different_set_of_dates_forces_a_full_run(monkeypatch):
    cursor = _cursor()
    _run(cursor, True)
    monkeypatch.setattr(rollups, "_determine_affected_dates", lambda c, now: ([date(2026, 10, 16), *DAY], OLD_MARK))
    assert _run(cursor, True) == [1]                 # the day rolled over


def test_the_first_run_after_a_restart_never_skips():
    cursor = _cursor()
    _run(cursor, True)
    rollups._last_run.clear()                        # a new process
    assert _run(cursor, True) == [1]


def test_a_failed_stage_is_not_remembered_and_is_retried():
    cursor = _cursor()
    rollups._ROLLUP_STAGES = (("boom", lambda c, conn, dates, source: (_ for _ in ()).throw(RuntimeError("x"))),)
    rollups.run_rollups_for_brand(1, "BBB", cursor, Conn())
    assert 1 not in rollups._last_run
    assert _run(cursor, True) == [1]


def test_no_watermark_means_a_full_run_even_if_the_signature_matches(monkeypatch):
    cursor = _cursor()
    _run(cursor, True)
    monkeypatch.setattr(rollups, "_determine_affected_dates", lambda c, now: (DAY, None))
    assert _run(cursor, True) == [1]


def test_an_unreadable_signature_never_skips():
    cursor = ScriptCursor()                          # fetchall returns [] for the signature query
    assert _run(cursor, True) == [1]
    assert _run(cursor, True) == [1]


def test_skipping_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("INTENT_ROLLUP_SKIP_UNCHANGED", "off")
    cursor = _cursor()
    _run(cursor, True)
    assert _run(cursor, True) == [1]


def test_brands_are_tracked_separately():
    cursor = _cursor()
    _run(cursor, True, brand=1)
    assert _run(cursor, True, brand=2) == [1]        # brand 2 has not run yet
    assert _run(cursor, True, brand=1) == []


# ---- offsets past the end of the log ----

def _ingestor(broker):
    return IntentIngestor(broker.factory, None, FakeDb([]), {"bbb_shop": 1}.get, ConsumerConfig())


def test_a_committed_offset_beyond_the_log_end_restarts_from_the_log_start():
    broker = FakeBroker()
    for i in range(3):
        broker.put("intent.other", 0, b"{}", ts=1000 + i)
    broker.committed[("intent.other", 0)] = 99       # the topic was recreated: 99 is past the end (3)
    ingestor = _ingestor(broker)

    bounds = ingestor._offset_bounds(broker.factory())

    assert bounds[("intent.other", 0)] == (0, 3)
    assert ingestor.pending_events() == 3


def test_a_normal_committed_offset_is_untouched():
    broker = FakeBroker()
    for i in range(5):
        broker.put("intent.other", 0, b"{}", ts=1000 + i)
    broker.committed[("intent.other", 0)] = 2
    assert _ingestor(broker)._offset_bounds(broker.factory())[("intent.other", 0)] == (2, 5)


def test_a_committed_offset_equal_to_the_log_end_means_caught_up():
    broker = FakeBroker()
    for i in range(4):
        broker.put("intent.other", 0, b"{}", ts=1000 + i)
    broker.committed[("intent.other", 0)] = 4
    assert _ingestor(broker)._offset_bounds(broker.factory())[("intent.other", 0)] == (4, 4)
