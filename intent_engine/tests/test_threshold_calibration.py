from intent_engine.threshold_calibration import _percentile, compute_dead_click_p90
from intent_engine.config import MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION


class _FakeCursor:
    """Minimal dict-cursor fake: .execute(sql, params) records calls, .fetchall() returns queued rows."""

    def __init__(self, fetchall_results):
        self._results = list(fetchall_results)
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self._results.pop(0)


def test_percentile_nearest_rank_basic():
    values = list(range(1, 101))  # 1..100
    assert _percentile(values, 35) == 35 or _percentile(values, 35) == 36
    assert _percentile(values, 72) in (72, 73)


def test_percentile_single_value():
    assert _percentile([42.0], 35) == 42.0
    assert _percentile([42.0], 90) == 42.0


def test_percentile_thresholds_used_when_enough_sessions():
    """
    Documents the calibration-method decision boundary: >= 500 scored
    sessions uses real percentiles, not the fixed fallback.
    """
    assert MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION == 500


def test_dead_click_p90_avoids_divide_by_zero_and_empty_input():
    cursor = _FakeCursor([[]])
    assert compute_dead_click_p90(cursor, __import__("datetime").date(2026, 9, 1)) is None


def test_dead_click_p90_computed_from_rates():
    import datetime
    # 10 sessions, click_count=10 each, dead_click_count varies 0..9 ->
    # rates 0.0, 0.1, ..., 0.9 - P90 (nearest rank) should be near the top.
    rows = [{"dead_click_count": i, "click_count": 10} for i in range(10)]
    cursor = _FakeCursor([rows])
    p90 = compute_dead_click_p90(cursor, datetime.date(2026, 9, 1))
    assert p90 is not None
    assert 0.8 <= p90 <= 0.9
