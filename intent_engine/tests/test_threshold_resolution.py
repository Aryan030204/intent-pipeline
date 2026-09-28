import datetime

from intent_engine.threshold_resolution import get_effective_thresholds
from intent_engine.config import FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return self._rows


def _row(score_date, p35, p72, p90=None):
    return {
        "score_date": score_date, "p35_threshold": p35, "p72_threshold": p72,
        "p90_dead_click_rate": p90,
    }


def test_exact_match_used_when_available():
    d = datetime.date(2026, 9, 20)
    cursor = _FakeCursor([_row(d, 30.0, 60.0, 0.5)])
    result = get_effective_thresholds(cursor, [d])
    assert result[d].p35 == 30.0
    assert result[d].p72 == 60.0
    assert result[d].p90_dead_click_rate == 0.5
    assert result[d].source == "exact"


def test_falls_back_to_most_recent_earlier_date():
    yesterday = datetime.date(2026, 9, 20)
    today = datetime.date(2026, 9, 21)  # not yet calibrated
    cursor = _FakeCursor([_row(yesterday, 28.0, 58.0, 0.4)])
    result = get_effective_thresholds(cursor, [today])
    assert result[today].p35 == 28.0
    assert result[today].source == "fallback_earlier_date"


def test_falls_back_to_config_constants_when_no_history_at_all():
    d = datetime.date(2026, 9, 21)
    cursor = _FakeCursor([])
    result = get_effective_thresholds(cursor, [d])
    assert result[d].p35 == FALLBACK_P35_THRESHOLD
    assert result[d].p72 == FALLBACK_P72_THRESHOLD
    assert result[d].p90_dead_click_rate is None
    assert result[d].source == "fallback_config"


def test_empty_dates_returns_empty_dict():
    cursor = _FakeCursor([])
    assert get_effective_thresholds(cursor, []) == {}
