"""
The struggle penalty at its edges. These document how the rule behaves TODAY (nothing here changes it):

    -12 only when click_count >= 10 AND dead_click_rate > the day's P90 dead-click rate.

The production thresholds show p90_dead_click_rate = 1.0000 on every day. A rate cannot exceed 1.0, so with that P90 no
session can ever be penalised, however many of its clicks are dead. Whether the rule should change (a tie-aware cut,
a different population for the percentile) needs the dead-click distribution first: scripts/diagnose_intent.py section 20.
"""

import pytest

from intent_engine.config import STRUGGLE_MIN_CLICK_COUNT, STRUGGLE_PENALTY
from intent_engine.penalties import struggle_penalty


def session(clicks, dead):
    return {"click_count": clicks, "dead_click_count": dead}


def test_the_penalty_and_the_click_minimum_are_the_specified_values():
    assert STRUGGLE_PENALTY == -12.0 and STRUGGLE_MIN_CLICK_COUNT == 10


@pytest.mark.parametrize("p90", [0.5, 0.75, 0.9, 0.95, 0.99])
def test_with_a_p90_below_one_a_mostly_dead_session_is_penalised(p90):
    assert struggle_penalty(session(20, 20), p90) == STRUGGLE_PENALTY          # 100% dead, above any P90 below 1.0


def test_a_session_at_exactly_the_p90_is_not_penalised_because_the_rule_is_strictly_above():
    assert struggle_penalty(session(10, 9), 0.9) == 0.0                         # 90% dead, P90 0.9: not above it
    assert struggle_penalty(session(100, 91), 0.9) == STRUGGLE_PENALTY          # 91% dead: above it


def test_when_p90_is_one_no_session_can_be_penalised_even_if_every_click_is_dead():
    for clicks in (10, 25, 500):
        assert struggle_penalty(session(clicks, clicks), 1.0) == 0.0            # 100% dead, P90 1.0: never "above"
    assert struggle_penalty(session(10, 10), 0.9999) == STRUGGLE_PENALTY        # the moment P90 is below 1.0 it can fire


def test_ninety_percent_dead_with_a_p90_of_one_is_not_penalised():
    assert struggle_penalty(session(10, 9), 1.0) == 0.0


def test_zero_percent_dead_is_never_penalised():
    for p90 in (0.0, 0.5, 1.0):
        assert struggle_penalty(session(40, 0), p90) == 0.0
    assert struggle_penalty(session(40, 1), 0.0) == STRUGGLE_PENALTY            # P90 0.0 and one dead click: above it


def test_fewer_than_ten_clicks_is_never_penalised():
    assert struggle_penalty(session(9, 9), 0.1) == 0.0
    assert struggle_penalty(session(10, 10), 0.1) == STRUGGLE_PENALTY


def test_a_missing_p90_skips_the_penalty():
    assert struggle_penalty(session(50, 50), None) == 0.0


def test_missing_counts_are_treated_as_zero():
    assert struggle_penalty({"click_count": None, "dead_click_count": None}, 0.5) == 0.0
    assert struggle_penalty({}, 0.5) == 0.0
