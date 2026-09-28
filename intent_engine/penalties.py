"""
Struggle and bounce penalties. Both operate on a session row already known
to have a non-NULL session_time_spent_ms (see feature_calculator.py).
"""

from typing import Any, Dict, Optional

from intent_engine.config import (
    STRUGGLE_PENALTY,
    STRUGGLE_MIN_CLICK_COUNT,
    BOUNCE_PENALTY,
    BOUNCE_MAX_EVENT_COUNT,
    BOUNCE_MAX_DURATION_MS,
)


def struggle_penalty(
    session_row: Dict[str, Any], effective_p90_dead_click_rate: Optional[float]
) -> float:
    """
    -12 only when click_count >= 10 AND dead_click_rate > that date's
    brand-specific P90. If no P90 is resolvable for this session's date
    (calibration hasn't run yet for any date at or before it), the penalty
    is skipped entirely rather than compared against a guessed global
    figure - see threshold_resolution.py.
    """
    click_count = session_row.get("click_count") or 0
    if click_count < STRUGGLE_MIN_CLICK_COUNT:
        return 0.0
    if effective_p90_dead_click_rate is None:
        return 0.0

    dead_click_count = session_row.get("dead_click_count") or 0
    dead_click_rate = dead_click_count / click_count

    return STRUGGLE_PENALTY if dead_click_rate > effective_p90_dead_click_rate else 0.0


def bounce_penalty(session_row: Dict[str, Any]) -> float:
    """
    -20 when event_count <= 2 OR session_time_spent_ms < 5000. Duration is
    never NULL here - callers only evaluate closed sessions.
    """
    event_count = session_row.get("event_count") or 0
    duration_ms = session_row["session_time_spent_ms"]

    if event_count <= BOUNCE_MAX_EVENT_COUNT or duration_ms < BOUNCE_MAX_DURATION_MS:
        return BOUNCE_PENALTY
    return 0.0
