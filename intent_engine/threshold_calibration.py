"""
Daily calibration WRITE path - the only code in this engine that ever
writes to intent_thresholds_daily or reassigns already-scored sessions'
buckets. Always targets a single date ("yesterday, IST"); never revisited
by a later run (see module docstring in runner.py for the full policy).
"""

import time
from datetime import date, datetime
from typing import List, Optional, Set

from pipeline.state import logger
from intent_engine.config import (
    SCORE_VERSION,
    FALLBACK_P35_THRESHOLD,
    FALLBACK_P72_THRESHOLD,
    MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION,
    CALIBRATION_MAX_SESSIONS_PER_DATE,
)
from intent_engine.repository import (
    fetch_dead_click_rates_for_date,
    fetch_scored_predictive_scores_for_date,
    upsert_daily_thresholds,
    bulk_reassign_buckets_for_date,
    fetch_actor_ids_scored_on_date,
)


def _percentile(sorted_values: List[float], pct: float) -> float:
    """
    Nearest-rank percentile over an already-sorted ascending list.
    pct in [0, 100].
    """
    if not sorted_values:
        raise ValueError("cannot compute a percentile of an empty list")
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = max(0, min(len(sorted_values) - 1, round(pct / 100 * (len(sorted_values) - 1))))
    return sorted_values[rank]


def compute_dead_click_p90(cursor, target_date: date) -> Optional[float]:
    pairs = fetch_dead_click_rates_for_date(cursor, target_date, CALIBRATION_MAX_SESSIONS_PER_DATE)
    if not pairs:
        return None
    rates = sorted(dead / click for dead, click in pairs if click > 0)
    if not rates:
        return None
    return _percentile(rates, 90)


def calibrate_date_for_brand(cursor, connection, target_date: date, now: datetime) -> Set[str]:
    """
    Runs the full daily calibration for one date on one brand's already-open
    cursor/connection: compute + persist thresholds, bulk-reassign that
    date's session buckets, return the actor_ids that need recomputing
    (every actor with a scored session on target_date - calibration can
    reclassify any of them, not just ones touched by new scoring activity).
    """
    started_at = time.monotonic()

    p90 = compute_dead_click_p90(cursor, target_date)

    scores = fetch_scored_predictive_scores_for_date(
        cursor, target_date, CALIBRATION_MAX_SESSIONS_PER_DATE
    )
    scored_session_count = len(scores)

    if scored_session_count >= MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION:
        sorted_scores = sorted(scores)
        p35 = _percentile(sorted_scores, 35)
        p72 = _percentile(sorted_scores, 72)
        calibration_method = "percentile"
    else:
        p35 = FALLBACK_P35_THRESHOLD
        p72 = FALLBACK_P72_THRESHOLD
        calibration_method = "fixed_fallback"

    upsert_daily_thresholds(
        cursor, connection, target_date, scored_session_count,
        p35, p72, p90, calibration_method, SCORE_VERSION,
    )

    reassigned = bulk_reassign_buckets_for_date(cursor, connection, target_date, p35, p72)
    affected_actor_ids = set(fetch_actor_ids_scored_on_date(cursor, target_date))

    duration = time.monotonic() - started_at
    logger.info(
        "[intent calibration] date=%s duration=%.2fs scored_sessions=%s method=%s "
        "p35=%.2f p72=%.2f p90_dead_click_rate=%s sessions_reassigned=%s "
        "actors_affected=%s score_version=%s",
        target_date, duration, scored_session_count, calibration_method,
        p35, p72, f"{p90:.4f}" if p90 is not None else "n/a",
        reassigned, len(affected_actor_ids), SCORE_VERSION,
    )

    return affected_actor_ids
