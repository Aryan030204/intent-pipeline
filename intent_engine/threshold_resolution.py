"""
READ-ONLY threshold lookup for the 2-hour scoring job. Never writes to
intent_thresholds_daily - see threshold_calibration.py for the write path,
which only the daily calibration job calls.
"""

from datetime import date
from typing import Dict, List, NamedTuple, Optional

from intent_engine.config import FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD
from intent_engine.repository import fetch_thresholds_up_to


class EffectiveThresholds(NamedTuple):
    p35: float
    p72: float
    p90_dead_click_rate: Optional[float]
    source: str  # "exact" | "fallback_earlier_date" | "fallback_config"


def get_effective_thresholds(cursor, session_dates: List[date]) -> Dict[date, EffectiveThresholds]:
    """
    For each date in session_dates, resolves:
      1. an exact intent_thresholds_daily row for that date, if one exists
      2. otherwise the most recent existing row with score_date < date
      3. otherwise the config fallback constants, with p90=None (struggle
         penalty is skipped for such dates - see penalties.py)

    One query covers the whole batch (fetch_thresholds_up_to is bounded by
    the table's total size, which grows one row/day - never per-date).
    """
    if not session_dates:
        return {}

    max_date = max(session_dates)
    rows = fetch_thresholds_up_to(cursor, max_date)
    # rows are ordered DESC by score_date already (see repository.py)
    by_date = {r["score_date"]: r for r in rows}

    result: Dict[date, EffectiveThresholds] = {}
    for d in session_dates:
        exact = by_date.get(d)
        if exact is not None:
            result[d] = EffectiveThresholds(
                p35=float(exact["p35_threshold"]),
                p72=float(exact["p72_threshold"]),
                p90_dead_click_rate=(
                    float(exact["p90_dead_click_rate"])
                    if exact["p90_dead_click_rate"] is not None else None
                ),
                source="exact",
            )
            continue

        earlier = next((r for r in rows if r["score_date"] < d), None)
        if earlier is not None:
            result[d] = EffectiveThresholds(
                p35=float(earlier["p35_threshold"]),
                p72=float(earlier["p72_threshold"]),
                p90_dead_click_rate=(
                    float(earlier["p90_dead_click_rate"])
                    if earlier["p90_dead_click_rate"] is not None else None
                ),
                source="fallback_earlier_date",
            )
            continue

        result[d] = EffectiveThresholds(
            p35=FALLBACK_P35_THRESHOLD,
            p72=FALLBACK_P72_THRESHOLD,
            p90_dead_click_rate=None,
            source="fallback_config",
        )

    return result
