"""
The core V1 scoring formula: capped weighted sum of behavioral features,
plus penalties, times the page-type modifier, clamped to [0, 100].
"""

from intent_engine.config import (
    EVIDENCE_MIN_DURATION_MS, EVIDENCE_STRONG_DURATION_MS,
    PRODUCT_VIEWS_CAP, PRODUCT_VIEWS_WEIGHT,
    USEFUL_CLICKS_CAP, USEFUL_CLICKS_WEIGHT,
    MINUTES_CAP, MINUTES_WEIGHT,
    SCROLLS_CAP, SCROLLS_WEIGHT,
    PAGE_VIEWS_CAP, PAGE_VIEWS_WEIGHT,
)
from intent_engine.feature_calculator import SessionFeatures


def compute_raw_score(features: SessionFeatures) -> float:
    return (
        min(features.product_views, PRODUCT_VIEWS_CAP) * PRODUCT_VIEWS_WEIGHT
        + min(features.useful_clicks, USEFUL_CLICKS_CAP) * USEFUL_CLICKS_WEIGHT
        + min(features.minutes, MINUTES_CAP) * MINUTES_WEIGHT
        + min(features.scrolls, SCROLLS_CAP) * SCROLLS_WEIGHT
        + min(features.page_views, PAGE_VIEWS_CAP) * PAGE_VIEWS_WEIGHT
    )


def compute_predictive_score(
    features: SessionFeatures,
    struggle_penalty_value: float,
    bounce_penalty_value: float,
    page_type_modifier: float,
) -> float:
    score = compute_raw_score(features) + struggle_penalty_value + bounce_penalty_value
    score *= page_type_modifier
    return max(0.0, min(score, 100.0))


def evidence_stage(product_views: int, page_views: int, useful_clicks: int, scrolls: int, duration_ms: float) -> int:
    """0 = passive (almost no behavioural evidence), 1 = some evidence, 2 = strong evidence. Browsing, interaction and
    the recorded duration only: never add-to-cart, checkout, orders or revenue.

    stage 2: (product_views >= 2 and page_views >= 2) or useful_clicks >= 2 or duration >= 30 s or scrolls >= 2
    stage 1: product_views >= 2 or page_views >= 2 or useful_clicks >= 1 or scrolls >= 1 or duration >= 10 s
    stage 0: everything else
    """
    if ((product_views >= 2 and page_views >= 2) or useful_clicks >= 2 or scrolls >= 2
            or duration_ms >= EVIDENCE_STRONG_DURATION_MS):
        return 2
    if product_views >= 2 or page_views >= 2 or useful_clicks >= 1 or scrolls >= 1 or duration_ms >= EVIDENCE_MIN_DURATION_MS:
        return 1
    return 0


def evidence_stage_for_row(session_row) -> int:
    """The stage of an intent_sessions row, from the same columns the scorer reads (extract_features)."""
    return evidence_stage(
        session_row.get("product_view_count") or 0, session_row.get("page_view_count") or 0,
        session_row.get("useful_click_count") or 0, session_row.get("scroll_count") or 0,
        session_row.get("session_time_spent_ms") or 0,
    )


def assign_bucket_with_evidence(predictive_score: float, p35_threshold: float, p72_threshold: float, stage: int) -> str:
    """The intent bucket: assign_bucket's cut-offs plus the evidence stage.

    stage 0 -> always low; stage 1 -> low or medium, never high; stage 2 -> low / medium / high.
    P35 / P72 must be percentiles of the positive scores of stage >= 1 sessions only (see threshold_calibration).
    """
    if stage <= 0:
        return "low"
    bucket = assign_bucket(predictive_score, p35_threshold, p72_threshold)
    if bucket == "high" and stage < 2:
        return "medium"
    return bucket


def assign_bucket(predictive_score: float, p35_threshold: float, p72_threshold: float) -> str:
    """
    score < P35 -> low; P35 <= score < P72 -> medium; score >= P72 -> high.
    P35/P72 are percentiles (or fixed fallback values), never fixed score
    constants themselves - see threshold_resolution.py / threshold_calibration.py.

    A score of 0 is always low. Short sessions (a bounce, a one-page visit) clamp to exactly 0; when
    they are a large share of a day, P35 itself is 0 and `score < P35` would put them in medium.
    """
    if predictive_score <= 0 or predictive_score < p35_threshold:
        return "low"
    if predictive_score < p72_threshold:
        return "medium"
    return "high"
