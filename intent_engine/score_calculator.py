"""
The core V1 scoring formula: capped weighted sum of behavioral features,
plus penalties, times the page-type modifier, clamped to [0, 100].
"""

from intent_engine.config import (
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


def assign_bucket(predictive_score: float, p35_threshold: float, p72_threshold: float) -> str:
    """
    score < P35 -> low; P35 <= score < P72 -> medium; score >= P72 -> high.
    P35/P72 are percentiles (or fixed fallback values), never fixed score
    constants themselves - see threshold_resolution.py / threshold_calibration.py.
    """
    if predictive_score < p35_threshold:
        return "low"
    if predictive_score < p72_threshold:
        return "medium"
    return "high"
