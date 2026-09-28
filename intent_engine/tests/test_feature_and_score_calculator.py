import pytest

from intent_engine.feature_calculator import extract_features
from intent_engine.score_calculator import compute_raw_score, compute_predictive_score, assign_bucket


def _row(**overrides):
    base = {
        "session_time_spent_ms": 120000,  # 2 minutes
        "product_view_count": 1,
        "useful_click_count": 1,
        "scroll_count": 1,
        "page_view_count": 1,
    }
    base.update(overrides)
    return base


def test_null_duration_raises_caller_must_filter_upstream():
    with pytest.raises(ValueError):
        extract_features(_row(session_time_spent_ms=None))


def test_low_engagement_session_scores_low():
    features = extract_features(_row(
        session_time_spent_ms=5000, product_view_count=0, useful_click_count=0,
        scroll_count=0, page_view_count=1,
    ))
    score = compute_predictive_score(features, 0.0, 0.0, 1.0)
    assert score < 25
    assert assign_bucket(score, 25.0, 55.0) == "low"


def test_moderate_session_scores_medium():
    features = extract_features(_row(
        session_time_spent_ms=180000, product_view_count=2, useful_click_count=4,
        scroll_count=3, page_view_count=3,
    ))
    score = compute_predictive_score(features, 0.0, 0.0, 1.0)
    assert 25 <= score < 55
    assert assign_bucket(score, 25.0, 55.0) == "medium"


def test_deeply_engaged_session_scores_high():
    features = extract_features(_row(
        session_time_spent_ms=600000, product_view_count=10, useful_click_count=20,
        scroll_count=10, page_view_count=10,
    ))
    score = compute_predictive_score(features, 0.0, 0.0, 1.0)
    assert score >= 55
    assert assign_bucket(score, 25.0, 55.0) == "high"


def test_product_view_cap():
    low = extract_features(_row(product_view_count=4))
    high = extract_features(_row(product_view_count=100))
    assert compute_raw_score(low) == compute_raw_score(high)


def test_useful_click_cap():
    low = extract_features(_row(useful_click_count=12))
    high = extract_features(_row(useful_click_count=999))
    assert compute_raw_score(low) == compute_raw_score(high)


def test_dwell_time_cap():
    low = extract_features(_row(session_time_spent_ms=6 * 60000))
    high = extract_features(_row(session_time_spent_ms=999 * 60000))
    assert compute_raw_score(low) == compute_raw_score(high)


def test_scroll_cap():
    low = extract_features(_row(scroll_count=8))
    high = extract_features(_row(scroll_count=500))
    assert compute_raw_score(low) == compute_raw_score(high)


def test_page_view_cap():
    low = extract_features(_row(page_view_count=6))
    high = extract_features(_row(page_view_count=500))
    assert compute_raw_score(low) == compute_raw_score(high)


def test_score_clamped_to_100_even_with_high_modifier():
    features = extract_features(_row(
        session_time_spent_ms=600000, product_view_count=100, useful_click_count=100,
        scroll_count=100, page_view_count=100,
    ))
    score = compute_predictive_score(features, 0.0, 0.0, 1.15)
    assert score == 100.0


def test_score_clamped_to_0_with_heavy_penalties():
    features = extract_features(_row(session_time_spent_ms=1000, product_view_count=0,
                                      useful_click_count=0, scroll_count=0, page_view_count=1))
    score = compute_predictive_score(features, -12.0, -20.0, 0.70)
    assert score == 0.0


def test_no_single_factor_dominates():
    only_product_views = extract_features(_row(
        product_view_count=100, useful_click_count=0, session_time_spent_ms=0,
        scroll_count=0, page_view_count=0,
    ))
    assert compute_raw_score(only_product_views) < 100
