"""
Idempotency at the pure-function level: scoring the same session data twice
must produce byte-identical results (deterministic, no incrementing
counters, no hidden state). The DB-level idempotency (overwrite-not-
increment SQL, guarded stale-key behavior) is structural - see repository.py
and threshold_calibration.py's docstrings/comments - and needs a live
database to exercise end-to-end.
"""

from intent_engine.feature_calculator import extract_features
from intent_engine.penalties import struggle_penalty, bounce_penalty
from intent_engine.page_modifier import highest_intent_page_type, modifier_for
from intent_engine.score_calculator import compute_predictive_score, assign_bucket
from intent_engine.overrides import apply_hard_overrides


def _score_once(session_row, page_types_seen, entry_page_type, p90):
    features = extract_features(session_row)
    struggle = struggle_penalty(session_row, p90)
    bounce = bounce_penalty(session_row)
    modifier = modifier_for(highest_intent_page_type(page_types_seen))
    predictive_score = compute_predictive_score(features, struggle, bounce, modifier)
    bucket = assign_bucket(predictive_score, 25.0, 55.0)
    operational = apply_hard_overrides(
        bucket, entry_page_type,
        session_row.get("checkout_started_count") or 0,
        session_row.get("add_to_cart_count") or 0,
    )
    return predictive_score, bucket, operational


def test_repeated_scoring_is_idempotent():
    session_row = {
        "session_time_spent_ms": 180000, "product_view_count": 3, "useful_click_count": 5,
        "scroll_count": 4, "page_view_count": 3, "event_count": 15,
        "click_count": 12, "dead_click_count": 3,
        "checkout_started_count": 0, "add_to_cart_count": 0,
    }
    result_1 = _score_once(session_row, {"pdp", "collection"}, "home", 0.5)
    result_2 = _score_once(session_row, {"pdp", "collection"}, "home", 0.5)
    result_3 = _score_once(session_row, {"pdp", "collection"}, "home", 0.5)

    assert result_1 == result_2 == result_3


def test_same_session_different_p90_snapshot_still_deterministic_per_call():
    """
    Simulates a late-arriving re-score where the resolved P90 for that date
    hasn't changed between runs (the documented "calibrate once" policy) -
    same inputs must still yield the same output.
    """
    session_row = {
        "session_time_spent_ms": 30000, "product_view_count": 1, "useful_click_count": 2,
        "scroll_count": 1, "page_view_count": 2, "event_count": 6,
        "click_count": 15, "dead_click_count": 14,
        "checkout_started_count": 0, "add_to_cart_count": 1,
    }
    first = _score_once(session_row, {"pdp"}, "pdp", 0.5)
    second = _score_once(session_row, {"pdp"}, "pdp", 0.5)
    assert first == second
    # add_to_cart_count=1 forces operational high regardless of predictive bucket
    assert first[2] == "high"


def test_multiple_sessions_for_same_actor_scored_independently():
    session_a = {
        "session_time_spent_ms": 600000, "product_view_count": 10, "useful_click_count": 10,
        "scroll_count": 8, "page_view_count": 6, "event_count": 30,
        "click_count": 10, "dead_click_count": 0,
        "checkout_started_count": 0, "add_to_cart_count": 0,
    }
    session_b = {
        "session_time_spent_ms": 2000, "product_view_count": 0, "useful_click_count": 0,
        "scroll_count": 0, "page_view_count": 1, "event_count": 2,
        "click_count": 0, "dead_click_count": 0,
        "checkout_started_count": 0, "add_to_cart_count": 0,
    }
    result_a = _score_once(session_a, {"pdp"}, "pdp", None)
    result_b = _score_once(session_b, {"home"}, "home", None)

    assert result_a[1] == "high"
    assert result_b[1] == "low"
    assert result_a != result_b
