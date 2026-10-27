"""A score of 0 is always "low", and zero scores do not drag the percentile cut-offs down."""

from datetime import date, datetime

from intent_engine import threshold_calibration
from intent_engine.score_calculator import assign_bucket


def test_a_zero_score_is_low_even_when_the_percentile_cutoff_is_zero():
    # a day where more than 35% of sessions scored 0 makes P35 itself 0; `score < 0` would never be true
    assert assign_bucket(0.0, 0.0, 12.0) == "low"
    assert assign_bucket(0.0, 25.0, 55.0) == "low"
    assert assign_bucket(0.5, 0.0, 12.0) == "medium"      # a real score is bucketed by the cut-offs as before
    assert assign_bucket(13.0, 0.0, 12.0) == "high"


def test_calibration_takes_the_percentiles_over_engaged_sessions_only(monkeypatch):
    # 700 bounces scoring 0, and 600 engaged sessions scoring 1..600
    scores = [0.0] * 700 + [float(i) for i in range(1, 601)]
    saved = {}
    monkeypatch.setattr(threshold_calibration, "compute_dead_click_p90", lambda c, d: None)
    monkeypatch.setattr(threshold_calibration, "fetch_scored_predictive_scores_for_date", lambda c, d, m: scores)
    monkeypatch.setattr(threshold_calibration, "upsert_daily_thresholds",
                        lambda c, conn, d, count, p35, p72, p90, method, version: saved.update(count=count, p35=p35, p72=p72, method=method))
    monkeypatch.setattr(threshold_calibration, "bulk_reassign_buckets_for_date", lambda *a: 0)
    monkeypatch.setattr(threshold_calibration, "fetch_actor_ids_scored_on_date", lambda c, d: [])

    threshold_calibration.calibrate_date_for_brand(object(), object(), date(2026, 10, 14), datetime(2026, 10, 15, 2, 0))

    assert saved["method"] == "percentile"
    assert saved["count"] == 1300                  # every scored session is still counted in the record
    assert saved["p35"] > 150 and saved["p72"] > 400   # cut-offs come from the 600 engaged scores, not from the zeros


def test_too_few_engaged_sessions_falls_back_to_the_fixed_cutoffs(monkeypatch):
    scores = [0.0] * 5000 + [10.0] * 100            # plenty of sessions, but only 100 engaged ones
    saved = {}
    monkeypatch.setattr(threshold_calibration, "compute_dead_click_p90", lambda c, d: None)
    monkeypatch.setattr(threshold_calibration, "fetch_scored_predictive_scores_for_date", lambda c, d, m: scores)
    monkeypatch.setattr(threshold_calibration, "upsert_daily_thresholds",
                        lambda c, conn, d, count, p35, p72, p90, method, version: saved.update(method=method))
    monkeypatch.setattr(threshold_calibration, "bulk_reassign_buckets_for_date", lambda *a: 0)
    monkeypatch.setattr(threshold_calibration, "fetch_actor_ids_scored_on_date", lambda c, d: [])

    threshold_calibration.calibrate_date_for_brand(object(), object(), date(2026, 10, 14), datetime(2026, 10, 15, 2, 0))

    assert saved["method"] == "fixed_fallback"
