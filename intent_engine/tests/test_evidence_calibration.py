"""The production fix: percentile thresholds are calibrated on sessions with behavioural evidence (stage >= 1) only,
stage 0 is always low, and stage 1 can never be high."""

import itertools
import random
import sqlite3
from datetime import date, datetime

import pytest

from intent_engine import repository, threshold_calibration
from intent_engine.config import FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD, MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION
from intent_engine.score_calculator import assign_bucket, assign_bucket_with_evidence, evidence_stage, evidence_stage_for_row


# ---------------- stage classification: boundary values ----------------

@pytest.mark.parametrize("pv,pages,useful,scrolls,duration_ms,expected", [
    (0, 0, 0, 0, 0, 0),
    (1, 1, 0, 0, 0, 0),                    # one product view and one page view alone: passive
    (2, 1, 0, 0, 0, 1), (1, 2, 0, 0, 0, 1),   # a second product view / page view
    (1, 1, 1, 0, 0, 1), (1, 1, 2, 0, 0, 2),   # 0 / 1 / 2 useful clicks
    (1, 1, 0, 1, 0, 1), (1, 1, 0, 2, 0, 2),   # 0 / 1 / 2 scrolls
    (1, 1, 0, 0, 9_990, 0), (1, 1, 0, 0, 9_999, 0), (1, 1, 0, 0, 10_000, 1),     # 9.99 s / 10 s
    (1, 1, 0, 0, 29_990, 1), (1, 1, 0, 0, 29_999, 1), (1, 1, 0, 0, 30_000, 2),   # 29.99 s / 30 s
    (2, 2, 0, 0, 0, 2), (2, 1, 0, 0, 0, 1), (5, 1, 1, 0, 0, 1), (1, 6, 1, 0, 0, 1),
])
def test_evidence_stage_boundaries(pv, pages, useful, scrolls, duration_ms, expected):
    assert evidence_stage(pv, pages, useful, scrolls, duration_ms) == expected


def test_the_stage_of_a_row_reads_the_scorer_columns_and_never_the_cart_or_checkout():
    row = {"product_view_count": 1, "page_view_count": 1, "useful_click_count": 0, "scroll_count": 0,
           "session_time_spent_ms": 500, "add_to_cart_count": 3, "checkout_started_count": 2}
    assert evidence_stage_for_row(row) == 0                     # commercial events do not lift a passive session
    assert evidence_stage_for_row({"session_time_spent_ms": None}) == 0
    assert evidence_stage_for_row({"product_view_count": 2, "page_view_count": 2, "session_time_spent_ms": 0}) == 2


# ---------------- the bucket rule ----------------

def test_stage_zero_is_always_low_and_stage_one_is_never_high():
    for score in (0.0, 5.0, 40.0, 99.0):
        assert assign_bucket_with_evidence(score, 10.0, 30.0, 0) == "low"
    assert assign_bucket_with_evidence(99.0, 10.0, 30.0, 1) == "medium"        # capped, not dropped to low
    assert assign_bucket_with_evidence(30.0, 10.0, 30.0, 1) == "medium"
    assert assign_bucket_with_evidence(9.99, 10.0, 30.0, 1) == "low"           # below P35 stays low
    assert assign_bucket_with_evidence(0.0, 0.0, 30.0, 1) == "low"             # a zero score is low even when P35 is 0
    assert assign_bucket_with_evidence(99.0, 10.0, 30.0, 2) == "high"


def test_exact_percentile_boundaries_are_unchanged():
    assert assign_bucket_with_evidence(10.0, 10.0, 30.0, 2) == "medium"        # exactly P35 is medium
    assert assign_bucket_with_evidence(30.0, 10.0, 30.0, 2) == "high"          # exactly P72 is high (stage 2)
    assert assign_bucket_with_evidence(29.99, 10.0, 30.0, 2) == "medium"
    for score in (0.0, 9.99, 10.0, 29.99, 30.0, 80.0):                         # stage 2 is exactly the old bucket
        assert assign_bucket_with_evidence(score, 10.0, 30.0, 2) == assign_bucket(score, 10.0, 30.0)


# ---------------- calibration population ----------------

def _row(score, pv=2, pages=3, useful=0, scrolls=0, duration_ms=60_000):
    return {"predictive_score": score, "product_view_count": pv, "page_view_count": pages, "useful_click_count": useful,
            "scroll_count": scrolls, "session_time_spent_ms": duration_ms}


def _passive(score=7.88):
    return _row(score, pv=1, pages=1, duration_ms=300)


def _calibrate(monkeypatch, population):
    saved = {}
    monkeypatch.setattr(threshold_calibration, "compute_dead_click_p90", lambda c, d: None)
    monkeypatch.setattr(threshold_calibration, "fetch_calibration_population_for_date", lambda c, d, m: population)
    monkeypatch.setattr(threshold_calibration, "upsert_daily_thresholds",
                        lambda c, conn, d, count, p35, p72, p90, method, version: saved.update(count=count, p35=p35, p72=p72, method=method))
    monkeypatch.setattr(threshold_calibration, "bulk_reassign_buckets_for_date", lambda *a: saved.update(reassigned_with=a[3:]) or 0)
    monkeypatch.setattr(threshold_calibration, "fetch_actor_ids_scored_on_date", lambda c, d: [])
    threshold_calibration.calibrate_date_for_brand(object(), object(), date(2026, 11, 14), datetime(2026, 11, 15, 2, 0))
    return saved


def _mixed_engaged(seed=1, n=800):
    rnd = random.Random(seed)
    rows = []
    for i in range(n):
        stage2 = i % 2 == 0
        rows.append(_row(round(rnd.uniform(1, 90), 2), pv=3 if stage2 else 2, pages=3 if stage2 else 1,
                         duration_ms=60_000 if stage2 else 2_000))
    return rows


def test_stage_zero_is_excluded_from_the_threshold_population_and_stage_one_and_two_are_included(monkeypatch):
    engaged = _mixed_engaged()
    stages = {evidence_stage_for_row(r) for r in engaged}
    assert stages == {1, 2}                                                    # both stages are in the population
    saved = _calibrate(monkeypatch, [_passive() for _ in range(3000)] + [_row(0.0) for _ in range(400)] + engaged)
    ordered = sorted(r["predictive_score"] for r in engaged)
    assert saved["method"] == "percentile"
    assert saved["p35"] == threshold_calibration._percentile(ordered, 35)
    assert saved["p72"] == threshold_calibration._percentile(ordered, 72)
    assert saved["count"] == 3000 + 400 + 800                                   # every scored session is still counted in the record
    assert saved["reassigned_with"] == (saved["p35"], saved["p72"])


def test_the_number_of_stage_zero_sessions_does_not_change_the_thresholds(monkeypatch):
    engaged = _mixed_engaged()
    few = _calibrate(monkeypatch, [_passive() for _ in range(10)] + engaged)
    many = _calibrate(monkeypatch, [_passive() for _ in range(5000)] + engaged)
    assert (few["p35"], few["p72"]) == (many["p35"], many["p72"])


def test_the_old_population_would_have_given_much_lower_cutoffs(monkeypatch):
    engaged = _mixed_engaged()
    everything = sorted([7.88] * 3000 + [r["predictive_score"] for r in engaged])
    saved = _calibrate(monkeypatch, [_passive() for _ in range(3000)] + engaged)
    assert threshold_calibration._percentile(everything, 35) == 7.88            # the plateau used to BE P35
    assert saved["p35"] > 3 * threshold_calibration._percentile(everything, 35)


def test_no_eligible_sessions_or_too_few_uses_the_fixed_fallback(monkeypatch):
    only_passive = _calibrate(monkeypatch, [_passive() for _ in range(5000)])
    assert (only_passive["method"], only_passive["p35"], only_passive["p72"]) == ("fixed_fallback", FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD)
    few = _calibrate(monkeypatch, [_passive() for _ in range(5000)] +
                     [_row(float(i + 1)) for i in range(MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION - 1)])
    assert few["method"] == "fixed_fallback"
    enough = _calibrate(monkeypatch, [_passive() for _ in range(5000)] +
                        [_row(float(i + 1)) for i in range(MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION)])
    assert enough["method"] == "percentile"                                     # 500 eligible positive scores


def test_zero_scores_of_eligible_sessions_stay_out_of_the_percentiles(monkeypatch):
    saved = _calibrate(monkeypatch, [_row(0.0) for _ in range(900)] + [_row(float(i)) for i in range(1, 601)])
    assert saved["p35"] > 150 and saved["p72"] > 400


# ---------------- the SQL reassignment applies exactly the same rule ----------------

def test_the_sql_reassignment_matches_the_python_rule_for_every_stage_combination():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE intent_sessions (session_id TEXT, session_start TEXT, intent_status TEXT, predictive_score REAL, "
        "product_view_count INT, page_view_count INT, useful_click_count INT, scroll_count INT, session_time_spent_ms INT, "
        "entry_page_type TEXT, checkout_started_count INT, add_to_cart_count INT, intent_bucket TEXT, operational_bucket TEXT, "
        "updated_at TEXT)")
    grid = list(itertools.product((0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 9_999, 10_000, 29_999, 30_000),
                                  (0.0, 5.0, 10.0, 20.0, 30.0, 60.0), (0, 1)))
    rows = []
    for i, (pv, pages, useful, scrolls, dur, score, commercial) in enumerate(grid):
        rows.append((f"s{i}", "2026-11-14 12:00:00", "scored", score, pv, pages, useful, scrolls, dur,
                     "home", commercial, 0, "x", "x", "t"))
    connection.executemany("INSERT INTO intent_sessions VALUES (" + ",".join("?" * 15) + ")", rows)

    class Cursor:
        rowcount = 0

        def execute(self, sql, params=()):
            self.rowcount = connection.execute(sql.replace("%s", "?"), params).rowcount

    repository.bulk_reassign_buckets_for_date(Cursor(), connection, date(2026, 11, 14), 10.0, 30.0)
    seen = set()
    for sid, pv, pages, useful, scrolls, dur, score, commercial, bucket, operational in connection.execute(
            "SELECT session_id, product_view_count, page_view_count, useful_click_count, scroll_count, session_time_spent_ms, "
            "predictive_score, checkout_started_count, intent_bucket, operational_bucket FROM intent_sessions"):
        stage = evidence_stage(pv, pages, useful, scrolls, dur)
        expected = assign_bucket_with_evidence(score, 10.0, 30.0, stage)
        assert bucket == expected, (pv, pages, useful, scrolls, dur, score)
        assert operational == ("high" if commercial else expected)               # the commercial override is untouched
        seen.add((stage, bucket))
    assert {(0, "low"), (1, "medium"), (2, "high"), (1, "low"), (2, "medium")} <= seen
    assert not {(0, "medium"), (0, "high"), (1, "high")} & seen
