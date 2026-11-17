"""
The scoring experiment engine (intent_engine/experiment.py): V1 is the production model on a common feature dataset,
V2 and V3 are placeholders, thresholds are per version, and nothing here touches a database.
"""

import csv
import json
import random
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta

import pytest

from intent_engine import experiment as ex
from intent_engine import replay as rp
from intent_engine.config import FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD, MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION
from intent_engine.feature_calculator import extract_features
from intent_engine.page_modifier import highest_intent_page_type, modifier_for
from intent_engine.penalties import bounce_penalty, struggle_penalty
from intent_engine.score_calculator import assign_bucket, compute_predictive_score

DAY = datetime(2026, 11, 13, 12, 0, 0)
HUMAN = "Mozilla/5.0 (iPhone)"


def make(sid="s1", duration_s=60, events=6, product_views=0, useful=0, dead=0, clicks=None, scrolls=0, page_views=2,
         atc=0, checkout=0, page_types=("home",), entry="home", ua=HUMAN, duration_ms="default", extras=None, **stored):
    row = {
        "session_id": sid, "actor_id": "a-" + sid, "session_start": DAY,
        "session_time_spent_ms": int(duration_s * 1000) if duration_ms == "default" else duration_ms,
        "event_count": events, "page_view_count": page_views, "product_view_count": product_views,
        "click_count": useful + dead if clicks is None else clicks, "useful_click_count": useful,
        "dead_click_count": dead, "add_to_cart_count": atc, "checkout_started_count": checkout, "scroll_count": scrolls,
    }
    row.update(stored)
    return ex.build_session(row, rp.Enrichment(entry, set(page_types), ua, set()), "BBB", extras)


def engaged_sessions(n, seed=3):
    rnd = random.Random(seed)
    return [make(f"s{i}", duration_s=rnd.choice([20, 60, 180, 400]), events=rnd.randint(3, 14),
                 product_views=rnd.randint(0, 5), useful=rnd.randint(0, 14), dead=rnd.randint(0, 4),
                 scrolls=rnd.randint(0, 9), page_views=rnd.randint(1, 8), atc=rnd.choice([0, 0, 1]),
                 page_types=rnd.choice([("home",), ("pdp", "home"), ("collection",), ("checkout", "pdp"), ("other",)]))
            for i in range(n)]


class FixedScores(ex.ScoringVersion):
    """A test version: the score of each session id is given."""
    name = "VX"
    description = "fixed scores"

    def __init__(self, scores):
        self.scores = scores

    def score(self, features, context):
        return ex.ScoreOutcome(self.scores[features.session_id])


# ---------------- V1 is the production model ----------------

def test_v1_equals_the_production_functions_composed_by_hand():
    p90 = 0.5
    context = ex.DayContext(p90)
    for s in engaged_sessions(200):
        features = extract_features(s.row)
        expected = compute_predictive_score(
            features, struggle_penalty(s.row, p90), bounce_penalty(s.row),
            modifier_for(highest_intent_page_type(s.page_types_seen)))
        out = ex.VERSIONS["V1"].score(s, context)
        assert out.score == pytest.approx(expected, abs=rp.PARITY_TOLERANCE)
        assert out.modifier == modifier_for(s.highest_page_type)


def test_v1_equals_the_replay_of_the_current_model_variant_a():
    sessions = engaged_sessions(300, seed=9)
    p90 = rp.dead_click_p90(s.row for s in sessions)
    context = ex.DayContext(p90)
    for s in sessions:
        enrichment = rp.Enrichment(s.entry_page_type, set(s.page_types_seen), s.user_agent, set())
        replay_score, _, _ = rp.score_session(dict(s.row), enrichment, rp.VARIANTS["A"], p90)
        assert ex.VERSIONS["V1"].score(s, context).score == pytest.approx(replay_score, abs=rp.PARITY_TOLERANCE)


def test_v1_reports_the_components():
    s = make(product_views=2, useful=3, scrolls=1, page_views=2, page_types=("pdp",), duration_s=120)
    out = ex.VERSIONS["V1"].score(s, ex.DayContext(None))
    assert out.raw_score == pytest.approx(2 * 11.0 + 3 * 2.2 + 2 * 4.5 + 1 * 1.8 + 2 * 1.5)
    assert out.struggle_penalty == 0.0 and out.bounce_penalty == 0.0 and out.modifier == 1.05
    assert out.components["page_type"] == "pdp"


# ---------------- buckets ----------------

@pytest.mark.parametrize("score,bucket", [(0, "low"), (24.99, "low"), (25, "medium"), (54.99, "medium"),
                                          (55, "high"), (80, "high")])
def test_bucket_boundaries_follow_production(score, bucket):
    assert assign_bucket(score, 25.0, 55.0) == bucket


def test_a_zero_score_is_low_even_when_p35_is_zero():
    assert assign_bucket(0, 0.0, 0.0) == "low"


def test_a_run_buckets_by_the_boundaries_and_rounds_scores_to_two_decimals(monkeypatch):
    scores = {"a": 0, "b": 24.994, "c": 25.0, "d": 54.99, "e": 55.0, "f": 90}
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores(scores))
    run = ex.run_experiment([make(sid) for sid in scores], ["VX"], threshold_mode="production")
    got = {r.session_id: (r.score, r.bucket) for r in run.results["VX"]}
    assert got == {"a": (0.0, "low"), "b": (24.99, "low"), "c": (25.0, "medium"), "d": (54.99, "medium"),
                   "e": (55.0, "high"), "f": (90.0, "high")}
    assert run.thresholds["VX"].method == "fixed_fallback"
    assert (run.thresholds["VX"].p35, run.thresholds["VX"].p72) == (FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD)


def test_the_operational_bucket_is_separate_and_never_changes_the_score():
    s = make("c1", duration_s=2, events=2, atc=1, page_types=("other",))          # a bounce that added to cart
    run = ex.run_experiment([s, make("c2", duration_s=90, product_views=3, useful=6, page_types=("pdp",))], ["V1"])
    by_id = {r.session_id: r for r in run.results["V1"]}
    assert by_id["c1"].operational_bucket == "high" and by_id["c1"].bucket == "low"
    assert by_id["c1"].score == 0.0


# ---------------- thresholds ----------------

def test_thresholds_are_nearest_rank_percentiles_of_the_engaged_scores():
    scores = [float(i) for i in range(1, 501)]
    th = ex.compute_thresholds(scores + [0.0] * 50)             # zeros stay out of the population
    assert th.population == 500 and th.method == "percentile"
    assert th.p35 == rp._percentile(sorted(scores), 35) and th.p72 == rp._percentile(sorted(scores), 72)


def test_common_and_production_differ_only_under_the_minimum_population():
    few = [float(i) for i in range(1, MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION)]        # 499
    common, production = ex.compute_thresholds(few, "common"), ex.compute_thresholds(few, "production")
    assert common.method == "percentile_small_sample" and common.p35 == rp._percentile(few, 35)
    assert production.method == "fixed_fallback" and (production.p35, production.p72) == (25.0, 55.0)
    enough = [float(i) for i in range(1, MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION + 1)]  # 500
    assert ex.compute_thresholds(enough, "common") == ex.compute_thresholds(enough, "production")


def test_no_engaged_sessions_gives_zero_thresholds_and_everything_low():
    th = ex.compute_thresholds([0.0, 0.0, 0.0])
    assert (th.p35, th.p72, th.method, th.population) == (0.0, 0.0, "no_engaged_sessions", 0)


def test_unknown_threshold_mode_is_refused():
    with pytest.raises(ValueError):
        ex.compute_thresholds([1.0], "mystery")


def test_each_version_gets_its_own_thresholds(monkeypatch):
    sessions = [make(f"s{i}") for i in range(10)]
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores({f"s{i}": float(i + 1) for i in range(10)}))
    monkeypatch.setitem(ex.VERSIONS, "VY", type("VY", (FixedScores,), {"name": "VY"})(
        {f"s{i}": float((i + 1) * 100) / 10 for i in range(10)}))
    run = ex.run_experiment(sessions, ["VX", "VY"])
    assert run.thresholds["VX"] != run.thresholds["VY"]


# ---------------- placeholders and version isolation ----------------

def test_a_registered_slot_without_a_formula_refuses_to_score_and_to_be_selected(monkeypatch):
    monkeypatch.setitem(ex.VERSIONS, "V9", ex.NotImplementedVersion("V9"))
    assert not ex.VERSIONS["V9"].implemented
    with pytest.raises(ex.VersionNotImplemented):
        ex.VERSIONS["V9"].score(make(), ex.DayContext(None))
    with pytest.raises(ex.VersionNotImplemented):
        ex.resolve_versions(["V9"])
    run = ex.run_experiment([make("s1", product_views=2, useful=4, duration_s=90)])       # the default skips it, and says so
    assert run.versions == ["V1", "V2", "V3", "V4", "V5", "V1C", "V6"] and run.skipped_versions == {"V9": "NOT_IMPLEMENTED"}
    assert "V9: NOT_IMPLEMENTED" in ex.build_report(ex.summarize(run))


def test_the_default_run_scores_v1_to_v5():
    run = ex.run_experiment([make("s1", product_views=2, useful=4, duration_s=90)])
    assert run.versions == ["V1", "V2", "V3", "V4", "V5", "V1C", "V6"] and run.skipped_versions == {}
    assert ex.DEFAULT_VERSIONS == ("V1", "V2", "V3", "V4", "V5", "V1C", "V6")
    assert all(ex.VERSIONS[n].experimental for n in ("V2", "V3", "V4", "V5")) and not ex.VERSIONS["V1"].experimental


def test_a_selection_runs_in_registry_order_whatever_the_order_asked():
    assert ex.resolve_versions(["v3", "v2"]) == ["V2", "V3"]
    assert ex.resolve_versions(["V2", "V1", "v2"]) == ["V1", "V2"]


def test_unknown_version_is_refused():
    with pytest.raises(ValueError):
        ex.resolve_versions(["V9"])


class Mutator(ex.ScoringVersion):
    name = "VM"

    def score(self, features, context):
        features.row["product_view_count"] = 99          # a version must not be able to change the shared input
        return ex.ScoreOutcome(1.0)


def test_a_version_cannot_mutate_the_features_or_change_v1(monkeypatch):
    sessions = engaged_sessions(40, seed=5)
    alone = ex.run_experiment(sessions, ["V1"])
    monkeypatch.setitem(ex.VERSIONS, "VM", Mutator())
    both = ex.run_experiment(sessions, ["V1", "VM"])
    assert [r.as_row() for r in both.results["V1"]] == [r.as_row() for r in alone.results["V1"]]
    assert all(r.status == "error" and "TypeError" in r.error_reason for r in both.results["VM"]
               if r.status != "excluded")
    assert sessions[0].row["product_view_count"] == engaged_sessions(40, seed=5)[0].row["product_view_count"]
    with pytest.raises(FrozenInstanceError):
        sessions[0].session_id = "other"
    with pytest.raises(TypeError):
        sessions[0].extras["x"] = 1


# ---------------- statuses and errors ----------------

def test_every_session_gets_a_status_and_a_reason():
    bot = make("bot", ua="Googlebot/2.1")
    zero = make("zero", duration_s=0.2, events=1, clicks=0, page_views=1)
    pending = make("pend", duration_ms=None)
    invalid = make("neg", duration_ms=-5)
    fine = make("ok", duration_s=120, product_views=2, useful=4)
    run = ex.run_experiment([bot, zero, pending, invalid, fine], ["V1"])
    got = {r.session_id: (r.status, r.exclusion_reason, r.score) for r in run.results["V1"]}
    assert got["bot"] == ("excluded", "bot", None)
    assert got["zero"] == ("excluded", "zero_signal", None)
    assert got["pend"] == ("pending", "missing_duration", None)
    assert got["neg"] == ("excluded", "invalid_session", None)
    assert got["ok"][0] == "scored" and got["ok"][2] > 0
    summary = ex.summarize(run)["versions"]["V1"]["population"]
    assert summary == {"total": 5, "eligible": 1, "scored": 1, "excluded": 3, "excluded_by_reason": {"bot": 1, "invalid_session": 1,
                       "zero_signal": 1}, "pending": 1, "errors": 0}


def test_one_failing_session_becomes_an_error_and_the_run_continues(monkeypatch):
    class Flaky(FixedScores):
        def score(self, features, context):
            if features.session_id == "boom":
                raise RuntimeError("bad session")
            return super().score(features, context)

    monkeypatch.setitem(ex.VERSIONS, "VX", Flaky({"a": 10.0, "boom": 0.0, "c": 30.0}))
    run = ex.run_experiment([make("a"), make("boom"), make("c")], ["VX"])
    got = {r.session_id: r for r in run.results["VX"]}
    assert got["boom"].status == "error" and "RuntimeError: bad session" in got["boom"].error_reason
    assert got["a"].status == "scored" and got["c"].status == "scored"
    assert ex.summarize(run)["versions"]["VX"]["population"]["errors"] == 1


def test_a_non_finite_score_is_an_error_not_a_bucket(monkeypatch):
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores({"a": float("nan"), "b": 5.0}))
    run = ex.run_experiment([make("a"), make("b")], ["VX"])
    got = {r.session_id: r.status for r in run.results["VX"]}
    assert got == {"a": "error", "b": "scored"}


# ---------------- outcomes ----------------

def test_purchases_are_unknown_by_default_and_nothing_is_claimed():
    run = ex.run_experiment([make("s1", product_views=1, useful=2, duration_s=60)], ["V1"])
    assert run.results["V1"][0].purchased == "unknown" and run.results["V1"][0].order_value == "unknown"
    assert ex.summarize(run)["versions"]["V1"]["purchase_outcomes"]["available"] is False


def test_a_provider_flows_into_the_results_and_the_per_bucket_report():
    class Fake(ex.OutcomeProvider):
        name = "fake"

        def lookup(self, session_ids):
            return {"s1": ex.Outcome(True, 120.0), "s2": ex.Outcome(False, None)}

    sessions = [make("s1", product_views=3, useful=8, duration_s=200, page_types=("pdp",)),
                make("s2", product_views=1, useful=2, duration_s=60)]
    run = ex.run_experiment(sessions, ["V1"], outcomes=Fake())
    got = {r.session_id: (r.purchased, r.order_value) for r in run.results["V1"]}
    assert got == {"s1": (True, 120.0), "s2": (False, None)}
    p = ex.summarize(run)["versions"]["V1"]["purchase_outcomes"]
    assert p["available"] and sum(b["purchases"] for b in p["by_bucket"].values()) == 1


def test_atc_and_checkout_rates_are_reported_as_behavioural_outcomes():
    sessions = [make(f"s{i}", product_views=2, useful=5, duration_s=100, atc=i % 2) for i in range(6)]
    summary = ex.summarize(ex.run_experiment(sessions, ["V1"]))["versions"]["V1"]
    assert "behavioural_outcomes_not_purchases" in summary
    assert sum(c["atc_rate_pct"] * c["sessions"] for c in summary["composition_by_bucket"].values()) == pytest.approx(300)


# ---------------- parity, transitions, Clarity ----------------

def test_parity_counts_matches_and_mismatches():
    s_ok = make("ok", duration_s=120, product_views=2, useful=4, predictive_score=None, intent_status="scored",
                intent_bucket="medium")
    probe = ex.run_experiment([s_ok], ["V1"]).results["V1"][0]
    same = make("same", duration_s=120, product_views=2, useful=4, predictive_score=probe.score,
                intent_status="scored", intent_bucket="low")
    off = make("off", duration_s=120, product_views=2, useful=4, predictive_score=probe.score + 3.5,
               intent_status="scored", intent_bucket="high")
    pending = make("pending", duration_s=120, product_views=2, useful=4, predictive_score=None, intent_status="pending")
    report = ex.parity_report(ex.run_experiment([same, off, pending], ["V1"]).results["V1"])
    assert report["compared"] == 2 and report["exact_matches"] == 1 and report["within_tolerance"] == 1
    assert report["mismatches"] == 1 and report["max_abs_difference"] == pytest.approx(3.5)
    assert report["mean_abs_difference"] == pytest.approx(1.75) and report["why_mismatches_happen"]


def test_parity_with_nothing_stored_is_an_empty_report():
    report = ex.parity_report(ex.run_experiment([make("s1")], ["V1"]).results["V1"])
    assert report["compared"] == 0 and report["max_abs_difference"] is None


def test_transitions_count_each_bucket_pair_for_sessions_scored_in_both(monkeypatch):
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores({"a": 1.0, "b": 2.0, "c": 3.0}))
    monkeypatch.setitem(ex.VERSIONS, "VY", type("VY", (FixedScores,), {"name": "VY"})({"a": 3.0, "b": 2.0, "c": 1.0}))
    run = ex.run_experiment([make("a"), make("b"), make("c")], ["VX", "VY"])
    matrix = ex.summarize(run)["transitions"]["VX->VY"]
    assert sum(matrix.values()) == 3 and matrix["low->high"] == 1 and matrix["high->low"] == 1


def test_clarity_distance_and_unavailable():
    assert ex.clarity_comparison({"low": 50, "medium": 30, "high": 20}, 100, None) == {
        "available": False, "message": "Clarity comparison: unavailable"}
    c = ex.clarity_comparison({"low": 50.0, "medium": 30.0, "high": 20.0}, 100,
                              {"low_intent_pct": 40, "medium_intent_pct": 40, "high_intent_pct": 20, "total_sessions": 120})
    assert c["delta_pp"] == {"low": 10.0, "medium": -10.0, "high": 0.0} and c["distance_pp"] == 10.0
    assert ex.clarity_comparison({"low": 1, "medium": 1, "high": 1}, 3, {"low_intent_pct": None})["available"] is False


# ---------------- output files ----------------

def test_a_small_fixture_writes_csv_json_and_report(tmp_path):
    sessions = [make("a", product_views=2, useful=5, duration_s=100, atc=1, page_types=("pdp",)),
                make("b", duration_s=0.2, events=1, clicks=0, page_views=1), make("c", ua="bingbot")]
    run = ex.run_experiment(sessions, ["V1"], brand_id="BBB", day=date(2026, 11, 13))
    summary = ex.summarize(run, {"low_intent_pct": 40, "medium_intent_pct": 45, "high_intent_pct": 15, "total_sessions": 9})
    paths = ex.write_outputs(run, summary, tmp_path)
    assert paths["csv"].parent == tmp_path / "BBB" / "2026-11-13"
    assert paths["csv"].name == "session_scores.csv" and paths["long_csv"].name == "session_scores_long.csv"
    with open(paths["long_csv"], newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert list(rows[0].keys()) == list(ex.RESULT_COLUMNS) and len(rows) == 3
    assert {r["session_id"]: r["status"] for r in rows} == {"a": "scored", "b": "excluded", "c": "excluded"}
    assert rows[0]["purchased"] == "unknown" and rows[0]["version"] == "V1"
    data = json.loads(paths["json"].read_text(encoding="utf-8"))
    for key in ("brand", "date", "versions", "transitions", "v1_parity", "threshold_mode", "outcome_source"):
        assert key in data
    assert data["versions"]["V1"]["clarity"]["available"] is True
    report = paths["report"].read_text(encoding="utf-8")
    for heading in ("Population", "intent_bucket", "Score distribution", "operational_bucket", "V1 parity", "Clarity",
                    "Purchase outcomes"):
        assert heading in report


def test_the_result_columns_cover_the_requested_fields():
    for column in ("session_id", "brand_id", "session_start", "version", "score", "bucket", "operational_bucket", "status",
                   "exclusion_reason", "product_views", "useful_clicks", "dead_clicks", "click_count", "scrolls",
                   "page_views", "minutes", "event_count", "add_to_cart_count", "checkout_started_count",
                   "entry_page_type", "highest_page_type", "struggle_penalty", "bounce_penalty", "page_modifier",
                   "p35", "p72", "p90", "threshold_method", "production_score", "production_bucket", "purchased",
                   "order_value"):
        assert column in ex.RESULT_COLUMNS


def test_event_names_are_read_from_the_stored_sequence():
    s = make("seq", event_sequence=json.dumps({"2": {"product_viewed": "e2"}, "1": {"page_viewed": "e1"}, "x": {"a": 1}}))
    assert s.event_names() == ["page_viewed", "product_viewed"]
    assert make("none").event_names() == []


# ================= three versions over one dataset (V1 production, V2 and V3 EXPERIMENTAL) =================

def three(sessions, mode="common", **kw):
    return ex.run_experiment(sessions, ["V1", "V2", "V3"], threshold_mode=mode, **kw)


def classify_scored(session):
    return ex.classify_session(session)[0] == "scored"


def test_one_dataset_goes_through_all_three_scorers_and_is_left_unchanged(monkeypatch):
    sessions = engaged_sessions(80, seed=21)
    before = [(s.session_id, dict(s.row), s.page_types_seen) for s in sessions]
    seen = {}

    class Spy(ex.ScoringVersion):
        def __init__(self, name):
            self.name = name

        def score(self, features, context):
            seen.setdefault(self.name, []).append(id(features))
            return ex.VERSIONS["V1"].score(features, context)

    monkeypatch.setitem(ex.VERSIONS, "SA", Spy("SA"))
    monkeypatch.setitem(ex.VERSIONS, "SB", Spy("SB"))
    run = ex.run_experiment(sessions, ["V1", "V2", "V3", "SA", "SB"])
    assert run.versions == ["V1", "V2", "V3", "SA", "SB"] and run.sessions == sessions
    assert seen["SA"] == seen["SB"] == [id(s) for s in sessions if classify_scored(s)]   # the very same objects
    assert [(s.session_id, dict(s.row), s.page_types_seen) for s in sessions] == before
    assert all(len(run.results[name]) == len(sessions) for name in run.versions)


def test_all_three_versions_are_deterministic():
    sessions = engaged_sessions(120, seed=4)
    first, second = three(sessions), three(sessions)
    for name in ("V1", "V2", "V3"):
        assert [r.as_row() for r in first.results[name]] == [r.as_row() for r in second.results[name]]
    assert first.thresholds == second.thresholds








def test_a_score_exactly_at_p35_is_medium_and_just_below_is_low(monkeypatch):
    ids = [f"s{i}" for i in range(600)]
    scores = {sid: float(i + 1) for i, sid in enumerate(ids)}               # 1.0 .. 600.0, all engaged
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores(scores))
    run = ex.run_experiment([make(sid) for sid in ids], ["VX"], threshold_mode="common")
    th = run.thresholds["VX"]
    by_score = {r.score: r.bucket for r in run.results["VX"]}
    assert th.p35 in by_score and by_score[th.p35] == "medium"              # score == P35 -> medium
    assert by_score[th.p35 - 1] == "low"                                    # score < P35 -> low
    assert by_score[th.p72] == "high" and by_score[th.p72 - 1] == "medium"  # score >= P72 -> high
    assert not any(r.bucket == "low" and r.score >= th.p35 for r in run.results["VX"])


def test_production_mode_is_still_available_and_uses_the_fixed_fallback_for_small_days():
    run = three(engaged_sessions(50, seed=1), mode="production")
    assert all(t.method == "fixed_fallback" and (t.p35, t.p72) == (25.0, 55.0) for t in run.thresholds.values())
    common = three(engaged_sessions(50, seed=1), mode="common")
    assert all(t.method == "percentile_small_sample" for t in common.thresholds.values())


def test_v1_in_a_three_version_run_is_unchanged_by_v2_and_v3():
    sessions = engaged_sessions(150, seed=12)
    alone = ex.run_experiment(sessions, ["V1"])
    assert [r.as_row() for r in three(sessions).results["V1"]] == [r.as_row() for r in alone.results["V1"]]



def test_the_wide_csv_has_one_row_per_session_with_every_version_side_by_side(tmp_path):
    sessions = [make("a", product_views=2, useful=5, duration_s=100, atc=1, page_types=("pdp",)),
                make("b", duration_s=0.2, events=1, clicks=0, page_views=1), make("c", ua="bingbot")]
    run = three(sessions, brand_id="BBB", day=date(2026, 11, 13))
    paths = ex.write_outputs(run, ex.summarize(run), tmp_path)
    with open(paths["csv"], newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert list(rows[0].keys()) == [
        "session_id", "brand", "session_start", "v1_score", "v1_bucket", "v1_operational_bucket",
        "v2_score", "v2_bucket", "v2_operational_bucket", "v3_score", "v3_bucket", "v3_operational_bucket",
        "product_views", "useful_clicks", "dead_clicks", "scrolls", "page_views", "duration", "event_count", "atc",
        "checkout_started", "status", "exclusion_reason", "error_versions", "production_score", "production_bucket"]
    assert [r["session_id"] for r in rows] == ["a", "b", "c"]
    a = rows[0]
    assert a["brand"] == "BBB" and a["atc"] == "1" and a["v1_bucket"] and a["v2_score"] and a["v3_score"]
    assert float(a["v3_score"]) > float(a["v2_score"])
    assert rows[1]["status"] == "excluded" and rows[1]["v1_score"] == "" and rows[1]["exclusion_reason"] == "zero_signal"
    with open(paths["long_csv"], newline="", encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 9                      # 3 sessions x 3 versions


def test_the_wide_csv_follows_the_selected_versions():
    run = ex.run_experiment([make("a", product_views=2, useful=5, duration_s=100)], ["V2", "V3"], brand_id="BBB",
                            day=date(2026, 11, 13))
    header = ex.wide_columns(run.versions)
    assert "v1_score" not in header and "v2_score" in header and "v3_score" in header


def test_the_summary_reports_each_version_with_transition_matrices_for_every_pair():
    run = three(engaged_sessions(200, seed=2))
    summary = ex.summarize(run, {"low_intent_pct": 40, "medium_intent_pct": 45, "high_intent_pct": 15,
                                 "total_sessions": 300})
    for name in ("V1", "V2", "V3"):
        v = summary["versions"][name]
        assert {"total", "eligible", "excluded", "scored"} <= set(v["population"])
        assert {"min", "P25", "P50", "P75", "P90", "max"} <= set(v["score_distribution"])
        assert set(v["behavioural_outcomes_not_purchases"]) == {"low", "medium", "high"}
        assert v["clarity"]["available"] and set(v["clarity"]["delta_pp"]) == {"low", "medium", "high"}
    assert set(summary["transitions"]) == {"V1->V2", "V1->V3", "V2->V3"}
    for matrix in summary["transitions"].values():
        assert sum(matrix.values()) == summary["versions"]["V1"]["population"]["scored"]
    assert summary["v1_parity"] is not None and "no_winner_selected" in summary
    assert summary["versions"]["V2"]["experimental"] and summary["versions"]["V3"]["experimental"]
    assert not summary["versions"]["V1"]["experimental"]



def test_without_clarity_the_report_says_unavailable():
    text = ex.build_report(ex.summarize(three(engaged_sessions(30, seed=2))))
    assert "Clarity comparison: unavailable" in text


def test_the_progress_hook_is_called_once_per_version_in_order():
    calls = []
    ex.run_experiment(engaged_sessions(5), ["V1", "V2", "V3"], progress=calls.append)
    assert calls == ["V1", "V2", "V3"]


# ================= V2 (non-destructive bounce) and V3 (behavioural progression): EXPERIMENTAL hypotheses =================
import ast
import inspect
import textwrap
from dataclasses import replace
from types import MappingProxyType


def landing(sid="land", page_type="pdp", **kw):
    """The dominant zero-score pattern: 1 product view, 1 page view, 2 events, a fraction of a second."""
    return make(sid, product_views=1, page_views=1, events=2, duration_s=0.01, page_types=(page_type,), **kw)


def score_of(name, session, p90=None):
    return ex.VERSIONS[name].score(session, ex.DayContext(p90))


class GuardedRow(dict):
    """A session row that fails the moment a scorer reads add-to-cart or checkout."""
    FORBIDDEN = {"add_to_cart_count", "checkout_started_count"}

    def __getitem__(self, key):
        if key in self.FORBIDDEN:
            raise AssertionError(f"a scorer read the outcome field {key}")
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key in self.FORBIDDEN:
            raise AssertionError(f"a scorer read the outcome field {key}")
        return super().get(key, default)


def guarded(session):
    return replace(session, row=MappingProxyType(GuardedRow(session.row)))


# ---------------- V1 stays the production model ----------------

def test_v1_still_wipes_the_landing_session_to_zero_which_is_what_v2_and_v3_test():
    out = score_of("V1", landing())
    assert out.raw_score == pytest.approx(12.5, abs=0.001) and out.bounce_penalty == -20.0 and out.score == 0.0


# ---------------- V2: a bounded bounce adjustment instead of a flat -20 ----------------

def test_v2_a_short_landing_session_gets_a_small_positive_score():
    out = score_of("V2", landing())
    assert out.bounce_penalty == -5.0 and out.struggle_penalty == 0.0
    assert out.score == pytest.approx((12.5 - 5.0) * 1.05, abs=0.001) and out.score > 0       # about 7.88
    assert score_of("V1", landing()).score == 0.0


@pytest.mark.parametrize("events,duration_s,expected", [
    (2, 0.01, -5.0),          # few events AND short
    (2, 60, -2.0),            # few events only
    (8, 2, -2.0),             # short only
    (8, 5, 0.0),              # exactly 5 s is not short (production: duration < 5000 ms)
    (3, 60, 0.0), (2, 4.99, -5.0), (8, 4.99, -2.0),
])
def test_v2_bounce_adjustment_cases(events, duration_s, expected):
    s = make("b", events=events, duration_s=duration_s, product_views=2, useful=3, page_views=3, page_types=("pdp",))
    assert score_of("V2", s).bounce_penalty == expected


def test_v2_bounce_adjustment_is_bounded_and_never_the_old_minus_20():
    seen = {score_of("V2", s).bounce_penalty for s in engaged_sessions(400, seed=31) + [landing(f"l{i}") for i in range(20)]}
    assert seen <= {-5.0, -2.0, 0.0} and min(seen) == -5.0


def test_v2_equals_v1_when_there_is_no_bounce():
    for s in engaged_sessions(200, seed=6):
        if ex.VERSIONS["V1"].score(s, ex.DayContext(None)).bounce_penalty == 0:
            assert score_of("V2", s).score == pytest.approx(score_of("V1", s).score)


def test_v2_keeps_the_production_struggle_penalty_and_page_modifier():
    s = make("s", duration_s=120, clicks=20, useful=4, dead=16, product_views=2, page_types=("home",))
    out = score_of("V2", s, p90=0.5)
    assert out.struggle_penalty == -12.0 and out.modifier == 0.85
    assert out.raw_score == score_of("V1", s, p90=0.5).raw_score


def test_v2_never_reads_or_depends_on_atc_or_checkout():
    base = dict(product_views=2, useful=3, page_views=3, duration_s=90, page_types=("pdp",))
    plain = score_of("V2", make("p", **base)).score
    assert score_of("V2", make("a", atc=4, **base)).score == plain
    assert score_of("V2", make("c", checkout=3, **base)).score == plain
    assert score_of("V2", guarded(make("g", atc=1, checkout=1, **base))).score == plain      # raises if it touches them


# ---------------- V3: a behavioural stage / progression model ----------------

def test_v3_matches_its_documented_formula():
    s = make("f", product_views=2, page_views=3, useful=5, scrolls=1, duration_s=120, events=8, page_types=("pdp",))
    stage = 2 * 8 + 3 * 3 + 5 * 4 + 2 * 4 + 1 * 1                               # 54
    bonuses = 5 * 5                                                              # all five progression bonuses
    out = score_of("V3", s)
    assert out.raw_score == pytest.approx(stage + bonuses) and out.bounce_penalty == 0.0
    assert out.score == pytest.approx((stage + bonuses) * 1.05)
    assert {"bonus_page_views_ge2", "bonus_product_views_ge2", "bonus_useful_clicks_ge2", "bonus_useful_clicks_ge5",
            "bonus_product_and_pages"} <= set(out.reason_codes)


def test_v3_caps_each_signal():
    s = make("cap", product_views=40, useful=90, scrolls=70, page_views=60, duration_s=5000, events=300, page_types=("home",))
    assert score_of("V3", s).raw_score == pytest.approx(4 * 8 + 6 * 3 + 12 * 4 + 6 * 4 + 8 * 1 + 25)


def test_v3_a_short_landing_session_is_not_wiped_to_zero():
    out = score_of("V3", landing())
    assert out.bounce_penalty == -2.0 and out.score == pytest.approx((8 + 3 - 2) * 1.05, abs=0.01) and out.score > 0   # about 9.45
    short_but_active = make("sa", product_views=1, page_views=1, useful=3, events=2, duration_s=1, page_types=("pdp",))
    assert score_of("V3", short_but_active).score > 20                          # a bounce cannot erase real interaction


@pytest.mark.parametrize("events,duration_s,expected", [(2, 0.01, -2.0), (2, 60, -1.0), (8, 2, -1.0), (8, 60, 0.0), (8, 5, 0.0)])
def test_v3_bounce_adjustment_cases(events, duration_s, expected):
    s = make("b", events=events, duration_s=duration_s, product_views=2, useful=3, page_views=3, page_types=("pdp",))
    assert score_of("V3", s).bounce_penalty == expected


def test_v3_bounce_adjustment_is_bounded():
    seen = {score_of("V3", s).bounce_penalty for s in engaged_sessions(400, seed=32) + [landing(f"l{i}") for i in range(20)]}
    assert seen <= {-2.0, -1.0, 0.0}


@pytest.mark.parametrize("label,low,high", [
    ("page views 1 -> 2", dict(page_views=1), dict(page_views=2)),
    ("product views 1 -> 2", dict(product_views=1, page_views=1), dict(product_views=2, page_views=1)),
    ("useful clicks 1 -> 2", dict(useful=1), dict(useful=2)),
    ("useful clicks 4 -> 5", dict(useful=4), dict(useful=5)),
])
def test_v3_progression_signals_raise_the_score_by_more_than_their_weight(label, low, high):
    common = dict(product_views=1, page_views=1, useful=0, duration_s=90, events=8, page_types=("home",))
    a = score_of("V3", make("lo", **{**common, **low})).raw_score
    b = score_of("V3", make("hi", **{**common, **high})).raw_score
    assert b - a > 4.0, label                                     # more than the plain weight: the bonus is included


def test_v3_progression_bonuses_are_behavioural_only():
    from intent_engine.feature_calculator import SessionFeatures
    none = ex.v3_progression_bonuses(SessionFeatures(0, 0, 0.0, 0, 1))
    assert none == {}
    full = ex.v3_progression_bonuses(SessionFeatures(2, 5, 1.0, 0, 2))
    assert set(full) == {"bonus_page_views_ge2", "bonus_product_views_ge2", "bonus_useful_clicks_ge2",
                         "bonus_useful_clicks_ge5", "bonus_product_and_pages"} and set(full.values()) == {5.0}


def test_v3_does_not_apply_the_struggle_penalty_and_says_so():
    s = make("s", duration_s=120, clicks=20, useful=4, dead=16, product_views=2, page_types=("home",))
    assert score_of("V2", s, p90=0.5).struggle_penalty == -12.0
    v3 = score_of("V3", s, p90=0.5)
    assert v3.struggle_penalty == 0.0 and "struggle_penalty" not in v3.reason_codes
    assert any("NO struggle penalty" in d for d in ex.VERSIONS["V3"].experimental_differences)
    assert "NO struggle penalty" in ex.VERSIONS["V3"].formula



# ---------------- V3 circularity: ATC and checkout must never be scoring inputs ----------------

def test_v3_scores_are_identical_whatever_the_atc_and_checkout_values_are():
    base = dict(product_views=2, useful=3, page_views=3, duration_s=90, events=7, page_types=("pdp",))
    plain = score_of("V3", make("p", **base))
    for extra in (dict(atc=1), dict(atc=9), dict(checkout=1), dict(checkout=5), dict(atc=2, checkout=2)):
        other = score_of("V3", make("x", **base, **extra))
        assert other.score == plain.score and other.raw_score == plain.raw_score and other.reason_codes == plain.reason_codes


def test_v3_never_touches_the_atc_or_checkout_fields():
    s = guarded(make("g", product_views=2, useful=3, page_views=3, duration_s=90, atc=3, checkout=2, page_types=("pdp",)))
    assert score_of("V3", s).score > 0                                  # the guarded row raises AssertionError on a read


def _identifiers(function):
    """Every name, attribute name and non-docstring string literal in a function's source."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    docstrings = set()
    for node in ast.walk(tree):                                          # the docstring literals, by identity
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.body and isinstance(node.body[0], ast.Expr)                 and isinstance(node.body[0].value, ast.Constant) and isinstance(node.body[0].value.value, str):
            docstrings.add(id(node.body[0].value))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and id(node) in docstrings:
            continue
        if isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.Attribute):
            found.add(node.attr)
        elif isinstance(node, ast.arg):
            found.add(node.arg)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            found.add(node.value)
    return found


def _mentions_an_outcome(names):
    for name in names:
        lowered = str(name).lower()
        if "add_to_cart" in lowered or "checkout" in lowered or "atc" in lowered.replace("-", "_").split("_"):
            return True
    return False


V3_SCORING_PATH = (ex.V3BehavioralProgression.score, ex.v3_stage_score, ex.v3_progression_bonuses, ex.session_evidence_stage,
                   ex.evidence_stage, ex.meaningful_evidence, ex.strong_evidence, ex.weighted_terms, ex.bounce_adjustment,
                   ex.clamp_score)


def test_v3_scoring_path_does_not_reference_atc_or_checkout():
    for function in V3_SCORING_PATH:
        names = _identifiers(function)
        assert not _mentions_an_outcome(names), (function.__qualname__, sorted(n for n in names if _mentions_an_outcome([n])))


def test_the_circularity_check_would_catch_someone_adding_them_back():
    evil = textwrap.dedent('''
        def score(self, features, context):
            bonus = 15.0 if features.add_to_cart_count > 0 else 0.0
            return bonus + (20.0 if features.checkout_started_count else 0.0)
    ''')
    tree = ast.parse(evil)
    names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert _mentions_an_outcome(names)
    assert not _mentions_an_outcome({"page_views", "product_views", "useful_clicks"})


def test_v2_scoring_path_also_does_not_reference_atc_or_checkout():
    assert not _mentions_an_outcome(_identifiers(ex.V2NonDestructiveBounce.score))


# ---------------- thresholds are unchanged and per version ----------------

def mixed_sessions(n_land=300, n_active=400, seed=9):
    return [landing(f"l{i}", page_type=("pdp", "collection", "home")[i % 3]) for i in range(n_land)] + engaged_sessions(n_active, seed=seed)




# ---------------- the new report sections ----------------

def test_zero_score_diagnostics_exist_for_every_version():
    summary = ex.summarize(three(mixed_sessions()))
    keys = {"zero_score_count", "zero_score_pct_of_scored", "zero_score_pct_of_all_sessions",
            "bounce_adjustment_or_penalty_applied_pct", "raw_positive_but_final_zero", "atc_among_zero", "checkout_among_zero",
            "duration_over_5s_among_zero"}
    for name in ("V1", "V2", "V3"):
        assert keys <= set(summary["versions"][name]["zero_score_diagnostics"])
    assert summary["versions"]["V1"]["zero_score_diagnostics"]["zero_score_count"] >= 300         # the landing sessions
    assert summary["versions"]["V1"]["zero_score_diagnostics"]["raw_positive_but_final_zero"] >= 300
    assert summary["versions"]["V2"]["zero_score_diagnostics"]["zero_score_count"] == 0
    assert summary["versions"]["V3"]["zero_score_diagnostics"]["zero_score_count"] == 0


def test_zero_reduction_is_quantified_against_v1():
    sessions = mixed_sessions(n_land=100, n_active=200)
    summary = ex.summarize(three(sessions))
    assert summary["versions"]["V1"]["zero_reduction_vs_v1"] is None
    for name in ("V2", "V3"):
        r = summary["versions"][name]["zero_reduction_vs_v1"]
        assert r["v1_zero_count"] >= 100 and r["this_zero_count"] == 0
        assert r["absolute_reduction"] == r["v1_zero_count"] and r["relative_reduction_pct"] == 100.0
        assert r["recovered"] == r["v1_zero_count"] and r["recovered_pct_of_v1_zero"] == 100.0 and r["new_zeros"] == 0
        assert r["recovered_score_distribution"]["P50"] > 0 and sum(r["recovered_bucket_mix"].values()) == r["recovered"]
        res = r["ranking_resolution_in_v1_zero_cohort"]
        assert res["v1_distinct_scores"] == 1 and res["v1_tied_share_pct"] == 100.0
        assert res["distinct_scores"] >= 3 and res["most_common_score_share_pct"] < 100.0      # three page-type modifiers
        assert r["remaining_zero_causes"] == {"raw_or_base_score_zero": 0, "penalised_to_zero": 0}


def test_sessions_with_a_zero_base_score_stay_zero_and_are_counted_as_such():
    # one dead click and nothing else: not a zero-signal session (it has a click), and its base score is 0
    dead_click_only = make("raw0", product_views=0, page_views=0, events=1, duration_s=0, clicks=1, dead=1, useful=0,
                           page_types=("other",))
    assert ex.classify_session(dead_click_only) == ("scored", None)
    run = three([dead_click_only, landing()])
    summary = ex.summarize(run)
    for name in ("V1", "V2", "V3"):
        assert {r.session_id: r.score for r in run.results[name]}["raw0"] == 0.0
        assert {r.session_id: r.raw_score for r in run.results[name]}["raw0"] == 0.0
    for name in ("V2", "V3"):
        reduction = summary["versions"][name]["zero_reduction_vs_v1"]
        assert reduction["remaining_zero_causes"] == {"raw_or_base_score_zero": 1, "penalised_to_zero": 0}
        assert reduction["recovered"] == 1                                    # only the landing session


def test_profile_by_bucket_has_scores_and_duration():
    summary = ex.summarize(three(mixed_sessions()))
    for name in ("V1", "V2", "V3"):
        for bucket, c in summary["versions"][name]["composition_by_bucket"].items():
            assert {"sessions", "avg_score", "median_score", "avg_product_views", "avg_useful_clicks", "avg_page_views",
                    "avg_duration_s", "avg_scrolls", "atc_rate_pct", "checkout_rate_pct"} <= set(c)
        assert {"count", "zero_score_count", "zero_score_pct", "P25", "P35", "P50", "P72", "P75", "P90", "max"} <= set(
            summary["versions"][name]["score_distribution"])


def test_agreement_movers_and_transition_cohort_outcomes_exist():
    summary = ex.summarize(three(mixed_sessions()))
    assert set(summary["version_agreement"]) == {"V1->V2", "V1->V3", "V2->V3"}
    a = summary["version_agreement"]["V1->V2"]
    assert 0 <= a["agreement_pct"] <= 100 and a["agreement_count"] == sum(a["outcome_cohorts"][f"{b}->{b}"]["sessions"] for b in BUCKETS_)
    assert set(a["movers"]) == {f"V1 {s} -> V2 {d}" for s in ("Low", "Medium", "High") for d in ("Low", "Medium", "High") if s != d}
    assert set(a["outcome_cohorts"]) == {f"{s}->{d}" for s in BUCKETS_ for d in BUCKETS_}
    for cohort in a["outcome_cohorts"].values():
        assert {"sessions", "atc_rate_pct", "checkout_rate_pct", "either_rate_pct", "lift_atc", "lift_checkout", "lift_either",
                "too_small", "kind"} <= set(cohort)
    assert sum(c["sessions"] for c in a["outcome_cohorts"].values()) == a["sessions_scored_in_both"]


BUCKETS_ = ("low", "medium", "high")


def test_small_cohorts_are_flagged_and_lift_is_relative_to_the_population():
    sessions = [make(f"s{i}", product_views=2, useful=4, duration_s=90, atc=int(i % 2 == 0), page_types=("pdp",)) for i in range(40)]
    sessions += [landing(f"l{i}") for i in range(10)]
    a = ex.summarize(three(sessions))["version_agreement"]["V1->V2"]
    assert any(c["too_small"] and c["sessions"] > 0 for c in a["outcome_cohorts"].values())
    pop = a["population_rates"]["atc_rate_pct"]
    for c in a["outcome_cohorts"].values():
        if c["sessions"]:
            assert c["lift_atc"] == round(c["atc_rate_pct"] / pop, 3)


def test_outcome_lift_and_rate_ratios_are_reported_per_bucket():
    ov = ex.summarize(three(mixed_sessions()))["versions"]["V1"]["outcome_validation"]
    assert set(ov["by_bucket"]) == set(BUCKETS_) and set(ov["rate_ratios"]) == {"high_vs_low", "high_vs_medium", "medium_vs_low"}
    for b in BUCKETS_:
        assert {"lift_atc", "lift_checkout", "lift_either", "either_rate_pct"} <= set(ov["by_bucket"][b])
    assert set(ov["four_groups"]) == {"zero_score", "positive_low", "medium", "high"} and "band_1" in ov["score_bands"]


def test_a_non_monotonic_outcome_pattern_is_flagged(monkeypatch):
    ids = [f"s{i}" for i in range(600)]
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores({sid: float(i + 1) for i, sid in enumerate(ids)}))
    # the lowest-scored sessions carry the ATC, the highest-scored carry none: the opposite of what a good score does
    sessions = [make(sid, atc=1 if i < 150 else 0) for i, sid in enumerate(ids)]
    ov = ex.outcome_validation(ex.run_experiment(sessions, ["VX"]), "VX")
    assert ov["monotonic_by_bucket"]["atc"] is False
    assert any("NOT MONOTONIC" in f and "atc" in f for f in ov["flags"])
    text = ex.build_report(ex.summarize(ex.run_experiment(sessions, ["VX"])))
    assert "NOT MONOTONIC" in text


def test_a_monotonic_pattern_is_not_flagged(monkeypatch):
    ids = [f"s{i}" for i in range(600)]
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores({sid: float(i + 1) for i, sid in enumerate(ids)}))
    sessions = [make(sid, atc=1 if i % 10 < (i * 10 // 600) + 1 else 0, checkout=1 if i % 10 < (i * 10 // 600) else 0)
                for i, sid in enumerate(ids)]
    ov = ex.outcome_validation(ex.run_experiment(sessions, ["VX"]), "VX")
    assert ov["monotonic_by_bucket"]["atc"] is True and ov["monotonic_by_bucket"]["checkout"] is True


def test_the_landing_cohort_is_described_for_every_version():
    summary = ex.summarize(three(mixed_sessions(n_land=200, n_active=300)))
    for name in ("V1", "V2", "V3"):
        c = summary["versions"][name]["landing_cohort"]["cohort"]
        assert c["sessions"] >= 200 and c["pct_of_v1_zero_cohort"] is not None
        assert {"P25", "P50", "P75", "P90", "min", "max"} <= set(c["score_distribution"]) and set(c["bucket_mix"]) == set(BUCKETS_)
        assert {"lift_atc", "lift_checkout"} <= set(c["outcomes"]) and {"P50", "P90", "over_5s_pct"} <= set(c["duration_s"])
        assert summary["versions"][name]["landing_cohort"]["strict"]["sessions"] <= c["sessions"]
    v1, v2 = (summary["versions"][n]["landing_cohort"]["strict"] for n in ("V1", "V2"))
    assert v1["zero_share_pct"] == 100.0 and v1["distinct_scores"] == 1 and v1["top_score_share_pct"] == 100.0
    assert v2["zero_share_pct"] == 0.0 and v2["distinct_scores"] > 1
    assert summary["versions"]["V1"]["landing_cohort"]["cohort"]["zero_share_pct"] > 90.0


def test_duration_and_scroll_diagnostics_are_reported_and_scroll_inactivity_is_loud():
    sessions = [make("a", duration_s=0), make("b", duration_s=0.01), make("c", duration_s=6), make("d", duration_s=12)]
    d = ex.duration_diagnostics(sessions)
    assert d["over_5s_pct"] == 50.0 and d["over_10s_pct"] == 25.0 and d["exactly_0_pct"] == 25.0 and d["under_1s_pct"] == 50.0
    summary = ex.summarize(three(sessions))
    assert summary["versions"]["V1"]["scroll_diagnostics"]["inactive"] is True
    text = ex.build_report(summary)
    assert "SCROLL FEATURE INACTIVE" in text and "Duration as recorded" in text
    with_scroll = ex.scroll_diagnostics([make("s", scrolls=3), make("t", scrolls=0)])
    assert with_scroll == {**with_scroll, "scroll_sessions": 1, "scroll_events": 3, "scroll_rate_pct": 50.0, "inactive": False}


def test_the_struggle_difference_of_v3_is_measured_not_assumed():
    rows = [make(f"n{i}", duration_s=120, clicks=10, useful=9, dead=1, product_views=1, page_types=("pdp",)) for i in range(9)]
    rows.append(make("struggling", duration_s=120, clicks=10, useful=1, dead=9, product_views=1, page_types=("pdp",)))
    summary = ex.summarize(three(rows))
    d = summary["versions"]["V3"]["experimental_differences"]
    assert d["production_struggle_penalty_would_fire_on_sessions"] == 1 and d["struggle_penalty_applied_by_this_version"] is False
    assert summary["versions"]["V2"]["experimental_differences"]["struggle_penalty_applied_by_this_version"] is True
    assert summary["versions"]["V1"]["experimental_differences"]["differences"] == []
    text = ex.build_report(summary)
    assert "Experimental differences from V1" in text and "NO struggle penalty" in text


def test_the_report_names_the_hypotheses_and_every_new_section_without_picking_a_winner():
    text = ex.build_report(ex.summarize(three(mixed_sessions()), {"low_intent_pct": 40, "medium_intent_pct": 45,
                                                               "high_intent_pct": 15, "total_sessions": 900}))
    for expected in ("EXPERIMENTAL", "non-destructive bounce", "behavioural progression", "adj = -5 if", "stage = min(product_views,4)*8",
                     "Zero-score diagnostics", "Zero-score reduction against V1", "Behavioural profile by intent_bucket",
                     "Outcome validation", "lift ATC", "monotonic Low < Medium < High", "Dominant landing cohort", "Scroll",
                     "V1 vs V2", "V1 vs V3", "V2 vs V3", "agreement", "movers:", "V1 Low -> V2 Medium", "low diff (pp)",
                     "No winner is selected", "ATC and checkout are NOT scoring inputs"):
        assert expected in text, expected
    assert "winner:" not in text.lower() and "best version" not in text.lower()


def test_the_results_carry_the_new_reason_codes_and_adjustments():
    run = three([landing("l"), make("ok", product_views=2, useful=4, duration_s=90, atc=1, page_types=("pdp",))])
    by = {name: {r.session_id: r for r in run.results[name]} for name in ("V1", "V2", "V3")}
    assert by["V1"]["l"].reason_codes.startswith("bounce_penalty") and by["V1"]["l"].bounce_penalty == -20.0
    assert "bounce_adjustment" in by["V2"]["l"].reason_codes and by["V2"]["l"].bounce_penalty == -5.0
    assert "bounce_adjustment" in by["V3"]["l"].reason_codes and by["V3"]["l"].bounce_penalty == -2.0
    assert "atc_bonus" not in by["V3"]["ok"].reason_codes and "checkout_bonus" not in by["V3"]["ok"].reason_codes
    for name in ("V1", "V2", "V3"):
        r = by[name]["ok"]
        assert r.raw_score is not None and r.struggle_penalty is not None and r.bounce_penalty is not None and r.page_modifier == 1.05


def test_an_empty_zero_score_group_is_not_treated_as_a_zero_rate_in_the_monotonicity_check():
    # V2 and V3 have no zero-score sessions here: the empty group must not be compared as "0%" and must not hide or
    # create a flag; its rates are simply absent
    run = three(mixed_sessions(n_land=100, n_active=300))
    for name in ("V2", "V3"):
        ov = ex.outcome_validation(run, name)
        assert ov["four_groups"]["zero_score"]["sessions"] == 0 and ov["four_groups"]["zero_score"]["lift_atc"] is None
        compared = [g for g, b in ov["four_groups"].items() if b["sessions"]]
        assert "zero_score" not in compared
        expected = ex._strictly_increasing([ov["four_groups"][g]["atc_rate_pct"] for g in compared])
        assert ov["monotonic_four_groups"]["atc"] is expected
    rows = ex._outcome_rows({"zero score": ex.outcome_validation(run, "V2")["four_groups"]["zero_score"]})
    assert rows == [["zero score", 0, "", "", "", "", "", ""]]


# ================= the evidence floor (V2 and V3): score != eligibility =================

def run_floor(run, name):
    return {r.session_id: r for r in run.results[name]}


def test_each_version_uses_its_own_positive_scores_and_the_production_boundary():
    run = three(mixed_sessions())
    for name in ("V1", "V2", "V3"):
        positive = sorted(r.score for r in run.results[name] if r.status == "scored" and r.score > 0)
        th = run.thresholds[name]
        # the percentiles are over ALL positive scores, floor-ineligible sessions included (production method, unchanged)
        assert th.p35 == rp._percentile(positive, 35) and th.p72 == rp._percentile(positive, 72) and th.population == len(positive)
        for r in run.results[name]:
            if r.status != "scored":
                continue
            assert r.bucket_without_floor == assign_bucket(r.score, th.p35, th.p72)           # the threshold bucket
            if r.evidence_stage == 0:
                assert r.bucket == "low"                                                      # the floor
            else:
                assert r.bucket == assign_bucket(r.score, th.p35, th.p72)
            assert not (r.score == 0 and r.bucket != "low")                                   # zero stays Low
    assert len({(t.p35, t.p72) for t in run.thresholds.values()}) == 3
    assert all(r.evidence_stage is None and r.bucket == r.bucket_without_floor for r in run.results["V1"] if r.status == "scored")


def test_a_score_equal_to_p35_is_medium_for_eligible_v2_and_v3_sessions():
    run = three(engaged_sessions(600, seed=8))                  # every session has evidence, so P35 is an eligible session's score
    for name in ("V2", "V3"):
        th = run.thresholds[name]
        equal = [r for r in run.results[name] if r.status == "scored" and r.score == th.p35 and r.evidence_stage >= 1]
        assert equal and all(r.bucket == "medium" for r in equal)


# ---------------- the stage definitions ----------------

STAGE_GRID = [(pv, pages, useful, scrolls, dur) for pv in (0, 1, 2, 3) for pages in (0, 1, 2, 3) for useful in (0, 1, 2, 5)
              for scrolls in (0, 1, 2) for dur in (0.0, 0.01, 9.99, 10.0, 29.9, 30.0, 120.0)]


def test_the_v2_evidence_floor_is_exactly_not_stage_0_of_v3():
    for pv, pages, useful, scrolls, dur in STAGE_GRID:
        stage_0_literal = pv <= 1 and pages <= 1 and useful == 0 and scrolls == 0 and dur < 10          # V3 stage 0
        meaningful_literal = pv >= 2 or pages >= 2 or useful >= 1 or scrolls >= 1 or dur >= 10          # V2 meaningful_evidence
        assert ex.meaningful_evidence(pv, pages, useful, scrolls, dur) == meaningful_literal
        assert (ex.evidence_stage(pv, pages, useful, scrolls, dur) == 0) == stage_0_literal
        assert stage_0_literal == (not meaningful_literal)


def test_stage_2_is_the_strong_evidence_subset_of_stage_1():
    for pv, pages, useful, scrolls, dur in STAGE_GRID:
        strong_literal = (pv >= 2 and pages >= 2) or useful >= 2 or dur >= 30 or scrolls >= 2
        stage = ex.evidence_stage(pv, pages, useful, scrolls, dur)
        assert ex.strong_evidence(pv, pages, useful, scrolls, dur) == strong_literal
        assert (stage == 2) == strong_literal
        if strong_literal:
            assert ex.meaningful_evidence(pv, pages, useful, scrolls, dur)            # strong implies meaningful


@pytest.mark.parametrize("label,kwargs", [
    ("two product views", dict(product_views=2, page_views=1)),
    ("two page views", dict(product_views=1, page_views=2)),
    ("one useful click", dict(useful=1)),
    ("one scroll", dict(scrolls=1)),
    ("ten seconds", dict(duration_s=10)),
])
def test_each_evidence_trigger_makes_a_session_eligible_in_v2_and_v3(label, kwargs):
    base = dict(product_views=1, page_views=1, events=2, duration_s=0.01, page_types=("pdp",))
    session = make("e", **{**base, **kwargs})
    for name in ("V2", "V3"):
        out = score_of(name, session)
        assert out.eligible is True and out.evidence_stage >= 1, (label, name)
    assert score_of("V2", landing("l")).eligible is False and score_of("V2", landing("l")).evidence_stage == 0


def test_just_under_every_trigger_stays_ineligible():
    for kwargs in (dict(duration_s=9.99), dict(product_views=1, page_views=1)):
        session = make("n", **{**dict(product_views=1, page_views=1, events=2, duration_s=0.01, page_types=("pdp",)), **kwargs})
        assert score_of("V2", session).eligible is False and score_of("V3", session).eligible is False


# ---------------- V2: the floor keeps a weak landing Low ----------------

def test_v2_a_one_product_one_page_view_session_is_low_even_when_its_score_clears_p35():
    run = three(mixed_sessions(n_land=300, n_active=100))
    for name in ("V2", "V3"):
        landings = [r for r in run.results[name] if r.session_id.startswith("l")]
        assert landings and all(r.bucket == "low" and r.evidence_stage == 0 and r.score > 0 for r in landings)
        # the floor actually did something: without it, some of these sessions would have been Medium or High
        assert any(r.bucket_without_floor in ("medium", "high") for r in landings)
    assert all(r.bucket == "low" for r in run.results["V1"] if r.session_id.startswith("l"))


def test_v2_eligible_sessions_are_bucketed_by_the_thresholds_as_before():
    sessions = mixed_sessions(n_land=100, n_active=400)
    sessions += [make("t_pv", product_views=2, page_views=1, events=2, duration_s=0.01, page_types=("pdp",)),
                 make("t_use", product_views=1, page_views=1, useful=1, events=2, duration_s=0.01, page_types=("pdp",)),
                 make("t_dur", product_views=1, page_views=1, events=2, duration_s=12, page_types=("pdp",))]
    run = three(sessions)
    by = run_floor(run, "V2")
    th = run.thresholds["V2"]
    for sid in ("t_pv", "t_use", "t_dur"):
        assert by[sid].evidence_stage >= 1 and by[sid].bucket == assign_bucket(by[sid].score, th.p35, th.p72)


def test_v2_and_v3_floor_never_raises_a_session_it_only_lowers():
    run = three(mixed_sessions())
    order = {"low": 0, "medium": 1, "high": 2}
    for name in ("V2", "V3"):
        for r in run.results[name]:
            if r.status == "scored":
                assert order[r.bucket] <= order[r.bucket_without_floor]


def test_atc_and_checkout_change_neither_score_nor_bucket_nor_stage_in_v2_and_v3():
    def dataset(with_outcomes):
        out = []
        for i, s in enumerate(mixed_sessions(n_land=120, n_active=260, seed=17)):
            row = dict(s.row)
            row["add_to_cart_count"] = (i % 3) if with_outcomes else 0
            row["checkout_started_count"] = (i % 2) if with_outcomes else 0
            out.append(ex.build_session(row, rp.Enrichment(s.entry_page_type, set(s.page_types_seen), s.user_agent, set()), "BBB"))
        return out

    plain, loud = three(dataset(False)), three(dataset(True))
    for name in ("V2", "V3"):
        assert [(r.score, r.bucket, r.evidence_stage, r.bucket_without_floor) for r in plain.results[name]] == \
               [(r.score, r.bucket, r.evidence_stage, r.bucket_without_floor) for r in loud.results[name]]
        assert plain.thresholds[name] == loud.thresholds[name]


def test_v2_and_v3_eligibility_never_touches_atc_or_checkout():
    for name in ("V2", "V3"):
        for s in (landing("g"), make("g2", product_views=2, useful=3, page_views=3, duration_s=90, page_types=("pdp",))):
            out = score_of(name, guarded(replace(s, row=MappingProxyType({**s.row, "add_to_cart_count": 5, "checkout_started_count": 5}))))
            assert out.evidence_stage in (0, 1, 2)                                   # a read of the outcome fields would raise


def test_v2_v1_parity_and_struggle_and_modifier_are_untouched_by_the_floor():
    s = make("s", duration_s=120, clicks=20, useful=4, dead=16, product_views=2, page_types=("home",))
    out = score_of("V2", s, p90=0.5)
    assert out.struggle_penalty == -12.0 and out.modifier == 0.85 and out.eligible
    sessions = engaged_sessions(200, seed=44)
    alone = ex.run_experiment(sessions, ["V1"])
    assert [r.as_row() for r in three(sessions).results["V1"] if True] == [r.as_row() for r in alone.results["V1"]]


# ---------------- V3 stages ----------------

def test_v3_stage_0_is_always_low_and_the_landing_session_is_stage_0_with_a_positive_score():
    out = score_of("V3", landing())
    assert out.evidence_stage == 0 and out.eligible is False and out.score > 0 and "stage_0_passive" in out.reason_codes
    run = three(mixed_sessions(n_land=200, n_active=50))
    assert all(r.bucket == "low" for r in run.results["V3"] if r.evidence_stage == 0)


def test_v3_stage_1_and_stage_2_triggers():
    assert ex.evidence_stage(2, 1, 0, 0, 0.0) == 1 and ex.evidence_stage(1, 1, 1, 0, 0.0) == 1
    assert ex.evidence_stage(1, 1, 0, 1, 0.0) == 1 and ex.evidence_stage(1, 1, 0, 0, 10.0) == 1
    assert ex.evidence_stage(2, 2, 0, 0, 0.0) == 2 and ex.evidence_stage(1, 1, 2, 0, 0.0) == 2
    assert ex.evidence_stage(1, 1, 0, 0, 30.0) == 2 and ex.evidence_stage(1, 1, 0, 2, 0.0) == 2
    assert ex.evidence_stage(2, 1, 1, 1, 20.0) == 1                              # several weak signals are still stage 1
    assert score_of("V3", make("a", product_views=2, page_views=2, duration_s=5, events=6)).evidence_stage == 2
    assert score_of("V3", make("b", product_views=1, page_views=1, duration_s=12, events=6)).evidence_stage == 1


def test_v3_has_no_forced_high_rule_and_stage_1_can_reach_high():
    run = three(mixed_sessions(n_land=50, n_active=500, seed=3))
    stage_1_high = [r for r in run.results["V3"] if r.evidence_stage == 1 and r.bucket == "high"]
    summary = ex.summarize(run)["versions"]["V3"]["evidence_stages"]
    assert summary["high_sessions_in_stage_1"] == len(stage_1_high)


# ---------------- the evidence-stage section, PASS / FAIL flags and the floor effect ----------------

def test_evidence_stage_summary_for_v2_and_v3_but_not_v1():
    summary = ex.summarize(three(mixed_sessions()))
    assert summary["versions"]["V1"]["evidence_stages"] is None
    for name in ("V2", "V3"):
        e = summary["versions"][name]["evidence_stages"]
        assert set(e["stages"]) == {"stage_0_passive", "stage_1_exploration", "stage_2_strong"}
        assert sum(b["sessions"] for b in e["stages"].values()) == summary["versions"][name]["population"]["scored"]
        for flag in ("STAGE_ATC_MONOTONIC", "STAGE_CHECKOUT_MONOTONIC"):
            assert e[flag] in ("PASS", "FAIL")
        assert set(e["stage_by_bucket"]) == set(e["stages"])
        assert e["stage_by_bucket"]["stage_0_passive"]["medium"] == 0 and e["stage_by_bucket"]["stage_0_passive"]["high"] == 0
        f = e["floor_effect"]
        assert f["demoted_to_low"] == f["from_medium"]["sessions"] + f["from_high"]["sessions"]


def _stage_dataset(atc_by_stage):
    sessions = []
    shapes = [dict(product_views=1, page_views=1, events=2, duration_s=0.01),                       # stage 0
              dict(product_views=2, page_views=1, events=4, duration_s=20),                         # stage 1
              dict(product_views=2, page_views=2, useful=2, events=8, duration_s=60)]               # stage 2
    for stage, shape in enumerate(shapes):
        for i in range(40):
            sessions.append(make(f"s{stage}_{i}", page_types=("pdp",), atc=int(i < atc_by_stage[stage]), **shape))
    return sessions


def test_stage_monotonicity_passes_when_atc_rises_with_the_stage_and_fails_when_it_does_not():
    good = ex.summarize(three(_stage_dataset((4, 12, 24))))["versions"]["V2"]["evidence_stages"]
    assert good["STAGE_ATC_MONOTONIC"] == "PASS"
    bad = ex.summarize(three(_stage_dataset((24, 12, 4))))["versions"]["V2"]["evidence_stages"]
    assert bad["STAGE_ATC_MONOTONIC"] == "FAIL"
    flat = ex.summarize(three(_stage_dataset((10, 10, 10))))["versions"]["V2"]["evidence_stages"]
    assert flat["STAGE_ATC_MONOTONIC"] == "FAIL"                                  # no tolerance: equal is not a rise


def test_bucket_monotonic_flags_are_pass_or_fail_and_an_empty_bucket_is_a_fail():
    ov = ex.summarize(three(mixed_sessions()))["versions"]["V1"]["outcome_validation"]
    assert set(ov["pass_fail"]) >= {"ATC_MONOTONIC", "CHECKOUT_MONOTONIC"} and set(ov["pass_fail"].values()) <= {"PASS", "FAIL"}
    only_landings = ex.summarize(three([landing(f"l{i}") for i in range(30)]))
    for name in ("V2", "V3"):                                                    # everything is floored to Low: Medium and High are empty
        assert only_landings["versions"][name]["outcome_validation"]["pass_fail"]["ATC_MONOTONIC"] == "FAIL"


def test_monotonic_pass_for_a_rising_pattern_and_fail_for_a_falling_pattern(monkeypatch):
    ids = [f"s{i}" for i in range(600)]
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores({sid: float(i + 1) for i, sid in enumerate(ids)}))
    rising = [make(sid, atc=int(i % 10 < (i * 10 // 600) + 1), checkout=int(i % 10 < (i * 10 // 600))) for i, sid in enumerate(ids)]
    falling = [make(sid, atc=int(i % 10 < 9 - (i * 10 // 600)), checkout=int(i % 10 < 8 - (i * 10 // 600))) for i, sid in enumerate(ids)]
    assert ex.outcome_validation(ex.run_experiment(rising, ["VX"]), "VX")["pass_fail"]["ATC_MONOTONIC"] == "PASS"
    assert ex.outcome_validation(ex.run_experiment(falling, ["VX"]), "VX")["pass_fail"]["ATC_MONOTONIC"] == "FAIL"


def test_lifts_are_reported_with_safe_division():
    ov = ex.summarize(three(_stage_dataset((0, 0, 0))))["versions"]["V1"]["outcome_validation"]
    assert ov["rate_ratios"]["high_vs_low"]["atc"] is None                       # 0 / 0 is None, not an error
    ov = ex.summarize(three(mixed_sessions()))["versions"]["V1"]["outcome_validation"]
    assert set(ov["rate_ratios"]) == {"high_vs_low", "high_vs_medium", "medium_vs_low"}


# ---------------- the dominant landing cohort, redefined ----------------

def test_the_landing_cohort_uses_the_new_definition_and_flags_a_promotion(monkeypatch):
    sessions = [make("in1", product_views=1, page_views=1, useful=0, scrolls=0, events=2, duration_s=0.01),
                make("in2", product_views=1, page_views=1, useful=0, scrolls=0, events=5, duration_s=3),      # more events: still in
                make("out_useful", product_views=1, page_views=1, useful=1, events=2, duration_s=0.01),
                make("out_scroll", product_views=1, page_views=1, scrolls=1, events=2, duration_s=0.01),
                make("out_pv", product_views=2, page_views=1, events=2, duration_s=0.01)]
    summary = ex.summarize(three(sessions))
    c = summary["versions"]["V2"]["landing_cohort"]
    assert c["cohort"]["sessions"] == 2 and c["strict"]["sessions"] == 1
    assert c["definition"].startswith("product_views == 1 and page_views == 1 and useful_clicks == 0 and scrolls == 0")
    # a version that lets these sessions into Medium or High is flagged FAIL, a version that keeps them Low is PASS
    ids = [f"s{i}" for i in range(600)]
    scores = {sid: float(i + 1) for i, sid in enumerate(ids)}
    monkeypatch.setitem(ex.VERSIONS, "VX", FixedScores(scores))
    promoting = [make(sid, product_views=1, page_views=1, useful=0, scrolls=0) for sid in ids]
    assert ex.landing_cohort(ex.run_experiment(promoting, ["VX"]), "VX")["cohort"]["promotion_flag"] == "FAIL"
    keep = ex.landing_cohort(three(mixed_sessions(n_land=200, n_active=100)), "V2")["cohort"]
    assert keep["promotion_flag"] == "PASS" and keep["above_low_pct"] == 0.0 and keep["zero_share_pct"] == 0.0


def test_the_landing_cohort_is_overwhelmingly_low_under_v2_and_v3_with_the_floor():
    summary = ex.summarize(three(mixed_sessions(n_land=300, n_active=300)))
    for name in ("V2", "V3"):
        c = summary["versions"][name]["landing_cohort"]["cohort"]
        assert c["sessions"] >= 300 and c["bucket_mix"]["low"]["pct"] >= 99.0 and c["promotion_flag"] == "PASS"
    assert summary["versions"]["V1"]["landing_cohort"]["cohort"]["zero_share_pct"] >= 99.0


# ---------------- recovered cohort, duration groups, scroll, profile, distribution ----------------

def test_the_recovered_cohort_is_split_by_outcome_bucket_and_stage():
    summary = ex.summarize(three(mixed_sessions(n_land=200, n_active=200)))
    for name in ("V2", "V3"):
        r = summary["versions"][name]["zero_reduction_vs_v1"]
        assert {"recovered_outcomes", "recovered_outcomes_by_bucket", "recovered_by_stage", "recovered_bucket_mix_pct",
                "v1_zero_outcomes", "not_recovered_outcomes"} <= set(r)
        assert r["recovered_by_stage"]["stage_0_passive"]["sessions"] == r["recovered"]       # the recovered ones are all passive landings
        assert r["recovered_outcomes_by_bucket"]["medium"]["sessions"] == 0                  # and none of them became Medium
        assert abs(sum(r["recovered_bucket_mix_pct"].values()) - 100.0) < 0.1


def test_duration_groups_cover_every_requested_population_and_flag_clustering_near_zero():
    groups = ex.summarize(three(mixed_sessions(n_land=300, n_active=100)))["duration_by_group"]
    assert {"all scored sessions", "V1 zero-score", "V2 stage 0", "V2 stage 1", "V3 stage 0", "V3 stage 1", "V3 stage 2",
            "clustered_near_zero"} <= set(groups)
    assert "V2 stage 2" in groups                      # stage 2 is reported for every staged version
    assert {"P25", "P50", "P75", "P90"} <= set(groups["V3 stage 2"])
    assert groups["clustered_near_zero"]["flag"] is True
    spread = ex.summarize(three(engaged_sessions(100, seed=2)))["duration_by_group"]["clustered_near_zero"]
    assert spread["flag"] is False


def test_scroll_diagnostics_report_the_numbers_when_the_feature_is_inactive():
    d = ex.scroll_diagnostics([make("a"), make("b")])
    assert d["inactive"] and d["total_scored_sessions"] == 2 and d["scroll_sessions"] == 0 and d["max_scroll_count"] == 0
    assert "SCROLL FEATURE INACTIVE" in d["message"] and "2 scored sessions" in d["message"] and "max scroll count 0" in d["message"]
    live = ex.scroll_diagnostics([make("a", scrolls=3), make("b")])
    assert not live["inactive"] and live["max_scroll_count"] == 3


def test_the_profile_and_distribution_have_the_requested_fields():
    summary = ex.summarize(three(mixed_sessions()))
    for name in ("V1", "V2", "V3"):
        v = summary["versions"][name]
        assert {"zero_score_count", "P10", "P25", "P35", "P50", "P72", "P75", "P90", "P95", "max"} <= set(v["score_distribution"])
        for c in v["composition_by_bucket"].values():
            assert {"sessions", "avg_score", "median_score", "avg_product_views", "avg_page_views", "avg_useful_clicks",
                    "avg_scrolls", "median_duration_s", "atc_rate_pct", "checkout_rate_pct", "either_rate_pct"} <= set(c)


def test_the_report_has_every_new_section_and_flag():
    sessions = [landing(f"l{i}", page_type=("pdp", "collection", "home")[i % 3]) for i in range(300)]
    sessions += [make(f"a{i}", product_views=1 + i % 3, useful=i % 6, page_views=2 + i % 4, duration_s=15 + i, scrolls=0,
                      atc=int(i % 4 == 0), checkout=int(i % 7 == 0), page_types=("pdp",)) for i in range(100)]   # no session scrolls
    text = ex.build_report(ex.summarize(three(sessions)))
    for expected in ("Evidence stages", "STAGE_ATC_MONOTONIC", "STAGE_CHECKOUT_MONOTONIC", "ATC_MONOTONIC =", "CHECKOUT_MONOTONIC =",
                     "Recovered V1-zero cohort", "HIGHLIGHT V1 zero -> V2 Medium", "HIGHLIGHT V1 zero -> V3 Medium",
                     "HIGHLIGHT V1 Low -> V2 Medium", "evidence floor forced", "Dominant landing cohort", "flag rule",
                     "DURATION CLUSTERED NEAR ZERO", "SCROLL FEATURE INACTIVE", "max scroll count", "P95", "median duration s",
                     "ATC lift:", "checkout lift:", "stage x bucket", "same for V2 and V3"):
        assert expected in text, expected
    assert "winner:" not in text.lower() and "best version" not in text.lower()


def test_an_all_excluded_day_and_a_landing_only_day_do_not_break_the_summary_or_report():
    bots = [make(f"b{i}", ua="Googlebot/2.1") for i in range(5)]
    for sessions in (bots, [landing(f"l{i}") for i in range(10)], [make("x", duration_ms=None)]):
        run = three(sessions)
        text = ex.build_report(ex.summarize(run))
        assert "Evidence stages" in text or "Population" in text


# ================= V4 (V2 score, eligible-only thresholds) and V5 (V3 score, eligible-only thresholds, stage-2 High) =================

def five(sessions, mode="common", **kw):
    return ex.run_experiment(sessions, ["V1", "V2", "V3", "V4", "V5"], threshold_mode=mode, **kw)


def pop_sessions(n_land=300, n_active=500, seed=9, outcomes=False):
    """Passive landings (stage 0) plus a varied engaged population; `outcomes` adds ATC / checkout to some of them."""
    rnd = random.Random(seed)
    engaged = []
    for i in range(n_active):
        engaged.append(make(f"e{i}", duration_s=rnd.choice([3, 12, 20, 40, 90, 300]), events=rnd.randint(2, 14),
                            product_views=rnd.randint(0, 4), useful=rnd.randint(0, 8), dead=rnd.randint(0, 3),
                            page_views=rnd.randint(1, 6), scrolls=0,
                            atc=int(outcomes and i % 3 == 0), checkout=int(outcomes and i % 5 == 0),
                            page_types=rnd.choice([("home",), ("pdp", "home"), ("collection",), ("other",)])))
    return [landing(f"l{i}", page_type=("pdp", "collection", "home")[i % 3]) for i in range(n_land)] + engaged


def by_id(run, name):
    return {r.session_id: r for r in run.results[name]}


def test_v4_and_v5_are_registered_experimental_and_inherit_the_score_unchanged():
    assert ex.V4EligibleThresholds.score is ex.V2NonDestructiveBounce.score           # nothing re-implemented
    assert ex.V5EligibleStage2High.score is ex.V3BehavioralProgression.score
    assert ex.VERSIONS["V4"].experimental and ex.VERSIONS["V5"].experimental
    assert ex.VERSIONS["V4"].threshold_population == ex.VERSIONS["V5"].threshold_population == "eligible"
    assert ex.VERSIONS["V4"].high_min_stage is None and ex.VERSIONS["V5"].high_min_stage == 2
    assert ex.VERSIONS["V2"].threshold_population == ex.VERSIONS["V3"].threshold_population == "all_positive"
    assert ex.VERSIONS["V2"].high_min_stage is None and ex.VERSIONS["V3"].high_min_stage is None


@pytest.mark.parametrize("new,old", [("V4", "V2"), ("V5", "V3")])
def test_score_bounce_and_stage_of_the_new_version_equal_its_parent_for_every_row(new, old):
    run = five(pop_sessions(outcomes=True))
    a_all, b_all = by_id(run, old), by_id(run, new)
    assert a_all.keys() == b_all.keys() and len(a_all) == 800
    for sid in a_all:
        a, b = a_all[sid], b_all[sid]
        assert (a.score, a.raw_score, a.bounce_penalty, a.struggle_penalty, a.page_modifier) == \
               (b.score, b.raw_score, b.bounce_penalty, b.struggle_penalty, b.page_modifier)
        assert a.evidence_stage == b.evidence_stage and a.reason_codes == b.reason_codes


@pytest.mark.parametrize("name", ["V4", "V5"])
def test_thresholds_come_from_eligible_scores_only(name):
    run = five(pop_sessions())
    results = [r for r in run.results[name] if r.status == "scored"]
    assert {r.evidence_stage for r in results} == {0, 1, 2}              # stage 1 and 2 are both in the data
    eligible = sorted(r.score for r in results if r.evidence_stage >= 1 and r.score > 0)
    passive_positive = sum(1 for r in results if r.evidence_stage == 0 and r.score > 0)
    th = run.thresholds[name]
    assert passive_positive > 0 and th.population == len(eligible)       # stage 0 is not counted
    assert th.p35 == rp._percentile(eligible, 35) and th.p72 == rp._percentile(eligible, 72)
    assert run.reference_thresholds[name].population == len(eligible) + passive_positive
    twin = "V2" if name == "V4" else "V3"                                # V2 / V3 still use every positive score
    assert run.thresholds[twin] == run.reference_thresholds[twin] == run.reference_thresholds[name]
    assert run.thresholds[name] != run.thresholds[twin]                  # the passive scores really do move the cut-offs


@pytest.mark.parametrize("name", ["V4", "V5"])
def test_the_stage_zero_population_size_does_not_change_the_thresholds(name):
    small = five(pop_sessions(n_land=50))
    large = five(pop_sessions(n_land=900))
    assert small.thresholds[name] == large.thresholds[name]
    twin = "V2" if name == "V4" else "V3"
    assert small.thresholds[twin] != large.thresholds[twin]              # while the all-positive cut-offs do move


@pytest.mark.parametrize("name", ["V4", "V5"])
def test_stage_zero_and_zero_scores_are_always_low(name):
    run = five(pop_sessions(n_land=400))
    results = [r for r in run.results[name] if r.status == "scored"]
    assert any(r.evidence_stage == 0 for r in results)
    assert all(r.bucket == "low" for r in results if r.evidence_stage == 0)
    assert all(r.bucket == "low" for r in results if r.score <= 0)


def test_v4_buckets_follow_the_stated_rule_exactly():
    run = five(pop_sessions())
    th = run.thresholds["V4"]
    seen = set()
    for r in run.results["V4"]:
        if r.score <= 0 or r.evidence_stage == 0 or r.score < th.p35:
            assert r.bucket == "low"
        elif r.score < th.p72:
            assert r.bucket == "medium"
        else:
            assert r.bucket == "high"
        seen.add(r.bucket)
    assert seen == {"low", "medium", "high"}


def test_v5_stage_one_can_never_be_high_but_stage_two_can():
    weak = [make(f"w{i}", product_views=0, page_views=1, useful=2, duration_s=40, events=5, page_types=("pdp",)) for i in range(100)]
    one_dimension = [make(f"h{i}", product_views=4, page_views=1, useful=1, duration_s=12, events=8, page_types=("pdp",))
                     for i in range(80)]                      # stage 1 (only one of the two stage-2 pairs) with a big score
    strong = [make(f"g{i}", product_views=4, page_views=4, useful=6, duration_s=120, events=12, page_types=("pdp",)) for i in range(20)]
    run = five(weak + one_dimension + strong + [landing(f"l{i}") for i in range(50)])
    th = run.thresholds["V5"]
    results = [r for r in run.results["V5"] if r.status == "scored"]
    stage1_over_p72 = [r for r in results if r.evidence_stage == 1 and r.score >= th.p72]
    assert stage1_over_p72, "the data must contain stage 1 sessions above P72 for this test to mean anything"
    assert all(r.bucket == "medium" for r in stage1_over_p72)            # capped, not dropped to Low
    assert not [r for r in results if r.evidence_stage == 1 and r.bucket == "high"]
    stage2_high = [r for r in results if r.evidence_stage == 2 and r.score >= th.p72]
    assert stage2_high and all(r.bucket == "high" for r in stage2_high)
    for r in results:                                                    # and the rest of the rule
        if r.score <= 0 or r.evidence_stage == 0 or r.score < th.p35:
            assert r.bucket == "low"
        elif r.evidence_stage < 2 or r.score < th.p72:
            assert r.bucket == "medium"
        else:
            assert r.bucket == "high"
    gate = ex.summarize(run)["versions"]["V5"]["bucket_stage_quality"]
    assert gate["high"]["stage_1_exploration"]["sessions"] == 0
    assert gate["capped_from_high"]["sessions"] == len(stage1_over_p72)


@pytest.mark.parametrize("name", ["V4", "V5"])
def test_atc_and_checkout_change_neither_score_stage_threshold_nor_bucket(name):
    plain = five(pop_sessions(outcomes=False))
    with_outcomes = five(pop_sessions(outcomes=True))
    a, b = by_id(plain, name), by_id(with_outcomes, name)
    for sid in a:
        assert (a[sid].score, a[sid].bucket, a[sid].evidence_stage) == (b[sid].score, b[sid].bucket, b[sid].evidence_stage)
    assert plain.thresholds[name] == with_outcomes.thresholds[name]


@pytest.mark.parametrize("name", ["V4", "V5"])
def test_no_outcome_field_is_read_by_the_score_or_the_stage(name):
    for s in [guarded(s) for s in pop_sessions(n_land=20, n_active=60)]:
        ex.VERSIONS[name].score(s, ex.DayContext(None))                  # GuardedRow raises if an outcome field is read
    for cls in (ex.V4EligibleThresholds, ex.V5EligibleStage2High):
        assert not _mentions_an_outcome(_identifiers(cls)), cls.__name__


def test_v1_is_untouched_by_the_new_versions():
    sessions = pop_sessions()
    run = five(sessions)
    assert [r.score for r in run.results["V1"]] == [r.score for r in three(sessions).results["V1"]]
    assert run.thresholds["V1"] == run.reference_thresholds["V1"]


# ---------------- the new diagnostics ----------------

def test_threshold_diagnostics_show_how_stage_zero_distorts_the_cutoffs():
    s = ex.summarize(five(pop_sessions()))["versions"]
    for name in ("V2", "V3", "V4", "V5"):
        t = s[name]["threshold_diagnostics"]
        assert t["all_positive"]["count"] == t["eligible"]["count"] + t["stages"]["stage_0_passive"]["count"]
        assert abs(sum(t["stages"][k]["pct"] for k in t["stages"]) - 100.0) < 0.1
        assert {"P35", "P50", "P72", "P90"} <= set(t["eligible"]) and {"P35", "P50", "P72", "P90"} <= set(t["all_positive"])
        assert t["population_used"] == ex.VERSIONS[name].threshold_population
    assert s["V1"]["threshold_diagnostics"] is None
    t4 = s["V4"]["threshold_diagnostics"]
    assert t4["p35_used"] == t4["eligible"]["P35"] and t4["p72_used"] == t4["eligible"]["P72"]
    t2 = s["V2"]["threshold_diagnostics"]
    assert t2["p35_used"] == t2["all_positive"]["P35"] and t2["p72_used"] == t2["all_positive"]["P72"]
    assert t2["shift_eligible_minus_all"]["P72"] > 0                     # passive scores are small: the eligible cut-off is higher


def test_engaged_only_validation_reports_pass_fail_and_excludes_stage_zero():
    run = five(pop_sessions(outcomes=True))
    s = ex.summarize(run)["versions"]
    assert s["V1"]["engaged_validation"] is None
    for name in ("V2", "V3", "V4", "V5"):
        e = s[name]["engaged_validation"]
        results = [r for r in run.results[name] if r.status == "scored"]
        assert e["engaged"]["sessions"] == sum(1 for r in results if r.evidence_stage >= 1)
        assert e["stage2"]["sessions"] == sum(1 for r in results if r.evidence_stage == 2)
        assert sum(b["sessions"] for b in e["engaged"]["by_bucket"].values()) == e["engaged"]["sessions"]
        assert e["engaged"]["ATC_MONOTONIC"] in ("PASS", "FAIL") and e["stage2"]["CHECKOUT_MONOTONIC"] in ("PASS", "FAIL")


def test_engaged_only_validation_fails_when_a_bucket_is_empty():
    sessions = [make(f"x{i}", product_views=2, page_views=1, useful=0, duration_s=12, atc=int(i % 2 == 0)) for i in range(40)]
    e = ex.summarize(five(sessions))["versions"]["V5"]["engaged_validation"]
    assert e["stage2"]["sessions"] == 0 and e["stage2"]["ATC_MONOTONIC"] == "FAIL"
    assert any(b["sessions"] == 0 for b in e["engaged"]["by_bucket"].values()) and e["engaged"]["ATC_MONOTONIC"] == "FAIL"


def test_stage_table_has_scores_and_the_stage_two_over_stage_one_lift():
    summary = ex.summarize(five(pop_sessions(outcomes=True)))
    for name in ("V2", "V3", "V4", "V5"):
        e = summary["versions"][name]["evidence_stages"]
        for block in e["stages"].values():
            assert block["avg_score"] is not None and block["median_score"] is not None
        s1, s2 = e["stages"]["stage_1_exploration"], e["stages"]["stage_2_strong"]
        assert e["stage2_vs_stage1_lift"]["atc"] == ex._ratio(s2["atc_rate_pct"], s1["atc_rate_pct"])
        assert e["stage2_vs_stage1_lift"]["checkout"] == ex._ratio(s2["checkout_rate_pct"], s1["checkout_rate_pct"])


def test_medium_and_high_are_split_by_stage_with_counts_and_shares():
    run = five(pop_sessions(outcomes=True))
    summary = ex.summarize(run)
    for name in ("V2", "V3", "V4", "V5"):
        q = summary["versions"][name]["bucket_stage_quality"]
        for bucket in ("medium", "high"):
            parts = sum(q[bucket][k]["sessions"] for k in ("stage_0_passive", "stage_1_exploration", "stage_2_strong"))
            assert parts == q[bucket]["all"]["sessions"] == summary["versions"][name]["buckets"]["intent_bucket"][bucket]["count"]
        assert q["medium"]["stage_0_passive"]["sessions"] == 0 and q["high"]["stage_0_passive"]["sessions"] == 0


def test_score_blind_diagnostic_counts_zero_score_checkout_sessions_and_warns():
    blind = [make(f"c{i}", product_views=0, page_views=1, events=2, duration_s=0, checkout=int(i < 6), page_types=("checkout",),
                  entry="checkout") for i in range(10)]
    run = five(blind + pop_sessions(n_land=20, n_active=60))
    summary = ex.summarize(run)
    for name in ("V1", "V2", "V3", "V4", "V5"):
        z = summary["versions"][name]["zero_score_diagnostics"]["score_blind"]
        assert z["zero_and_checkout"] == sum(1 for s, r in zip(run.sessions, run.results[name])
                                             if r.status == "scored" and r.score == 0 and s.checkout_started_count > 0)
        assert 0 <= z["share_of_all_checkout_sessions_pct"] <= 100
    v1 = summary["versions"]["V1"]["zero_score_diagnostics"]["score_blind"]
    assert v1["zero_and_checkout"] >= 6 and v1["warn"] is True and "SCORE-BLIND COMMERCIAL SESSIONS DETECTED" in v1["warning"]
    quiet = ex.summarize(five(pop_sessions(n_land=0, n_active=80)))["versions"]["V2"]["zero_score_diagnostics"]["score_blind"]
    assert quiet["warn"] is False and quiet["warning"] == ""


def test_duration_bands_have_the_five_bands_with_outcomes():
    bands = ex.summarize(five(pop_sessions(outcomes=True)))["duration_bands"]
    assert set(bands) == {"all scored sessions", "landing cohort", "everything except the landing cohort"}
    allb = bands["all scored sessions"]
    assert list(allb) == ["< 1 s", "1-5 s", "5-10 s", "10-30 s", "30 s +"]
    assert sum(b["sessions"] for b in allb.values()) == 800 and abs(sum(b["pct"] for b in allb.values()) - 100.0) < 0.1
    assert bands["landing cohort"]["< 1 s"]["sessions"] == 300                         # the 0.01 s landings
    assert all({"atc_rate_pct", "checkout_rate_pct"} <= set(b) for b in allb.values())


def test_score_ties_list_the_five_most_common_scores_with_outcomes_and_buckets():
    summary = ex.summarize(five(pop_sessions(n_land=400)))
    for name in ("V2", "V3", "V4", "V5"):
        t = summary["versions"][name]["score_ties"]
        assert len(t["top"]) <= 5 and t["top"][0]["sessions"] >= t["top"][-1]["sessions"]
        top = t["top"][0]
        assert top["sessions"] >= 400 // 3 and sum(top["bucket_mix"].values()) == top["sessions"]
        assert {"atc_rate_pct", "checkout_rate_pct", "pct"} <= set(top)


def test_the_five_version_report_has_every_requested_section():
    text = ex.build_report(ex.summarize(five(pop_sessions(outcomes=True))))
    for expected in ("COMPARISON", "ENGAGED ATC monotonic", "Threshold population", "all positive", "eligible (stage>=1)",
                     "ENGAGED-ONLY validation", "ENGAGED_ATC_MONOTONIC =", "ENGAGED_CHECKOUT_MONOTONIC =", "STAGE2_ATC_MONOTONIC =",
                     "STAGE >= 2 ONLY", "High quality by evidence stage", "Medium quality by evidence stage", "Stage2/Stage1 ATC lift",
                     "Score-blind commercial sessions", "Duration bands", "Most common exact scores",
                     "HIGHLIGHT V1 Low -> V4 Medium", "V1 Low -> V4 High", "V1 zero -> V4 Medium", "V1 zero -> V4 High",
                     "V1 Low -> V5 Medium", "V1 Low -> V5 High", "V1 zero -> V5 Medium", "V1 zero -> V5 High",
                     "Dominant landing cohort", "SCROLL FEATURE INACTIVE"):
        assert expected in text, expected
    assert "winner:" not in text.lower() and "best version" not in text.lower()


def test_the_five_version_run_handles_an_empty_stage_and_a_landing_only_day():
    for sessions in ([landing(f"l{i}") for i in range(10)], pop_sessions(n_land=0, n_active=5)):
        assert "COMPARISON" in ex.build_report(ex.summarize(five(sessions)))


# ---------------- the experiment's V5 is the production candidate ----------------

def test_the_experiment_stage_rule_equals_the_production_stage_rule_on_a_grid():
    from intent_engine.score_calculator import evidence_stage as production_stage
    import itertools
    for pv, pages, useful, scrolls, ms in itertools.product((0, 1, 2, 5), (0, 1, 2, 7), (0, 1, 2, 9), (0, 1, 2, 4),
                                                            (0, 9_999, 10_000, 29_999, 30_000, 120_000)):
        assert production_stage(pv, pages, useful, scrolls, ms) == ex.evidence_stage(pv, pages, useful, scrolls, ms / 1000.0)


def test_v5_buckets_equal_the_production_bucket_function_for_every_session():
    from intent_engine.score_calculator import assign_bucket_with_evidence
    run = five(pop_sessions(n_land=300, n_active=800))
    th = run.thresholds["V5"]
    for r in run.results["V5"]:
        if r.status == "scored":
            assert r.bucket == assign_bucket_with_evidence(r.score, th.p35, th.p72, r.evidence_stage)


# ---------------- V1C: the production candidate as shipped (V1 score + the new calibration) ----------------

def test_v1c_score_is_exactly_v1_and_its_buckets_are_the_production_function():
    from intent_engine.score_calculator import assign_bucket_with_evidence
    assert ex.V1CandidateProduction.formula.startswith(ex.V1CurrentProduction.formula)
    run = ex.run_experiment(pop_sessions(n_land=300, n_active=800), ["V1", "V1C"])
    v1, vc = by_id(run, "V1"), by_id(run, "V1C")
    th = run.thresholds["V1C"]
    for sid, a in v1.items():
        b = vc[sid]
        assert (a.score, a.raw_score, a.bounce_penalty, a.struggle_penalty) == (b.score, b.raw_score, b.bounce_penalty, b.struggle_penalty)
        assert b.bucket == assign_bucket_with_evidence(b.score, th.p35, th.p72, b.evidence_stage)
    eligible = sorted(r.score for r in run.results["V1C"] if r.evidence_stage >= 1 and r.score > 0)
    assert th.population == len(eligible) and th.p35 == rp._percentile(eligible, 35)
    assert run.thresholds["V1"] == run.reference_thresholds["V1"]                 # V1 itself is untouched
    assert not [r for r in run.results["V1C"] if r.evidence_stage == 1 and r.bucket == "high"]


# ---------------- V6: V1C with ONLY the bounce penalty changed (-5 / -2 / 0) ----------------

def six(sessions, **kw):
    return ex.run_experiment(sessions, ["V1", "V1C", "V6"], **kw)


def test_v6_equals_v1c_except_for_the_bounce_term():
    run = six(pop_sessions(n_land=300, n_active=800, outcomes=True))
    c, v = by_id(run, "V1C"), by_id(run, "V6")
    for sid, a in c.items():
        b = v[sid]
        assert (a.raw_score, a.struggle_penalty, a.page_modifier, a.evidence_stage) == (b.raw_score, b.struggle_penalty, b.page_modifier, b.evidence_stage)
        expected = ex.clamp_score(a.raw_score, a.struggle_penalty, b.bounce_penalty, a.page_modifier)
        assert b.score == round(expected, 2)
    assert ex.V6BounceControlled.threshold_population == "eligible" and ex.V6BounceControlled.high_min_stage == 2


def test_v6_with_the_v1_penalty_forced_back_reproduces_v1_exactly(monkeypatch):
    monkeypatch.setattr(ex, "V6_BOUNCE_BOTH", -20.0)
    monkeypatch.setattr(ex, "V6_BOUNCE_EITHER", -20.0)
    run = six(pop_sessions(n_land=200, n_active=500))
    v1, v6 = by_id(run, "V1"), by_id(run, "V6")
    assert all(v1[sid].score == v6[sid].score for sid in v1)            # nothing else was changed


@pytest.mark.parametrize("events,duration_s,expected", [(2, 0.01, -5.0), (2, 60, -2.0), (8, 2, -2.0), (8, 5, 0.0), (3, 60, 0.0), (2, 4.99, -5.0), (1, 4.99, -5.0)])
def test_v6_bounce_cases(events, duration_s, expected):
    s = make("b", events=events, duration_s=duration_s, product_views=2, useful=3, page_views=3, page_types=("pdp",))
    assert score_of("V6", s).bounce_penalty == expected


def test_v6_stage_one_never_high_thresholds_eligible_only_and_outcomes_are_not_inputs():
    plain, with_outcomes = six(pop_sessions(outcomes=False)), six(pop_sessions(outcomes=True))
    a, b = by_id(plain, "V6"), by_id(with_outcomes, "V6")
    assert all((a[s].score, a[s].bucket) == (b[s].score, b[s].bucket) for s in a)
    assert not [r for r in plain.results["V6"] if r.status == "scored" and r.evidence_stage == 1 and r.bucket == "high"]
    eligible = sorted(r.score for r in plain.results["V6"] if r.status == "scored" and r.evidence_stage >= 1 and r.score > 0)
    assert plain.thresholds["V6"].population == len(eligible) and plain.thresholds["V6"].p35 == rp._percentile(eligible, 35)
    for s in [guarded(x) for x in pop_sessions(n_land=10, n_active=40)]:
        ex.VERSIONS["V6"].score(s, ex.DayContext(None))
    assert not _mentions_an_outcome(_identifiers(ex.V6BounceControlled))
