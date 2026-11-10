"""
The offline V2 replay (intent_engine/replay.py): Variant A is the production model, the other variants change one
thing each, and nothing about the operational bucket moves.
"""

import random
from datetime import date, datetime, timedelta

import pytest

from intent_engine import replay as rp
from intent_engine import session_scorer
from intent_engine.config import FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD, MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION
from intent_engine.feature_calculator import extract_features
from intent_engine.overrides import apply_hard_overrides
from intent_engine.page_classifier import classify_page_type
from intent_engine.page_modifier import highest_intent_page_type, modifier_for
from intent_engine.penalties import bounce_penalty, struggle_penalty
from intent_engine.score_calculator import assign_bucket, compute_predictive_score
from intent_engine.url_normalizer import normalize_page_path

DAY = datetime(2026, 10, 28, 12, 0, 0)
HUMAN = "Mozilla/5.0 (iPhone)"


def session(sid="s1", duration_s=60, events=6, product_views=0, useful=0, dead=0, clicks=None, scrolls=0, page_views=2,
            atc=0, checkout=0, start=DAY, page_types=("home",), entry="home", ua=HUMAN, **stored):
    row = {
        "session_id": sid, "actor_id": "a-" + sid, "session_start": start, "session_time_spent_ms": int(duration_s * 1000),
        "event_count": events, "page_view_count": page_views, "product_view_count": product_views,
        "click_count": useful + dead if clicks is None else clicks, "useful_click_count": useful, "dead_click_count": dead,
        "add_to_cart_count": atc, "checkout_started_count": checkout, "scroll_count": scrolls,
    }
    row.update(stored)
    return rp.ReplaySession(row, rp.Enrichment(entry, set(page_types), ua, set()))


def one(sess, variant="A"):
    result = rp.replay([sess], rp.VARIANTS[variant])
    return result.results[0]


# ---------------------------
# Variant A is the production model
# ---------------------------
def test_variant_a_reproduces_the_production_functions_exactly():
    rnd = random.Random(7)
    types = ["checkout", "pdp", "collection", "home", "other"]
    sessions = []
    for i in range(1200):
        sessions.append(session(
            f"s{i}", duration_s=rnd.choice([0.5, 2, 4.9, 5, 20, 90, 400]), events=rnd.randint(1, 30),
            product_views=rnd.randint(0, 6), useful=rnd.randint(0, 15), dead=rnd.randint(0, 20), scrolls=rnd.randint(0, 10),
            page_views=rnd.randint(0, 8), atc=rnd.choice([0, 0, 0, 1]), checkout=rnd.choice([0, 0, 0, 1]),
            page_types=rnd.sample(types, rnd.randint(0, 3)), entry=rnd.choice(types),
        ))
    result = rp.replay(sessions, rp.VARIANTS["A"])
    threshold = result.thresholds[DAY.date()]
    p90 = rp.dead_click_p90(s.row for s in sessions)
    by_id = result.by_id()
    checked = 0
    for s in sessions:
        got = by_id[s.row["session_id"]]
        if got.status != "scored":
            continue
        expected = compute_predictive_score(
            extract_features(s.row), struggle_penalty(s.row, p90), bounce_penalty(s.row),
            modifier_for(highest_intent_page_type(s.enrichment.page_types_seen)),
        )
        assert got.score == round(expected, 2)
        bucket = assign_bucket(expected, threshold.p35, threshold.p72)
        assert got.bucket == bucket
        assert got.operational == apply_hard_overrides(bucket, s.enrichment.entry_page_type,
                                                       s.row["checkout_started_count"], s.row["add_to_cart_count"])
        checked += 1
    assert checked > 1000


def test_enrichment_from_rows_matches_the_scorers_enrichment(monkeypatch):
    rows = [
        {"session_id": "a", "occurred_at": datetime(2026, 10, 28, 10, 0, 5), "url": "/products/x?v=1", "user_agent": None},
        {"session_id": "a", "occurred_at": datetime(2026, 10, 28, 10, 0, 1), "url": "/", "user_agent": HUMAN},
        {"session_id": "b", "occurred_at": datetime(2026, 10, 28, 10, 0, 1), "url": "/cart", "user_agent": None},
    ]
    monkeypatch.setattr(session_scorer, "fetch_page_viewed_enrichment", lambda cursor, ids: rows)
    monkeypatch.setattr(session_scorer, "fetch_click_user_agents", lambda cursor, ids: {"b": "curl/8", "c": "Click UA"})
    ids = ["a", "b", "c"]
    theirs = session_scorer._build_enrichment(None, ids)
    ours = rp.enrich_from_rows(ids, rows, {"b": "curl/8", "c": "Click UA"})
    for sid in ids:
        assert (ours[sid].entry_page_type, ours[sid].page_types_seen, ours[sid].user_agent) == (
            theirs[sid].entry_page_type, theirs[sid].page_types_seen, theirs[sid].user_agent)
    assert ours["a"].paths == {"/products/x", "/"}


# ---------------------------
# Bounce
# ---------------------------
BOUNCE_CASES = [
    # name, session, penalty-mode low?, behavioural-mode forced low?
    ("true short bounce", dict(duration_s=2, events=1, clicks=0, page_views=1), True, True),
    ("2 events under 5 s", dict(duration_s=3, events=2, page_views=2), True, True),
    ("2 events over 5 s", dict(duration_s=30, events=2, page_views=2), False, False),
    ("product view over 5 s", dict(duration_s=30, events=2, product_views=1, page_views=1), False, False),
    ("click over 5 s", dict(duration_s=30, events=3, useful=1), False, False),
    ("scroll over 5 s", dict(duration_s=30, events=3, scrolls=2), False, False),
    ("short with a useful click", dict(duration_s=3, events=2, useful=1), True, False),
    ("short with a scroll", dict(duration_s=3, events=2, scrolls=1), True, False),
    ("long session, few events", dict(duration_s=600, events=2, page_views=2), False, False),
]


@pytest.mark.parametrize("name,kwargs,_penalty_low,forced", BOUNCE_CASES, ids=[c[0] for c in BOUNCE_CASES])
def test_behavioural_bounce_forces_low_only_for_a_genuine_bounce(name, kwargs, _penalty_low, forced):
    s = session(page_types=("pdp",), **kwargs)
    assert rp.is_genuine_bounce(s.row, rp.VARIANTS["C"]) is forced


def test_penalty_mode_is_the_production_bounce_rule():
    long_two_events = session(duration_s=600, events=2, page_types=("pdp",))
    assert bounce_penalty(long_two_events.row) == -20.0              # production punishes this session...
    assert one(long_two_events, "A").score == round((6 * 4.5 + 2 * 1.5 - 20.0) * 1.05, 2)   # minutes are capped at 6
    assert one(long_two_events, "C").score == round((6 * 4.5 + 2 * 1.5) * 1.05, 2)           # ...C does not


def test_variant_b_has_no_bounce_penalty_and_c_zeroes_only_genuine_bounces():
    short = session(duration_s=3, events=2, useful=2, page_types=("pdp",))
    assert one(short, "A").score < one(short, "B").score
    assert one(short, "B").score == one(short, "C").score          # engaged short session: not a bounce for C
    genuine = session(duration_s=3, events=1, clicks=0, page_views=1, page_types=("pdp",))
    got = one(genuine, "C")
    assert got.score == 0 and got.bucket == "low" and got.forced_low


def test_the_bounce_rule_is_configurable():
    strict = rp.ReplayConfig("X", bounce_mode="behavioral", bounce_max_duration_ms=2000)
    assert not rp.is_genuine_bounce(session(duration_s=3, events=1, clicks=0).row, strict)
    assert rp.is_genuine_bounce(session(duration_s=1.5, events=1, clicks=0).row, strict)


# ---------------------------
# Score
# ---------------------------
def test_feature_caps_clamp_and_multiplier():
    huge = session(duration_s=3600, events=200, product_views=50, useful=100, scrolls=100, page_views=100,
                   page_types=("checkout",))
    assert one(huge).score == 100.0                                  # capped features sum to 120.4 -> x1.15 -> clamp
    negative = session(duration_s=3, events=1, clicks=0, page_views=0, page_types=())
    assert rp.score_session(negative.row, negative.enrichment, rp.VARIANTS["A"], None)[0] == 0.0

    base = dict(duration_s=60, events=6, product_views=1, useful=2, scrolls=1, page_views=2)
    raw = 11 + 2 * 2.2 + 1 * 4.5 + 1 * 1.8 + 2 * 1.5
    for page_type, current in (("pdp", 1.05), ("home", 0.85), ("other", 0.70), ("collection", 0.95), ("checkout", 1.15)):
        assert one(session(page_types=(page_type,), **base)).score == round(raw * current, 2)
    for page_type, revised in (("home", 0.95), ("other", 0.90), ("collection", 1.00)):
        assert one(session(page_types=(page_type,), **base), "D").score == round(raw * revised, 2)


# ---------------------------
# Page types
# ---------------------------
@pytest.mark.parametrize("url,expected", [
    ("/", "home"), ("/products/shirt-1", "pdp"), ("/collections/new", "collection"), ("/checkout", "checkout"),
    ("/checkouts/abc", "checkout"), ("/cart", "other"), ("/pages/about", "other"), ("/search", "other"),
    ("/en-in/products/shirt", "other"), ("/products/shirt?variant=1#top", "pdp"), ("/collections/new/", "collection"),
    ("", "other"), (None, "other"),
])
def test_page_type_of_a_url(url, expected):
    assert classify_page_type(normalize_page_path(url)) == expected


def test_session_without_page_views_is_other_and_other_paths_are_counted():
    ours = rp.enrich_from_rows(["x", "y"], [
        {"session_id": "y", "occurred_at": DAY, "url": "/cart?x=1", "user_agent": HUMAN},
        {"session_id": "y", "occurred_at": DAY + timedelta(seconds=1), "url": "/products/a", "user_agent": None},
    ])
    assert ours["x"].entry_page_type == "other" and ours["x"].page_types_seen == set()
    assert rp.other_path_counts(ours.values()) == [("/cart", 1)]


# ---------------------------
# Calibration
# ---------------------------
def day_of(n, score_each, start=DAY, prefix="d"):
    """n sessions that all score the same (a 1 minute, 1 product view, home page session scales with `score_each`)."""
    return [session(f"{prefix}{start.day}-{i}", duration_s=60, events=6, product_views=0, useful=score_each,
                    page_views=2, page_types=("pdp",), start=start) for i in range(n)]


def test_fewer_than_500_sessions_use_the_fixed_fallback():
    result = rp.replay(day_of(MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION - 1, 3), rp.VARIANTS["A"])
    t = result.thresholds[DAY.date()]
    assert (t.method, t.p35, t.p72) == ("fixed_fallback", FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD)


def test_500_sessions_use_percentiles_and_a_mixed_distribution_splits_three_ways():
    sessions = [session(f"m{i}", duration_s=60, events=6, useful=i % 13, page_types=("pdp",)) for i in range(600)]
    result = rp.replay(sessions, rp.VARIANTS["A"])
    t = result.thresholds[DAY.date()]
    assert t.method == "percentile" and t.population == 600 and t.p35 < t.p72
    buckets = {b: sum(1 for r in result.results if r.bucket == b) for b in rp.BUCKETS}
    assert all(buckets[b] > 0 for b in rp.BUCKETS)


def test_all_zero_scores_fall_back_and_everything_is_low():
    sessions = [session(f"z{i}", duration_s=3, events=2, clicks=0, page_views=1) for i in range(700)]
    result = rp.replay(sessions, rp.VARIANTS["A"])
    assert result.thresholds[DAY.date()].method == "fixed_fallback"
    assert {r.bucket for r in result.results} == {"low"}


def test_all_eligible_population_counts_the_zeros_and_can_pull_p35_down_to_zero():
    bounces = [session(f"b{i}", duration_s=3, events=2, clicks=0, page_views=1) for i in range(700)]
    engaged = [session(f"e{i}", duration_s=120, events=8, useful=i % 9, product_views=1, page_types=("pdp",)) for i in range(400)]
    positive = rp.replay(bounces + engaged, rp.VARIANTS["A"]).thresholds[DAY.date()]
    everyone = rp.replay(bounces + engaged, rp.VARIANTS["E"]).thresholds[DAY.date()]
    assert positive.population <= 400 + 0 and positive.method == "fixed_fallback"     # < 500 engaged sessions
    assert everyone.population == 1100 and everyone.method == "percentile" and everyone.p35 == 0.0


def test_percentile_cut_offs_fix_the_shape_of_the_distribution():
    import dataclasses
    sessions = [session(f"m{i}", duration_s=20 + i % 300, events=6, useful=i % 13, dead=0, scrolls=i % 7,
                        product_views=i % 4, page_types=("pdp",)) for i in range(2600)]
    default = rp.distribution(rp.replay(sessions, rp.VARIANTS["A"]))
    shifted = rp.distribution(rp.replay(sessions, dataclasses.replace(rp.VARIANTS["A"], low_percentile=47, high_percentile=90)))
    assert shifted["low_pct"] > default["low_pct"] and shifted["high_pct"] < default["high_pct"]
    assert shifted["medium_pct"] > default["medium_pct"]


def test_each_day_is_calibrated_on_its_own_sessions():
    d1, d2 = DAY, DAY + timedelta(days=1)
    sessions = day_of(600, 1, d1, "p") + [session(f"q{i}", duration_s=60, events=6, useful=10, page_types=("pdp",), start=d2)
                                           for i in range(600)]
    result = rp.replay(sessions, rp.VARIANTS["A"])
    assert set(result.thresholds) == {d1.date(), d2.date()}
    assert result.thresholds[d1.date()].p35 != result.thresholds[d2.date()].p35


# ---------------------------
# Bucket boundaries (the production rule the replay uses)
# ---------------------------
@pytest.mark.parametrize("score,expected", [
    (0, "low"), (34.99, "low"), (35.0, "medium"), (71.99, "medium"), (72.0, "high"), (100.0, "high"),
])
def test_bucket_boundaries(score, expected):
    assert assign_bucket(score, 35.0, 72.0) == expected


# ---------------------------
# Operational bucket and summaries
# ---------------------------
def test_operational_bucket_is_the_same_in_every_variant():
    sessions = [
        session("c1", entry="checkout", duration_s=3, events=1, clicks=0),
        session("c2", atc=1, duration_s=3, events=2, useful=0),
        session("c3", checkout=1, duration_s=90, events=9, useful=3, page_types=("pdp",)),
        session("c4", duration_s=3, events=1, clicks=0),
    ]
    forced = {"c1", "c2", "c3"}
    for name in rp.VARIANTS:
        result = rp.replay(sessions, rp.VARIANTS[name])
        for r in result.results:
            assert (r.operational == "high") == (r.session_id in forced), (name, r.session_id)


def test_excluded_sessions_get_no_bucket_and_are_counted():
    sessions = [session("bot", ua="Googlebot/2.1"), session("zero", duration_s=0.5, events=1, clicks=0, scrolls=0),
                session("ok", page_types=("pdp",), useful=2)]
    d = rp.distribution(rp.replay(sessions, rp.VARIANTS["A"]))
    assert d["excluded"] == 2 and d["excluded_by_reason"] == {"bot": 1, "zero_signal": 1} and d["scored"] == 1


def test_transition_matrix_accounts_for_every_session_scored_in_both():
    rnd = random.Random(3)
    sessions = [session(f"t{i}", duration_s=rnd.choice([3, 20, 120]), events=rnd.randint(1, 12), useful=rnd.randint(0, 8),
                        page_types=(rnd.choice(["home", "pdp", "other"]),)) for i in range(800)]
    a, f = rp.replay(sessions, rp.VARIANTS["A"]), rp.replay(sessions, rp.VARIANTS["F2"])
    matrix = rp.transition_matrix(a, f)
    assert sum(matrix.values()) == sum(1 for r in a.results if r.status == "scored")
    assert sum(v for (x, _), v in matrix.items() if x == "low") == rp.distribution(a)["low"]
    same = rp.transition_matrix(a, a)
    assert all(v == 0 for (x, y), v in same.items() if x != y)


def test_struggle_report_shows_why_the_penalty_cannot_fire_when_p90_is_one():
    sessions = [session(f"k{i}", clicks=20, useful=0, dead=20, page_types=("pdp",)) for i in range(30)]
    sessions += [session(f"k2{i}", clicks=20, useful=18, dead=2, page_types=("pdp",)) for i in range(3)]
    result = rp.replay(sessions, rp.VARIANTS["A"])
    report = rp.struggle_report(sessions, result)
    assert report["p90"] == 1.0 and report["would_fire_gt"] == 0 and report["would_fire_gte"] == 30
    assert report["all_dead"] == 30 and report["population_replay_status"] == {"scored": 33}


def test_parity_compares_with_the_stored_scores():
    base = [session(f"p{i}", duration_s=60, events=6, useful=i % 5, page_types=("pdp",)) for i in range(10)]
    a = rp.replay(base, rp.VARIANTS["A"])
    stored = [rp.ReplaySession({**s.row, "intent_status": "scored", "predictive_score": a.by_id()[s.row["session_id"]].score,
                                "intent_bucket": a.by_id()[s.row["session_id"]].bucket}, s.enrichment) for s in base]
    stored[0].row["predictive_score"] += 5
    report = rp.parity(stored, a)
    assert report["compared"] == 10 and report["score_equal"] == 9 and report["max_score_diff"] == 5.0
