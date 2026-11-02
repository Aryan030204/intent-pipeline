"""
product_intent_daily: the product_behavior_daily counts split by the intent level of the session they happened in,
once for the behaviour rule and once for the score bucket. Sessions with no level are kept as 'unclassified'.
"""

from datetime import date

from pipeline import rollups
from tests.test_intent_phase2_phase3 import Conn, ScriptCursor

DAY = date(2026, 10, 28)


def agg(product_id, behavior, score, views=0, atc=0, checkouts=0, viewers=None, sessions=None, title="Shirt"):
    return {"d": DAY, "product_id": product_id, "product_title": title, "behavior_level": behavior,
            "score_level": score, "product_views": views, "unique_viewers": views if viewers is None else viewers,
            "unique_sessions": views if sessions is None else sessions, "add_to_cart_count": atc,
            "checkout_started_count": checkouts}


def cart(product_ids, behavior, score, checkouts=1, titles="['A', 'B']"):
    return {"d": DAY, "product_id": product_ids, "product_title": titles, "behavior_level": behavior,
            "score_level": score, "checkouts": checkouts}


def run(agg_rows, cart_rows=(), lookback_days=1):
    cursor = ScriptCursor(rows_for={"AND LEFT(e.product_id, 1) <> '['": list(agg_rows),
                                    "AND LEFT(e.product_id, 1) = '['": list(cart_rows)})
    rollups._rollup_product_intent_daily(cursor, Conn(), [DAY], lookback_days=lookback_days)
    return cursor


def stored(cursor):
    """{(basis, level, product): (views, viewers, sessions, atc, checkouts)}"""
    out = {}
    for sql, rows in cursor.many_calls:
        if "INSERT INTO product_intent_daily" in sql:
            for r in rows:
                out[(r[1], r[2], r[3])] = tuple(r[5:10])
    return out


def test_each_product_is_written_once_per_basis_at_that_sessions_level():
    cursor = run([agg("11", "high", "medium", views=3, atc=1)])
    assert stored(cursor) == {
        ("behavior", "high", "11"): (3, 3, 3, 1, 0),
        ("score", "medium", "11"): (3, 3, 3, 1, 0),
    }


def test_a_product_seen_in_sessions_of_different_levels_is_split_between_them():
    cursor = run([agg("11", "low", "low", views=5), agg("11", "high", "high", views=2, atc=2)])
    got = stored(cursor)
    assert got[("behavior", "low", "11")][0] == 5 and got[("behavior", "high", "11")][0] == 2
    assert sum(v[0] for k, v in got.items() if k[0] == "behavior") == 7


def test_sessions_without_a_level_are_kept_as_unclassified():
    cursor = run([agg("11", "unclassified", "unclassified", views=4)])
    assert set(stored(cursor)) == {("behavior", "unclassified", "11"), ("score", "unclassified", "11")}


def test_checkout_carts_credit_every_product_in_the_cart_to_the_sessions_level():
    cursor = run([], [cart("[1.0, 2.0]", "high", "high", checkouts=3)])
    got = stored(cursor)
    for pid in ("1", "2"):
        assert got[("behavior", "high", pid)][4] == 3
        assert got[("score", "high", pid)][4] == 3


def test_the_level_rules_match_the_dashboard():
    assert "s.session_time_spent_ms < 5000 OR (s.click_count = 0 AND s.scroll_count = 0)" in rollups._BEHAVIOR_LEVEL_EXPR
    assert "s.click_count > 0 AND (s.add_to_cart_count > 0 OR s.checkout_started_count > 0)" in rollups._BEHAVIOR_LEVEL_EXPR
    assert "intent_exclusion_reason = 'bot' THEN 'unclassified'" in rollups._BEHAVIOR_LEVEL_EXPR
    assert "s.intent_status = 'scored'" in rollups._SCORE_LEVEL_EXPR


def test_the_recent_days_are_always_recomputed():
    assert rollups._product_intent_dates([DAY], 3) == [date(2026, 10, 26), date(2026, 10, 27), DAY]
    assert rollups._product_intent_dates([date(2026, 10, 20), DAY], 3)[0] == date(2026, 10, 20)


def test_it_is_a_registered_stage():
    assert "product_intent_daily" in [name for name, _ in rollups._ROLLUP_STAGES]
