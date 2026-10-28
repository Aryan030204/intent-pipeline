"""The pure helpers of scripts/diagnose_intent.py: percentiles, histogram, score frequency and tie summary."""

from intent_engine.score_calculator import assign_bucket
from scripts import diagnose_intent as d


def test_percentiles_use_the_calibration_method_and_cover_the_requested_points():
    table = d.percentile_table(list(range(1, 101)))
    assert set(table) == {f"P{p}" for p in d.PERCENTILES}
    assert table["P35"] in (35, 36) and table["P72"] in (72, 73) and table["P50"] in (50, 51)
    assert d.percentile_table([]) == {}


def test_describe_gives_count_min_max_mean_median():
    assert d.describe([1, 2, 3, 10]) == {"count": 4, "min": 1, "max": 10, "mean": 4.0, "median": 2.5}
    assert d.describe([5, 1, 3])["median"] == 3
    assert d.describe([]) == {}


def test_histogram_ranges_are_inclusive_and_add_up():
    scores = [0, 0, 3, 5, 5.5, 12, 50, 99.5, 100, 100]
    rows = d.histogram(scores)
    assert dict((label, count) for label, count, _ in rows) == {
        "0": 2, "0.01-5": 2, "5.01-10": 1, "10.01-20": 1, "20.01-40": 0, "40.01-60": 1, "60.01-80": 0, "80.01-99.99": 1, "100": 2}
    assert sum(count for _l, count, _s in rows) == len(scores)
    assert round(sum(share for _l, _c, share in rows), 1) == 100.0


def test_score_frequency_lists_the_most_common_scores_with_a_running_share():
    rows = d.score_frequency([0] * 6 + [10] * 3 + [20])
    assert rows[0] == (0, 6, 60.0) and rows[1] == (10, 3, 90.0) and rows[2] == (20, 1, 100.0)


def test_a_big_mass_on_one_score_shows_up_in_the_tie_summary():
    # 6 sessions at 0, 2 at 10 (the P35 cut-off), 2 at 40
    summary = d.tie_summary([0.0] * 6 + [10.0] * 2 + [40.0] * 2, p35=10.0, p72=30.0)
    assert summary["most_common_score"] == 0 and summary["most_common_share_pct"] == 60.0
    assert summary["at_p35"] == 2 and summary["at_p72"] == 0 and summary["unique_scores"] == 3
    assert summary["low_pct"] == 60.0 and summary["medium_pct"] == 20.0 and summary["high_pct"] == 20.0


def test_the_tie_summary_agrees_with_the_real_bucket_rule():
    scores = [0.0, 5.0, 25.0, 25.0, 55.0, 90.0]
    summary = d.tie_summary(scores, 25.0, 55.0)
    expected = [assign_bucket(s, 25.0, 55.0) for s in scores]
    assert summary["low_pct"] == round(100 * expected.count("low") / 6, 2)
    assert summary["high_pct"] == round(100 * expected.count("high") / 6, 2)


def test_the_behaviour_rule_text_matches_the_dashboard_rule():
    flat = " ".join(d.LEVEL_SQL.split())
    assert "session_time_spent_ms < 5000 OR (click_count = 0 AND scroll_count = 0) THEN 'low'" in flat
    assert "click_count > 0 AND (product_view_count > 0 OR add_to_cart_count > 0 OR checkout_started_count > 0) THEN 'high'" in flat


def test_no_sql_alias_in_the_script_is_a_mysql_reserved_word():
    # MySQL rejects `... AS both` (1064): `both` belongs to TRIM(BOTH ...). Catch the whole class of mistakes.
    import re

    reserved = {"both", "leading", "trailing", "group", "order", "rank", "rows", "row", "window", "groups", "lead", "lag", "system",
                "function", "condition", "current", "key", "keys", "match", "read", "rename", "replace", "repeat", "range", "release",
                "show", "index", "interval", "left", "right", "limit", "lock", "long", "option", "or", "and", "not", "outer", "primary",
                "procedure", "purge", "select", "from", "where", "having", "cube", "dense_rank", "first_value", "last_value", "over",
                "partition", "recursive", "usage", "use", "when", "then", "case", "end", "else", "if", "in", "is", "like", "null",
                "true", "false", "table", "to", "unique", "values", "with", "write", "xor", "zerofill", "year_month"}
    datatypes = {"char", "signed", "unsigned", "decimal", "date", "datetime", "time"}
    with open(d.__file__, encoding="utf-8") as handle:
        aliases = set(re.findall(r"\sAS\s+([A-Za-z_][A-Za-z0-9_]*)", handle.read()))
    assert not ((aliases - datatypes) & reserved), sorted((aliases - datatypes) & reserved)


def test_arguments():
    args = d.parse_args(["--brand", "BBB", "--date", "2026-10-28", "--score-table"])
    assert str(args.date) == "2026-10-28" and args.score_table is True
    assert args.clarity_values == (42.87, 44.77, 12.36) and args.clarity_sessions is None


def test_the_clarity_numbers_can_be_given_and_are_validated():
    ok = d.parse_args(["--brand", "BBB", "--date", "2026-10-28", "--clarity", "40,45,15", "--clarity-sessions", "12251"])
    assert ok.clarity_values == (40.0, 45.0, 15.0) and ok.clarity_sessions == 12251
    for bad in ("40,45", "a,b,c"):
        try:
            d.parse_args(["--brand", "BBB", "--date", "2026-10-28", "--clarity", bad])
        except SystemExit:
            continue
        raise AssertionError(f"{bad!r} should be rejected")


def test_the_comparison_table_gives_percentage_point_deltas():
    rows = d.comparison_rows({"low": 3500, "medium": 4500, "high": 2000}, (42.87, 44.77, 12.36), clarity_sessions=12251)
    assert rows[0] == {"metric": "Sessions", "datum": 10000, "clarity": 12251, "delta": -2251}
    low, medium, high = rows[1:]
    assert (low["datum"], low["delta_pp"]) == (35.0, round(35.0 - 42.87, 2))
    assert (medium["datum"], medium["delta_pp"]) == (45.0, round(45.0 - 44.77, 2))
    assert (high["datum"], high["delta_pp"]) == (20.0, round(20.0 - 12.36, 2))
    assert d.comparison_rows({})[0]["datum"] == 0.0           # no sessions: zeros, no division error


def test_the_feature_pivot_puts_each_bucket_in_its_column_and_the_rollup_row_in_overall():
    rows = [{"bucket": "low", "avg_scrolls": 1.0}, {"bucket": "medium", "avg_scrolls": 2.0}, {"bucket": "high", "avg_scrolls": 4.0},
            {"bucket": None, "avg_scrolls": 2.5}]
    out = d.pivot_by_bucket(rows, ["avg_scrolls", "avg_missing"])
    assert out[0] == {"feature": "avg_scrolls", "low": 1.0, "medium": 2.0, "high": 4.0, "overall": 2.5}
    assert out[1] == {"feature": "avg_missing", "low": None, "medium": None, "high": None, "overall": None}
