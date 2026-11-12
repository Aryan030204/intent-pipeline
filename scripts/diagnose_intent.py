"""
READ-ONLY diagnostics for one brand and one day of intent data. It runs SELECTs only (on the read replica when
DB_READ_REPLICA_HOST is set) and prints the numbers needed to explain a gap between the Datum intent
distribution and another tool's:

    docker exec intent-kafka-worker python scripts/diagnose_intent.py --brand BBB --date 2026-10-28

Sections: 1 reconciliation of the session counts, 2 behaviour-rule levels and why, 3 score distribution and
thresholds, 4 predictive vs operational buckets, 5 cohorts, 6 features by bucket, 7 bounce rule, 8 clicks,
9 page types and overrides, 10 freshness (last ingestion / scoring / calibration / rollup), 11 duplicates,
12 Score + Sessions next to Clarity (deltas in percentage points), 13 Feature | Low | Medium | High | Overall,
14 do readers who do not click end up Low, 15 product view duplication, 16 bounce penalty impact,
17 hard override impact, 18 scoring backlog by day, 19 event-name census (do scroll events reach scroll_count?),
20 dead-click rate distribution (why P90 is 1.0), 21 canonical session reconciliation (add --deep for the per-session check).
Compare with another tool's numbers with --clarity 42.87,44.77,12.36 --clarity-sessions 12251, and run only some
sections with --only 10,18,19. A day the scoring worker has already finished (say yesterday) gives the real Score view.

Nothing is written, no schema is changed, and the heaviest query is one pass over the day's intent_sessions
rows plus one DISTINCT over the day's events. Dates are the store-local calendar day the events carry.
"""

import argparse
import os
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from intent_engine.score_calculator import assign_bucket  # noqa: E402
from intent_engine.threshold_calibration import _percentile  # noqa: E402

PERCENTILES = (10, 25, 35, 50, 72, 75, 90, 95, 99)
SCORE_BINS = ((0, 0), (0.01, 5), (5.01, 10), (10.01, 20), (20.01, 40), (40.01, 60), (60.01, 80), (80.01, 99.99), (100, 100))
MAX_SCORES = 400000

# The dashboard's behaviour rule (analytics/services/intentFunnelService.js BEHAVIOR_LEVEL_SQL), same text.
LEVEL_SQL = """CASE
    WHEN intent_exclusion_reason = 'bot' THEN 'bot'
    WHEN session_time_spent_ms < 5000 OR (click_count = 0 AND scroll_count = 0) THEN 'low'
    WHEN click_count > 0 AND (product_view_count > 0 OR add_to_cart_count > 0 OR checkout_started_count > 0) THEN 'high'
    ELSE 'medium'
  END"""
BOUNCE_SQL = "(event_count <= 2 OR session_time_spent_ms < 5000)"   # penalties.bounce_penalty


# ---------------- pure helpers (unit tested) ----------------

def percentile_table(scores: Sequence[float], pcts: Sequence[int] = PERCENTILES) -> Dict[str, float]:
    """Nearest-rank percentiles exactly as the daily calibration computes them (_percentile)."""
    ordered = sorted(scores)
    return {f"P{p}": _percentile(ordered, p) for p in pcts} if ordered else {}


def describe(scores: Sequence[float]) -> Dict[str, float]:
    if not scores:
        return {}
    ordered = sorted(scores)
    n = len(ordered)
    median = ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
    return {"count": n, "min": ordered[0], "max": ordered[-1], "mean": round(sum(ordered) / n, 3), "median": median}


def histogram(scores: Sequence[float], bins=SCORE_BINS) -> List[Tuple[str, int, float]]:
    """[(label, sessions, share %)] for inclusive score ranges."""
    total = len(scores) or 1
    out = []
    for low, high in bins:
        count = sum(1 for s in scores if low <= s <= high)
        out.append((f"{low:g}" if low == high else f"{low:g}-{high:g}", count, round(100 * count / total, 2)))
    return out


def score_frequency(scores: Sequence[float], limit: int = 25) -> List[Tuple[float, int, float]]:
    """score | session_count | cumulative_percentage, most common scores first."""
    total = len(scores) or 1
    running, rows = 0, []
    for score, count in Counter(round(s, 2) for s in scores).most_common(limit):
        running += count
        rows.append((score, count, round(100 * running / total, 2)))
    return rows


def tie_summary(scores: Sequence[float], p35: float, p72: float) -> Dict[str, Any]:
    """How many sessions sit exactly on a cut-off, and what the stored cut-offs make of the population."""
    total = len(scores) or 1
    buckets = Counter(assign_bucket(s, p35, p72) for s in scores)
    top_value, top_count = (Counter(round(s, 2) for s in scores).most_common(1) or [(None, 0)])[0]
    return {
        "unique_scores": len({round(s, 2) for s in scores}),
        "most_common_score": top_value, "most_common_count": top_count,
        "most_common_share_pct": round(100 * top_count / total, 2),
        "at_p35": sum(1 for s in scores if s == p35), "at_p72": sum(1 for s in scores if s == p72),
        "low_pct": round(100 * buckets["low"] / total, 2),
        "medium_pct": round(100 * buckets["medium"] / total, 2),
        "high_pct": round(100 * buckets["high"] / total, 2),
    }


CLARITY_DEFAULT = (42.87, 44.77, 12.36)    # low, medium, high % of sessions, the Clarity screenshot compared against


def comparison_rows(counts: Dict[str, int], clarity: Sequence[float] = CLARITY_DEFAULT,
                    clarity_sessions: Optional[int] = None) -> List[Dict[str, Any]]:
    """Metric | Datum | Clarity | Delta (percentage points), from session counts per predictive bucket."""
    total = sum(int(counts.get(level, 0)) for level in ("low", "medium", "high"))
    rows: List[Dict[str, Any]] = []
    if clarity_sessions is not None:
        rows.append({"metric": "Sessions", "datum": total, "clarity": clarity_sessions, "delta": total - clarity_sessions})
    for level, reference in zip(("low", "medium", "high"), clarity):
        share = round(100 * int(counts.get(level, 0)) / total, 2) if total else 0.0
        rows.append({"metric": f"{level.capitalize()} %", "datum": share, "clarity": reference, "delta_pp": round(share - reference, 2)})
    return rows


def pivot_by_bucket(rows: Sequence[Dict[str, Any]], features: Sequence[str]) -> List[Dict[str, Any]]:
    """Rows from a GROUP BY bucket WITH ROLLUP (bucket None = everything) -> Feature | Low | Medium | High | Overall."""
    by_bucket = {(r.get("bucket") or "overall"): r for r in rows}
    out = []
    for feature in features:
        out.append({"feature": feature, **{b: (None if by_bucket.get(b) is None or by_bucket[b].get(feature) is None
                                               else float(by_bucket[b][feature])) for b in ("low", "medium", "high", "overall")}})
    return out


DEAD_RATE_BUCKETS = ((0.0, 0.0, "0% dead"), (0.0, 0.25, "up to 25%"), (0.25, 0.5, "25-50%"), (0.5, 0.75, "50-75%"),
                     (0.75, 0.9, "75-90%"), (0.9, 1.0, "90% up to 100%"), (1.0, 1.0, "exactly 100% dead"))


def dead_click_summary(rates: Sequence[float]) -> Dict[str, Any]:
    """Distribution of per-session dead-click rates (dead clicks / clicks, sessions with at least 10 clicks): the
    percentiles the daily calibration takes (nearest rank), the maximum, and how many sessions sit at each level.
    `at_or_above_p90` / `above_p90` show why a rule of "rate > P90" can never fire when P90 is the maximum."""
    ordered = sorted(rates)
    n = len(ordered)
    if not n:
        return {"count": 0}
    pct = {f"P{p}": _percentile(ordered, p) for p in (50, 75, 90, 95, 99)}
    buckets = []
    for low, high, label in DEAD_RATE_BUCKETS:
        if low == high:
            count = sum(1 for r in ordered if r == low)
        else:
            count = sum(1 for r in ordered if low < r <= high and not (high == 1.0 and r == 1.0))
        buckets.append({"dead_click_rate": label, "sessions": count, "share_pct": round(100 * count / n, 2)})
    p90 = pct["P90"]
    return {
        "count": n, **pct, "max": ordered[-1],
        "above_p90": sum(1 for r in ordered if r > p90), "at_or_above_p90": sum(1 for r in ordered if r >= p90),
        "buckets": buckets,
    }


def reconciliation_report(n: Dict[str, Optional[int]]) -> List[Dict[str, Any]]:
    """Canonical session counts next to every other session figure, each with its difference from the canonical total.
    Canonical = finished rows of intent_sessions + open session records (the dashboard's Total Sessions)."""
    closed, open_ = int(n.get("closed") or 0), int(n.get("open") or 0)
    canonical = closed + open_
    status = int(n.get("scored") or 0) + int(n.get("pending") or 0) + int(n.get("excluded") or 0)

    def row(label, value, note=""):
        return {"figure": label, "sessions": value, "diff_from_canonical": None if value is None else value - canonical, "note": note}

    out = [
        row("canonical sessions (closed rows + open)", canonical, "= dashboard Total Sessions"),
        row("closed rows in intent_sessions", closed),
        row("open session records (cursors)", open_, "live + waiting to be swept"),
        row("scored + pending + excluded", status, "must equal the closed rows"),
        row("  scored", n.get("scored")), row("  pending", n.get("pending")),
        row("  excluded (all reasons)", n.get("excluded")), row("  of which bots", n.get("bots")),
        row("  of which zero-signal", n.get("zero_signal")),
        row("intent distribution population (closed, not bot)", n.get("distribution"), "behaviour donut, plus live sessions"),
        row("rollup sessions_started", n.get("rollup"), "stale by up to one rollup run"),
    ]
    out[3]["note"] += "" if status == closed else f"  MISMATCH: {status - closed:+d} against the closed rows"
    return out


# ---------------- output ----------------

def say(text: str = "") -> None:
    print(text, flush=True)


def heading(number: int, title: str) -> None:
    say()
    say(f"==== {number}. {title} " + "=" * max(0, 70 - len(title)))


def show(rows: Sequence[Dict[str, Any]], columns: Optional[Sequence[str]] = None) -> None:
    if not rows:
        say("  (no rows)")
        return
    columns = list(columns or rows[0].keys())
    cells = [[("" if r.get(c) is None else (f"{r[c]:.2f}" if isinstance(r[c], float) else str(r[c]))) for c in columns] for r in rows]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(columns)]
    say("  " + "  ".join(c.ljust(widths[i]) for i, c in enumerate(columns)))
    for row in cells:
        say("  " + "  ".join(row[i].ljust(widths[i]) for i in range(len(columns))))


def table(pairs: Sequence[Tuple[str, Any]]) -> None:
    for label, value in pairs:
        say(f"  {label:<46} {value}")


# ---------------- queries ----------------

class Reader:
    def __init__(self, cursor) -> None:
        self.cursor = cursor

    def rows(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        self.cursor.execute(sql, tuple(params))
        return list(self.cursor.fetchall())

    def one(self, sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
        found = self.rows(sql, params)
        return found[0] if found else {}

    def optional(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        try:
            return self.rows(sql, params)
        except Exception as exc:    # a missing table or column in an older database: say so, keep going
            say(f"  (skipped: {type(exc).__name__}: {str(exc)[:140]})")
            return []


def reconciliation(q: Reader, start: datetime, end: datetime, day: date) -> None:
    heading(1, "SESSION COUNT RECONCILIATION (why two numbers for 'sessions' differ)")
    events = q.one("SELECT COUNT(DISTINCT session_id) AS n FROM behavioral_events WHERE occurred_at >= %s AND occurred_at < %s "
                   "AND session_id IS NOT NULL AND session_id <> ''", (start, end))
    rollup = q.optional("SELECT sessions_started, updated_at FROM intent_daily_summary WHERE summary_date = %s", (day,))
    rows = q.one("SELECT COUNT(*) AS total, SUM(session_time_spent_ms IS NULL) AS no_duration, "
                 "SUM(intent_exclusion_reason = 'bot') AS bots FROM intent_sessions WHERE session_start >= %s AND session_start < %s",
                 (start, end))
    cursors = q.optional("SELECT COUNT(*) AS n, MAX(last_event_at) AS newest FROM intent_actor_cursors "
                         "WHERE session_id <> '' AND session_start >= %s AND session_start < %s", (start, end))
    table([
        ("distinct session_id in behavioral_events (live)", events.get("n")),
        ("intent_daily_summary.sessions_started (rollup)", rollup[0]["sessions_started"] if rollup else None),
        ("  ...rollup row last updated", rollup[0]["updated_at"] if rollup else None),
        ("intent_sessions rows (closed + swept)", rows.get("total")),
        ("  of which still without a duration", rows.get("no_duration")),
        ("  of which bots (only known once scored)", rows.get("bots")),
        ("open cursors that started this day (live/unswept)", cursors[0]["n"] if cursors else None),
    ])
    started = rollup[0]["sessions_started"] if rollup and rollup[0]["sessions_started"] is not None else None
    if started is not None and rows.get("total") is not None:
        gap = int(rows["total"]) + (int(cursors[0]["n"]) if cursors else 0) - int(started)
        say(f"  closed + open - sessions_started = {gap}   (positive: the rollup number is older than the live tables)")
    status = q.optional("SELECT intent_status, COALESCE(intent_exclusion_reason, '-') AS reason, COUNT(*) AS sessions "
                        "FROM intent_sessions WHERE session_start >= %s AND session_start < %s GROUP BY 1, 2 ORDER BY 3 DESC", (start, end))
    say("  by scoring status:")
    show(status)


def behaviour_levels(q: Reader, start: datetime, end: datetime) -> None:
    heading(2, "BEHAVIOUR-RULE LEVELS (what the dashboard donut shows by default)")
    say("  rule: low = under 5 s OR no clicks and no scrolls; high = clicks AND (product view OR add to cart OR checkout); else medium")
    levels = q.rows(f"SELECT lvl AS level, COUNT(*) AS sessions, COUNT(DISTINCT actor_id) AS visitors FROM "
                    f"(SELECT actor_id, {LEVEL_SQL} AS lvl FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                    f"AND session_time_spent_ms IS NOT NULL) s GROUP BY lvl ORDER BY FIELD(lvl, 'high', 'medium', 'low', 'bot')", (start, end))
    total = sum(int(r["sessions"]) for r in levels if r["level"] != "bot") or 1
    for r in levels:
        r["share_pct"] = round(100 * int(r["sessions"]) / total, 2) if r["level"] != "bot" else None
    show(levels)
    say("  why LOW (a session can be both):")
    show(q.rows("SELECT SUM(session_time_spent_ms < 5000) AS under_5s, SUM(click_count = 0 AND scroll_count = 0) AS no_click_no_scroll, "
                "SUM(session_time_spent_ms < 5000 AND click_count = 0 AND scroll_count = 0) AS both_conditions, "
                "SUM(event_count = 1) AS single_event_sessions FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                "AND session_time_spent_ms IS NOT NULL AND (intent_exclusion_reason IS NULL OR intent_exclusion_reason <> 'bot')", (start, end)))
    say("  MEDIUM (5 s or more, some interaction, no key action) split:")
    show(q.rows("SELECT SUM(click_count = 0 AND scroll_count > 0) AS scroll_only, SUM(click_count > 0 AND scroll_count = 0) AS click_only, "
                "SUM(click_count > 0 AND scroll_count > 0) AS click_and_scroll FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND session_time_spent_ms >= 5000 AND (click_count > 0 OR scroll_count > 0) "
                "AND NOT (click_count > 0 AND (product_view_count > 0 OR add_to_cart_count > 0 OR checkout_started_count > 0)) "
                "AND (intent_exclusion_reason IS NULL OR intent_exclusion_reason <> 'bot')", (start, end)))
    say("  HIGH split by the key action that qualified it:")
    show(q.rows("SELECT SUM(product_view_count > 0 AND add_to_cart_count = 0 AND checkout_started_count = 0) AS product_view_only, "
                "SUM(add_to_cart_count > 0 AND checkout_started_count = 0) AS add_to_cart_no_checkout, "
                "SUM(checkout_started_count > 0) AS checkout_started FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND session_time_spent_ms >= 5000 AND click_count > 0 "
                "AND (product_view_count > 0 OR add_to_cart_count > 0 OR checkout_started_count > 0) "
                "AND (intent_exclusion_reason IS NULL OR intent_exclusion_reason <> 'bot')", (start, end)))
    say("  WHAT-IF (not applied): if only add to cart / checkout counted as the key action for HIGH:")
    what_if = q.rows("SELECT lvl AS level, COUNT(*) AS sessions FROM (SELECT CASE "
                     "WHEN intent_exclusion_reason = 'bot' THEN 'bot' "
                     "WHEN session_time_spent_ms < 5000 OR (click_count = 0 AND scroll_count = 0) THEN 'low' "
                     "WHEN click_count > 0 AND (add_to_cart_count > 0 OR checkout_started_count > 0) THEN 'high' ELSE 'medium' END AS lvl "
                     "FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND session_time_spent_ms IS NOT NULL) s "
                     "GROUP BY lvl ORDER BY FIELD(lvl, 'high', 'medium', 'low', 'bot')", (start, end))
    wtotal = sum(int(r["sessions"]) for r in what_if if r["level"] != "bot") or 1
    for r in what_if:
        r["share_pct"] = round(100 * int(r["sessions"]) / wtotal, 2) if r["level"] != "bot" else None
    show(what_if)


def score_view(q: Reader, start: datetime, end: datetime, day: date, score_table: bool) -> List[float]:
    heading(3, "SCORE DISTRIBUTION AND THRESHOLDS (the percentile model)")
    scored = q.rows("SELECT predictive_score FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                    "AND intent_status = 'scored' AND predictive_score IS NOT NULL LIMIT %s", (start, end, MAX_SCORES))
    scores = [float(r["predictive_score"]) for r in scored]
    say(f"  scored sessions read: {len(scores)}")
    if not scores:
        return scores
    table(sorted(describe(scores).items()))
    say("  percentiles (nearest rank, the calibration's own method):")
    table(sorted(percentile_table(scores).items(), key=lambda kv: int(kv[0][1:])))
    say("  score histogram:")
    show([{"score_range": a, "sessions": b, "share_pct": c} for a, b, c in histogram(scores)])
    say("  most common exact scores: score | session_count | cumulative_percentage")
    show([{"score": a, "session_count": b, "cumulative_pct": c} for a, b, c in score_frequency(scores)])
    if score_table:
        say("  full table (--score-table):")
        show([{"score": a, "session_count": b, "cumulative_pct": c} for a, b, c in score_frequency(scores, limit=10 ** 6)])
    thresholds = q.optional("SELECT score_date, scored_session_count, p35_threshold, p72_threshold, p90_dead_click_rate, calibration_method "
                            "FROM intent_thresholds_daily WHERE score_date BETWEEN %s AND %s ORDER BY score_date DESC",
                            (day - timedelta(days=4), day))
    say("  stored thresholds (this day and the days before; a day without its own row is scored with the latest earlier one):")
    show(thresholds)
    used = next((t for t in thresholds if t["score_date"] == day), None) or (thresholds[0] if thresholds else None)
    if used:
        p35, p72 = float(used["p35_threshold"]), float(used["p72_threshold"])
        say(f"  applying the stored cut-offs P35={p35} P72={p72} (row of {used['score_date']}) to today's scores:")
        table(sorted(tie_summary(scores, p35, p72).items()))
    engaged = [s for s in scores if s > 0]
    if engaged:
        say("  cut-offs recomputed from today's own scores, for comparison:")
        table([("all scored sessions", {k: v for k, v in percentile_table(scores).items() if k in ("P35", "P72")}),
               ("engaged only (score above 0)", {k: v for k, v in percentile_table(engaged).items() if k in ("P35", "P72")}),
               ("sessions scoring exactly 0", f"{len(scores) - len(engaged)} ({round(100 * (len(scores) - len(engaged)) / len(scores), 2)}%)")])
    return scores


def buckets(q: Reader, start: datetime, end: datetime) -> None:
    heading(4, "PREDICTIVE vs OPERATIONAL BUCKETS (scored sessions)")
    show(q.rows("SELECT COALESCE(intent_bucket, '(null)') AS predictive_bucket, COUNT(*) AS sessions FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' GROUP BY 1 ORDER BY 2 DESC", (start, end)))
    show(q.rows("SELECT COALESCE(operational_bucket, '(null)') AS operational_bucket, COUNT(*) AS sessions FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' GROUP BY 1 ORDER BY 2 DESC", (start, end)))
    say("  visitor level (actor_intent): scored visitors by their best session's bucket, whole table")
    show(q.optional("SELECT COALESCE(intent_bucket, '(null)') AS visitor_bucket, COUNT(*) AS visitors FROM intent_actors GROUP BY 1"))


def cohorts(q: Reader, start: datetime, end: datetime) -> None:
    heading(5, "COHORTS (scored sessions): size, average score and bucket mix")
    defs = [
        ("with product views", "product_view_count > 0"), ("without product views", "product_view_count = 0"),
        ("with add to cart", "add_to_cart_count > 0"), ("with checkout started", "checkout_started_count > 0"),
        ("page views only", "click_count = 0 AND scroll_count = 0 AND product_view_count = 0 AND add_to_cart_count = 0 AND checkout_started_count = 0"),
        ("bounced (rule)", BOUNCE_SQL), ("not bounced", f"NOT {BOUNCE_SQL}"),
    ]
    out = []
    for label, cond in defs:
        r = q.one(f"SELECT COUNT(*) AS sessions, AVG(predictive_score) AS avg_score, "
                  f"SUM(intent_bucket = 'low') AS low, SUM(intent_bucket = 'medium') AS medium, SUM(intent_bucket = 'high') AS high "
                  f"FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' AND ({cond})", (start, end))
        out.append({"cohort": label, **{k: (float(v) if k == "avg_score" and v is not None else v) for k, v in r.items()}})
    show(out)


def features_by_bucket(q: Reader, start: datetime, end: datetime) -> None:
    heading(6, "FEATURES BY BUCKET (what drives HIGH): averages over scored sessions")
    for column in ("intent_bucket", "operational_bucket"):
        say(f"  grouped by {column}:")
        show(q.rows(
            f"SELECT {column} AS bucket, COUNT(*) AS sessions, AVG(product_view_count) AS avg_product_views, AVG(page_view_count) AS avg_page_views, "
            f"AVG(session_time_spent_ms) / 1000 AS avg_seconds, AVG(scroll_count) AS avg_scrolls, AVG(useful_click_count) AS avg_useful_clicks, "
            f"AVG(dead_click_count) AS avg_dead_clicks, AVG(event_count) AS avg_events, 100 * AVG(add_to_cart_count > 0) AS pct_atc, "
            f"100 * AVG(checkout_started_count > 0) AS pct_checkout, 100 * AVG({BOUNCE_SQL}) AS pct_bounced, AVG(predictive_score) AS avg_score "
            f"FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' GROUP BY {column} "
            f"ORDER BY FIELD({column}, 'high', 'medium', 'low')", (start, end)))
    say("  (raw score before penalties and multiplier is not stored; entry page types are in section 9)")


def bounce_rule(q: Reader, start: datetime, end: datetime) -> None:
    heading(7, "BOUNCE RULE: event_count <= 2 OR duration < 5 s  (event_count = EVERY stored event of the session)")
    show(q.rows(f"SELECT SUM(event_count <= 2) AS two_or_fewer_events, SUM(session_time_spent_ms < 5000) AS under_5s, "
                f"SUM(event_count <= 2 AND session_time_spent_ms >= 5000) AS bounced_only_by_event_count, "
                f"SUM(session_time_spent_ms < 5000 AND event_count > 2) AS bounced_only_by_duration, COUNT(*) AS scored "
                f"FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored'", (start, end)))
    say("  two-event sessions that last 5 s or more (-20 applied), by what the two events were:")
    show(q.rows("SELECT page_view_count AS page_views, product_view_count AS product_views, click_count AS clicks, scroll_count AS scrolls, "
                "add_to_cart_count AS atc, checkout_started_count AS checkout, COUNT(*) AS sessions FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' AND event_count = 2 "
                "AND session_time_spent_ms >= 5000 GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY sessions DESC LIMIT 10", (start, end)))
    say("  example session ids of that kind (page view + product view, 5 s or more) to inspect their events:")
    show(q.rows("SELECT session_id, session_time_spent_ms, predictive_score, intent_bucket FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' AND event_count = 2 AND page_view_count = 1 "
                "AND product_view_count = 1 AND session_time_spent_ms >= 5000 ORDER BY session_start DESC LIMIT 10", (start, end)))
    say("  duration = last event - first event of the session (pipeline/intent_session_state.py closed_session_doc); there is no")
    say("  heartbeat or visibility event, so time on the last page and time in a hidden tab are not in it.")


def clicks(q: Reader, start: datetime, end: datetime) -> None:
    heading(8, "CLICKS (click_events): useful vs dead")
    show(q.rows("SELECT COUNT(*) AS clicks, SUM(click_bucket = 'useful_click') AS useful, SUM(click_bucket = 'dead_click') AS dead, "
                "SUM(click_bucket IS NULL) AS unclassified, 100 * SUM(click_bucket = 'dead_click') / COUNT(*) AS dead_pct "
                "FROM click_events WHERE occurred_at >= %s AND occurred_at < %s", (start, end)))
    say("  useful = the browser reported url_changed OR cart_changed OR ui_changed OR meaningful_scroll after the click")
    say("  (alerts-service/services/intent/normalize.js classifyClick); the signals are computed in the web pixel.")
    show(q.rows("SELECT SUM(url_changed) AS url_changed, SUM(cart_changed) AS cart_changed, SUM(ui_changed) AS ui_changed, "
                "SUM(meaningful_scroll) AS meaningful_scroll FROM click_events WHERE occurred_at >= %s AND occurred_at < %s", (start, end)))


def page_types_and_overrides(q: Reader, start: datetime, end: datetime) -> None:
    heading(9, "PAGE TYPES AND THE HARD HIGH OVERRIDE (scored sessions)")
    say("  entry page type (home '/', pdp '/products/', collection '/collections/', checkout '/checkout', else other):")
    show(q.rows("SELECT COALESCE(entry_page_type, '(null)') AS entry_page_type, COUNT(*) AS sessions FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' GROUP BY 1 ORDER BY 2 DESC", (start, end)))
    show(q.rows("SELECT COUNT(*) AS scored_sessions, SUM(add_to_cart_count > 0) AS with_atc, SUM(checkout_started_count > 0) AS with_checkout_started, "
                "SUM(entry_page_type = 'checkout') AS entered_on_checkout, SUM(operational_bucket = 'high') AS operational_high, "
                "SUM(intent_bucket = 'high') AS predictive_high, SUM(operational_bucket = 'high' AND intent_bucket <> 'high') AS forced_high_by_override, "
                "100 * SUM(operational_bucket = 'high') / COUNT(*) AS operational_high_pct "
                "FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored'", (start, end)))


def freshness(q: Reader, start: datetime) -> None:
    heading(10, "FRESHNESS: last ingestion, scoring, calibration, rollup")
    # Cheap by design: the newest primary-key row, and a bounded window for the scoring time. MAX() over a column
    # with no index (ingested_at, intent_scored_at) scans the whole table.
    table([
        ("last event ingested (newest behavioral_events row)", (q.rows("SELECT ingested_at AS t FROM behavioral_events ORDER BY id DESC LIMIT 1") or [{}])[0].get("t")),
        ("last session row written (newest intent_sessions row)", (q.rows("SELECT updated_at AS t FROM intent_sessions ORDER BY id DESC LIMIT 1") or [{}])[0].get("t")),
        ("last session scored (sessions of the last 3 days)", q.one("SELECT MAX(intent_scored_at) AS t FROM intent_sessions WHERE session_start >= %s",
                                                                    (start - timedelta(days=2),)).get("t")),
        ("last daily threshold row", (q.optional("SELECT MAX(score_date) AS t FROM intent_thresholds_daily") or [{}])[0].get("t")),
    ])
    show(q.optional("SELECT key_name, key_value FROM pipeline_metadata WHERE key_name LIKE 'intent%'"))
    say("  scoring runs every 2 h and calibrates 'yesterday' at 02:00 IST; the rollups run on ROLLUP_CRON_MINUTE.")


def scoring_backlog(q: Reader, start: datetime, days: int = 10) -> None:
    heading(18, "SCORING BACKLOG BY DAY (is the scoring worker keeping up?)")
    say("  'pending' = closed or swept, not scored yet. A day with mostly pending sessions has an almost empty Score view.")
    rows = q.rows("SELECT DATE_FORMAT(session_start, '%%Y-%%m-%%d') AS day, SUM(intent_status = 'scored') AS scored, "
                  "SUM(intent_status = 'pending') AS pending, SUM(intent_status = 'excluded') AS excluded, COUNT(*) AS sessions "
                  "FROM intent_sessions WHERE session_start >= %s AND session_start < %s GROUP BY 1 ORDER BY 1 DESC",
                  (start - timedelta(days=days), start + timedelta(days=1)))
    for r in rows:
        total = int(r["sessions"]) or 1
        r["scored_pct"] = round(100 * int(r["scored"] or 0) / total, 1)
    show(rows)
    say("  check the scoring worker: docker logs intent-scoring-worker 2>&1 | grep -E 'catchup|\\[intent scoring\\]' | tail -20")


def event_census(q: Reader, start: datetime, end: datetime) -> None:
    heading(19, "EVENT CENSUS: which event names exist, and do scroll events reach the scroll features?")
    say("  scroll_count counts events named exactly 'scroll_depth' (pipeline/intent_events.py _SESSION_EVENT_NAME_COUNTERS).")
    show(q.rows("SELECT event_name, COUNT(*) AS events, COUNT(DISTINCT session_id) AS sessions, SUM(scroll_percent IS NOT NULL) AS with_scroll_percent, "
                "SUM(JSON_EXTRACT(data, '$.percent') IS NOT NULL) AS with_percent_in_data FROM behavioral_events "
                "WHERE occurred_at >= %s AND occurred_at < %s GROUP BY event_name ORDER BY events DESC", (start, end)))
    say("  sessions of the day with a scroll_count above 0 (from the closed rows):")
    show(q.rows("SELECT COUNT(*) AS sessions, SUM(scroll_count > 0) AS with_scroll_count, SUM(click_count > 0) AS with_clicks, "
                "SUM(page_view_count > 0) AS with_page_views FROM intent_sessions WHERE session_start >= %s AND session_start < %s", (start, end)))
    say("  click events by what the browser reported (a 'meaningful_scroll' of 0 means that signal never fires):")
    show(q.rows("SELECT SUM(meaningful_scroll) AS meaningful_scroll_true, SUM(meaningful_scroll IS NULL) AS meaningful_scroll_null, COUNT(*) AS clicks "
                "FROM click_events WHERE occurred_at >= %s AND occurred_at < %s", (start, end)))


def dead_click_distribution(q: Reader, start: datetime, end: datetime, day: date) -> None:
    heading(20, "DEAD-CLICK RATE DISTRIBUTION (sessions with at least 10 clicks): why is P90 exactly 1.0?")
    say("  The struggle penalty needs click_count >= 10 AND dead_click_rate > the day's P90. The calibration takes P90 over")
    say("  intent_repository.fetch_dead_click_rates_for_date: ALL sessions of the day with >= 10 clicks, bots and unscored")
    say("  ones included. (A rate can never exceed 1.0, so if P90 is 1.0 the rule cannot fire.)")
    thresholds = q.optional("SELECT score_date, p90_dead_click_rate FROM intent_thresholds_daily WHERE score_date BETWEEN %s AND %s ORDER BY score_date DESC",
                            (day - timedelta(days=4), day))
    say("  stored P90 dead-click rate:")
    show(thresholds)
    for label, extra in (("all sessions with >= 10 clicks (what the calibration reads)", ""),
                         ("excluding bots and zero-signal (what the scoring actually penalises)",
                          "AND (intent_exclusion_reason IS NULL OR intent_exclusion_reason NOT IN ('bot', 'zero_signal'))")):
        rows = q.rows(f"SELECT click_count, dead_click_count FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                      f"AND click_count >= 10 AND session_time_spent_ms IS NOT NULL {extra} LIMIT %s", (start, end, MAX_SCORES))
        rates = [min(1.0, (int(r["dead_click_count"] or 0) / int(r["click_count"]))) for r in rows if int(r["click_count"] or 0) > 0]
        summary = dead_click_summary(rates)
        say(f"  {label}: {summary.get('count', 0)} sessions")
        if summary.get("count"):
            table([(k, summary[k]) for k in ("P50", "P75", "P90", "P95", "P99", "max", "above_p90", "at_or_above_p90")])
            show(summary["buckets"])
    say("  bots among the >= 10-click sessions:")
    show(q.rows("SELECT COALESCE(intent_exclusion_reason, '(not excluded)') AS reason, COUNT(*) AS sessions, "
                "100 * AVG(dead_click_count = click_count) AS pct_all_dead FROM intent_sessions WHERE session_start >= %s "
                "AND session_start < %s AND click_count >= 10 GROUP BY 1 ORDER BY 2 DESC", (start, end)))


def canonical_reconciliation(q: Reader, start: datetime, end: datetime, day: date, deep: bool) -> None:
    heading(21, "CANONICAL SESSION RECONCILIATION (one definition: a session is a row of intent_sessions, or an open record)")
    row = q.one("SELECT COUNT(*) AS closed, SUM(intent_status = 'scored') AS scored, SUM(intent_status = 'pending') AS pending, "
                "SUM(intent_status = 'excluded') AS excluded, SUM(intent_exclusion_reason = 'bot') AS bots, "
                "SUM(intent_exclusion_reason = 'zero_signal') AS zero_signal, "
                "SUM((intent_exclusion_reason IS NULL OR intent_exclusion_reason <> 'bot') AND session_time_spent_ms IS NOT NULL) AS distribution "
                "FROM intent_sessions WHERE session_start >= %s AND session_start < %s", (start, end))
    open_row = q.optional("SELECT COUNT(*) AS n FROM intent_actor_cursors WHERE session_id <> '' AND last_event_at >= %s "
                          "AND last_event_at <= UTC_TIMESTAMP() + INTERVAL 14 HOUR AND session_start >= %s AND session_start < %s",
                          (start, start, end))
    rollup = q.optional("SELECT sessions, sessions_started FROM intent_daily_summary WHERE summary_date = %s", (day,))
    numbers = {k: (int(v) if v is not None else None) for k, v in row.items()}
    numbers["open"] = int(open_row[0]["n"]) if open_row else 0
    numbers["rollup"] = int(rollup[0]["sessions_started"]) if rollup and rollup[0]["sessions_started"] is not None else None
    show(reconciliation_report(numbers))
    if rollup:
        say(f"  rollup row: sessions={rollup[0]['sessions']} sessions_started={rollup[0]['sessions_started']} "
            f"(sessions_started should equal closed + open as of the last rollup run)")
    if deep:
        say("  sessions of the day that have NO row in behavioral_events (click-only or scroll-only sessions), the old events-based count missed them:")
        show(q.optional("SELECT COUNT(*) AS sessions_without_behavioral_events FROM intent_sessions s WHERE s.session_start >= %s AND s.session_start < %s "
                        "AND NOT EXISTS (SELECT 1 FROM behavioral_events e WHERE e.session_id = s.session_id AND e.occurred_at >= %s)", (start, end, start)))
    else:
        say("  (add --deep to also count sessions that have no behavioral_events row; it does one lookup per session)")


def duplicates(q: Reader, start: datetime, end: datetime) -> None:
    heading(11, "DUPLICATES")
    show(q.rows("SELECT COUNT(*) AS rows_, COUNT(DISTINCT session_id) AS distinct_sessions FROM intent_sessions "
                "WHERE session_start >= %s AND session_start < %s", (start, end)))
    say("  (intent_sessions.session_id and behavioral_events.event_id are UNIQUE keys; a re-score updates the same row in place.)")


def score_vs_clarity(q: Reader, start: datetime, end: datetime, clarity: Sequence[float], clarity_sessions: Optional[int]) -> None:
    heading(12, "SCORE (percentile) + SESSIONS vs CLARITY")
    say("  Datum side = what the dashboard's Score basis counts as sessions: intent_sessions rows with intent_status='scored'")
    say("  grouped by intent_bucket (buildBucketQuery cur_sessions). The UI cannot show this in the donut (see the report); the")
    say("  three High/Medium/Low KPI cards can, with their U/S toggle on S.")
    rows = q.rows("SELECT intent_bucket AS bucket, COUNT(*) AS n FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                  "AND intent_status = 'scored' AND intent_bucket IN ('low', 'medium', 'high') GROUP BY intent_bucket", (start, end))
    counts = {r["bucket"]: int(r["n"]) for r in rows}
    show(comparison_rows(counts, clarity, clarity_sessions))
    other = q.rows("SELECT intent_status AS status, COUNT(*) AS sessions FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                   "GROUP BY intent_status", (start, end))
    say("  sessions of the day by status (only 'scored' is in the percentile buckets):")
    show(other)
    say("  Score + VISITORS (what the donut shows today in Score mode): distinct visitors per bucket, a visitor can be in several:")
    show(q.rows("SELECT intent_bucket AS bucket, COUNT(DISTINCT actor_id) AS visitors FROM intent_sessions WHERE session_start >= %s "
                "AND session_start < %s AND intent_status = 'scored' AND intent_bucket IN ('low', 'medium', 'high') GROUP BY intent_bucket", (start, end)))
    table([("distinct scored visitors", q.one("SELECT COUNT(DISTINCT actor_id) AS n FROM intent_sessions WHERE session_start >= %s "
                                              "AND session_start < %s AND intent_status = 'scored'", (start, end)).get("n"))])


def feature_table(q: Reader, start: datetime, end: datetime) -> None:
    heading(13, "FEATURE TABLE: Feature | Low | Medium | High | Overall  (scored sessions, by predictive bucket)")
    features = {
        "sessions": "COUNT(*)", "avg_duration_seconds": "AVG(session_time_spent_ms) / 1000", "avg_page_views": "AVG(page_view_count)",
        "avg_product_views": "AVG(product_view_count)", "avg_scrolls": "AVG(scroll_count)", "avg_clicks": "AVG(click_count)",
        "avg_useful_clicks": "AVG(useful_click_count)", "avg_dead_clicks": "AVG(dead_click_count)", "avg_events": "AVG(event_count)",
        "pct_add_to_cart": "100 * AVG(add_to_cart_count > 0)", "pct_checkout_started": "100 * AVG(checkout_started_count > 0)",
        "pct_entered_on_checkout_page": "100 * AVG(entry_page_type = 'checkout')", "pct_bounced": f"100 * AVG({BOUNCE_SQL})",
        "avg_score": "AVG(predictive_score)", "pct_zero_score": "100 * AVG(predictive_score = 0)",
    }
    select = ", ".join(f"{expr} AS {name}" for name, expr in features.items())
    rows = q.rows(f"SELECT intent_bucket AS bucket, {select} FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
                  f"AND intent_status = 'scored' AND intent_bucket IN ('low', 'medium', 'high') GROUP BY intent_bucket WITH ROLLUP", (start, end))
    show(pivot_by_bucket(rows, list(features)))
    say("  active (engaged) time is not stored anywhere: only the first-to-last event span is, so no 'active duration' row.")
    say("  A healthy score is monotonic: each row should rise from Low to Medium to High (bounced and zero-score should fall).")


def engagement_cohorts(q: Reader, start: datetime, end: datetime) -> None:
    heading(14, "LOW CLASSIFICATION: are readers and browsers who do not click pushed into Low?")
    defs = [
        ("page view only (no other signal)", "page_view_count > 0 AND product_view_count = 0 AND click_count = 0 AND scroll_count = 0 "
                                            "AND add_to_cart_count = 0 AND checkout_started_count = 0"),
        ("page view + one product view (2 events)", "event_count = 2 AND page_view_count = 1 AND product_view_count = 1"),
        ("duration over 30 s", "session_time_spent_ms > 30000"), ("duration over 60 s", "session_time_spent_ms > 60000"),
        ("duration over 2 min", "session_time_spent_ms > 120000"), ("with a scroll event", "scroll_count > 0"),
        ("with a click", "click_count > 0"),
        ("over 60 s, no click", "session_time_spent_ms > 60000 AND click_count = 0"),
        ("over 60 s, no click and no scroll", "session_time_spent_ms > 60000 AND click_count = 0 AND scroll_count = 0"),
        ("1 to 3 min, no click", "session_time_spent_ms BETWEEN 60000 AND 180000 AND click_count = 0"),
    ]
    show(cohort_table(q, start, end, defs))


def cohort_table(q: Reader, start: datetime, end: datetime, defs: Sequence[Tuple[str, str]]) -> List[Dict[str, Any]]:
    out = []
    for label, cond in defs:
        r = q.one(f"SELECT COUNT(*) AS sessions, SUM(intent_status = 'scored') AS scored, AVG(predictive_score) AS avg_score, "
                  f"100 * SUM(intent_bucket = 'low') / NULLIF(SUM(intent_status = 'scored'), 0) AS low_pct, "
                  f"100 * SUM(intent_bucket = 'medium') / NULLIF(SUM(intent_status = 'scored'), 0) AS medium_pct, "
                  f"100 * SUM(intent_bucket = 'high') / NULLIF(SUM(intent_status = 'scored'), 0) AS high_pct "
                  f"FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND session_time_spent_ms IS NOT NULL AND ({cond})",
                  (start, end))
        out.append({"cohort": label, **{k: (float(v) if isinstance(v, (int, float)) or hasattr(v, "as_tuple") else v) for k, v in r.items()}})
    return out


def product_view_duplication(q: Reader, start: datetime, end: datetime) -> None:
    heading(15, "PRODUCT VIEW DUPLICATION: real repeat visits or duplicate instrumentation?")
    say("  sessions by number of product_view events (intent_sessions.product_view_count):")
    show(q.rows("SELECT CASE WHEN product_view_count BETWEEN 1 AND 4 THEN CAST(product_view_count AS CHAR) "
                "WHEN product_view_count BETWEEN 5 AND 9 THEN '5-9' ELSE '10+' END AS product_views, COUNT(*) AS sessions "
                "FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND product_view_count >= 1 GROUP BY 1 ORDER BY MIN(product_view_count)",
                (start, end)))
    say("  event level (behavioral_events, event_name = 'product_viewed'):")
    show(q.optional("SELECT COUNT(*) AS sessions_with_product_views, SUM(events) AS product_view_events, SUM(distinct_products) AS distinct_products_total, "
                    "SUM(distinct_urls) AS distinct_urls_total, SUM(events > distinct_urls) AS sessions_with_repeated_url, "
                    "100 * SUM(events - distinct_urls) / SUM(events) AS repeat_events_pct FROM "
                    "(SELECT session_id, COUNT(*) AS events, COUNT(DISTINCT product_id) AS distinct_products, COUNT(DISTINCT url) AS distinct_urls "
                    "FROM behavioral_events WHERE event_name = 'product_viewed' AND occurred_at >= %s AND occurred_at < %s "
                    "AND session_id IS NOT NULL GROUP BY session_id) s", (start, end)))
    say("  repeats of the SAME url inside a session, by the time spread between the first and last of them:")
    show(q.optional("SELECT COUNT(*) AS repeated_url_groups, SUM(span_s <= 2) AS all_within_2s_likely_duplicate_instrumentation, "
                    "SUM(span_s > 2 AND span_s <= 60) AS within_a_minute, SUM(span_s > 60) AS spread_over_a_minute_real_revisits, "
                    "SUM(events) AS events_in_those_groups FROM "
                    "(SELECT session_id, url, COUNT(*) AS events, TIMESTAMPDIFF(SECOND, MIN(occurred_at), MAX(occurred_at)) AS span_s "
                    "FROM behavioral_events WHERE event_name = 'product_viewed' AND occurred_at >= %s AND occurred_at < %s "
                    "AND session_id IS NOT NULL GROUP BY session_id, url HAVING COUNT(*) > 1) g", (start, end)))
    say("  (a product view is worth 11 points, capped at 4: duplicate firing inflates score only up to that cap.)")


def bounce_impact(q: Reader, start: datetime, end: datetime) -> None:
    heading(16, "BOUNCE PENALTY (-20) IMPACT: A event_count <= 2, B duration < 5 s, C both")
    rows = q.rows("SELECT CASE WHEN event_count <= 2 AND session_time_spent_ms < 5000 THEN 'C both' "
                  "WHEN event_count <= 2 THEN 'A events only (5 s or more)' WHEN session_time_spent_ms < 5000 THEN 'B duration only (3+ events)' "
                  "ELSE 'no penalty' END AS group_, COUNT(*) AS sessions, AVG(predictive_score) AS avg_score, 100 * AVG(predictive_score = 0) AS pct_zero_score, "
                  "100 * AVG(intent_bucket = 'low') AS low_pct, 100 * AVG(intent_bucket = 'medium') AS medium_pct, 100 * AVG(intent_bucket = 'high') AS high_pct "
                  "FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored' GROUP BY 1 ORDER BY 1", (start, end))
    show(rows)


def override_impact(q: Reader, start: datetime, end: datetime) -> None:
    heading(17, "HARD HIGH OVERRIDE (operational bucket) IMPACT")
    show(q.rows("SELECT COUNT(*) AS scored_sessions, SUM(add_to_cart_count > 0) AS atc_sessions, SUM(checkout_started_count > 0) AS checkout_started_sessions, "
                "SUM(entry_page_type = 'checkout') AS entered_on_checkout_page, "
                "SUM(entry_page_type = 'checkout' OR checkout_started_count > 0 OR add_to_cart_count > 0) AS any_override_condition, "
                "SUM(operational_bucket = 'high') AS operational_high, SUM(intent_bucket = 'high') AS predictive_high, "
                "SUM(operational_bucket = 'high' AND intent_bucket <> 'high') AS forced_high_by_override, "
                "100 * SUM(operational_bucket = 'high') / COUNT(*) AS operational_high_pct, "
                "100 * SUM(intent_bucket = 'high') / COUNT(*) AS predictive_high_pct, "
                "100 * SUM(operational_bucket = 'high' AND intent_bucket <> 'high') / COUNT(*) AS forced_high_pct "
                "FROM intent_sessions WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored'", (start, end)))
    say("  the override reads entry_page_type = 'checkout' (the landing page), not 'any page of the session was checkout'.")


# ---------------- entry point ----------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only intent diagnostics for one brand and day.")
    parser.add_argument("--brand", required=True, help="database name (e.g. BBB) or brand index")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="store-local day, YYYY-MM-DD")
    parser.add_argument("--score-table", action="store_true", help="also print every distinct score with its cumulative share")
    parser.add_argument("--clarity", default=",".join(str(v) for v in CLARITY_DEFAULT),
                        help="Clarity low,medium,high percentages to compare with (default %(default)s)")
    parser.add_argument("--clarity-sessions", type=int, default=None, help="Clarity's session count for the day (e.g. 12251)")
    parser.add_argument("--only", default=None, help="run only these section numbers, e.g. 10,18,19 (default: all)")
    parser.add_argument("--deep", action="store_true", help="also run the lookups that cost one read per session (section 21)")
    args = parser.parse_args(argv)
    try:
        args.only_sections = {int(n) for n in args.only.split(",") if n.strip()} if args.only else None
    except ValueError:
        parser.error("--only needs section numbers like 10,18,19")
    try:
        args.clarity_values = tuple(float(v) for v in args.clarity.split(","))
        if len(args.clarity_values) != 3:
            raise ValueError
    except ValueError:
        parser.error("--clarity needs three numbers: low,medium,high")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from scripts.backfill_utm import resolve_brand_index
    from pipeline.db import get_db_read_cursor

    index = resolve_brand_index(args.brand)
    start = datetime.combine(args.date, datetime.min.time())
    end = start + timedelta(days=1)
    with get_db_read_cursor(index) as cursor:
        try:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
        except Exception:
            pass
        q = Reader(cursor)
        say(f"intent diagnostics: brand={args.brand} (index {index}) day={args.date}  (read-only)")
        steps = [
            (1, lambda: reconciliation(q, start, end, args.date)),
            (2, lambda: behaviour_levels(q, start, end)),
            (3, lambda: score_view(q, start, end, args.date, args.score_table)),
            (4, lambda: buckets(q, start, end)),
            (5, lambda: cohorts(q, start, end)),
            (6, lambda: features_by_bucket(q, start, end)),
            (7, lambda: bounce_rule(q, start, end)),
            (8, lambda: clicks(q, start, end)),
            (9, lambda: page_types_and_overrides(q, start, end)),
            (10, lambda: freshness(q, start)),
            (11, lambda: duplicates(q, start, end)),
            (12, lambda: score_vs_clarity(q, start, end, args.clarity_values, args.clarity_sessions)),
            (13, lambda: feature_table(q, start, end)),
            (14, lambda: engagement_cohorts(q, start, end)),
            (15, lambda: product_view_duplication(q, start, end)),
            (16, lambda: bounce_impact(q, start, end)),
            (17, lambda: override_impact(q, start, end)),
            (18, lambda: scoring_backlog(q, start)),
            (19, lambda: event_census(q, start, end)),
            (20, lambda: dead_click_distribution(q, start, end, args.date)),
            (21, lambda: canonical_reconciliation(q, start, end, args.date, args.deep)),
        ]
        for number, run in steps:
            if args.only_sections is None or number in args.only_sections:
                run()
    say()
    say("done. Paste the whole output back to continue the investigation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
