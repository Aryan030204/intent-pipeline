"""
Offline replay of the intent scoring model and its V2 candidates over real closed sessions. READ ONLY.

    docker exec intent-scoring-worker python scripts/replay_intent_v2.py \
        --brand BBB --from 2026-10-20 --to 2026-10-28 --sleep 0.5

It reads the closed sessions of the date range (and the page/user-agent facts the scorer reads), re-scores them in
memory with the current model (Variant A) and the candidates B-F2 (intent_engine/replay.py), and prints the
bucket distributions, score and threshold percentiles, the transitions from A to each variant, the page-type
audit and the dead-click (struggle) audit. Nothing is written: no UPDATE, no DDL, no temporary table. It reads
through the read replica when one is configured. Gentle by design: sessions are read in --chunk sized keyset pages
with --sleep seconds between pages.

Read the "Parity" section first: it says how closely the replayed current model reproduces the scores stored in
intent_sessions. Anything far from 100% means the replay differs from production for that data (sessions scored
under older rules, calibration days that were never refreshed) and the variant table has to be read with that in mind.
"""

import argparse
import dataclasses
import os
import sys
import time
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from intent_engine import replay as rp  # noqa: E402
from intent_engine.repository import (  # noqa: E402
    _ELIGIBLE_SESSION_COLUMNS,
    _day_bounds,
    fetch_click_user_agents,
    fetch_page_viewed_enrichment,
)

STORED_COLUMNS = ("predictive_score", "intent_status", "intent_bucket")


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [replay_intent_v2] {message}", file=sys.stderr, flush=True)


# ---------------------------
# Loading (SELECT only)
# ---------------------------
def fetch_session_page(cursor, start: datetime, end: datetime, limit: int,
                       after: Optional[Tuple[datetime, int]] = None) -> List[Dict[str, Any]]:
    """Closed sessions of [start, end) in (session_start, id) order, `limit` at a time."""
    columns = ", ".join((*_ELIGIBLE_SESSION_COLUMNS, "id", *STORED_COLUMNS))
    sql = (f"SELECT {columns} FROM intent_sessions WHERE session_start >= %s AND session_start < %s "
           "AND session_time_spent_ms IS NOT NULL")
    params: List[Any] = [start, end]
    if after is not None:
        sql += " AND (session_start > %s OR (session_start = %s AND id > %s))"
        params += [after[0], after[0], after[1]]
    sql += " ORDER BY session_start, id LIMIT %s"
    params.append(int(limit))
    cursor.execute(sql, tuple(params))
    return cursor.fetchall()


def enrich_page(cursor, rows: Sequence[Dict[str, Any]]) -> Dict[str, rp.Enrichment]:
    """The same reads session_scorer._build_enrichment does for a page of sessions."""
    ids = [r["session_id"] for r in rows]
    page_rows = fetch_page_viewed_enrichment(cursor, ids)
    enrichment = rp.enrich_from_rows(ids, page_rows)
    missing = [sid for sid, e in enrichment.items() if e.user_agent is None]
    if missing:
        enrichment = rp.enrich_from_rows(ids, page_rows, fetch_click_user_agents(cursor, missing))
    return enrichment


def load_sessions(cursor, start: datetime, end: datetime, chunk: int, sleep_s: float,
                  max_sessions: Optional[int] = None,
                  sleep: Callable[[float], None] = time.sleep) -> List[rp.ReplaySession]:
    sessions: List[rp.ReplaySession] = []
    after = None
    while True:
        limit = chunk if max_sessions is None else min(chunk, max_sessions - len(sessions))
        if limit <= 0:
            break
        rows = fetch_session_page(cursor, start, end, limit, after)
        if not rows:
            break
        enrichment = enrich_page(cursor, rows)
        sessions.extend(rp.ReplaySession(row, enrichment[row["session_id"]]) for row in rows)
        last = rows[-1]
        after = (last["session_start"], last["id"])
        _log(f"loaded {len(sessions)} sessions (up to {last['session_start']})")
        if len(rows) < limit:
            break
        if sleep_s > 0:
            sleep(sleep_s)
    return sessions


# ---------------------------
# Report
# ---------------------------
def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[("" if c is None else str(c)) for c in row] for row in rows]
    widths = [max(len(str(h)), *(len(r[i]) for r in cells)) if cells else len(str(h)) for i, h in enumerate(headers)]
    line = lambda values: "  ".join(str(v).rjust(w) for v, w in zip(values, widths))  # noqa: E731
    return "\n".join([line(headers), line(["-" * w for w in widths]), *(line(r) for r in cells)])


def _section(title: str) -> str:
    return f"\n=== {title} ===\n"


def build_report(sessions: Sequence[rp.ReplaySession], names: Sequence[str], top_paths: int = 25,
                low_percentile: Optional[float] = None, high_percentile: Optional[float] = None) -> str:
    """`low_percentile` / `high_percentile` replace the 35 / 72 cut-offs of every variant except A (the baseline
    stays exactly as production), to see where the percentiles would have to sit."""
    def configured(name: str) -> rp.ReplayConfig:
        config = rp.VARIANTS[name]
        if name == "A":
            return config
        return dataclasses.replace(config, low_percentile=low_percentile or config.low_percentile,
                                   high_percentile=high_percentile or config.high_percentile)

    results = {name: rp.replay(sessions, configured(name)) for name in names}
    base = results.get("A") or rp.replay(sessions, rp.VARIANTS["A"])
    out: List[str] = []

    dist_a = rp.distribution(base)
    out.append(_section("Loaded"))
    out.append(f"closed sessions with a duration: {len(sessions)}   "
               f"excluded by the current rules: {dist_a['excluded']} ({dist_a['excluded_pct']}%) {dist_a['excluded_by_reason']}")

    parity = rp.parity(sessions, base)
    out.append(_section("Parity: replayed Variant A against the scores stored in intent_sessions"))
    out.append(f"sessions with a stored score compared: {parity['compared']}\n"
               f"score equal (+/-{rp.PARITY_TOLERANCE}): {parity['score_equal']} ({parity['score_equal_pct']}%)   "
               f"largest difference: {parity['max_score_diff']}\n"
               f"stored bucket equals replayed bucket: {parity['bucket_equal']} ({parity['bucket_equal_pct']}%)\n"
               f"stored bucket mix: {parity['stored_bucket_mix']}")

    out.append(_section("Variants: predictive bucket distribution (of the scored sessions)"))
    rows = []
    for name in names:
        res = results[name]
        d, t = rp.distribution(res), rp.threshold_summary(res)
        pct = rp.score_percentiles(res)
        rows.append([name, d["low"], f"{d['low_pct']}%", d["medium"], f"{d['medium_pct']}%", d["high"],
                     f"{d['high_pct']}%", t["p35"], t["p72"], t["p90_dead_click_rate"], pct[50],
                     f"{d['zero_score_pct']}%", t["fallback_days"]])
    out.append(_table(["var", "low", "low%", "medium", "med%", "high", "high%", "P35", "P72", "P90dead", "median",
                       "score=0", "fallback_days"], rows))
    out.append("\n" + "\n".join(f"  {n}: {rp.VARIANTS[n].description}" for n in names))

    out.append(_section("Score percentiles (all scored sessions | only scores above 0)"))
    rows = []
    for name in names:
        every, positive = rp.score_percentiles(results[name]), rp.score_percentiles(results[name], positive_only=True)
        rows.append([name, *(f"{every[p]}|{positive[p]}" for p in rp.SCORE_PERCENTILES)])
    out.append(_table(["var", *(f"P{p}" for p in rp.SCORE_PERCENTILES)], rows))

    out.append(_section("Per-day thresholds, Variant A"))
    out.append(_table(["day", "population", "method", "P35", "P72", "P90dead"],
                      [[t.day, t.population, t.method, round(t.p35, 2), round(t.p72, 2), t.p90_dead_click_rate]
                       for t in rp.threshold_summary(base)["days"]]))

    out.append(_section("Transitions from Variant A (rows: A, columns: variant)"))
    for name in names:
        if name == "A":
            continue
        matrix = rp.transition_matrix(base, results[name])
        out.append(f"A -> {name}")
        out.append(_table(["from \\ to", *rp.BUCKETS],
                          [[b, *(matrix[(b, c)] for c in rp.BUCKETS)] for b in rp.BUCKETS]))
        out.append("")

    out.append(_section("Operational bucket (must be identical in the share forced to high by the overrides)"))
    out.append(_table(["var", "low", "medium", "high"],
                      [[n, *(rp.distribution(results[n], "operational")[b] for b in rp.BUCKETS)] for n in names]))

    out.append(_section("Page types (highest-intent page type of the session), Variant A"))
    out.append(_table(["page_type", "sessions", "pct", "mean", "median", "P25", "P75", "x", "score=0%"],
                      [[r["page_type"], r["sessions"], f"{r['pct']}%", r["mean"], r["median"], r["p25"], r["p75"],
                        r["multiplier"], f"{r['zero_score_pct']}%"] for r in rp.page_type_table(base)]))
    no_pages = sum(1 for s in sessions if not s.enrichment.page_types_seen)
    out.append(f"\nsessions with no page_viewed event at all (page type 'other'): {no_pages} "
               f"({rp._pct(no_pages, len(sessions))}%)")
    out.append(f"\nTop {top_paths} normalised paths classified 'other' (sessions that saw them):")
    out.append(_table(["sessions", "path"], [[n, p] for p, n in rp.other_path_counts((s.enrichment for s in sessions), top_paths)]))

    out.append(_section("Struggle / dead-click audit (sessions with >= 10 clicks)"))
    report = rp.struggle_report(sessions, base)
    if not report.get("sessions"):
        out.append("no session has 10 or more clicks")
    else:
        out.append(f"sessions: {report['sessions']}   P90 dead-click rate: {report['p90']}   max: {report['max']}")
        out.append("dead-click rate percentiles: " + "  ".join(f"P{p}={v:.3f}" for p, v in report["percentiles"].items()))
        out.append(f"rate = 100%: {report['all_dead']} ({report['all_dead_pct']}%)   rate >= 90%: {report['ninety_plus']}   "
                   f"rate >= 50%: {report['fifty_plus']}")
        out.append(f"would trip the penalty with '>' P90: {report['would_fire_gt']}   with '>=' P90: {report['would_fire_gte']} "
                   "(information only, the rule is not changed)")
        out.append(f"population by replay status: {report['population_replay_status']}   "
                   f"by stored status: {report['population_stored_status']}")
    return "\n".join(out)


# ---------------------------
# CLI
# ---------------------------
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline replay of the intent scoring model and the V2 candidates (read only).")
    parser.add_argument("--brand", required=True, help="database name (e.g. BBB) or brand index")
    parser.add_argument("--from", dest="date_from", required=True, type=date.fromisoformat, help="first session day (IST), YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, type=date.fromisoformat, help="last session day (IST), inclusive")
    parser.add_argument("--variants", default="A,B,C,D,E,F,F2", help="comma separated, default all")
    parser.add_argument("--chunk", type=int, default=2000, help="sessions per read (default 2000)")
    parser.add_argument("--sleep", type=float, default=0.5, help="seconds between reads (default 0.5)")
    parser.add_argument("--max-sessions", type=int, default=None, help="stop loading after this many sessions")
    parser.add_argument("--top-other-paths", type=int, default=25)
    parser.add_argument("--low-percentile", type=float, default=None,
                        help="experiment: use this percentile instead of 35 for the Low/Medium cut-off (variants B-F2)")
    parser.add_argument("--high-percentile", type=float, default=None,
                        help="experiment: use this percentile instead of 72 for the Medium/High cut-off (variants B-F2)")
    args = parser.parse_args(argv)
    if args.date_from > args.date_to:
        parser.error("--from must be on or before --to")
    names = [v.strip().upper() for v in args.variants.split(",") if v.strip()]
    unknown = [v for v in names if v not in rp.VARIANTS]
    if unknown:
        parser.error(f"unknown variants {unknown}; choose from {', '.join(rp.VARIANTS)}")
    args.names = names
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from pipeline.db import get_db_read_cursor
    from scripts.backfill_utm import resolve_brand_index

    index = resolve_brand_index(args.brand)
    start, end = _day_bounds(args.date_from)[0], _day_bounds(args.date_to)[1]
    with get_db_read_cursor(index) as cursor:
        sessions = load_sessions(cursor, start, end, args.chunk, args.sleep, args.max_sessions)
    if not sessions:
        _log("no closed sessions in that range")
        return 1
    print(build_report(sessions, args.names, args.top_other_paths, args.low_percentile, args.high_percentile))
    return 0


if __name__ == "__main__":
    sys.exit(main())
