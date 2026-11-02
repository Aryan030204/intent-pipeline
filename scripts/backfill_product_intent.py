"""
Fills product_intent_daily for past days (the regular rollup only recomputes the last few days).

    docker exec intent-data-aggregation python scripts/backfill_product_intent.py \
        --brand BBB --from 2026-10-01 --to 2026-10-27 --sleep 2

Run it by hand, never from a container start. One day at a time, committed after each day, sleeping --sleep
seconds between days so the instance's EBS balance is not drained. Safe to stop and run again: a day is
recomputed whole and overwritten. Note that a day's levels are those its sessions have NOW: days whose sessions are
still unscored show them under 'unclassified' until scoring catches up (run it again afterwards).
"""

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional, Sequence

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [backfill_product_intent] {message}", flush=True)


def days_between(first: date, last: date) -> List[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def backfill_days(cursor, connection, days: Sequence[date], sleep_s: float,
                  sleep: Callable[[float], None] = time.sleep) -> Dict[str, int]:
    """Recompute each day on its own through the same stage the rollup runs (it commits per day)."""
    from pipeline import rollups

    totals = {"days": 0, "generated": 0, "upserted": 0, "deleted": 0}
    rollups._ensure_product_intent_daily_table(cursor, connection)
    for index, day in enumerate(days):
        # lookback_days=1 so the stage recomputes exactly this day, not the days before it
        stats = rollups._rollup_product_intent_daily(cursor, connection, [day], lookback_days=1)
        totals["days"] += 1
        for key in ("generated", "upserted", "deleted"):
            totals[key] += stats[key]
        _log(f"{day}: rows={stats['generated']} upserted={stats['upserted']} stale_deleted={stats['deleted']}")
        if sleep_s > 0 and index < len(days) - 1:
            sleep(sleep_s)
    return totals


def resolve_brand_index(brand: str) -> int:
    """--brand is a database index (1) or a database name (BBB, case-insensitive) from the brand configs."""
    if brand.isdigit():
        return int(brand)
    import aws_background
    from pipeline.state import active_brand_indices

    active_brand_indices.clear()
    aws_background.initialize_brand_configs()
    for index in active_brand_indices:
        if os.environ.get(f"DB_DATABASE_{index}", "").strip().lower() == brand.strip().lower():
            return index
    raise SystemExit(f"no active brand database named {brand!r}; use its index instead")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fill product_intent_daily for past days.")
    parser.add_argument("--brand", required=True, help="database name (e.g. BBB) or brand index")
    parser.add_argument("--from", dest="date_from", required=True, type=date.fromisoformat, help="first day, YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, type=date.fromisoformat, help="last day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--sleep", type=float, default=2.0, help="seconds between days (default 2)")
    args = parser.parse_args(argv)
    if args.date_from > args.date_to:
        parser.error("--from must be on or before --to")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from pipeline.db import get_db_cursor

    index = resolve_brand_index(args.brand)
    with get_db_cursor(index) as (cursor, connection):
        totals = backfill_days(cursor, connection, days_between(args.date_from, args.date_to), args.sleep)
    _log(f"finished, {totals['days']} day(s), {totals['generated']} rows written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
