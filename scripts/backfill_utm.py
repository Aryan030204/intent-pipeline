"""
Fills the utm_* columns of rows that were stored before the columns existed, from each row's own `url`.

    docker exec intent-kafka-worker python scripts/backfill_utm.py \
        --brand BBB --from 2026-10-01 --to 2026-10-27 --tables events,clicks --batch 2000 --sleep 0.5

Run it by hand, never from a container start. It is built to be gentle on the database:
- it only looks at rows in the date range (found through idx_occurred_at) whose five utm_* columns are all NULL
  and whose url mentions "utm_", so rows with nothing to extract are never read or written;
- it works in batches of --batch rows by id, commits each batch, and sleeps --sleep seconds between batches,
  so the instance's EBS balance is not drained;
- it can be stopped at any time and run again: finished rows no longer match, so it picks up where it was;
- --dry-run reads and counts but writes nothing; --max-rows stops after that many updated rows.
Needs the utm_* columns (migrations/003_event_utm_columns.sql); it refuses to run without them.
"""

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.utm import UTM_COLUMNS, extract_utm  # noqa: E402

TABLES = {"events": "behavioral_events", "clicks": "click_events"}
_ALL_NULL = " AND ".join(f"{c} IS NULL" for c in UTM_COLUMNS)


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [backfill_utm] {message}", flush=True)


def id_range(cursor, table: str, start: datetime, end: datetime) -> Optional[Tuple[int, int]]:
    """Smallest and largest row id with occurred_at in [start, end), through idx_occurred_at."""
    cursor.execute(f"SELECT MIN(id) AS lo, MAX(id) AS hi FROM {table} WHERE occurred_at >= %s AND occurred_at < %s",
                   (start, end))
    row = cursor.fetchall()[0]
    lo, hi = (row["lo"], row["hi"]) if isinstance(row, dict) else (row[0], row[1])
    return None if lo is None or hi is None else (int(lo), int(hi))


def fetch_candidates(cursor, table: str, low: int, high: int) -> List[Tuple[int, str]]:
    """Rows in the id window that still have no UTM values and a url that can contain some."""
    cursor.execute(
        f"SELECT id, url FROM {table} WHERE id BETWEEN %s AND %s AND {_ALL_NULL} AND url LIKE %s",
        (low, high, "%utm\\_%"),
    )
    return [((r["id"], r["url"]) if isinstance(r, dict) else (r[0], r[1])) for r in cursor.fetchall()]


def write_batch(cursor, table: str, rows: Sequence[Tuple[int, tuple]]) -> None:
    """One multi-row UPDATE ... JOIN (SELECT ... UNION ALL ...). The derived-table form works on every MySQL
    version (no VALUES ROW), the same pattern the scoring job uses."""
    names = ("id", *UTM_COLUMNS)
    first = "SELECT " + ", ".join(f"%s AS {n}" for n in names)
    more = "SELECT " + ", ".join(["%s"] * len(names))
    derived = " UNION ALL ".join([first] + [more] * (len(rows) - 1))
    assignments = ", ".join(f"t.{c} = v.{c}" for c in UTM_COLUMNS)
    params: List = []
    for row_id, values in rows:
        params.append(row_id)
        params.extend(values)
    cursor.execute(f"UPDATE {table} t JOIN ({derived}) v ON v.id = t.id SET {assignments}", params)


def backfill_table(cursor, connection, table: str, start: datetime, end: datetime, batch: int, sleep_s: float,
                   dry_run: bool, max_rows: Optional[int], sleep: Callable[[float], None] = time.sleep) -> Dict[str, int]:
    stats = {"scanned_windows": 0, "candidates": 0, "updated": 0}
    bounds = id_range(cursor, table, start, end)
    if bounds is None:
        _log(f"{table}: no rows between {start} and {end}")
        return stats
    low, high = bounds
    _log(f"{table}: ids {low}..{high}, batch={batch}, sleep={sleep_s}s, dry_run={dry_run}")
    window = low
    while window <= high:
        window_end = min(window + batch - 1, high)
        candidates = fetch_candidates(cursor, table, window, window_end)
        stats["scanned_windows"] += 1
        stats["candidates"] += len(candidates)
        rows = []
        for row_id, url in candidates:
            values = extract_utm(url)
            if any(v is not None for v in values):
                rows.append((row_id, values))
        if rows and max_rows is not None:
            rows = rows[: max(0, max_rows - stats["updated"])]
        if rows and not dry_run:
            write_batch(cursor, table, rows)
            connection.commit()
        stats["updated"] += len(rows)
        if stats["scanned_windows"] % 50 == 0 or window_end == high:
            _log(f"{table}: up to id {window_end}/{high}  candidates={stats['candidates']}  updated={stats['updated']}")
        if max_rows is not None and stats["updated"] >= max_rows:
            _log(f"{table}: reached --max-rows={max_rows}")
            break
        window = window_end + 1
        if sleep_s > 0 and window <= high:
            sleep(sleep_s)
    return stats


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
    parser = argparse.ArgumentParser(description="Fill utm_* columns from the stored url of existing rows.")
    parser.add_argument("--brand", required=True, help="database name (e.g. BBB) or brand index")
    parser.add_argument("--from", dest="date_from", required=True, type=date.fromisoformat, help="first day, YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, type=date.fromisoformat, help="last day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--tables", default="events,clicks", help="events, clicks or both (default both)")
    parser.add_argument("--batch", type=int, default=2000, help="ids per batch (default 2000)")
    parser.add_argument("--sleep", type=float, default=0.5, help="seconds between batches (default 0.5)")
    parser.add_argument("--max-rows", type=int, default=None, help="stop after updating this many rows per table")
    parser.add_argument("--dry-run", action="store_true", help="count what would change, write nothing")
    args = parser.parse_args(argv)
    if args.date_from > args.date_to:
        parser.error("--from must be on or before --to")
    if args.batch < 1:
        parser.error("--batch must be at least 1")
    unknown = [t for t in args.tables.split(",") if t.strip() and t.strip() not in TABLES]
    if unknown:
        parser.error(f"unknown --tables {unknown}; use events and/or clicks")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from pipeline.db import get_db_cursor
    from pipeline.intent_kafka_store import utm_columns_present

    index = resolve_brand_index(args.brand)
    # The date range is in the store-local clock the events carry (occurred_at), midnight to midnight.
    start = datetime.combine(args.date_from, datetime.min.time())
    end = datetime.combine(args.date_to + timedelta(days=1), datetime.min.time())
    with get_db_cursor(index) as (cursor, connection):
        if not utm_columns_present(cursor):
            _log("the utm_* columns do not exist yet; apply migrations/003_event_utm_columns.sql first (the "
                 "containers do it on start on MySQL 8.0.12+)")
            return 2
        total = 0
        for name in [t.strip() for t in args.tables.split(",") if t.strip()]:
            stats = backfill_table(cursor, connection, TABLES[name], start, end, args.batch, args.sleep,
                                   args.dry_run, args.max_rows)
            total += stats["updated"]
            _log(f"{TABLES[name]}: done, updated={stats['updated']} of candidates={stats['candidates']}"
                 f"{' (dry run, nothing written)' if args.dry_run else ''}")
    _log(f"finished, {total} rows {'would be ' if args.dry_run else ''}updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
