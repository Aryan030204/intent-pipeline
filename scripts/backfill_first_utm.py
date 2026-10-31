"""
Fills intent_sessions.first_utm_source for sessions that closed before the column existed (or before the pipeline
recorded it), from the utm_source of each session's own events.

    docker exec intent-kafka-worker python scripts/backfill_first_utm.py \
        --brand BBB --from 2026-10-01 --to 2026-10-27 --batch 1000 --sleep 0.5

Run it by hand, never from a container start. It is built to be gentle on the database:
- it only looks at sessions that started in the date range (found through idx_session_start) and still have no
  first_utm_source, in batches of --batch sessions by id, and reads their events through idx_session_time;
- it commits each batch and sleeps --sleep seconds between batches, so the instance's EBS balance is not drained;
- it can be stopped at any time and run again: finished sessions no longer match, so it picks up where it was;
- sessions whose events carry no utm_source stay NULL (the dashboard counts them as "Others", as before);
- --dry-run reads and counts but writes nothing; --max-rows stops after that many updated sessions.
Needs intent_sessions.first_utm_source (migrations/004_session_first_utm.sql) and the utm_* columns of
behavioral_events (migrations/003_event_utm_columns.sql, filled for old events by scripts/backfill_utm.py); it refuses
to run without them.
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

UTM_MAX_LEN = 255


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [backfill_first_utm] {message}", flush=True)


def _cell(row, index: int, name: str):
    return row.get(name) if isinstance(row, dict) else row[index]


def id_range(cursor, start: datetime, end: datetime) -> Optional[Tuple[int, int]]:
    """Smallest and largest session row id with session_start in [start, end), through idx_session_start."""
    cursor.execute("SELECT MIN(id) AS lo, MAX(id) AS hi FROM intent_sessions WHERE session_start >= %s AND session_start < %s",
                   (start, end))
    row = cursor.fetchall()[0]
    lo, hi = _cell(row, 0, "lo"), _cell(row, 1, "hi")
    return None if lo is None or hi is None else (int(lo), int(hi))


def fetch_sessions_without_source(cursor, low: int, high: int, start: datetime, end: datetime) -> List[str]:
    cursor.execute(
        "SELECT session_id FROM intent_sessions "
        "WHERE id BETWEEN %s AND %s AND first_utm_source IS NULL AND session_start >= %s AND session_start < %s",
        (low, high, start, end),
    )
    return [str(_cell(r, 0, "session_id")) for r in cursor.fetchall()]


def first_sources(cursor, session_ids: Sequence[str]) -> Dict[str, str]:
    """session_id -> utm_source of its first event that has one (events in time order)."""
    if not session_ids:
        return {}
    marks = ", ".join(["%s"] * len(session_ids))
    cursor.execute(
        f"SELECT session_id, utm_source FROM behavioral_events "
        f"WHERE session_id IN ({marks}) AND utm_source IS NOT NULL AND utm_source <> '' "
        f"ORDER BY session_id, occurred_at, id",
        list(session_ids),
    )
    found: Dict[str, str] = {}
    for row in cursor.fetchall():
        session_id = str(_cell(row, 0, "session_id"))
        if session_id not in found:
            value = str(_cell(row, 1, "utm_source")).strip()
            if value:
                found[session_id] = value[:UTM_MAX_LEN]
    return found


def write_batch(cursor, found: Dict[str, str]) -> None:
    """One multi-row UPDATE ... JOIN (SELECT ... UNION ALL ...), the same pattern as scripts/backfill_utm.py. The
    first_utm_source IS NULL condition means a session the pipeline filled in meanwhile is never overwritten."""
    items = list(found.items())
    first = "SELECT %s AS session_id, %s AS first_utm"
    more = "SELECT %s, %s"
    derived = " UNION ALL ".join([first] + [more] * (len(items) - 1))
    params: List = []
    for session_id, value in items:
        params.extend((session_id, value))
    cursor.execute(
        f"UPDATE intent_sessions s JOIN ({derived}) v ON v.session_id = s.session_id "
        f"SET s.first_utm_source = v.first_utm WHERE s.first_utm_source IS NULL",
        params,
    )


def backfill(cursor, connection, start: datetime, end: datetime, batch: int, sleep_s: float, dry_run: bool,
             max_rows: Optional[int], sleep: Callable[[float], None] = time.sleep) -> Dict[str, int]:
    stats = {"windows": 0, "sessions": 0, "updated": 0}
    bounds = id_range(cursor, start, end)
    if bounds is None:
        _log(f"no sessions between {start} and {end}")
        return stats
    low, high = bounds
    _log(f"sessions ids {low}..{high}, batch={batch}, sleep={sleep_s}s, dry_run={dry_run}")
    window = low
    while window <= high:
        window_end = min(window + batch - 1, high)
        session_ids = fetch_sessions_without_source(cursor, window, window_end, start, end)
        stats["windows"] += 1
        stats["sessions"] += len(session_ids)
        found = first_sources(cursor, session_ids)
        if found and max_rows is not None:
            found = dict(list(found.items())[: max(0, max_rows - stats["updated"])])
        if found and not dry_run:
            write_batch(cursor, found)
            connection.commit()
        stats["updated"] += len(found)
        if stats["windows"] % 50 == 0 or window_end == high:
            _log(f"up to id {window_end}/{high}  without source={stats['sessions']}  updated={stats['updated']}")
        if max_rows is not None and stats["updated"] >= max_rows:
            _log(f"reached --max-rows={max_rows}")
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
    parser = argparse.ArgumentParser(description="Fill intent_sessions.first_utm_source from each session's events.")
    parser.add_argument("--brand", required=True, help="database name (e.g. BBB) or brand index")
    parser.add_argument("--from", dest="date_from", required=True, type=date.fromisoformat, help="first day, YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, type=date.fromisoformat, help="last day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--batch", type=int, default=1000, help="session row ids per batch (default 1000)")
    parser.add_argument("--sleep", type=float, default=0.5, help="seconds between batches (default 0.5)")
    parser.add_argument("--max-rows", type=int, default=None, help="stop after updating this many sessions")
    parser.add_argument("--dry-run", action="store_true", help="count what would change, write nothing")
    args = parser.parse_args(argv)
    if args.date_from > args.date_to:
        parser.error("--from must be on or before --to")
    if args.batch < 1:
        parser.error("--batch must be at least 1")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from pipeline.db import get_db_cursor
    from pipeline.intent_kafka_store import first_utm_column_present, utm_columns_present

    index = resolve_brand_index(args.brand)
    start = datetime.combine(args.date_from, datetime.min.time())
    end = datetime.combine(args.date_to + timedelta(days=1), datetime.min.time())
    with get_db_cursor(index) as (cursor, connection):
        if not first_utm_column_present(cursor):
            _log("intent_sessions.first_utm_source does not exist yet; apply migrations/004_session_first_utm.sql first "
                 "(the containers do it on start on MySQL 8.0.12+)")
            return 2
        if not utm_columns_present(cursor):
            _log("the utm_* columns of the event tables do not exist yet; apply migrations/003_event_utm_columns.sql first")
            return 2
        stats = backfill(cursor, connection, start, end, args.batch, args.sleep, args.dry_run, args.max_rows)
    _log(f"finished: {stats['sessions']} session(s) without a source, {stats['updated']} "
         f"{'would be ' if args.dry_run else ''}updated{' (dry run, nothing written)' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
