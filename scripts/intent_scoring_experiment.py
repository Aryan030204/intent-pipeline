"""
Local intent-scoring experiment: ONE invocation loads the sessions of one day ONCE and runs that same immutable dataset
through the selected scoring versions (default v1..v5 and v1c: V1 = the current production model, V1C = the production candidate, V2-V5 = EXPERIMENTAL
candidates), then writes the comparison. READ ONLY.

    python scripts/intent_scoring_experiment.py --brand BBB --date 2026-11-13
    python scripts/intent_scoring_experiment.py --brand BBB --date 2026-11-13 --versions v1,v2
    python scripts/intent_scoring_experiment.py --brand ALL --date 2026-11-13

Output: experiment_results/<brand>/<date>/{session_scores.csv, session_scores_long.csv, summary.json, report.txt}.
Nothing is written to a table.

How it stays read-only (enforced here, not assumed):
- every statement goes through ReadOnlyCursor, which accepts one SELECT or SHOW and nothing else (no write, no DDL,
  no chained statements, no GET_LOCK, no FOR UPDATE, no INTO OUTFILE);
- at start the session is set to READ ONLY and that is verified (@@transaction_read_only); the run stops if it is not;
- it reads through get_db_read_cursor (the read replica when DB_READ_REPLICA_HOST is set, else the primary);
- it never imports the scoring runner, the calibration or actor writers, or the schema ensure helpers (a test checks).

The date is the store-local session day, as in production: session_start >= 'D 00:00:00' and < 'D+1 00:00:00'. The
loader never uses updated_at. Sessions are read in keyset pages (--chunk, --sleep); the page and event facts are read
once, in batches, and every version scores that one dataset.
"""

import argparse
import json
from pathlib import Path
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from intent_engine import experiment as ex  # noqa: E402
from intent_engine import replay as rp  # noqa: E402
from intent_engine.repository import _ELIGIBLE_SESSION_COLUMNS, IN_LIST_CHUNK, _day_bounds  # noqa: E402
from scripts.replay_intent_v2 import enrich_page  # noqa: E402

logger = logging.getLogger("intent_scoring_experiment")

CLARITY_TABLE = "daily_user_intent_summary"
# Columns read besides the scoring ones: only those the database actually has are selected.
OPTIONAL_SESSION_COLUMNS = ("event_sequence", "predictive_score", "intent_status", "intent_bucket",
                            "intent_exclusion_reason", "first_utm_source")
REQUIRED_SESSION_COLUMNS = (*_ELIGIBLE_SESSION_COLUMNS, "id", "session_end")


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [intent_scoring_experiment] {message}", file=sys.stderr,
          flush=True)


# ---------------------------
# Read-only guard
# ---------------------------
class ReadOnlyViolation(RuntimeError):
    """Something other than a plain read was about to reach the database, or read-only could not be established."""


_ALLOWED_STATEMENT = re.compile(r"^\s*(SELECT|SHOW)\b", re.IGNORECASE)
_FORBIDDEN_FRAGMENTS = re.compile(
    r"\b(INTO\s+(OUTFILE|DUMPFILE)|FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE|GET_LOCK|RELEASE_LOCK|SLEEP)\b", re.IGNORECASE)


def check_read_only_sql(sql: Any) -> None:
    """Raises ReadOnlyViolation unless `sql` is exactly one SELECT or SHOW statement."""
    if not isinstance(sql, str) or not _ALLOWED_STATEMENT.match(sql):
        raise ReadOnlyViolation(f"only SELECT or SHOW statements are allowed, got: {str(sql)[:80]!r}")
    body = sql.strip()
    if body.endswith(";"):
        body = body[:-1]
    if ";" in body:
        raise ReadOnlyViolation("chained statements are not allowed")
    if _FORBIDDEN_FRAGMENTS.search(body):
        raise ReadOnlyViolation(f"a locking or file-writing clause is not allowed: {body[:80]!r}")


class ReadOnlyCursor:
    """Wraps a cursor so that it can only read. Everything the loader and the enrichment reads use is here (execute,
    the fetch methods, rowcount); executemany, callproc and the rest raise."""
    _PASSTHROUGH = frozenset({"fetchall", "fetchone", "fetchmany", "rowcount", "description", "column_names", "close"})

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self.statement_count = 0

    def execute(self, sql: Any, params: Any = None):
        check_read_only_sql(sql)
        self.statement_count += 1
        return self._cursor.execute(sql) if params is None else self._cursor.execute(sql, params)

    def __getattr__(self, name: str):
        if name in self._PASSTHROUGH:
            return getattr(self._cursor, name)
        raise ReadOnlyViolation(f"cursor.{name} is not available on a read-only cursor")


def enforce_read_only_session(cursor: Any) -> None:
    """Sets this connection's session to READ ONLY and verifies it. Called once, on the raw cursor, before any read.
    The statement guard above is the primary protection; this makes the server refuse a write as well."""
    try:
        cursor.execute("SET SESSION TRANSACTION READ ONLY")
    except Exception as exc:
        raise ReadOnlyViolation(f"could not set the session READ ONLY ({type(exc).__name__}: {exc}); not running") from exc
    value = None
    for variable in ("@@transaction_read_only", "@@tx_read_only"):
        try:
            cursor.execute(f"SELECT {variable} AS ro")
            rows = cursor.fetchall()
            value = rows[0]["ro"] if isinstance(rows[0], dict) else rows[0][0]
            break
        except Exception:
            continue
    if value is None or int(value) != 1:
        raise ReadOnlyViolation("the session could not be verified as READ ONLY; not running")


# ---------------------------
# Loading (SELECT only)
# ---------------------------
def present_session_columns(cursor: Any) -> set:
    cursor.execute("SELECT column_name AS name FROM information_schema.columns "
                   "WHERE table_schema = DATABASE() AND table_name = 'intent_sessions'")
    return {(r["name"] if isinstance(r, dict) else r[0]) for r in cursor.fetchall()}


def session_columns(present: set) -> Tuple[str, ...]:
    return (*REQUIRED_SESSION_COLUMNS, *(c for c in OPTIONAL_SESSION_COLUMNS if c in present))


def fetch_day_sessions_page(cursor: Any, start: datetime, end: datetime, limit: int,
                            after: Optional[Tuple[datetime, int]], columns: Sequence[str]) -> List[Dict[str, Any]]:
    """Every intent_sessions row whose session_start is in [start, end), (session_start, id) order, `limit` at a time.
    No duration filter: a session without one is reported as pending. No updated_at."""
    sql = f"SELECT {', '.join(columns)} FROM intent_sessions WHERE session_start >= %s AND session_start < %s"
    params: List[Any] = [start, end]
    if after is not None:
        sql += " AND (session_start > %s OR (session_start = %s AND id > %s))"
        params += [after[0], after[0], after[1]]
    sql += " ORDER BY session_start, id LIMIT %s"
    params.append(int(limit))
    cursor.execute(sql, tuple(params))
    return cursor.fetchall()


def _number(value: Any) -> Any:
    return float(value) if value is not None and hasattr(value, "as_tuple") else value


def load_behavioral_extras(cursor: Any, session_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """Optional raw features per session, one grouped query per batch of ids: the deepest scroll milestone and the
    distinct products viewed and added. Not used by V1; here for V2 and V3."""
    out: Dict[str, Dict[str, Any]] = {}
    ids = list(session_ids)
    for i in range(0, len(ids), IN_LIST_CHUNK):
        chunk = ids[i:i + IN_LIST_CHUNK]
        cursor.execute(
            "SELECT session_id, MAX(scroll_percent) AS max_scroll_percent, "
            "COUNT(DISTINCT CASE WHEN event_name = 'product_viewed' THEN product_id END) AS distinct_products_viewed, "
            "COUNT(DISTINCT CASE WHEN event_name = 'product_added_to_cart' THEN product_id END) AS distinct_products_added "
            f"FROM behavioral_events WHERE session_id IN ({', '.join(['%s'] * len(chunk))}) GROUP BY session_id",
            tuple(chunk))
        for row in cursor.fetchall():
            out[row["session_id"]] = {k: _number(v) for k, v in row.items() if k != "session_id"}
    return out


def _collect_extras(cursor: Any, rows: Sequence[Dict[str, Any]], loaders: Sequence[Callable]) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    ids = [r["session_id"] for r in rows]
    for loader in loaders:
        try:
            for session_id, values in loader(cursor, ids).items():
                merged.setdefault(session_id, {}).update(values)
        except ReadOnlyViolation:
            raise
        except Exception as exc:     # extras are optional: V1 does not need them
            logger.warning("extra feature loader %s failed (%s: %s); continuing without it",
                           getattr(loader, "__name__", loader), type(exc).__name__, str(exc)[:160])
    return merged


def load_sessions(cursor: Any, brand_id: str, start: datetime, end: datetime, chunk: int = 2000, sleep_s: float = 0.5,
                  max_sessions: Optional[int] = None, extra_loaders: Optional[Sequence[Callable]] = None,
                  sleep: Callable[[float], None] = time.sleep) -> List[ex.ExperimentSession]:
    """The common feature dataset of the day, built once. Pages by keyset; one enrichment read and one extras read
    per page, never one per session and never one per version."""
    columns = session_columns(present_session_columns(cursor))
    loaders = [load_behavioral_extras] if extra_loaders is None else list(extra_loaders)
    sessions: List[ex.ExperimentSession] = []
    after = None
    while True:
        limit = chunk if max_sessions is None else min(chunk, max_sessions - len(sessions))
        if limit <= 0:
            break
        rows = fetch_day_sessions_page(cursor, start, end, limit, after, columns)
        if not rows:
            break
        scorable = [r for r in rows if r.get("session_id") and r.get("session_time_spent_ms") is not None]
        enrichment = enrich_page(cursor, scorable) if scorable else {}
        extras = _collect_extras(cursor, scorable, loaders) if scorable else {}
        for row in rows:
            session_id = row.get("session_id")
            enriched = enrichment.get(session_id) or rp.Enrichment("other", set(), None, set())
            sessions.append(ex.build_session(row, enriched, brand_id, extras.get(session_id)))
        last = rows[-1]
        after = (last["session_start"], last["id"])
        _log(f"loaded {len(sessions)} sessions (up to {last['session_start']})")
        if len(rows) < limit:
            break
        if sleep_s > 0:
            sleep(sleep_s)
    return sessions


def load_reconstructed_sessions(directory: Any, brand_id: str) -> List[ex.ExperimentSession]:
    """The sessions written by scripts/rebuild_sessions_dry_run.py (sessions.jsonl): no database is read. They carry no stored score, so
    the V1 parity check does not apply to them (it is run on the stored sessions)."""
    sessions: List[ex.ExperimentSession] = []
    path = Path(directory) / "sessions.jsonl"
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            row = dict(record["row"])
            for key in ("session_start", "session_end", "occurred_at"):
                if row.get(key):
                    row[key] = datetime.fromisoformat(row[key])
            enrichment = rp.Enrichment(record["entry_page_type"], set(record["page_types_seen"]), record.get("user_agent"), set(record["paths"]))
            sessions.append(ex.build_session(row, enrichment, brand_id, None))
    return sessions


def load_clarity_row(cursor: Any, day: date) -> Optional[Dict[str, Any]]:
    """The day's Clarity aggregate (synced by clarity_intent_sync), or None when the table or the day is missing."""
    try:
        cursor.execute(
            "SELECT low_intent_pct, medium_intent_pct, high_intent_pct, low_sessions, medium_sessions, high_sessions, "
            f"total_sessions FROM {CLARITY_TABLE} WHERE `date` = %s LIMIT 1", (day,))
        rows = cursor.fetchall()
    except ReadOnlyViolation:
        raise
    except Exception:
        return None
    if not rows:
        return None
    row = rows[0]
    return {k: _number(v) for k, v in row.items()} if isinstance(row, dict) else None


# ---------------------------
# CLI
# ---------------------------
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local, read-only intent-scoring experiment over one day of sessions.")
    parser.add_argument("--brand", required=True, help="database name (e.g. BBB), brand index, or ALL for every mapped brand")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="store-local session day, YYYY-MM-DD")
    parser.add_argument("--versions", default="v1,v2,v3,v4,v5,v1c,v6",
                        help="comma separated, e.g. v1,v2 or v2,v3 (default %(default)s). The sessions are loaded once "
                             "whatever the selection. Registered: " + ", ".join(ex.VERSIONS))
    parser.add_argument("--threshold-mode", choices=ex.THRESHOLD_MODES, default="common",
                        help="common (default): percentiles of each version's own scores; production: also the fixed "
                             "25/55 fallback under 500 engaged sessions, as the daily calibration does")
    parser.add_argument("--out-dir", default="experiment_results", help="output directory (default %(default)s)")
    parser.add_argument("--chunk", type=int, default=2000, help="sessions per read (default 2000)")
    parser.add_argument("--sleep", type=float, default=0.5, help="seconds between reads (default 0.5)")
    parser.add_argument("--max-sessions", type=int, default=None, help="stop loading after this many sessions")
    parser.add_argument("--sessions-from", default=None,
                        help="score the sessions rebuilt by scripts/rebuild_sessions_dry_run.py in this directory instead of reading the "
                             "database (no connection is opened; the brand label is --brand)")
    args = parser.parse_args(argv)
    if args.chunk < 1:
        parser.error("--chunk must be at least 1")
    requested = [v.strip().upper() for v in args.versions.split(",") if v.strip()] if args.versions else None
    try:
        args.version_names = ex.resolve_versions(requested)
    except (ValueError, NotImplementedError) as exc:
        parser.error(str(exc))
    args.requested_versions = requested
    return args


def resolve_brands(brand: str) -> List[Tuple[str, int]]:
    """[(label, database index)]. ALL is every brand mapped by INTENT_DB_MAP."""
    import aws_background
    from pipeline.state import active_brand_indices

    if brand.strip().upper() == "ALL":
        from pipeline.migrations import mapped_brand_indices

        active_brand_indices.clear()
        aws_background.initialize_brand_configs()
        return [(os.environ.get(f"DB_DATABASE_{i}") or f"brand_{i}", i) for i in mapped_brand_indices()]
    if brand.isdigit():
        active_brand_indices.clear()
        aws_background.initialize_brand_configs()
        return [(os.environ.get(f"DB_DATABASE_{brand}") or f"brand_{brand}", int(brand))]
    from scripts.backfill_utm import resolve_brand_index

    index = resolve_brand_index(brand)
    return [(os.environ.get(f"DB_DATABASE_{index}") or brand, index)]


def run_brand(label: str, index: int, args: argparse.Namespace, cursor_factory: Callable,
              outcomes: Optional[ex.OutcomeProvider] = None) -> Optional[Dict[str, Any]]:
    """One brand: the sessions are loaded ONCE into the immutable dataset, then every selected version scores that same
    dataset in memory. Writes the files and prints the progress. Returns the summary, or None when the day has no
    sessions."""
    start, end = _day_bounds(args.date)
    print("Loading sessions...", flush=True)
    if getattr(args, "sessions_from", None):
        sessions = load_reconstructed_sessions(args.sessions_from, label)
        clarity = None
        _log(f"brand={label}: {len(sessions)} reconstructed sessions read from {args.sessions_from}; no database connection")
    else:
        with cursor_factory(index) as raw:
            enforce_read_only_session(raw)
            cursor = ReadOnlyCursor(raw)
            sessions = load_sessions(cursor, label, start, end, args.chunk, args.sleep, args.max_sessions)
            loading_statements = cursor.statement_count
            clarity = load_clarity_row(cursor, args.date) if sessions else None
            _log(f"brand={label}: {loading_statements} read statements to load {len(sessions)} sessions "
                 f"(+{cursor.statement_count - loading_statements} for Clarity), once for all versions, no writes")
    print(f"Loaded {len(sessions)} sessions.\n", flush=True)
    if not sessions:
        _log(f"brand={label}: no sessions with session_start on {args.date}")
        return None
    run = ex.run_experiment(sessions, args.requested_versions, args.threshold_mode, outcomes, brand_id=label,
                            day=args.date, progress=lambda name: print(f"Running {name}...", flush=True))
    summary = ex.summarize(run, clarity)
    paths = ex.write_outputs(run, summary, args.out_dir)
    print("\nExperiment complete.", flush=True)
    print(f"Results: {paths['csv'].parent}", flush=True)
    for key in ("csv", "long_csv", "json", "report"):
        print(f"  {paths[key].name}", flush=True)
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.sessions_from:
        return 0 if run_brand(args.brand, 0, args, None) is not None else 1
    from pipeline.db import get_db_read_cursor, read_replica_host

    brands = resolve_brands(args.brand)
    if not brands:
        _log("no brand to run")
        return 1
    _log(f"read replica configured: {'yes' if read_replica_host() else 'no (reading the primary)'}")
    done = failed = 0
    for label, index in brands:
        if len(brands) > 1:
            print(f"\n=== {label} ===", flush=True)
        try:
            if run_brand(label, index, args, get_db_read_cursor) is not None:
                done += 1
        except Exception as exc:
            failed += 1
            _log(f"brand={label} failed: {type(exc).__name__}: {exc}")
    if failed:
        return 2
    return 0 if done else 1


if __name__ == "__main__":
    sys.exit(main())
