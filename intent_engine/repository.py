"""
All raw SQL used by the intent scoring engine lives here: idempotent DDL
(_ensure_scoring_schema) and the batched read/write helpers used by
session_scorer.py, threshold_resolution.py, threshold_calibration.py, and
actor_scorer.py.

Reuses pipeline/intent_events.py's _ensure_column_exists and
pipeline/db.py's executemany_chunked - imported, never modified.
"""

import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.state import IST
from pipeline.intent_events import _ensure_column_exists
from pipeline.db import executemany_chunked

_log = logging.getLogger("intent_pipeline")

# The batched statements below are new SQL. If MySQL rejects one (a syntax, collation or type problem) the
# original statement is used instead, so scoring keeps working; INTENT_SCORING_MODE=legacy forces the
# originals. Connection loss, deadlocks and the like are not SQL rejections and still propagate.
IN_LIST_CHUNK = 1000        # ids per IN (...) list
UPDATE_CHUNK = 500          # sessions per multi-row UPDATE
_SQL_REJECTION_ERRNOS = {1064, 1054, 1052, 1267, 1271, 1366, 1292, 1265, 1406, 1048, 1136, 1222, 1241, 1305}


def _legacy_forced() -> bool:
    return os.environ.get("INTENT_SCORING_MODE", "batch").strip().lower() == "legacy"


def _is_sql_rejection(exc: BaseException) -> bool:
    if getattr(exc, "errno", None) in _SQL_REJECTION_ERRNOS:
        return True
    try:
        from mysql.connector import errors

        return isinstance(exc, (errors.ProgrammingError, errors.DataError, errors.NotSupportedError))
    except ImportError:  # pragma: no cover
        return False


def _chunks(items: Sequence[Any], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _day_bounds(target_date: date) -> Tuple[datetime, datetime]:
    """
    [start, end) datetime bounds for a calendar date, IST wall-clock -
    mirrors the already-proven pattern in pipeline/rollups.py's
    _date_range_bounds (private to that module, so duplicated here rather
    than imported). Used to turn `DATE(session_start) = ?` predicates
    (which defeat idx_session_start by wrapping it in a function) into a
    sargable `session_start >= ? AND session_start < ?` range that MySQL
    can actually use the index for.
    """
    start = datetime.combine(target_date, datetime.min.time(), tzinfo=IST)
    return start, start + timedelta(days=1)


# ---------------------------
# Idempotent DDL
# ---------------------------
def _ensure_intent_actors_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS intent_actors (
            actor_id VARCHAR(100) NOT NULL,

            predictive_score DECIMAL(5, 2) NULL,
            intent_bucket ENUM('low', 'medium', 'high') NULL,
            best_session_id VARCHAR(100) NULL,
            best_session_score DECIMAL(5, 2) NULL,
            session_count INT UNSIGNED NOT NULL DEFAULT 0,

            scored_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            score_version VARCHAR(20) NOT NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (actor_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_intent_thresholds_daily_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS intent_thresholds_daily (
            score_date DATE NOT NULL,

            scored_session_count INT UNSIGNED NOT NULL DEFAULT 0,
            p35_threshold DECIMAL(5, 2) NOT NULL,
            p72_threshold DECIMAL(5, 2) NOT NULL,
            p90_dead_click_rate DECIMAL(6, 4) NULL,
            calibration_method ENUM('percentile', 'fixed_fallback') NOT NULL,
            score_version VARCHAR(20) NOT NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (score_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


_SCORING_COLUMNS_ON_INTENT_SESSIONS = (
    ("predictive_score", "DECIMAL(5, 2) NULL"),
    ("intent_bucket", "ENUM('low', 'medium', 'high') NULL"),
    ("operational_bucket", "ENUM('low', 'medium', 'high') NULL"),
    ("entry_page_type", "ENUM('home', 'collection', 'pdp', 'checkout', 'other') NULL"),
    ("intent_status", "ENUM('pending', 'scored', 'excluded') NOT NULL DEFAULT 'pending'"),
    ("intent_exclusion_reason", "ENUM('bot', 'zero_signal') NULL"),
    ("intent_scored_at", "DATETIME(6) NULL"),
    ("intent_score_version", "VARCHAR(20) NULL"),
)


_schema_ensured_lock = threading.Lock()
_schema_ensured_brands: Set[int] = set()


def _ensure_scoring_schema(brand_index: int, cursor, connection) -> None:
    """
    Adds the intent-scoring columns to the EXISTING intent_sessions table
    (owned by pipeline/intent_events.py, which is never modified - this
    reuses that module's own idempotent _ensure_column_exists helper) and
    creates the two new scoring tables.

    Cached per brand_index for the lifetime of this process: the schema
    only needs to be checked once (these are idempotent, unchanging
    additions), but this was previously called unconditionally on every
    single run of both the 2-hour scoring job and the daily calibration
    job - 8x SHOW COLUMNS + 2x CREATE TABLE IF NOT EXISTS every time,
    forever, on an already I/O-constrained instance. The lock guards the
    cache since both jobs process brands concurrently via
    ThreadPoolExecutor within the same process. A process restart
    naturally re-checks once, which is correct and cheap.
    """
    with _schema_ensured_lock:
        if brand_index in _schema_ensured_brands:
            return

    for column_name, column_def in _SCORING_COLUMNS_ON_INTENT_SESSIONS:
        _ensure_column_exists(cursor, connection, "intent_sessions", column_name, column_def)

    _ensure_intent_actors_table(cursor, connection)
    _ensure_intent_thresholds_daily_table(cursor, connection)

    with _schema_ensured_lock:
        _schema_ensured_brands.add(brand_index)


# ---------------------------
# Session scorer reads/writes
# ---------------------------
_ELIGIBLE_SESSION_COLUMNS = (
    "session_id", "actor_id", "session_start", "session_time_spent_ms",
    "event_count", "page_view_count", "product_view_count", "click_count",
    "useful_click_count", "dead_click_count", "add_to_cart_count",
    "checkout_started_count", "scroll_count",
)


def fetch_eligible_sessions(
    cursor, window_start: datetime, window_end: datetime,
    limit: Optional[int] = None, after: Optional[tuple] = None,
) -> List[Dict[str, Any]]:
    """Closed sessions updated in [window_start, window_end).

    Without `limit` every row is returned (the original behaviour). With `limit` the rows come back
    oldest first in pages of at most `limit`; pass the (updated_at, session_id) of the last row of the
    previous page as `after` to get the next one."""
    columns_sql = ", ".join(_ELIGIBLE_SESSION_COLUMNS)
    sql = f"""
        SELECT {columns_sql}
        FROM intent_sessions
        WHERE updated_at >= %s AND updated_at < %s
              AND session_time_spent_ms IS NOT NULL
        """
    params: List[Any] = [window_start, window_end]
    if limit is not None:
        if after is not None:
            sql += " AND (updated_at > %s OR (updated_at = %s AND session_id > %s))"
            params += [after[0], after[0], after[1]]
        sql += " ORDER BY updated_at, session_id LIMIT %s"
        params.append(int(limit))
    cursor.execute(sql, tuple(params))
    return cursor.fetchall()


def fetch_last_processed_updated_at(cursor) -> Optional[datetime]:
    """updated_at (the database clock) of the most recent session the engine has already
    processed, scored or excluded. None if it has never processed one."""
    cursor.execute(
        "SELECT MAX(updated_at) AS last_updated FROM intent_sessions WHERE intent_status IN ('scored', 'excluded')"
    )
    row = cursor.fetchone()
    if not row:
        return None
    value = row["last_updated"] if isinstance(row, dict) else row[0]
    return value if isinstance(value, datetime) else None


def fetch_pending_sessions(
    cursor, window_start: datetime, window_end: datetime, limit: int,
    after: Optional[Tuple[datetime, str]] = None,
) -> List[Dict[str, Any]]:
    """Closed sessions still `pending` with updated_at in [window_start, window_end), oldest first.
    `after` is the (updated_at, session_id) of the last row of the previous chunk, so each chunk
    starts where the last one ended and the loop always moves forward, whatever happens to
    a row's status. Each row also carries updated_at, for that key."""
    columns_sql = ", ".join(_ELIGIBLE_SESSION_COLUMNS)
    sql = f"""
        SELECT {columns_sql}, updated_at
        FROM intent_sessions
        WHERE intent_status = 'pending'
              AND session_time_spent_ms IS NOT NULL
              AND updated_at >= %s AND updated_at < %s
    """
    params: List[Any] = [window_start, window_end]
    if after is not None:
        sql += " AND (updated_at > %s OR (updated_at = %s AND session_id > %s))"
        params += [after[0], after[0], after[1]]
    sql += " ORDER BY updated_at, session_id LIMIT %s"
    params.append(int(limit))
    cursor.execute(sql, tuple(params))
    return cursor.fetchall()


def fetch_pending_sessions_by_start(
    cursor, since: datetime, limit: int, before: Optional[Tuple[datetime, int]] = None,
) -> List[Dict[str, Any]]:
    """Closed sessions still `pending` that started at or after `since`, NEWEST first, in pages of at most
    `limit`. `before` is the (session_start, id) of the last row of the previous page.

    Pages by (session_start, id), which is exactly idx_session_start (a secondary index carries the primary key),
    so no extra index is needed and nothing is sorted: the page is read straight off the index, newest day first.
    The updated_at paging of fetch_pending_sessions needs an index that the table does not have by default and
    sorts the whole pending range for every page without it. Each row also carries `id` for that key."""
    columns_sql = ", ".join(_ELIGIBLE_SESSION_COLUMNS)
    sql = f"""
        SELECT {columns_sql}, id
        FROM intent_sessions
        WHERE session_start >= %s
              AND intent_status = 'pending'
              AND session_time_spent_ms IS NOT NULL
    """
    params: List[Any] = [since]
    if before is not None:
        sql += " AND (session_start < %s OR (session_start = %s AND id < %s))"
        params += [before[0], before[0], before[1]]
    sql += " ORDER BY session_start DESC, id DESC LIMIT %s"
    params.append(int(limit))
    cursor.execute(sql, tuple(params))
    return cursor.fetchall()


def count_pending_sessions(cursor, since: datetime, cap: int = 100000) -> int:
    """How many closed sessions are still pending since `since`, counted up to `cap` (a bounded read, never a
    table-wide count)."""
    cursor.execute(
        "SELECT COUNT(*) AS n FROM (SELECT 1 FROM intent_sessions WHERE session_start >= %s "
        "AND intent_status = 'pending' AND session_time_spent_ms IS NOT NULL LIMIT %s) pending",
        (since, int(cap)),
    )
    row = cursor.fetchone()
    return int((row["n"] if isinstance(row, dict) else row[0]) or 0) if row else 0


def updated_at_index_present(cursor) -> bool:
    """Whether intent_sessions has an index that starts with updated_at (the windowed scoring pass filters on it)."""
    cursor.execute(
        "SELECT 1 AS present FROM information_schema.statistics WHERE table_schema = DATABASE() "
        "AND table_name = 'intent_sessions' AND column_name = 'updated_at' AND seq_in_index = 1 LIMIT 1"
    )
    return bool(cursor.fetchall())


def _legacy_fetch_page_viewed_enrichment(cursor, session_ids: Sequence[str]) -> List[Dict[str, Any]]:
    """The original query: one row per page_viewed event, with its full url and user agent."""
    if not session_ids:
        return []
    placeholders = ", ".join(["%s"] * len(session_ids))
    cursor.execute(
        f"""
        SELECT session_id, occurred_at, url, user_agent
        FROM behavioral_events
        WHERE session_id IN ({placeholders}) AND event_name = 'page_viewed'
        """,
        tuple(session_ids),
    )
    return cursor.fetchall()


def fetch_page_viewed_enrichment(cursor, session_ids: Sequence[str]) -> List[Dict[str, Any]]:
    """
    One row per (session, page path) instead of one per page_viewed event: the path with its query string
    and fragment stripped in SQL, the time it was first seen, and a user agent. That is everything
    session_scorer._build_enrichment reads (entry page = earliest path, page types = the distinct paths,
    user agent = any non-null one), with far fewer rows and no repeated url/user-agent text.
    normalize_page_path strips the query and fragment itself, so a pre-stripped path normalises identically.
    Falls back to the original query if MySQL rejects this one. Ids are sent in bounded IN lists.
    """
    if not session_ids:
        return []
    if _legacy_forced():
        return _legacy_fetch_page_viewed_enrichment(cursor, session_ids)
    ids = list(session_ids)
    try:
        rows: List[Dict[str, Any]] = []
        for chunk in _chunks(ids, IN_LIST_CHUNK):
            cursor.execute(
                f"""
                SELECT session_id,
                       SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS page_path,
                       MIN(occurred_at) AS occurred_at,
                       MAX(user_agent) AS user_agent
                FROM behavioral_events
                WHERE session_id IN ({", ".join(["%s"] * len(chunk))}) AND event_name = 'page_viewed'
                GROUP BY session_id, page_path
                """,
                tuple(chunk),
            )
            for row in cursor.fetchall():
                row["url"] = row.pop("page_path")
                rows.append(row)
        return rows
    except Exception as exc:
        if not _is_sql_rejection(exc):
            raise
        _log.warning("[intent scoring] grouped enrichment query rejected (%s: %s); using the original query",
                     type(exc).__name__, str(exc)[:200])
        rows = []
        for chunk in _chunks(ids, IN_LIST_CHUNK):
            rows.extend(_legacy_fetch_page_viewed_enrichment(cursor, chunk))
        return rows


def fetch_click_user_agents(cursor, session_ids: Sequence[str]) -> Dict[str, str]:
    """
    Fallback UA source for click-only sessions with zero page_viewed rows -
    only ever called on the residual subset still missing a user_agent,
    never the full batch. Ids are sent in bounded IN lists.
    """
    if not session_ids:
        return {}
    found: Dict[str, str] = {}
    for chunk in _chunks(list(session_ids), IN_LIST_CHUNK):
        placeholders = ", ".join(["%s"] * len(chunk))
        cursor.execute(
            f"""
            SELECT session_id, ANY_VALUE(user_agent) AS user_agent
            FROM click_events
            WHERE session_id IN ({placeholders})
            GROUP BY session_id
            """,
            tuple(chunk),
        )
        found.update({row["session_id"]: row["user_agent"] for row in cursor.fetchall() if row["user_agent"]})
    return found


def _legacy_bulk_update_scored_sessions(cursor, connection, rows: List[Tuple]) -> int:
    """The original: one UPDATE per session through executemany.

    
    Plain bulk UPDATE, not INSERT-on-duplicate: this engine only ever scores
    sessions it just read from intent_sessions, so the row always pre-exists
    - an INSERT path here would risk a NOT NULL failure on columns
    (actor_id, session_start, ...) this engine never provides.

    rows: (predictive_score, intent_bucket, operational_bucket,
    entry_page_type, intent_status, intent_scored_at, intent_score_version,
    session_id) - session_id LAST, matching the WHERE clause. Every column
    is an overwrite (never an increment) - safe to re-run over the same
    sessions any number of times.

    `updated_at = updated_at` is deliberate: intent_sessions.updated_at has
    ON UPDATE CURRENT_TIMESTAMP (pipeline/intent_events.py, unmodified), and
    that same column is what fetch_eligible_sessions uses as the
    incremental watermark. Without this, every scoring write would bump
    updated_at, which would re-qualify that same session as "newly
    eligible" on the very next run - a self-perpetuating loop where every
    session ever scored stays eligible forever. Explicitly assigning a
    column back to itself in the SET list is how MySQL lets you suppress
    the automatic ON UPDATE behavior for one statement.
    """
    if not rows:
        return 0
    sql = """
        UPDATE intent_sessions
        SET predictive_score = %s,
            intent_bucket = %s,
            operational_bucket = %s,
            entry_page_type = %s,
            intent_status = %s,
            intent_scored_at = %s,
            intent_score_version = %s,
            updated_at = updated_at
        WHERE session_id = %s
    """
    return executemany_chunked(cursor, connection, sql, rows)


def _legacy_bulk_update_excluded_sessions(cursor, connection, rows: List[Tuple]) -> int:
    """The original: one UPDATE per session through executemany.

    
    rows: (exclusion_reason, intent_scored_at, intent_score_version, session_id).
    See bulk_update_scored_sessions's docstring for why updated_at = updated_at
    is required here too - the same watermark-loop risk applies to excluded
    sessions.
    """
    if not rows:
        return 0
    sql = """
        UPDATE intent_sessions
        SET predictive_score = NULL,
            intent_bucket = NULL,
            operational_bucket = NULL,
            entry_page_type = NULL,
            intent_status = 'excluded',
            intent_exclusion_reason = %s,
            intent_scored_at = %s,
            intent_score_version = %s,
            updated_at = updated_at
        WHERE session_id = %s
    """
    return executemany_chunked(cursor, connection, sql, rows)


def _batched_update(cursor, connection, rows: List[Tuple], columns: Tuple[str, ...], assignments: str) -> int:
    """
    UPDATE intent_sessions s JOIN (SELECT ... UNION ALL SELECT ...) v ON v.session_id = s.session_id SET ...
    One statement per UPDATE_CHUNK sessions instead of one per session. The UNION ALL derived table works on
    every MySQL version (no VALUES ROW). `s.updated_at = s.updated_at` is kept for the same reason as in the
    original (it suppresses ON UPDATE CURRENT_TIMESTAMP, which is the scoring watermark). `columns` are the
    row's fields in order, the session id LAST. Commits per chunk, as executemany_chunked did.
    """
    one_row = "SELECT " + ", ".join(["%s"] * len(columns))
    first_row = "SELECT " + ", ".join(f"%s AS {name}" for name in columns)
    for chunk in _chunks(rows, UPDATE_CHUNK):
        derived = " UNION ALL ".join([first_row] + [one_row] * (len(chunk) - 1))
        params = tuple(value for row in chunk for value in row)
        cursor.execute(
            f"UPDATE intent_sessions s JOIN ({derived}) v ON v.session_id = s.session_id SET {assignments}", params)
        connection.commit()
    return len(rows)


def _with_original_fallback(batched, original, cursor, connection, rows, what: str) -> int:
    if _legacy_forced():
        return original(cursor, connection, rows)
    try:
        return batched(cursor, connection, rows)
    except Exception as exc:
        if not _is_sql_rejection(exc):
            raise
        try:
            connection.rollback()
        except Exception:
            pass
        _log.warning("[intent scoring] batched %s update rejected (%s: %s); using the original per-session update",
                     what, type(exc).__name__, str(exc)[:200])
        return original(cursor, connection, rows)


def bulk_update_scored_sessions(cursor, connection, rows: List[Tuple]) -> int:
    """Same contract as the original (rows end with the session_id; every column overwritten; updated_at
    untouched), written as multi-row UPDATEs. See _legacy_bulk_update_scored_sessions for the full reasoning."""
    if not rows:
        return 0
    columns = ("predictive_score", "intent_bucket", "operational_bucket", "entry_page_type", "intent_status",
               "intent_scored_at", "intent_score_version", "session_id")
    assignments = (
        "s.predictive_score = v.predictive_score, s.intent_bucket = v.intent_bucket, "
        "s.operational_bucket = v.operational_bucket, s.entry_page_type = v.entry_page_type, "
        "s.intent_status = v.intent_status, s.intent_scored_at = v.intent_scored_at, "
        "s.intent_score_version = v.intent_score_version, s.updated_at = s.updated_at"
    )
    return _with_original_fallback(
        lambda c, conn, r: _batched_update(c, conn, r, columns, assignments),
        _legacy_bulk_update_scored_sessions, cursor, connection, rows, "scored")


def bulk_update_excluded_sessions(cursor, connection, rows: List[Tuple]) -> int:
    """rows: (exclusion_reason, intent_scored_at, intent_score_version, session_id). Same contract as the original."""
    if not rows:
        return 0
    columns = ("exclusion_reason", "intent_scored_at", "intent_score_version", "session_id")
    assignments = (
        "s.predictive_score = NULL, s.intent_bucket = NULL, s.operational_bucket = NULL, s.entry_page_type = NULL, "
        "s.intent_status = 'excluded', s.intent_exclusion_reason = v.exclusion_reason, "
        "s.intent_scored_at = v.intent_scored_at, s.intent_score_version = v.intent_score_version, "
        "s.updated_at = s.updated_at"
    )
    return _with_original_fallback(
        lambda c, conn, r: _batched_update(c, conn, r, columns, assignments),
        _legacy_bulk_update_excluded_sessions, cursor, connection, rows, "excluded")


# ---------------------------
# Threshold resolution (read-only, used by the 2-hour job)
# ---------------------------
def fetch_thresholds_up_to(cursor, max_date: date, limit: int = 400) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT score_date, p35_threshold, p72_threshold, p90_dead_click_rate
        FROM intent_thresholds_daily
        WHERE score_date <= %s
        ORDER BY score_date DESC
        LIMIT %s
        """,
        (max_date, limit),
    )
    return cursor.fetchall()


# ---------------------------
# Threshold calibration (write path, used by the daily job)
# ---------------------------
def fetch_dead_click_rates_for_date(cursor, target_date: date, max_rows: int) -> List[Tuple[int, int]]:
    start, end = _day_bounds(target_date)
    cursor.execute(
        """
        SELECT dead_click_count, click_count
        FROM intent_sessions
        WHERE session_start >= %s AND session_start < %s AND click_count >= 10
        LIMIT %s
        """,
        (start, end, max_rows),
    )
    return [(r["dead_click_count"] or 0, r["click_count"]) for r in cursor.fetchall()]


def fetch_scored_predictive_scores_for_date(cursor, target_date: date, max_rows: int) -> List[float]:
    start, end = _day_bounds(target_date)
    cursor.execute(
        """
        SELECT predictive_score
        FROM intent_sessions
        WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored'
        LIMIT %s
        """,
        (start, end, max_rows),
    )
    return [float(r["predictive_score"]) for r in cursor.fetchall() if r["predictive_score"] is not None]


def upsert_daily_thresholds(
    cursor, connection, target_date: date, scored_session_count: int,
    p35: float, p72: float, p90_dead_click_rate: Optional[float],
    calibration_method: str, score_version: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO intent_thresholds_daily (
            score_date, scored_session_count, p35_threshold, p72_threshold,
            p90_dead_click_rate, calibration_method, score_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            scored_session_count = VALUES(scored_session_count),
            p35_threshold = VALUES(p35_threshold),
            p72_threshold = VALUES(p72_threshold),
            p90_dead_click_rate = VALUES(p90_dead_click_rate),
            calibration_method = VALUES(calibration_method),
            score_version = VALUES(score_version)
        """,
        (target_date, scored_session_count, p35, p72, p90_dead_click_rate,
         calibration_method, score_version),
    )
    connection.commit()


def bulk_reassign_buckets_for_date(
    cursor, connection, target_date: date, p35: float, p72: float,
) -> int:
    start, end = _day_bounds(target_date)
    cursor.execute(
        """
        UPDATE intent_sessions
        SET intent_bucket = CASE
                WHEN predictive_score <= 0 OR predictive_score < %s THEN 'low'
                WHEN predictive_score < %s THEN 'medium'
                ELSE 'high' END,
            operational_bucket = CASE
                WHEN entry_page_type = 'checkout' OR checkout_started_count > 0
                     OR add_to_cart_count > 0 THEN 'high'
                WHEN predictive_score <= 0 OR predictive_score < %s THEN 'low'
                WHEN predictive_score < %s THEN 'medium'
                ELSE 'high' END,
            updated_at = updated_at
        WHERE intent_status = 'scored' AND session_start >= %s AND session_start < %s
        """,
        (p35, p72, p35, p72, start, end),
    )
    connection.commit()
    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0


def fetch_actor_ids_scored_on_date(cursor, target_date: date) -> List[str]:
    start, end = _day_bounds(target_date)
    cursor.execute(
        """
        SELECT DISTINCT actor_id
        FROM intent_sessions
        WHERE session_start >= %s AND session_start < %s AND intent_status = 'scored'
        """,
        (start, end),
    )
    return [r["actor_id"] for r in cursor.fetchall()]


# ---------------------------
# Actor recompute (shared by both jobs)
# ---------------------------
def fetch_scored_sessions_for_actors(cursor, actor_ids: Sequence[str]) -> List[Dict[str, Any]]:
    """
    Callers pass sorted, unique ids. They are sent in bounded IN lists; every chunk is ordered by
    (actor_id, predictive_score DESC) and the chunks cover disjoint, ascending actor ranges, so the
    concatenation keeps the order actor_scorer.recompute_actors relies on (best session first per actor).
    """
    if not actor_ids:
        return []
    rows: List[Dict[str, Any]] = []
    for chunk in _chunks(list(actor_ids), IN_LIST_CHUNK):
        placeholders = ", ".join(["%s"] * len(chunk))
        cursor.execute(
            f"""
            SELECT actor_id, session_id, predictive_score, intent_bucket
            FROM intent_sessions
            WHERE actor_id IN ({placeholders}) AND intent_status = 'scored'
            ORDER BY actor_id, predictive_score DESC
            """,
            tuple(chunk),
        )
        rows.extend(cursor.fetchall())
    return rows


def bulk_upsert_actors(cursor, connection, rows: List[Tuple]) -> int:
    """
    rows: (actor_id, predictive_score, intent_bucket, best_session_id,
    best_session_score, session_count, scored_at, score_version).
    Full overwrite - never a running max.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO intent_actors (
            actor_id, predictive_score, intent_bucket, best_session_id,
            best_session_score, session_count, scored_at, score_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            predictive_score = VALUES(predictive_score),
            intent_bucket = VALUES(intent_bucket),
            best_session_id = VALUES(best_session_id),
            best_session_score = VALUES(best_session_score),
            session_count = VALUES(session_count),
            scored_at = VALUES(scored_at),
            score_version = VALUES(score_version)
    """
    return executemany_chunked(cursor, connection, sql, rows)
