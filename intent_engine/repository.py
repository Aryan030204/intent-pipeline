"""
All raw SQL used by the intent scoring engine lives here: idempotent DDL
(_ensure_scoring_schema) and the batched read/write helpers used by
session_scorer.py, threshold_resolution.py, threshold_calibration.py, and
actor_scorer.py.

Reuses pipeline/intent_events.py's _ensure_column_exists and
pipeline/db.py's executemany_chunked - imported, never modified.
"""

import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.state import IST
from pipeline.intent_events import _ensure_column_exists
from pipeline.db import executemany_chunked


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


def fetch_eligible_sessions(cursor, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
    columns_sql = ", ".join(_ELIGIBLE_SESSION_COLUMNS)
    cursor.execute(
        f"""
        SELECT {columns_sql}
        FROM intent_sessions
        WHERE updated_at >= %s AND updated_at < %s
              AND session_time_spent_ms IS NOT NULL
        """,
        (window_start, window_end),
    )
    return cursor.fetchall()


def fetch_page_viewed_enrichment(cursor, session_ids: Sequence[str]) -> List[Dict[str, Any]]:
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


def fetch_click_user_agents(cursor, session_ids: Sequence[str]) -> Dict[str, str]:
    """
    Fallback UA source for click-only sessions with zero page_viewed rows -
    only ever called on the residual subset still missing a user_agent,
    never the full batch.
    """
    if not session_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(session_ids))
    cursor.execute(
        f"""
        SELECT session_id, ANY_VALUE(user_agent) AS user_agent
        FROM click_events
        WHERE session_id IN ({placeholders})
        GROUP BY session_id
        """,
        tuple(session_ids),
    )
    return {row["session_id"]: row["user_agent"] for row in cursor.fetchall() if row["user_agent"]}


def bulk_update_scored_sessions(cursor, connection, rows: List[Tuple]) -> int:
    """
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


def bulk_update_excluded_sessions(cursor, connection, rows: List[Tuple]) -> int:
    """
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
                WHEN predictive_score < %s THEN 'low'
                WHEN predictive_score < %s THEN 'medium'
                ELSE 'high' END,
            operational_bucket = CASE
                WHEN entry_page_type = 'checkout' OR checkout_started_count > 0
                     OR add_to_cart_count > 0 THEN 'high'
                WHEN predictive_score < %s THEN 'low'
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
    if not actor_ids:
        return []
    placeholders = ", ".join(["%s"] * len(actor_ids))
    cursor.execute(
        f"""
        SELECT actor_id, session_id, predictive_score, intent_bucket
        FROM intent_sessions
        WHERE actor_id IN ({placeholders}) AND intent_status = 'scored'
        ORDER BY actor_id, predictive_score DESC
        """,
        tuple(actor_ids),
    )
    return cursor.fetchall()


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
