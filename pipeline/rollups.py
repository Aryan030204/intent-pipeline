"""
Analytical rollup layer for the Intent Data Aggregation pipeline.

Reads the already-committed raw tables written by pipeline/intent_events.py
(behavioral_events, click_events, intent_sessions) and maintains five small
per-brand aggregate tables for dashboard consumption:

    intent_daily_summary   - grain: date
    page_behavior_daily    - grain: date + page_path
    click_behavior_daily   - grain: date + page_path + click_target
    product_behavior_daily - grain: date + product_id
    behavioral_path_daily  - grain: date + sequence_hash

This module does NOT touch Mongo and does NOT modify raw ingestion. It is
purely a read-from-MySQL / upsert-to-MySQL stage, invoked after ingestion
by pipeline/orchestration.py.

Incremental processing: a single watermark (pipeline_metadata key
ROLLUP_METADATA_KEY, distinct from the three ingestion watermarks) tracks
wall-clock time processed-through. Each run recomputes COMPLETE buckets
(never increments) for every date in [watermark - overlap, now], then
upserts with column overwrites - this is what keeps double-processing
(retries, the overlap window itself, a crashed mid-run retry) safe. The
watermark only advances once all five rollups succeed for the run.
"""

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from pipeline.state import IST, logger
from pipeline.db import (
    get_db_cursor,
    executemany_chunked,
    get_pipeline_metadata_timestamp,
    update_pipeline_metadata_timestamp,
)

# ---------------------------
# Constants
# ---------------------------
ROLLUP_METADATA_KEY = "intent_rollup_last_processed_at"
ROLLUP_OVERLAP = timedelta(hours=1)
ROLLUP_DEFAULT_LOOKBACK = timedelta(hours=6)
ROLLUP_MAX_WINDOW_DAYS = 31

PAGE_PATH_MAX_LEN = 255
SEQUENCE_MAX_LEN = 500
SEQUENCE_MAX_STEPS = 15
ROLLUP_MAX_SESSIONS_PER_DATE = 50000

_NUMERIC_SEGMENT = re.compile(r"^\d+$")
_UUID_SEGMENT = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_HEX_SEGMENT = re.compile(r"^[0-9a-f]{16,}$")


# ---------------------------
# Idempotent DDL
# ---------------------------
def _ensure_intent_daily_summary_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS intent_daily_summary (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            summary_date DATE NOT NULL,

            sessions INT UNSIGNED NOT NULL DEFAULT 0,
            unique_actors INT UNSIGNED NOT NULL DEFAULT 0,
            total_events INT UNSIGNED NOT NULL DEFAULT 0,
            page_views INT UNSIGNED NOT NULL DEFAULT 0,
            product_views INT UNSIGNED NOT NULL DEFAULT 0,
            clicks INT UNSIGNED NOT NULL DEFAULT 0,
            useful_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            dead_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            add_to_carts INT UNSIGNED NOT NULL DEFAULT 0,
            checkout_starts INT UNSIGNED NOT NULL DEFAULT 0,
            scroll_events INT UNSIGNED NOT NULL DEFAULT 0,

            avg_session_time_ms DECIMAL(14, 2) NULL,
            avg_events_per_session DECIMAL(10, 4) NULL,
            avg_product_views_per_session DECIMAL(10, 4) NULL,
            avg_clicks_per_session DECIMAL(10, 4) NULL,
            useful_click_rate DECIMAL(6, 4) NULL,
            dead_click_rate DECIMAL(6, 4) NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_summary_date (summary_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_page_behavior_daily_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS page_behavior_daily (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            summary_date DATE NOT NULL,
            page_path VARCHAR(255) NOT NULL,

            page_views INT UNSIGNED NOT NULL DEFAULT 0,
            unique_actors INT UNSIGNED NOT NULL DEFAULT 0,
            unique_sessions INT UNSIGNED NOT NULL DEFAULT 0,
            clicks INT UNSIGNED NOT NULL DEFAULT 0,
            useful_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            dead_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            dead_click_rate DECIMAL(6, 4) NULL,
            avg_scroll_percent DECIMAL(5, 2) NULL,
            max_scroll_percent DECIMAL(5, 2) NULL,
            add_to_cart_events INT UNSIGNED NOT NULL DEFAULT 0,
            checkout_started_events INT UNSIGNED NOT NULL DEFAULT 0,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_date_path (summary_date, page_path),
            KEY idx_page_path_date (page_path, summary_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_click_behavior_daily_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS click_behavior_daily (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            summary_date DATE NOT NULL,
            page_path VARCHAR(255) NOT NULL,
            click_target_hash CHAR(32) NOT NULL,

            tag_name VARCHAR(50) NULL,
            element_id VARCHAR(255) NULL,
            element_name VARCHAR(255) NULL,
            element_type VARCHAR(100) NULL,
            href TEXT NULL,

            total_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            useful_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            dead_clicks INT UNSIGNED NOT NULL DEFAULT 0,
            useful_click_rate DECIMAL(6, 4) NULL,
            dead_click_rate DECIMAL(6, 4) NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_date_path_target (summary_date, page_path, click_target_hash),
            KEY idx_summary_date (summary_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_product_behavior_daily_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product_behavior_daily (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            summary_date DATE NOT NULL,
            product_id VARCHAR(100) NOT NULL,
            product_title VARCHAR(500) NULL,

            product_views INT UNSIGNED NOT NULL DEFAULT 0,
            unique_viewers INT UNSIGNED NOT NULL DEFAULT 0,
            unique_sessions INT UNSIGNED NOT NULL DEFAULT 0,
            sessions_with_product_view INT UNSIGNED NOT NULL DEFAULT 0,
            add_to_cart_count INT UNSIGNED NOT NULL DEFAULT 0,
            checkout_started_count INT UNSIGNED NOT NULL DEFAULT 0,
            view_to_atc_rate DECIMAL(6, 4) NULL,
            view_to_checkout_rate DECIMAL(6, 4) NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_date_product (summary_date, product_id),
            KEY idx_product_date (product_id, summary_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_behavioral_path_daily_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS behavioral_path_daily (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            summary_date DATE NOT NULL,
            sequence_hash CHAR(40) NOT NULL,
            sequence VARCHAR(500) NOT NULL,

            session_count INT UNSIGNED NOT NULL DEFAULT 0,
            unique_actor_count INT UNSIGNED NOT NULL DEFAULT 0,
            sessions_with_atc INT UNSIGNED NOT NULL DEFAULT 0,
            sessions_with_checkout INT UNSIGNED NOT NULL DEFAULT 0,
            conversion_to_atc DECIMAL(6, 4) NULL,
            conversion_to_checkout DECIMAL(6, 4) NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_date_hash (summary_date, sequence_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_all_rollup_tables(cursor, connection) -> None:
    _ensure_intent_daily_summary_table(cursor, connection)
    _ensure_page_behavior_daily_table(cursor, connection)
    _ensure_click_behavior_daily_table(cursor, connection)
    _ensure_product_behavior_daily_table(cursor, connection)
    _ensure_behavioral_path_daily_table(cursor, connection)


# ---------------------------
# Shared normalization helpers
# ---------------------------
def _normalize_page_path(raw_path: Optional[str]) -> str:
    """
    Deterministic page-path normalization, reused by page_behavior_daily
    and click_behavior_daily so both rollups group under identical keys.

    `raw_path` is expected to already have the query string and #fragment
    stripped (done in SQL via SUBSTRING_INDEX before this is called) - this
    function only handles the parts that can't cleanly be expressed in SQL.
    """
    if not raw_path:
        return "(unknown)"

    # Defensive: strip query string/fragment here too, not just at the SQL
    # layer (SUBSTRING_INDEX) - this function is shared/reusable and must
    # not silently mis-normalize if ever called with an un-pre-stripped
    # value (e.g. directly on a raw `url`/`click_href`).
    path = raw_path.strip().split("?", 1)[0].split("#", 1)[0]
    if "://" in path:
        # Defensive: strip scheme+host if a full URL slipped through.
        after_scheme = path.split("://", 1)[1]
        slash_idx = after_scheme.find("/")
        path = after_scheme[slash_idx:] if slash_idx != -1 else "/"

    if not path.startswith("/"):
        path = "/" + path

    path = path.lower()
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
        if not path:
            path = "/"

    segments = path.split("/")
    normalized_segments = []
    for segment in segments:
        if _NUMERIC_SEGMENT.match(segment) or _UUID_SEGMENT.match(segment) or _HEX_SEGMENT.match(segment):
            normalized_segments.append(":id")
        else:
            normalized_segments.append(segment)
    path = "/".join(normalized_segments)

    if not path:
        path = "/"

    if len(path) > PAGE_PATH_MAX_LEN:
        path = path[:PAGE_PATH_MAX_LEN]

    return path


def _normalize_click_target(
    tag_name: Optional[str],
    element_id: Optional[str],
    element_name: Optional[str],
    element_type: Optional[str],
    href: Optional[str],
) -> Tuple[str, str, str, str, str, str]:
    """
    Reusable click-target normalization. Returns
    (tag_name, element_id, element_name, element_type, normalized_href, click_target_hash).
    The hash (MD5 - display-grouping only, not business-metric identity) is
    what bounds cardinality in click_behavior_daily's UNIQUE KEY; the four
    raw-ish fields are kept for dashboard readability.
    """
    def _clean(v: Optional[str]) -> str:
        return (str(v).strip().lower()) if v not in (None, "") else ""

    tag = _clean(tag_name)
    eid = _clean(element_id)
    ename = _clean(element_name)
    etype = _clean(element_type)

    href_path = _normalize_page_path(href) if href else ""
    if href_path == "(unknown)":
        href_path = ""

    key_material = "|".join([tag, eid, ename, etype, href_path])
    click_target_hash = hashlib.md5(key_material.encode("utf-8")).hexdigest()

    return (tag, eid, ename, etype, href_path, click_target_hash)


def _compute_sequence_path(event_sequence_raw: Any) -> Tuple[str, str, int]:
    """
    Given intent_sessions.event_sequence's JSON shape
    ({"1": {"page_viewed": "<event_id>"}, "2": {"click": "<event_id>"}, ...}),
    returns (sequence_hash, sequence_summary, step_count).

    Collapses consecutive duplicate event names, caps at SEQUENCE_MAX_STEPS
    collapsed steps, and hashes with SHA1 (stronger than the click-target
    MD5 since this hash groups business metrics - ATC/checkout conversion -
    not just display rows).
    """
    try:
        if isinstance(event_sequence_raw, (dict, list)):
            parsed = event_sequence_raw
        else:
            parsed = json.loads(event_sequence_raw)
        if not isinstance(parsed, dict):
            raise ValueError("event_sequence is not a JSON object")
    except (TypeError, ValueError, json.JSONDecodeError):
        summary = "(invalid)"
        return (hashlib.sha1(summary.encode("utf-8")).hexdigest(), summary, 0)

    ordered: List[Tuple[int, str]] = []
    for key, sub_event in parsed.items():
        if not isinstance(sub_event, dict) or not sub_event:
            continue
        try:
            step = int(key)
        except (TypeError, ValueError):
            continue
        event_name = str(next(iter(sub_event.keys())) or "").strip()
        if event_name:
            ordered.append((step, event_name))
    ordered.sort(key=lambda item: item[0])

    names: List[str] = []
    for _, event_name in ordered:
        if names and names[-1] == event_name:
            continue
        names.append(event_name)

    truncated = False
    if len(names) > SEQUENCE_MAX_STEPS:
        names = names[:SEQUENCE_MAX_STEPS]
        truncated = True

    step_count = len(names)
    summary = ">".join(names) if names else "(empty)"
    if truncated:
        summary = f"{summary}>..."
    if len(summary) > SEQUENCE_MAX_LEN:
        summary = summary[:SEQUENCE_MAX_LEN]

    sequence_hash = hashlib.sha1(summary.encode("utf-8")).hexdigest()
    return (sequence_hash, summary, step_count)


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if not denominator:
        return None
    return round(numerator / denominator, 4)


# ---------------------------
# Affected-date-window determination
# ---------------------------
def _determine_affected_dates(
    cursor,
    now: datetime,
    metadata_key: str = ROLLUP_METADATA_KEY,
    overlap: timedelta = ROLLUP_OVERLAP,
    default_lookback: timedelta = ROLLUP_DEFAULT_LOOKBACK,
    max_window_days: int = ROLLUP_MAX_WINDOW_DAYS,
) -> Tuple[List[date], Optional[datetime]]:
    """
    Returns (sorted list of IST calendar dates to recompute, stored_watermark).
    The caller persists `now` as the new watermark only after all rollups
    succeed - see run_rollups_for_brand.
    """
    stored_watermark = get_pipeline_metadata_timestamp(cursor, metadata_key)
    if stored_watermark is not None:
        window_start = stored_watermark - overlap
    else:
        window_start = now - default_lookback

    earliest_allowed = now - timedelta(days=max_window_days)
    if window_start < earliest_allowed:
        logger.warning(
            "[rollup] affected-date window start %s clamped to max_window_days=%s cap (%s)",
            window_start, max_window_days, earliest_allowed,
        )
        window_start = earliest_allowed

    start_date = window_start.astimezone(IST).date()
    end_date = now.astimezone(IST).date()

    dates: List[date] = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    return dates, stored_watermark


def _date_range_bounds(target_date: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(target_date, datetime.min.time(), tzinfo=IST)
    end = start + timedelta(days=1)
    return start, end


def _window_bounds(dates: List[date]) -> Tuple[datetime, datetime]:
    start, _ = _date_range_bounds(min(dates))
    _, end = _date_range_bounds(max(dates))
    return start, end


# ---------------------------
# Shared upsert + stale-key cleanup
# ---------------------------
def _upsert_rollup_rows(
    cursor,
    connection,
    table_name: str,
    insert_sql: str,
    rows: List[Tuple],
    key_columns: List[str],
    dates: List[date],
    computed_keys_by_date: Dict[date, Set[Tuple]],
) -> Tuple[int, int]:
    """
    Bulk-upserts `rows` (overwrite semantics - every non-key column in
    insert_sql's UPDATE clause must be `col = VALUES(col)`, never an
    increment), then deletes any row for each processed date whose key
    tuple is NOT in that date's freshly computed set (guarded: a date
    with an empty computed set is skipped, not wiped - see module
    docstring).
    """
    upserted = executemany_chunked(cursor, connection, insert_sql, rows) if rows else 0

    deleted_total = 0
    key_cols_sql = ", ".join(key_columns[1:])  # exclude summary_date, handled separately
    for target_date in dates:
        computed_keys = computed_keys_by_date.get(target_date, set())
        if not computed_keys:
            logger.warning(
                "[rollup] %s: skipping stale-key cleanup for %s (empty computed set)",
                table_name, target_date,
            )
            continue

        if len(key_columns) == 2:
            # Single non-date key column (e.g. page_path, product_id, sequence_hash).
            values = [k[0] for k in computed_keys]
            placeholders = ", ".join(["%s"] * len(values))
            sql = (
                f"DELETE FROM {table_name} WHERE summary_date = %s "
                f"AND {key_columns[1]} NOT IN ({placeholders})"
            )
            cursor.execute(sql, (target_date, *values))
        else:
            # Composite non-date key (click_behavior_daily: page_path + click_target_hash).
            tuple_placeholders = ", ".join(
                "(" + ", ".join(["%s"] * (len(key_columns) - 1)) + ")" for _ in computed_keys
            )
            sql = (
                f"DELETE FROM {table_name} WHERE summary_date = %s "
                f"AND ({key_cols_sql}) NOT IN ({tuple_placeholders})"
            )
            params: List[Any] = [target_date]
            for key_tuple in computed_keys:
                params.extend(key_tuple)
            cursor.execute(sql, params)

        deleted_total += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    connection.commit()
    return upserted, deleted_total


# ---------------------------
# Rollup 1: intent_daily_summary
# ---------------------------
def _rollup_intent_daily_summary(cursor, connection, dates: List[date]) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        """
        SELECT
            DATE(session_start) AS d,
            COUNT(*) AS sessions,
            COUNT(DISTINCT actor_id) AS unique_actors,
            COALESCE(SUM(event_count), 0) AS total_events,
            COALESCE(SUM(page_view_count), 0) AS page_views,
            COALESCE(SUM(product_view_count), 0) AS product_views,
            COALESCE(SUM(click_count), 0) AS clicks,
            COALESCE(SUM(useful_click_count), 0) AS useful_clicks,
            COALESCE(SUM(dead_click_count), 0) AS dead_clicks,
            COALESCE(SUM(add_to_cart_count), 0) AS add_to_carts,
            COALESCE(SUM(checkout_started_count), 0) AS checkout_starts,
            COALESCE(SUM(scroll_count), 0) AS scroll_events,
            AVG(session_time_spent_ms) AS avg_session_time_ms,
            AVG(event_count) AS avg_events_per_session,
            AVG(product_view_count) AS avg_product_views_per_session,
            AVG(click_count) AS avg_clicks_per_session
        FROM intent_sessions
        WHERE session_start >= %s AND session_start < %s
        GROUP BY DATE(session_start)
        """,
        (range_start, range_end),
    )
    agg_rows = cursor.fetchall()
    scanned = sum(r["sessions"] for r in agg_rows) if agg_rows else 0

    by_date = {r["d"]: r for r in agg_rows}
    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {}

    for target_date in dates:
        r = by_date.get(target_date)
        if r is None:
            computed_keys_by_date[target_date] = set()
            continue
        useful_click_rate = _safe_div(r["useful_clicks"], r["clicks"])
        dead_click_rate = _safe_div(r["dead_clicks"], r["clicks"])
        rows.append((
            target_date, r["sessions"], r["unique_actors"], r["total_events"],
            r["page_views"], r["product_views"], r["clicks"], r["useful_clicks"],
            r["dead_clicks"], r["add_to_carts"], r["checkout_starts"], r["scroll_events"],
            r["avg_session_time_ms"], r["avg_events_per_session"],
            r["avg_product_views_per_session"], r["avg_clicks_per_session"],
            useful_click_rate, dead_click_rate,
        ))
        computed_keys_by_date[target_date] = {(target_date,)}

    insert_sql = """
        INSERT INTO intent_daily_summary (
            summary_date, sessions, unique_actors, total_events, page_views,
            product_views, clicks, useful_clicks, dead_clicks, add_to_carts,
            checkout_starts, scroll_events, avg_session_time_ms,
            avg_events_per_session, avg_product_views_per_session,
            avg_clicks_per_session, useful_click_rate, dead_click_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            sessions = VALUES(sessions),
            unique_actors = VALUES(unique_actors),
            total_events = VALUES(total_events),
            page_views = VALUES(page_views),
            product_views = VALUES(product_views),
            clicks = VALUES(clicks),
            useful_clicks = VALUES(useful_clicks),
            dead_clicks = VALUES(dead_clicks),
            add_to_carts = VALUES(add_to_carts),
            checkout_starts = VALUES(checkout_starts),
            scroll_events = VALUES(scroll_events),
            avg_session_time_ms = VALUES(avg_session_time_ms),
            avg_events_per_session = VALUES(avg_events_per_session),
            avg_product_views_per_session = VALUES(avg_product_views_per_session),
            avg_clicks_per_session = VALUES(avg_clicks_per_session),
            useful_click_rate = VALUES(useful_click_rate),
            dead_click_rate = VALUES(dead_click_rate)
    """
    upserted, deleted = _upsert_rollup_rows(
        cursor, connection, "intent_daily_summary", insert_sql, rows,
        ["summary_date"], dates, computed_keys_by_date,
    )

    duration = time.monotonic() - started_at
    logger.info(
        "[rollup intent_daily_summary] range=%s..%s duration=%.2fs rows_scanned=%s "
        "rows_generated=%s rows_upserted=%s rows_deleted_stale=%s",
        range_start, range_end, duration, scanned, len(rows), upserted, deleted,
    )
    return {"scanned": scanned, "generated": len(rows), "upserted": upserted, "deleted": deleted}


# ---------------------------
# Rollup 2: page_behavior_daily
# ---------------------------
def _rollup_page_behavior_daily(cursor, connection, dates: List[date]) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        """
        SELECT
            DATE(occurred_at) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS raw_path,
            COUNT(*) AS page_views,
            COUNT(DISTINCT actor_id) AS unique_actors,
            COUNT(DISTINCT session_id) AS unique_sessions,
            AVG(scroll_percent) AS avg_scroll_percent,
            MAX(scroll_percent) AS max_scroll_percent,
            SUM(event_name = 'product_added_to_cart') AS add_to_cart_events,
            SUM(event_name = 'checkout_started') AS checkout_started_events
        FROM behavioral_events
        WHERE occurred_at >= %s AND occurred_at < %s AND url IS NOT NULL AND url <> ''
        GROUP BY DATE(occurred_at), raw_path
        """,
        (range_start, range_end),
    )
    behavioral_rows = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            DATE(occurred_at) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS raw_path,
            COUNT(*) AS clicks,
            SUM(click_bucket = 'useful_click') AS useful_clicks,
            SUM(click_bucket = 'dead_click') AS dead_clicks
        FROM click_events
        WHERE occurred_at >= %s AND occurred_at < %s
        GROUP BY DATE(occurred_at), raw_path
        """,
        (range_start, range_end),
    )
    click_rows = cursor.fetchall()

    scanned = sum(r["page_views"] for r in behavioral_rows) + sum(r["clicks"] for r in click_rows)

    # merged[(date, normalized_path)] -> metrics dict
    merged: Dict[Tuple[date, str], Dict[str, Any]] = {}

    def _bucket(target_date: date, normalized_path: str) -> Dict[str, Any]:
        key = (target_date, normalized_path)
        if key not in merged:
            merged[key] = {
                "page_views": 0, "unique_actors": 0, "unique_sessions": 0,
                "avg_scroll_sum": 0.0, "avg_scroll_n": 0, "max_scroll_percent": None,
                "add_to_cart_events": 0, "checkout_started_events": 0,
                "clicks": 0, "useful_clicks": 0, "dead_clicks": 0,
            }
        return merged[key]

    for r in behavioral_rows:
        norm_path = _normalize_page_path(r["raw_path"])
        b = _bucket(r["d"], norm_path)
        b["page_views"] += r["page_views"]
        b["unique_actors"] += r["unique_actors"]
        b["unique_sessions"] += r["unique_sessions"]
        if r["avg_scroll_percent"] is not None:
            b["avg_scroll_sum"] += float(r["avg_scroll_percent"]) * r["page_views"]
            b["avg_scroll_n"] += r["page_views"]
        if r["max_scroll_percent"] is not None:
            b["max_scroll_percent"] = max(
                b["max_scroll_percent"] or 0, float(r["max_scroll_percent"])
            )
        b["add_to_cart_events"] += int(r["add_to_cart_events"] or 0)
        b["checkout_started_events"] += int(r["checkout_started_events"] or 0)

    for r in click_rows:
        norm_path = _normalize_page_path(r["raw_path"])
        b = _bucket(r["d"], norm_path)
        b["clicks"] += r["clicks"]
        b["useful_clicks"] += int(r["useful_clicks"] or 0)
        b["dead_clicks"] += int(r["dead_clicks"] or 0)

    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {d: set() for d in dates}

    for (target_date, page_path), b in merged.items():
        avg_scroll = (
            round(b["avg_scroll_sum"] / b["avg_scroll_n"], 2) if b["avg_scroll_n"] else None
        )
        dead_click_rate = _safe_div(b["dead_clicks"], b["clicks"])
        rows.append((
            target_date, page_path, b["page_views"], b["unique_actors"], b["unique_sessions"],
            b["clicks"], b["useful_clicks"], b["dead_clicks"], dead_click_rate,
            avg_scroll, b["max_scroll_percent"], b["add_to_cart_events"], b["checkout_started_events"],
        ))
        computed_keys_by_date[target_date].add((page_path,))

    insert_sql = """
        INSERT INTO page_behavior_daily (
            summary_date, page_path, page_views, unique_actors, unique_sessions,
            clicks, useful_clicks, dead_clicks, dead_click_rate, avg_scroll_percent,
            max_scroll_percent, add_to_cart_events, checkout_started_events
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            page_views = VALUES(page_views),
            unique_actors = VALUES(unique_actors),
            unique_sessions = VALUES(unique_sessions),
            clicks = VALUES(clicks),
            useful_clicks = VALUES(useful_clicks),
            dead_clicks = VALUES(dead_clicks),
            dead_click_rate = VALUES(dead_click_rate),
            avg_scroll_percent = VALUES(avg_scroll_percent),
            max_scroll_percent = VALUES(max_scroll_percent),
            add_to_cart_events = VALUES(add_to_cart_events),
            checkout_started_events = VALUES(checkout_started_events)
    """
    upserted, deleted = _upsert_rollup_rows(
        cursor, connection, "page_behavior_daily", insert_sql, rows,
        ["summary_date", "page_path"], dates, computed_keys_by_date,
    )

    duration = time.monotonic() - started_at
    logger.info(
        "[rollup page_behavior_daily] range=%s..%s duration=%.2fs rows_scanned=%s "
        "rows_generated=%s rows_upserted=%s rows_deleted_stale=%s",
        range_start, range_end, duration, scanned, len(rows), upserted, deleted,
    )
    return {"scanned": scanned, "generated": len(rows), "upserted": upserted, "deleted": deleted}


# ---------------------------
# Rollup 3: click_behavior_daily
# ---------------------------
def _rollup_click_behavior_daily(cursor, connection, dates: List[date]) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        """
        SELECT
            DATE(occurred_at) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS raw_path,
            click_tag, click_element_id, click_element_name, click_element_type, click_href,
            COUNT(*) AS total_clicks,
            SUM(click_bucket = 'useful_click') AS useful_clicks,
            SUM(click_bucket = 'dead_click') AS dead_clicks
        FROM click_events
        WHERE occurred_at >= %s AND occurred_at < %s
        GROUP BY DATE(occurred_at), raw_path, click_tag, click_element_id,
                 click_element_name, click_element_type, click_href
        """,
        (range_start, range_end),
    )
    grouped_rows = cursor.fetchall()
    scanned = sum(r["total_clicks"] for r in grouped_rows) if grouped_rows else 0

    merged: Dict[Tuple[date, str, str], Dict[str, Any]] = {}

    for r in grouped_rows:
        norm_path = _normalize_page_path(r["raw_path"])
        tag, eid, ename, etype, href, target_hash = _normalize_click_target(
            r["click_tag"], r["click_element_id"], r["click_element_name"],
            r["click_element_type"], r["click_href"],
        )
        key = (r["d"], norm_path, target_hash)
        if key not in merged:
            merged[key] = {
                "tag_name": tag, "element_id": eid, "element_name": ename,
                "element_type": etype, "href": href,
                "total_clicks": 0, "useful_clicks": 0, "dead_clicks": 0,
            }
        b = merged[key]
        b["total_clicks"] += r["total_clicks"]
        b["useful_clicks"] += int(r["useful_clicks"] or 0)
        b["dead_clicks"] += int(r["dead_clicks"] or 0)

    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {d: set() for d in dates}

    for (target_date, page_path, target_hash), b in merged.items():
        useful_click_rate = _safe_div(b["useful_clicks"], b["total_clicks"])
        dead_click_rate = _safe_div(b["dead_clicks"], b["total_clicks"])
        rows.append((
            target_date, page_path, target_hash, b["tag_name"], b["element_id"],
            b["element_name"], b["element_type"], b["href"],
            b["total_clicks"], b["useful_clicks"], b["dead_clicks"],
            useful_click_rate, dead_click_rate,
        ))
        computed_keys_by_date[target_date].add((page_path, target_hash))

    insert_sql = """
        INSERT INTO click_behavior_daily (
            summary_date, page_path, click_target_hash, tag_name, element_id,
            element_name, element_type, href, total_clicks, useful_clicks,
            dead_clicks, useful_click_rate, dead_click_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            tag_name = VALUES(tag_name),
            element_id = VALUES(element_id),
            element_name = VALUES(element_name),
            element_type = VALUES(element_type),
            href = VALUES(href),
            total_clicks = VALUES(total_clicks),
            useful_clicks = VALUES(useful_clicks),
            dead_clicks = VALUES(dead_clicks),
            useful_click_rate = VALUES(useful_click_rate),
            dead_click_rate = VALUES(dead_click_rate)
    """
    upserted, deleted = _upsert_rollup_rows(
        cursor, connection, "click_behavior_daily", insert_sql, rows,
        ["summary_date", "page_path", "click_target_hash"], dates, computed_keys_by_date,
    )

    duration = time.monotonic() - started_at
    logger.info(
        "[rollup click_behavior_daily] range=%s..%s duration=%.2fs rows_scanned=%s "
        "rows_generated=%s rows_upserted=%s rows_deleted_stale=%s",
        range_start, range_end, duration, scanned, len(rows), upserted, deleted,
    )
    return {"scanned": scanned, "generated": len(rows), "upserted": upserted, "deleted": deleted}


# ---------------------------
# Rollup 4: product_behavior_daily
# ---------------------------
def _rollup_product_behavior_daily(cursor, connection, dates: List[date]) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        """
        SELECT
            DATE(occurred_at) AS d,
            product_id,
            ANY_VALUE(product_title) AS product_title,
            SUM(event_name = 'product_viewed') AS product_views,
            COUNT(DISTINCT CASE WHEN event_name = 'product_viewed' THEN actor_id END) AS unique_viewers,
            COUNT(DISTINCT CASE WHEN event_name = 'product_viewed' THEN session_id END) AS unique_sessions,
            SUM(event_name = 'product_added_to_cart') AS add_to_cart_count,
            SUM(event_name = 'checkout_started') AS checkout_started_count
        FROM behavioral_events
        WHERE occurred_at >= %s AND occurred_at < %s
              AND product_id IS NOT NULL AND product_id <> ''
        GROUP BY DATE(occurred_at), product_id
        """,
        (range_start, range_end),
    )
    agg_rows = cursor.fetchall()
    scanned = sum(r["product_views"] for r in agg_rows) if agg_rows else 0

    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {d: set() for d in dates}

    for r in agg_rows:
        product_views = int(r["product_views"] or 0)
        view_to_atc_rate = _safe_div(r["add_to_cart_count"], product_views)
        view_to_checkout_rate = _safe_div(r["checkout_started_count"], product_views)
        unique_sessions = int(r["unique_sessions"] or 0)
        rows.append((
            r["d"], r["product_id"], r["product_title"], product_views,
            r["unique_viewers"], unique_sessions, unique_sessions,
            r["add_to_cart_count"], r["checkout_started_count"],
            view_to_atc_rate, view_to_checkout_rate,
        ))
        computed_keys_by_date[r["d"]].add((r["product_id"],))

    insert_sql = """
        INSERT INTO product_behavior_daily (
            summary_date, product_id, product_title, product_views, unique_viewers,
            unique_sessions, sessions_with_product_view, add_to_cart_count,
            checkout_started_count, view_to_atc_rate, view_to_checkout_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            product_title = VALUES(product_title),
            product_views = VALUES(product_views),
            unique_viewers = VALUES(unique_viewers),
            unique_sessions = VALUES(unique_sessions),
            sessions_with_product_view = VALUES(sessions_with_product_view),
            add_to_cart_count = VALUES(add_to_cart_count),
            checkout_started_count = VALUES(checkout_started_count),
            view_to_atc_rate = VALUES(view_to_atc_rate),
            view_to_checkout_rate = VALUES(view_to_checkout_rate)
    """
    upserted, deleted = _upsert_rollup_rows(
        cursor, connection, "product_behavior_daily", insert_sql, rows,
        ["summary_date", "product_id"], dates, computed_keys_by_date,
    )

    duration = time.monotonic() - started_at
    logger.info(
        "[rollup product_behavior_daily] range=%s..%s duration=%.2fs rows_scanned=%s "
        "rows_generated=%s rows_upserted=%s rows_deleted_stale=%s",
        range_start, range_end, duration, scanned, len(rows), upserted, deleted,
    )
    return {"scanned": scanned, "generated": len(rows), "upserted": upserted, "deleted": deleted}


# ---------------------------
# Rollup 5: behavioral_path_daily
# ---------------------------
def _rollup_behavioral_path_daily(cursor, connection, dates: List[date]) -> Dict[str, int]:
    started_at = time.monotonic()
    total_scanned = 0
    total_generated = 0
    total_upserted = 0
    total_deleted = 0
    range_start, range_end = _window_bounds(dates)

    for target_date in dates:
        day_start, day_end = _date_range_bounds(target_date)
        cursor.execute(
            """
            SELECT session_id, actor_id, event_sequence, add_to_cart_count, checkout_started_count
            FROM intent_sessions
            WHERE session_start >= %s AND session_start < %s
            LIMIT %s
            """,
            (day_start, day_end, ROLLUP_MAX_SESSIONS_PER_DATE),
        )
        session_rows = cursor.fetchall()
        total_scanned += len(session_rows)
        if len(session_rows) >= ROLLUP_MAX_SESSIONS_PER_DATE:
            logger.warning(
                "[rollup behavioral_path_daily] date=%s hit ROLLUP_MAX_SESSIONS_PER_DATE=%s cap",
                target_date, ROLLUP_MAX_SESSIONS_PER_DATE,
            )

        buckets: Dict[str, Dict[str, Any]] = {}
        for row in session_rows:
            sequence_hash, summary, _step_count = _compute_sequence_path(row["event_sequence"])
            b = buckets.setdefault(sequence_hash, {
                "sequence": summary, "session_count": 0, "unique_actors": set(),
                "sessions_with_atc": 0, "sessions_with_checkout": 0,
            })
            b["session_count"] += 1
            if row["actor_id"]:
                b["unique_actors"].add(row["actor_id"])
            if (row["add_to_cart_count"] or 0) > 0:
                b["sessions_with_atc"] += 1
            if (row["checkout_started_count"] or 0) > 0:
                b["sessions_with_checkout"] += 1

        rows: List[Tuple] = []
        computed_keys: Set[Tuple] = set()
        for sequence_hash, b in buckets.items():
            conversion_to_atc = _safe_div(b["sessions_with_atc"], b["session_count"])
            conversion_to_checkout = _safe_div(b["sessions_with_checkout"], b["session_count"])
            rows.append((
                target_date, sequence_hash, b["sequence"], b["session_count"],
                len(b["unique_actors"]), b["sessions_with_atc"], b["sessions_with_checkout"],
                conversion_to_atc, conversion_to_checkout,
            ))
            computed_keys.add((sequence_hash,))

        insert_sql = """
            INSERT INTO behavioral_path_daily (
                summary_date, sequence_hash, sequence, session_count, unique_actor_count,
                sessions_with_atc, sessions_with_checkout, conversion_to_atc, conversion_to_checkout
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sequence = VALUES(sequence),
                session_count = VALUES(session_count),
                unique_actor_count = VALUES(unique_actor_count),
                sessions_with_atc = VALUES(sessions_with_atc),
                sessions_with_checkout = VALUES(sessions_with_checkout),
                conversion_to_atc = VALUES(conversion_to_atc),
                conversion_to_checkout = VALUES(conversion_to_checkout)
        """
        upserted, deleted = _upsert_rollup_rows(
            cursor, connection, "behavioral_path_daily", insert_sql, rows,
            ["summary_date", "sequence_hash"], [target_date], {target_date: computed_keys},
        )
        total_generated += len(rows)
        total_upserted += upserted
        total_deleted += deleted

    duration = time.monotonic() - started_at
    logger.info(
        "[rollup behavioral_path_daily] range=%s..%s duration=%.2fs rows_scanned=%s "
        "rows_generated=%s rows_upserted=%s rows_deleted_stale=%s",
        range_start, range_end, duration, total_scanned, total_generated, total_upserted, total_deleted,
    )
    return {
        "scanned": total_scanned, "generated": total_generated,
        "upserted": total_upserted, "deleted": total_deleted,
    }


# ---------------------------
# Orchestration entrypoint
# ---------------------------
_ROLLUP_STAGES = (
    ("intent_daily_summary", _rollup_intent_daily_summary),
    ("page_behavior_daily", _rollup_page_behavior_daily),
    ("click_behavior_daily", _rollup_click_behavior_daily),
    ("product_behavior_daily", _rollup_product_behavior_daily),
    ("behavioral_path_daily", _rollup_behavioral_path_daily),
)


def run_rollups_for_brand(
    brand_index: int,
    brand_label: str,
    cursor=None,
    connection=None,
) -> None:
    """
    Runs all five rollups, in fixed order, over the current affected-date
    window for one brand's database. Opens its own cursor/connection via
    get_db_cursor if not passed in (same convention as
    sync_intent_events_for_brand etc. in pipeline/intent_events.py).

    Each rollup stage is independently try/excepted so one failing stage
    doesn't block the others from running this tick - but the shared
    watermark only advances if ALL FIVE succeed, so a partial failure
    causes the whole window to be retried (safely - recompute-and-overwrite
    makes re-running an already-succeeded stage a harmless no-op).
    """
    def _do_run(c, conn):
        run_started_at = time.monotonic()
        now = datetime.now(IST)

        _ensure_all_rollup_tables(c, conn)

        dates, stored_watermark = _determine_affected_dates(c, now)
        logger.info(
            "[rollup] brand=%s starting: watermark=%s affected_dates=%s (%s..%s)",
            brand_label, stored_watermark, len(dates), dates[0], dates[-1],
        )

        any_failed = False
        for stage_name, stage_fn in _ROLLUP_STAGES:
            try:
                stage_fn(c, conn, dates)
            except Exception as e:
                any_failed = True
                logger.error(
                    "[rollup %s] brand=%s failed: %s", stage_name, brand_label, e
                )

        if any_failed:
            logger.info(
                "[rollup] brand=%s watermark NOT advanced due to a stage failure "
                "(window will be retried next run)", brand_label,
            )
        else:
            update_pipeline_metadata_timestamp(c, conn, ROLLUP_METADATA_KEY, now)
            logger.info(
                "[rollup] brand=%s watermark advanced to %s (duration=%.2fs)",
                brand_label, now, time.monotonic() - run_started_at,
            )

    if cursor is not None:
        _do_run(cursor, connection)
    else:
        with get_db_cursor(brand_index) as (c, conn):
            _do_run(c, conn)
