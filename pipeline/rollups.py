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

and a sixth, heatmap_page_daily (grain: date + page_path + device), behind the dashboard's Heatmaps
"popular pages" list.

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

import ast
import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from pipeline.state import IST, logger
from pipeline.intent_events import _ensure_column_exists
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

CLICK_VALUE_MAX_LEN = 60
_VALUE_EXCLUDED_TAGS = {"input", "textarea", "select"}

PAGE_PATH_MAX_LEN = 255
SEQUENCE_MAX_LEN = 500
SEQUENCE_MAX_STEPS = 15
# Safety valve per day, and the chunk size the path rollup reads sessions in. Every session is stored now
# (one-visit sessions included), so a busy brand has far more than the old 50,000 a day.
ROLLUP_MAX_SESSIONS_PER_DATE = int(os.environ.get("ROLLUP_MAX_SESSIONS_PER_DATE", "300000") or 300000)
ROLLUP_PATH_CHUNK = 20000

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
    # Added after the table first shipped (CREATE TABLE IF NOT EXISTS will not add it to an existing table).
    # Distinct sessions that had any event that day, closed or not: what Shopify and Clarity count as sessions.
    _ensure_column_exists(cursor, connection, "intent_daily_summary", "sessions_started", "INT UNSIGNED NULL")


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
            element_value VARCHAR(100) NULL,

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
    # Added after the table first shipped: CREATE TABLE IF NOT EXISTS alone
    # won't add it to a table that already exists in a brand's database.
    _ensure_column_exists(
        cursor, connection, "click_behavior_daily", "element_value", "VARCHAR(100) NULL"
    )


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


# behavioral_path_daily.first_event: the first event of a path ("page_viewed" in "page_viewed>click"), as its own
# indexed column, so the dashboard's Funnel Exploration can ask "paths that start with X" from an index instead of
# cutting every stored sequence apart with SUBSTRING_INDEX. Added to an existing table in place (a small rollup table,
# so the ALTER is quick) and filled for older rows in bounded batches; every step is best effort: if it cannot be done
# the rollup keeps writing the table without the column and the dashboard keeps using the sequence text.
PATH_FIRST_EVENT_MAX_LEN = 100
PATH_FIRST_EVENT_BACKFILL_BATCH = 5000
PATH_FIRST_EVENT_BACKFILL_MAX_BATCHES = 50   # per process start; the rest is picked up at the next start


def _ensure_index_exists(cursor, connection, table_name: str, index_name: str, columns_sql: str) -> None:
    """Idempotent ADD KEY, the index counterpart of _ensure_column_exists."""
    cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = %s", (index_name,))
    if cursor.fetchone() is not None:
        return
    cursor.execute(f"ALTER TABLE {table_name} ADD KEY {index_name} {columns_sql}")
    connection.commit()


def _backfill_path_first_event(cursor, connection) -> int:
    """Fills first_event for rows that have none yet, a batch at a time (committed each time). Returns rows updated."""
    total = 0
    for _ in range(PATH_FIRST_EVENT_BACKFILL_MAX_BATCHES):
        cursor.execute(
            "UPDATE behavioral_path_daily SET first_event = LEFT(SUBSTRING_INDEX(sequence, '>', 1), %s) "
            "WHERE first_event IS NULL LIMIT %s",
            (PATH_FIRST_EVENT_MAX_LEN, PATH_FIRST_EVENT_BACKFILL_BATCH),
        )
        changed = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        connection.commit()
        total += changed
        if changed < PATH_FIRST_EVENT_BACKFILL_BATCH:
            break
    return total


def _ensure_path_first_event(cursor, connection) -> None:
    try:
        _ensure_column_exists(cursor, connection, "behavioral_path_daily", "first_event", f"VARCHAR({PATH_FIRST_EVENT_MAX_LEN}) NULL")
        _ensure_index_exists(cursor, connection, "behavioral_path_daily", "idx_first_event_date", "(first_event, summary_date)")
        filled = _backfill_path_first_event(cursor, connection)
        if filled:
            logger.info("[rollup behavioral_path_daily] first_event filled for %s existing row(s)", filled)
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            pass
        logger.warning("[rollup behavioral_path_daily] first_event column not set up (%s: %s); paths keep using the "
                       "sequence text", type(exc).__name__, str(exc)[:160])


def _path_first_event_present(cursor) -> bool:
    try:
        cursor.execute("SHOW COLUMNS FROM behavioral_path_daily LIKE %s", ("first_event",))
        return cursor.fetchone() is not None
    except Exception:
        return False


def _first_event_of(sequence: str) -> str:
    return str(sequence or "").split(">", 1)[0][:PATH_FIRST_EVENT_MAX_LEN]


def _ensure_heatmap_page_daily_table(cursor, connection) -> None:
    # Same definition as analytics/scripts/heatmap-pages-rollup.cjs (which stays for back-filling old days).
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS heatmap_page_daily (
            summary_date DATE NOT NULL,
            page_path VARCHAR(255) NOT NULL,
            device_type VARCHAR(10) NOT NULL,
            page_views INT UNSIGNED NOT NULL DEFAULT 0,
            sessions INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            PRIMARY KEY (summary_date, page_path, device_type),
            KEY idx_page_date (page_path, summary_date)
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
    _ensure_path_first_event(cursor, connection)
    _ensure_heatmap_page_daily_table(cursor, connection)


# ---------------------------
# Where the event rollups read from
# ---------------------------
@dataclass(frozen=True)
class _Source:
    """The tables the page, click and product rollups read. DIRECT is the real tables. A TEMP source is a
    one-pass copy of the affected window (same column names, only the columns the rollups use), so the
    five queries that used to scan behavioral_events / click_events each scan a small private table instead."""
    events: str
    clicks: str
    temp: bool


_DIRECT = _Source("behavioral_events", "click_events", False)
_TEMP_EVENTS, _TEMP_CLICKS = "tmp_rollup_events", "tmp_rollup_clicks"
_temp_unavailable = False   # set when MySQL refuses temporary tables (privilege, GTID rules); then read directly
_TEMP_REFUSED_ERRNOS = {1044, 1142, 1227, 1785, 1786, 1787}


def _src(sql: str, source: Optional[_Source]) -> str:
    """The statement unchanged, or with its event tables swapped for the temp copies."""
    if source is None or not source.temp:
        return sql
    return sql.replace("FROM behavioral_events", f"FROM {source.events}").replace("FROM click_events", f"FROM {source.clicks}")


def _set_autocommit(connection, value: bool) -> None:
    try:
        connection.autocommit = value
    except Exception:
        pass


def _prepare_source(cursor, connection, dates: List[date]) -> _Source:
    """INTENT_ROLLUP_SOURCE=direct reads the real tables as before; auto (default) copies the window once into
    temporary tables. The copy is made outside a transaction (autocommit on) because MySQL with GTIDs refuses
    CREATE/DROP TEMPORARY TABLE inside one. Any failure means 'read directly', never a failed run."""
    global _temp_unavailable
    if os.environ.get("INTENT_ROLLUP_SOURCE", "auto").strip().lower() == "direct" or _temp_unavailable:
        return _DIRECT
    start, end = _window_bounds(dates)
    try:
        connection.commit()
        _set_autocommit(connection, True)
        try:
            cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {_TEMP_EVENTS}")
            cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {_TEMP_CLICKS}")
            cursor.execute(
                f"CREATE TEMPORARY TABLE {_TEMP_EVENTS} AS "
                "SELECT occurred_at, url, actor_id, session_id, event_name, scroll_percent, product_id, product_title "
                "FROM behavioral_events WHERE occurred_at >= %s AND occurred_at < %s", (start, end))
            cursor.execute(
                f"CREATE TEMPORARY TABLE {_TEMP_CLICKS} AS "
                "SELECT occurred_at, url, click_tag, click_element_id, click_element_name, click_element_type, "
                "click_href, click_element_value, click_bucket "
                "FROM click_events WHERE occurred_at >= %s AND occurred_at < %s", (start, end))
        finally:
            _set_autocommit(connection, False)
        return _Source(_TEMP_EVENTS, _TEMP_CLICKS, True)
    except Exception as exc:
        if getattr(exc, "errno", None) in _TEMP_REFUSED_ERRNOS:
            _temp_unavailable = True
        logger.warning("[rollup] temporary-table copy not used (%s: %s)%s; reading the tables directly",
                       type(exc).__name__, str(exc)[:160],
                       " - disabled for this process" if _temp_unavailable else "")
        _release_source(cursor, connection, _Source(_TEMP_EVENTS, _TEMP_CLICKS, True))
        return _DIRECT


def _release_source(cursor, connection, source: _Source) -> None:
    if not source.temp:
        return
    try:
        _set_autocommit(connection, True)
        cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {source.events}")
        cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {source.clicks}")
    except Exception as exc:  # the tables vanish with the connection anyway
        logger.debug("[rollup] could not drop temporary tables: %s", exc)
    finally:
        _set_autocommit(connection, False)


def _run_stage(stage_name, stage_fn, cursor, connection, dates, source: _Source, brand_label: str) -> bool:
    """Runs one rollup stage. If it fails while reading the temporary copies, it is retried once against the
    real tables, which is what it always did. Returns whether the stage succeeded."""
    try:
        stage_fn(cursor, connection, dates, source)
        return True
    except Exception as exc:
        failure = exc
        if source.temp:
            logger.warning("[rollup %s] brand=%s failed on the temporary copy (%s); retrying on the real tables",
                           stage_name, brand_label, exc)
            try:
                connection.rollback()
            except Exception:
                pass
            try:
                stage_fn(cursor, connection, dates, _DIRECT)
                return True
            except Exception as retry_exc:
                failure = retry_exc
        logger.error("[rollup %s] brand=%s failed: %s", stage_name, brand_label, failure)
        return False


# The five CREATE TABLE IF NOT EXISTS checks are idempotent and never change, so they run once per brand per
# process (a restart re-checks), the same as intent_engine's scoring schema.
_schema_lock = threading.Lock()
_schema_ensured_brands: Set[int] = set()

# Skip-when-unchanged. Every run recomputes whole days, so a brand that received nothing since the last full run
# would read and rewrite identical numbers. The cheap test is the newest row id of the three source tables
# (MAX(id) on an AUTO_INCREMENT primary key reads one index entry): ids only grow, a burned id from a duplicate
# insert is not a row, and a new event, click or closed session always raises its table's maximum. The signature
# of the last fully successful run is remembered per brand in this process only, so a restart always runs once.
_last_run: Dict[int, Tuple[Tuple[date, ...], Tuple]] = {}
_SIGNATURE_SQL = (
    "SELECT (SELECT MAX(id) FROM behavioral_events) AS events, "
    "(SELECT MAX(id) FROM click_events) AS clicks, "
    "(SELECT MAX(id) FROM intent_sessions) AS sessions"
)


def _skip_unchanged_enabled() -> bool:
    return os.environ.get("INTENT_ROLLUP_SKIP_UNCHANGED", "on").strip().lower() not in ("off", "false", "0", "no")


def _change_signature(cursor) -> Optional[Tuple]:
    """Newest event, click and session row ids, or None when they cannot be read (then the run is never skipped)."""
    try:
        cursor.execute(_SIGNATURE_SQL)
        rows = cursor.fetchall()
        if not rows:
            return None
        row = rows[0]
        return tuple(row[k] for k in ("events", "clicks", "sessions")) if isinstance(row, dict) else tuple(row)
    except Exception as exc:
        logger.debug("[rollup] change signature unavailable (%s); running in full", exc)
        return None


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


def _normalize_product_id(raw: Any) -> str:
    """
    Canonical product id string: "10114912190759.0" and "10114912190759"
    (cart lists serialize ids as floats) both become "10114912190759".
    """
    pid = str(raw).strip().strip("'\"") if raw is not None else ""
    if pid.endswith(".0"):
        pid = pid[:-2]
    return pid


def _parse_cart_products(
    product_id_raw: Optional[str], product_title_raw: Optional[str]
) -> List[Tuple[str, Optional[str]]]:
    """
    checkout_started events store the whole cart as a list-shaped string
    (e.g. "[10114912190759.0, 9863072350503.0]", titles likewise) because
    ingestion stringifies non-scalar payload values. Returns the unique
    (product_id, title) pairs in that cart, or [] if it can't be parsed.

    A value not ending in "]" was truncated by the raw column width, so its
    last token may be a partial id and is dropped rather than mis-attributed.
    """
    text_value = (product_id_raw or "").strip()
    if not text_value.startswith("["):
        return []
    truncated = not text_value.endswith("]")
    inner = text_value.strip("[]")
    tokens = [t.strip() for t in inner.split(",") if t.strip()]
    if truncated and tokens:
        tokens = tokens[:-1]

    ids = [_normalize_product_id(t) for t in tokens]
    ids = [i for i in ids if i.isdigit()]

    titles: List[Optional[str]] = []
    try:
        parsed_titles = ast.literal_eval((product_title_raw or "").strip())
        if isinstance(parsed_titles, (list, tuple)):
            titles = [str(t) for t in parsed_titles]
    except (ValueError, SyntaxError):
        titles = []

    result: List[Tuple[str, Optional[str]]] = []
    seen: Set[str] = set()
    for idx, pid in enumerate(ids):
        if pid in seen:
            continue
        seen.add(pid)
        title = titles[idx] if len(titles) == len(ids) else None
        result.append((pid, title))
    return result


def _normalize_click_target(
    tag_name: Optional[str],
    element_id: Optional[str],
    element_name: Optional[str],
    element_type: Optional[str],
    href: Optional[str],
    element_value: Optional[str] = None,
) -> Tuple[str, str, str, str, str, str, str]:
    """
    Reusable click-target normalization. Returns
    (tag_name, element_id, element_name, element_type, normalized_href,
     element_value, click_target_hash).
    The hash (MD5 - display-grouping only, not business-metric identity) is
    what bounds cardinality in click_behavior_daily's UNIQUE KEY; the raw-ish
    fields are kept for dashboard readability.

    element_value (the clicked element's text/value) is what separates
    otherwise-anonymous elements (e.g. many different <div>s with no id/name)
    into distinct targets. It is whitespace-collapsed, lowercased and capped,
    and is deliberately ignored for input/textarea/select tags, whose value
    can be user-typed text.
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

    value = ""
    if tag not in _VALUE_EXCLUDED_TAGS and element_value:
        value = " ".join(str(element_value).split()).lower()[:CLICK_VALUE_MAX_LEN]

    key_material = "|".join([tag, eid, ename, etype, href_path, value])
    click_target_hash = hashlib.md5(key_material.encode("utf-8")).hexdigest()

    return (tag, eid, ename, etype, href_path, value, click_target_hash)


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
        if len(key_columns) == 1:
            # Grain is the date alone (intent_daily_summary): the upsert's
            # UNIQUE KEY(summary_date) already fully owns the single row for
            # this date, so there is no "extra" stale row a NOT IN cleanup
            # could ever remove, and an empty computed set here is a
            # legitimate zero-session day, not a bug. Skip the DELETE
            # entirely, no warning.
            continue

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


def _open_sessions_by_date(cursor, range_start, range_end) -> Optional[Dict[date, int]]:
    """Open (not yet closed) sessions that started in [range_start, range_end), by day: one per visitor, from
    intent_actor_cursors. The read is a range on last_event_at (migration 002's index; a session that started in the
    window has a last event inside it or later), then session_start picks the sessions that began in the window.
    None when the table cannot be read, in which case only the closed sessions are counted and the run says so."""
    try:
        cursor.execute(
            """
            SELECT DATE(session_start) AS d, COUNT(*) AS open_sessions
            FROM intent_actor_cursors
            WHERE session_id <> '' AND last_event_at >= %s AND session_start >= %s AND session_start < %s
            GROUP BY DATE(session_start)
            """,
            (range_start, range_start, range_end),
        )
        return {r["d"]: int(r["open_sessions"] or 0) for r in cursor.fetchall()}
    except Exception as exc:
        logger.warning("[rollup intent_daily_summary] open sessions not counted (%s: %s); sessions_started is the "
                       "closed sessions only", type(exc).__name__, str(exc)[:160])
        return None


# ---------------------------
# Rollup 1: intent_daily_summary
# ---------------------------
def _rollup_intent_daily_summary(cursor, connection, dates: List[date], source: Optional[_Source] = None) -> Dict[str, int]:
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

    # Sessions that started that day, by the one canonical definition: a session is a row of intent_sessions
    # (closed, or closed by the idle-session sweep) or, until it closes, the visitor's open session record in
    # intent_actor_cursors. So the count is the closed rows above plus the open ones, and it reconciles with every
    # other session figure built on those two tables (the dashboard's Total Sessions and the intent distribution).
    # It used to be COUNT(DISTINCT session_id) over behavioral_events, a third population that misses sessions whose
    # events are all clicks (they have no row there) and that cost a full pass over the day's events.
    closed_by_date = {r["d"]: int(r["sessions"] or 0) for r in agg_rows}
    open_by_date = _open_sessions_by_date(cursor, range_start, range_end)
    started_by_date: Dict[date, int] = {}
    for target_date in dates:
        total = closed_by_date.get(target_date, 0) + (open_by_date or {}).get(target_date, 0)
        if total:
            started_by_date[target_date] = total

    by_date = {r["d"]: r for r in agg_rows}
    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {}

    for target_date in dates:
        r = by_date.get(target_date)
        started = started_by_date.get(target_date)
        if r is None and started is None:
            computed_keys_by_date[target_date] = set()
            continue
        if r is None:
            # Events exist but no session has closed yet (typical for today): keep the started count.
            r = {k: 0 for k in ("sessions", "unique_actors", "total_events", "page_views", "product_views", "clicks",
                                "useful_clicks", "dead_clicks", "add_to_carts", "checkout_starts", "scroll_events")}
            r.update(avg_session_time_ms=None, avg_events_per_session=None,
                     avg_product_views_per_session=None, avg_clicks_per_session=None)
        useful_click_rate = _safe_div(r["useful_clicks"], r["clicks"])
        dead_click_rate = _safe_div(r["dead_clicks"], r["clicks"])
        rows.append((
            target_date, r["sessions"], r["unique_actors"], r["total_events"],
            r["page_views"], r["product_views"], r["clicks"], r["useful_clicks"],
            r["dead_clicks"], r["add_to_carts"], r["checkout_starts"], r["scroll_events"],
            r["avg_session_time_ms"], r["avg_events_per_session"],
            r["avg_product_views_per_session"], r["avg_clicks_per_session"],
            useful_click_rate, dead_click_rate, started,
        ))
        computed_keys_by_date[target_date] = {(target_date,)}

    insert_sql = """
        INSERT INTO intent_daily_summary (
            summary_date, sessions, unique_actors, total_events, page_views,
            product_views, clicks, useful_clicks, dead_clicks, add_to_carts,
            checkout_starts, scroll_events, avg_session_time_ms,
            avg_events_per_session, avg_product_views_per_session,
            avg_clicks_per_session, useful_click_rate, dead_click_rate, sessions_started
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            dead_click_rate = VALUES(dead_click_rate),
            sessions_started = VALUES(sessions_started)
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
def _rollup_page_behavior_daily(cursor, connection, dates: List[date], source: Optional[_Source] = None) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        _src("""
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
        """, source),
        (range_start, range_end),
    )
    behavioral_rows = cursor.fetchall()

    cursor.execute(
        _src("""
        SELECT
            DATE(occurred_at) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS raw_path,
            COUNT(*) AS clicks,
            SUM(click_bucket = 'useful_click') AS useful_clicks,
            SUM(click_bucket = 'dead_click') AS dead_clicks
        FROM click_events
        WHERE occurred_at >= %s AND occurred_at < %s
        GROUP BY DATE(occurred_at), raw_path
        """, source),
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
def _rollup_click_behavior_daily(cursor, connection, dates: List[date], source: Optional[_Source] = None) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        _src("""
        SELECT
            DATE(occurred_at) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS raw_path,
            click_tag, click_element_id, click_element_name, click_element_type, click_href,
            LEFT(click_element_value, 200) AS element_value,
            COUNT(*) AS total_clicks,
            SUM(click_bucket = 'useful_click') AS useful_clicks,
            SUM(click_bucket = 'dead_click') AS dead_clicks
        FROM click_events
        WHERE occurred_at >= %s AND occurred_at < %s
        GROUP BY DATE(occurred_at), raw_path, click_tag, click_element_id,
                 click_element_name, click_element_type, click_href, element_value
        """, source),
        (range_start, range_end),
    )
    grouped_rows = cursor.fetchall()
    scanned = sum(r["total_clicks"] for r in grouped_rows) if grouped_rows else 0

    merged: Dict[Tuple[date, str, str], Dict[str, Any]] = {}

    for r in grouped_rows:
        norm_path = _normalize_page_path(r["raw_path"])
        tag, eid, ename, etype, href, value, target_hash = _normalize_click_target(
            r["click_tag"], r["click_element_id"], r["click_element_name"],
            r["click_element_type"], r["click_href"], r["element_value"],
        )
        key = (r["d"], norm_path, target_hash)
        if key not in merged:
            merged[key] = {
                "tag_name": tag, "element_id": eid, "element_name": ename,
                "element_type": etype, "href": href, "element_value": value,
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
            b["element_name"], b["element_type"], b["href"], b["element_value"] or None,
            b["total_clicks"], b["useful_clicks"], b["dead_clicks"],
            useful_click_rate, dead_click_rate,
        ))
        computed_keys_by_date[target_date].add((page_path, target_hash))

    insert_sql = """
        INSERT INTO click_behavior_daily (
            summary_date, page_path, click_target_hash, tag_name, element_id,
            element_name, element_type, href, element_value, total_clicks, useful_clicks,
            dead_clicks, useful_click_rate, dead_click_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            tag_name = VALUES(tag_name),
            element_id = VALUES(element_id),
            element_name = VALUES(element_name),
            element_type = VALUES(element_type),
            href = VALUES(href),
            element_value = VALUES(element_value),
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
def _rollup_product_behavior_daily(cursor, connection, dates: List[date], source: Optional[_Source] = None) -> Dict[str, int]:
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    # Single-product events (product_viewed, add-to-cart): product_id is one
    # plain ID. checkout_started events instead carry the WHOLE CART as a
    # list-shaped string ("[123.0, 456.0]"), handled separately below.
    cursor.execute(
        _src("""
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
              AND LEFT(product_id, 1) <> '['
        GROUP BY DATE(occurred_at), product_id
        """, source),
        (range_start, range_end),
    )
    agg_rows = cursor.fetchall()

    cursor.execute(
        _src("""
        SELECT
            DATE(occurred_at) AS d,
            product_id,
            ANY_VALUE(product_title) AS product_title,
            COUNT(*) AS checkouts
        FROM behavioral_events
        WHERE occurred_at >= %s AND occurred_at < %s
              AND event_name = 'checkout_started'
              AND LEFT(product_id, 1) = '['
        GROUP BY DATE(occurred_at), product_id
        """, source),
        (range_start, range_end),
    )
    cart_rows = cursor.fetchall()
    scanned = (
        sum(int(r["product_views"] or 0) for r in agg_rows)
        + sum(r["checkouts"] for r in cart_rows)
    )

    merged: Dict[Tuple[date, str], Dict[str, Any]] = {}

    def _product_bucket(target_date: date, product_id: str) -> Dict[str, Any]:
        key = (target_date, product_id)
        if key not in merged:
            merged[key] = {
                "title": None, "views": 0, "viewers": 0, "sessions": 0,
                "atc": 0, "checkouts": 0,
            }
        return merged[key]

    for r in agg_rows:
        pid = _normalize_product_id(r["product_id"])
        if not pid:
            continue
        b = _product_bucket(r["d"], pid)
        b["title"] = b["title"] or r["product_title"]
        b["views"] += int(r["product_views"] or 0)
        b["viewers"] += int(r["unique_viewers"] or 0)
        b["sessions"] += int(r["unique_sessions"] or 0)
        b["atc"] += int(r["add_to_cart_count"] or 0)
        b["checkouts"] += int(r["checkout_started_count"] or 0)

    unparsed_carts = 0
    for r in cart_rows:
        parsed = _parse_cart_products(r["product_id"], r["product_title"])
        if not parsed:
            unparsed_carts += r["checkouts"]
            continue
        for pid, title in parsed:
            b = _product_bucket(r["d"], pid)
            b["title"] = b["title"] or title
            b["checkouts"] += r["checkouts"]
    if unparsed_carts:
        logger.warning(
            "[rollup product_behavior_daily] %s checkout_started event(s) had unparseable "
            "cart product_id lists and were not attributed to products",
            unparsed_carts,
        )

    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {d: set() for d in dates}

    for (target_date, pid), b in merged.items():
        # Rates can exceed 1 (ATC/checkout without a same-day view); cap so a
        # skewed ratio can never overflow DECIMAL(6, 4) and fail the whole batch.
        view_to_atc_rate = _safe_div(b["atc"], b["views"])
        view_to_checkout_rate = _safe_div(b["checkouts"], b["views"])
        if view_to_atc_rate is not None:
            view_to_atc_rate = min(view_to_atc_rate, 99.9999)
        if view_to_checkout_rate is not None:
            view_to_checkout_rate = min(view_to_checkout_rate, 99.9999)
        rows.append((
            target_date, pid, (b["title"] or None), b["views"],
            b["viewers"], b["sessions"], b["sessions"],
            b["atc"], b["checkouts"],
            view_to_atc_rate, view_to_checkout_rate,
        ))
        computed_keys_by_date[target_date].add((pid,))

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
def _rollup_behavioral_path_daily(cursor, connection, dates: List[date], source: Optional[_Source] = None) -> Dict[str, int]:
    started_at = time.monotonic()
    has_first_event = _path_first_event_present(cursor)
    total_scanned = 0
    total_generated = 0
    total_upserted = 0
    total_deleted = 0
    range_start, range_end = _window_bounds(dates)

    for target_date in dates:
        day_start, day_end = _date_range_bounds(target_date)

        # Every session is read now (the idle-session sweep gives one-visit sessions a row too), which can be
        # tens of thousands a day each with its JSON event sequence. They are read in keyset-paged chunks and
        # folded into the buckets as they arrive, so memory stays small and the single-statement scan is gone.
        buckets: Dict[str, Dict[str, Any]] = {}
        day_total = 0
        last_key = None
        while day_total < ROLLUP_MAX_SESSIONS_PER_DATE:
            limit = min(ROLLUP_PATH_CHUNK, ROLLUP_MAX_SESSIONS_PER_DATE - day_total)
            if last_key is None:
                cursor.execute(
                    """
                    SELECT id, session_start, session_id, actor_id, event_sequence, add_to_cart_count,
                           checkout_started_count
                    FROM intent_sessions
                    WHERE session_start >= %s AND session_start < %s
                    ORDER BY session_start, id
                    LIMIT %s
                    """,
                    (day_start, day_end, limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, session_start, session_id, actor_id, event_sequence, add_to_cart_count,
                           checkout_started_count
                    FROM intent_sessions
                    WHERE session_start >= %s AND session_start < %s
                          AND (session_start > %s OR (session_start = %s AND id > %s))
                    ORDER BY session_start, id
                    LIMIT %s
                    """,
                    (day_start, day_end, last_key[0], last_key[0], last_key[1], limit),
                )
            chunk = cursor.fetchall()
            for row in chunk:
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
            day_total += len(chunk)
            if len(chunk) < limit:
                break
            last_key = (chunk[-1]["session_start"], chunk[-1]["id"])
        else:
            logger.warning(
                "[rollup behavioral_path_daily] date=%s hit ROLLUP_MAX_SESSIONS_PER_DATE=%s cap",
                target_date, ROLLUP_MAX_SESSIONS_PER_DATE,
            )
        total_scanned += day_total

        rows: List[Tuple] = []
        computed_keys: Set[Tuple] = set()
        for sequence_hash, b in buckets.items():
            conversion_to_atc = _safe_div(b["sessions_with_atc"], b["session_count"])
            conversion_to_checkout = _safe_div(b["sessions_with_checkout"], b["session_count"])
            row = (
                target_date, sequence_hash, b["sequence"], b["session_count"],
                len(b["unique_actors"]), b["sessions_with_atc"], b["sessions_with_checkout"],
                conversion_to_atc, conversion_to_checkout,
            )
            rows.append(row + (_first_event_of(b["sequence"]),) if has_first_event else row)
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
        if has_first_event:
            insert_sql = """
            INSERT INTO behavioral_path_daily (
                summary_date, sequence_hash, sequence, session_count, unique_actor_count,
                sessions_with_atc, sessions_with_checkout, conversion_to_atc, conversion_to_checkout, first_event
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sequence = VALUES(sequence),
                session_count = VALUES(session_count),
                unique_actor_count = VALUES(unique_actor_count),
                sessions_with_atc = VALUES(sessions_with_atc),
                sessions_with_checkout = VALUES(sessions_with_checkout),
                conversion_to_atc = VALUES(conversion_to_atc),
                conversion_to_checkout = VALUES(conversion_to_checkout),
                first_event = VALUES(first_event)
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
# Rollup 6: heatmap_page_daily
# ---------------------------
# Page views per day, page path and device, for the Heatmaps "popular pages" list. Page paths use the same
# normalisation as page_behavior_daily / click_behavior_daily, so a page chosen in the list finds its clicks.
# The device comes from the user agent, which the temporary event copy does not carry, so this stage always
# reads behavioral_events itself (page_viewed events only: the (event_name, occurred_at) index covers it).
# INTENT_HEATMAP_ROLLUP=off switches the stage off.
_HEATMAP_DEVICE_CASE_SQL = """CASE
            WHEN user_agent LIKE '%%iPad%%' OR user_agent LIKE '%%Tablet%%'
              OR (user_agent LIKE '%%Android%%' AND user_agent NOT LIKE '%%Mobile%%') THEN 'tablet'
            WHEN user_agent LIKE '%%iPhone%%' OR user_agent LIKE '%%Android%%' OR user_agent LIKE '%%Mobile%%' THEN 'mobile'
            ELSE 'desktop'
        END"""


def _heatmap_rollup_enabled() -> bool:
    return os.environ.get("INTENT_HEATMAP_ROLLUP", "on").strip().lower() not in ("off", "false", "0", "no")


def _rollup_heatmap_page_daily(cursor, connection, dates: List[date], source: Optional[_Source] = None) -> Dict[str, int]:
    if not _heatmap_rollup_enabled():
        return {"scanned": 0, "generated": 0, "upserted": 0, "deleted": 0}
    started_at = time.monotonic()
    range_start, range_end = _window_bounds(dates)

    cursor.execute(
        f"""
        SELECT
            DATE(occurred_at) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(url, '?', 1), '#', 1) AS raw_path,
            {_HEATMAP_DEVICE_CASE_SQL} AS device_type,
            COUNT(*) AS page_views,
            COUNT(DISTINCT session_id) AS sessions
        FROM behavioral_events
        WHERE event_name = 'page_viewed'
          AND occurred_at >= %s AND occurred_at < %s
          AND url IS NOT NULL AND url <> ''
        GROUP BY DATE(occurred_at), raw_path, device_type
        """,
        (range_start, range_end),
    )
    fetched = cursor.fetchall()
    scanned = sum(int(r["page_views"] or 0) for r in fetched)

    # Different raw paths can normalise to the same page ("/a" and "/a/"): their counts add up. Sessions are
    # summed the same way page_behavior_daily sums its unique sessions.
    merged: Dict[Tuple[date, str, str], List[int]] = {}
    for r in fetched:
        path = _normalize_page_path(r["raw_path"])
        if path == "(unknown)":
            continue
        counts = merged.setdefault((r["d"], path, r["device_type"]), [0, 0])
        counts[0] += int(r["page_views"] or 0)
        counts[1] += int(r["sessions"] or 0)

    rows: List[Tuple] = []
    computed_keys_by_date: Dict[date, Set[Tuple]] = {d: set() for d in dates}
    for (target_date, path, device), (views, sessions) in merged.items():
        if target_date not in computed_keys_by_date:
            continue
        rows.append((target_date, path, device, views, sessions))
        computed_keys_by_date[target_date].add((path, device))

    insert_sql = """
        INSERT INTO heatmap_page_daily (summary_date, page_path, device_type, page_views, sessions)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            page_views = VALUES(page_views),
            sessions = VALUES(sessions)
    """
    upserted, deleted = _upsert_rollup_rows(
        cursor, connection, "heatmap_page_daily", insert_sql, rows,
        ["summary_date", "page_path", "device_type"], dates, computed_keys_by_date,
    )

    logger.info(
        "[rollup heatmap_page_daily] range=%s..%s duration=%.2fs rows_scanned=%s rows_generated=%s "
        "rows_upserted=%s rows_deleted_stale=%s",
        range_start, range_end, time.monotonic() - started_at, scanned, len(rows), upserted, deleted,
    )
    return {"scanned": scanned, "generated": len(rows), "upserted": upserted, "deleted": deleted}


# ---------------------------
# Orchestration entrypoint
# ---------------------------
_ROLLUP_STAGES = (
    ("intent_daily_summary", _rollup_intent_daily_summary),
    ("page_behavior_daily", _rollup_page_behavior_daily),
    ("click_behavior_daily", _rollup_click_behavior_daily),
    ("product_behavior_daily", _rollup_product_behavior_daily),
    ("behavioral_path_daily", _rollup_behavioral_path_daily),
    ("heatmap_page_daily", _rollup_heatmap_page_daily),
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

        with _schema_lock:
            needs_schema_check = brand_index not in _schema_ensured_brands
        if needs_schema_check:
            _ensure_all_rollup_tables(c, conn)
            with _schema_lock:
                _schema_ensured_brands.add(brand_index)

        dates, stored_watermark = _determine_affected_dates(c, now)
        logger.info(
            "[rollup] brand=%s starting: watermark=%s affected_dates=%s (%s..%s)",
            brand_label, stored_watermark, len(dates), dates[0], dates[-1],
        )

        signature = _change_signature(c) if _skip_unchanged_enabled() else None
        if (
            signature is not None
            and stored_watermark is not None
            and _last_run.get(brand_index) == (tuple(dates), signature)
        ):
            # Nothing was added since a run that completed over these same dates, so the tables are already
            # right. The watermark still advances: the window is then measured from now, not from a stale mark.
            update_pipeline_metadata_timestamp(c, conn, ROLLUP_METADATA_KEY, now)
            logger.info(
                "[rollup] brand=%s unchanged since the last complete run (dates=%s..%s); skipped, watermark advanced to %s",
                brand_label, dates[0], dates[-1], now,
            )
            return

        any_failed = False
        source = _prepare_source(c, conn, dates)
        try:
            for stage_name, stage_fn in _ROLLUP_STAGES:
                if not _run_stage(stage_name, stage_fn, c, conn, dates, source, brand_label):
                    any_failed = True
        finally:
            _release_source(c, conn, source)

        if any_failed:
            _last_run.pop(brand_index, None)
            logger.info(
                "[rollup] brand=%s watermark NOT advanced due to a stage failure "
                "(window will be retried next run)", brand_label,
            )
        else:
            update_pipeline_metadata_timestamp(c, conn, ROLLUP_METADATA_KEY, now)
            if signature is not None:   # taken before the stages ran, so anything written meanwhile forces the next run
                _last_run[brand_index] = (tuple(dates), signature)
            else:
                _last_run.pop(brand_index, None)
            logger.info(
                "[rollup] brand=%s watermark advanced to %s (duration=%.2fs)",
                brand_label, now, time.monotonic() - run_started_at,
            )

    if cursor is not None:
        _do_run(cursor, connection)
    else:
        with get_db_cursor(brand_index) as (c, conn):
            _do_run(c, conn)
