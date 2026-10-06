"""
Intent behavioral events ingestion: reads tracking events (product_viewed,
etc.) from the Intent MongoDB cluster (INTENT_MONGO_URI, intent_sessions.events
- distinct from any order-pipeline Mongo cluster) and ingests them into each
applicable brand's own behavioral_events table. Also ingests click events
(intent_sessions.click_events -> click_events table) and session rollups
(intent_sessions.session_history -> intent_sessions table).

Brand routing is driven entirely by INTENT_DB_MAP (a JSON object mapping the
Mongo document's brand_id to this pipeline's existing DB_DATABASE_<i>
values - the actual per-brand MySQL database name, matched
case-insensitively by the worker), independent of any other brand gating.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient

from pipeline.state import IST, logger
from pipeline.db import (
    get_db_cursor,
    executemany_chunked,
    get_pipeline_metadata_timestamp,
    update_pipeline_metadata_timestamp,
    EXECUTEMANY_CHUNK_SIZE,
)

# The two per-event streams are watermarked on createdAt (server-set insert
# time), NOT occurred_at: occurred_at is written by the tracker and can be
# skewed (e.g. IST wall-clock labelled as UTC = +5h30m), which pushed the old
# occurred_at-based watermark into the future and made later runs match 0
# docs. Fresh key names (vs. the old *_last_occurred_at rows) deliberately
# discard those possibly-poisoned watermarks.
INTENT_METADATA_KEY = "intent_events_last_created_at"
CLICK_EVENTS_METADATA_KEY = "intent_click_events_last_created_at"
INTENT_SESSION_HISTORY_METADATA_KEY = "intent_session_history_last_updated_at"
INTENT_OVERLAP = timedelta(minutes=5)
INTENT_DEFAULT_LOOKBACK = timedelta(hours=1)
# First run under a fresh per-event watermark key: look further back to
# recover docs the old occurred_at watermark skipped. Safe because the upsert
# is idempotent and ingested docs are deleted from Mongo, so what's left is
# small.
INTENT_EVENT_STREAM_DEFAULT_LOOKBACK = timedelta(hours=24)
# Log a "read N documents so far" line every this many docs while streaming
# from Mongo, so a large backlog isn't a silent multi-minute gap.
INTENT_READ_PROGRESS_EVERY = 5000

# Kill-switch for the delete-after-ingest behavior below - flip to "false"
# to disable without a code revert/redeploy if anything looks wrong.
INTENT_DELETE_INGESTED_DOCS = (
    os.environ.get("INTENT_DELETE_INGESTED_DOCS", "true").strip().lower() == "true"
)

# event_name -> intent_sessions counter column. Anything not in this map
# still counts toward event_count but not a specific typed counter -
# forward-compatible with future event types.
_SESSION_EVENT_NAME_COUNTERS = {
    "page_viewed": "page_view_count",
    "product_viewed": "product_view_count",
    "click": "click_count",
    "product_added_to_cart": "add_to_cart_count",
    "checkout_started": "checkout_started_count",
    "scroll_depth": "scroll_count",
}

_VALID_CLICK_BUCKETS = {"useful_click", "dead_click"}
_CLICK_FIELDS = (
    "x",
    "y",
    "tag_name",
    "element_id",
    "element_name",
    "element_type",
    "element_value",
    "href",
)
_SIGNAL_FIELDS = ("url_changed", "cart_changed", "ui_changed", "meaningful_scroll")

# Table's fixed, nullable columns pulled from the nested `raw` payload - not
# dynamic/on-the-fly like a per-event-type dynamic columns table.
_RAW_NUMERIC_FIELDS = {"quantity", "price", "checkout_total", "scroll_percent"}
_RAW_FIELDS = (
    "product_id",
    "variant_id",
    "product_title",
    "variant_title",
    "quantity",
    "price",
    "currency",
    "checkout_total",
    "scroll_percent",
)
# Most _RAW_FIELDS use the same key name in Mongo's raw payload as the MySQL
# column - scroll events are the one exception observed so far: raw only
# has {"percent": <value>}, not {"scroll_percent": <value>}.
_RAW_FIELD_SOURCE_KEYS = {
    "scroll_percent": "percent",
}


def _resolve_intent_events_collection():
    mongo_uri = os.environ.get("INTENT_MONGO_URI")
    if not mongo_uri:
        raise ValueError("INTENT_MONGO_URI is not configured")

    client = MongoClient(mongo_uri, tz_aware=True)
    return client, client["intent_sessions"]["events"]


def _resolve_intent_click_events_collection():
    mongo_uri = os.environ.get("INTENT_MONGO_URI")
    if not mongo_uri:
        raise ValueError("INTENT_MONGO_URI is not configured")

    client = MongoClient(mongo_uri, tz_aware=True)
    return client, client["intent_sessions"]["click_events"]


def _resolve_intent_session_history_collection():
    mongo_uri = os.environ.get("INTENT_MONGO_URI")
    if not mongo_uri:
        raise ValueError("INTENT_MONGO_URI is not configured")

    client = MongoClient(mongo_uri, tz_aware=True)
    return client, client["intent_sessions"]["session_history"]


def _parse_intent_db_map() -> Dict[str, str]:
    raw = os.environ.get("INTENT_DB_MAP", "{}")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid JSON in INTENT_DB_MAP. Using empty mapping.")
        return {}

    if not isinstance(parsed, dict):
        logger.warning("INTENT_DB_MAP must be a JSON object. Using empty mapping.")
        return {}

    return {str(k): str(v) for k, v in parsed.items()}


def _coerce_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_common_event_fields(doc: Dict[str, Any]) -> Optional[Tuple]:
    """
    The fields every Intent event document carries regardless of type
    (event_id, event_name, actor_id, client_id, visitor_id, session_id,
    url, referrer, user_agent, occurred_at) - shared by both
    _extract_behavioral_event_row and _extract_click_event_row. Returns
    None (and logs) if event_id/occurred_at - both NOT NULL columns on
    every table - are missing.
    """
    event_id = str(doc.get("event_id") or "").strip()
    occurred_at = doc.get("occurred_at")
    if not event_id or not isinstance(occurred_at, datetime):
        logger.warning("Skipping intent event with missing event_id/occurred_at")
        return None

    return (
        event_id,
        str(doc.get("event_name") or "").strip() or None,
        (str(doc.get("actor_id")).strip() or None) if doc.get("actor_id") is not None else None,
        (str(doc.get("client_id")).strip() or None) if doc.get("client_id") is not None else None,
        (str(doc.get("visitor_id")).strip() or None) if doc.get("visitor_id") is not None else None,
        (str(doc.get("session_id")).strip() or None) if doc.get("session_id") is not None else None,
        doc.get("url") or None,
        doc.get("referrer") or None,
        doc.get("user_agent") or None,
        occurred_at,
    )


def _extract_behavioral_event_row(doc: Dict[str, Any]) -> Optional[Tuple]:
    common = _extract_common_event_fields(doc)
    if common is None:
        return None
    (
        event_id, event_name, actor_id, client_id, visitor_id, session_id,
        url, referrer, user_agent, occurred_at,
    ) = common

    raw = doc.get("raw") or {}
    if not isinstance(raw, dict):
        raw = {}

    raw_values = {}
    for field in _RAW_FIELDS:
        source_key = _RAW_FIELD_SOURCE_KEYS.get(field, field)
        value = raw.get(source_key)
        if field in _RAW_NUMERIC_FIELDS:
            value = _coerce_numeric(value)
        elif value is not None:
            value = str(value)
        raw_values[field] = value

    return (
        event_id,
        event_name,
        actor_id,
        client_id,
        visitor_id,
        session_id,
        url,
        referrer,
        user_agent,
        raw_values["product_id"],
        raw_values["variant_id"],
        raw_values["product_title"],
        raw_values["variant_title"],
        raw_values["quantity"],
        raw_values["price"],
        raw_values["currency"],
        raw_values["checkout_total"],
        raw_values["scroll_percent"],
        json.dumps(raw, default=str),
        occurred_at,
    )


def _extract_click_event_row(doc: Dict[str, Any]) -> Optional[Tuple]:
    common = _extract_common_event_fields(doc)
    if common is None:
        return None
    (
        event_id, event_name, actor_id, client_id, visitor_id, session_id,
        url, referrer, user_agent, occurred_at,
    ) = common

    click = doc.get("click") or {}
    if not isinstance(click, dict):
        click = {}
    signals = doc.get("signals") or {}
    if not isinstance(signals, dict):
        signals = {}

    click_bucket = doc.get("click_bucket")
    if click_bucket not in _VALID_CLICK_BUCKETS:
        click_bucket = None

    data_payload = {
        "click": {field: click.get(field) for field in _CLICK_FIELDS},
        "signals": {field: signals.get(field) for field in _SIGNAL_FIELDS},
    }

    return (
        event_id,
        event_name,
        actor_id,
        client_id,
        visitor_id,
        session_id,
        url,
        referrer,
        user_agent,
        _coerce_int(click.get("x")),
        _coerce_int(click.get("y")),
        (str(click.get("tag_name")).strip() or None) if click.get("tag_name") is not None else None,
        (str(click.get("element_id")).strip() or None) if click.get("element_id") is not None else None,
        (str(click.get("element_name")).strip() or None) if click.get("element_name") is not None else None,
        (str(click.get("element_type")).strip() or None) if click.get("element_type") is not None else None,
        click.get("element_value") if click.get("element_value") is not None else None,
        click.get("href") if click.get("href") is not None else None,
        bool(signals["url_changed"]) if signals.get("url_changed") is not None else None,
        bool(signals["cart_changed"]) if signals.get("cart_changed") is not None else None,
        bool(signals["ui_changed"]) if signals.get("ui_changed") is not None else None,
        bool(signals["meaningful_scroll"]) if signals.get("meaningful_scroll") is not None else None,
        click_bucket,
        json.dumps(data_payload, default=str),
        occurred_at,
    )


def _extract_session_history_row(doc: Dict[str, Any]) -> Optional[Tuple]:
    """
    session_history documents are mutable rollups of an entire session:
    identity/timing at the top level, plus a nested events_seq object
    ({"1": {...full sub-event...}, "2": {...}, ...}) holding every
    sub-event that happened in the session, in order. This derives the
    intent_sessions row from that - counts per event type, the session's
    final (last-known-non-null) client_id/visitor_id, and a compact
    {step: {event_name: event_id}} event_sequence, not the full sub-event
    payloads (those are already ingested separately via
    behavioral_events/click_events).
    """
    session_id = str(doc.get("session_id") or "").strip()
    actor_id = str(doc.get("actor_id") or "").strip()
    updated_at = doc.get("updatedAt")
    session_start = doc.get("session_start")
    if (
        not session_id
        or not actor_id
        or not isinstance(updated_at, datetime)
        or not isinstance(session_start, datetime)
    ):
        logger.warning(
            "Skipping session_history document with missing session_id/actor_id/"
            "updatedAt/session_start"
        )
        return None

    events_seq = doc.get("events_seq") or {}
    if not isinstance(events_seq, dict):
        events_seq = {}

    ordered_steps: List[Tuple[int, Dict[str, Any]]] = []
    for key, sub_event in events_seq.items():
        if not isinstance(sub_event, dict):
            continue
        try:
            step = int(key)
        except (TypeError, ValueError):
            continue
        ordered_steps.append((step, sub_event))
    ordered_steps.sort(key=lambda item: item[0])

    counters = {column: 0 for column in set(_SESSION_EVENT_NAME_COUNTERS.values())}
    counters["useful_click_count"] = 0
    counters["dead_click_count"] = 0
    client_id: Optional[str] = None
    visitor_id: Optional[str] = None
    event_sequence: Dict[str, Dict[str, Any]] = {}

    for step, sub_event in ordered_steps:
        sub_event_name = str(sub_event.get("event_name") or "").strip()
        sub_event_id = sub_event.get("event_id")

        counter_column = _SESSION_EVENT_NAME_COUNTERS.get(sub_event_name)
        if counter_column:
            counters[counter_column] += 1
        if sub_event_name == "click":
            click_bucket = sub_event.get("click_bucket")
            if click_bucket in _VALID_CLICK_BUCKETS:
                counters[f"{click_bucket}_count"] += 1

        if sub_event.get("client_id") is not None:
            client_id = str(sub_event.get("client_id")).strip() or client_id
        if sub_event.get("visitor_id") is not None:
            visitor_id = str(sub_event.get("visitor_id")).strip() or visitor_id

        event_sequence[str(step)] = {sub_event_name: sub_event_id}

    session_end = doc.get("session_end")
    session_time_spent_ms = _coerce_int(doc.get("session_time_spent"))

    return (
        session_id,
        actor_id,
        client_id,
        visitor_id,
        session_start,
        session_end if isinstance(session_end, datetime) else None,
        session_time_spent_ms,
        len(ordered_steps),
        counters["page_view_count"],
        counters["product_view_count"],
        counters["click_count"],
        counters["useful_click_count"],
        counters["dead_click_count"],
        counters["add_to_cart_count"],
        counters["checkout_started_count"],
        counters["scroll_count"],
        json.dumps(event_sequence, default=str),
        session_start,  # occurred_at - a session spans a range, so this mirrors session_start
        updated_at,
    )


def _ensure_column_exists(cursor, connection, table_name: str, column_name: str, column_def: str) -> None:
    """
    Idempotent ALTER for a column that was added to the schema after a
    table already existed in some brand's database - CREATE TABLE IF NOT
    EXISTS alone won't add it to a pre-existing table.
    """
    cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    if cursor.fetchone() is not None:
        return
    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
    connection.commit()


def _ensure_behavioral_events_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS behavioral_events (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

            event_id VARCHAR(100) NOT NULL,
            event_name VARCHAR(100) NOT NULL,
            actor_id VARCHAR(100) NULL,

            client_id VARCHAR(100) NULL,
            visitor_id VARCHAR(100) NULL,
            session_id VARCHAR(100) NULL,

            url TEXT NULL,
            referrer TEXT NULL,
            user_agent TEXT NULL,

            product_id VARCHAR(100) NULL,
            variant_id VARCHAR(100) NULL,
            product_title VARCHAR(500) NULL,
            variant_title VARCHAR(500) NULL,

            quantity INT NULL,
            price DECIMAL(18, 4) NULL,
            currency VARCHAR(10) NULL,

            checkout_total DECIMAL(18, 4) NULL,

            scroll_percent DECIMAL(5, 2) NULL,

            data JSON NULL,

            occurred_at DATETIME(6) NOT NULL,
            ingested_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_event_id (event_id),
            KEY idx_occurred_at (occurred_at),
            KEY idx_session_time (session_id, occurred_at),
            KEY idx_client_time (client_id, occurred_at),
            KEY idx_event_time (event_name, occurred_at),
            KEY idx_product_time (product_id, occurred_at),
            KEY idx_variant_time (variant_id, occurred_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()
    _ensure_column_exists(cursor, connection, "behavioral_events", "actor_id", "VARCHAR(100) NULL")


def _ensure_click_events_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS click_events (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

            event_id VARCHAR(100) NOT NULL,
            event_name VARCHAR(100) NOT NULL,
            actor_id VARCHAR(100) NULL,

            client_id VARCHAR(100) NULL,
            visitor_id VARCHAR(100) NULL,
            session_id VARCHAR(100) NULL,

            url TEXT NULL,
            referrer TEXT NULL,
            user_agent TEXT NULL,

            product_id VARCHAR(100) NULL,
            variant_id VARCHAR(100) NULL,
            product_title VARCHAR(500) NULL,
            variant_title VARCHAR(500) NULL,

            quantity INT NULL,
            price DECIMAL(18, 4) NULL,
            currency VARCHAR(10) NULL,

            checkout_total DECIMAL(18, 4) NULL,

            scroll_percent DECIMAL(5, 2) NULL,

            click_x INT NULL,
            click_y INT NULL,
            click_tag VARCHAR(50) NULL,
            click_element_id VARCHAR(255) NULL,
            click_element_name VARCHAR(255) NULL,
            click_element_type VARCHAR(100) NULL,
            click_element_value TEXT NULL,
            click_href TEXT NULL,

            url_changed BOOLEAN NULL,
            cart_changed BOOLEAN NULL,
            ui_changed BOOLEAN NULL,
            meaningful_scroll BOOLEAN NULL,

            click_bucket ENUM('useful_click', 'dead_click') NULL,

            data JSON NULL,

            occurred_at DATETIME(6) NOT NULL,
            ingested_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_event_id (event_id),
            KEY idx_occurred_at (occurred_at),
            KEY idx_session_time (session_id, occurred_at),
            KEY idx_client_time (client_id, occurred_at),
            KEY idx_event_time (event_name, occurred_at),
            KEY idx_product_time (product_id, occurred_at),
            KEY idx_variant_time (variant_id, occurred_at),
            KEY idx_click_bucket_time (click_bucket, occurred_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()
    _ensure_column_exists(cursor, connection, "click_events", "actor_id", "VARCHAR(100) NULL")


def _ensure_intent_sessions_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS intent_sessions (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

            session_id VARCHAR(100) NOT NULL,
            actor_id VARCHAR(100) NOT NULL,

            client_id VARCHAR(100) NULL,
            visitor_id VARCHAR(100) NULL,

            session_start DATETIME(6) NOT NULL,
            session_end DATETIME(6) NULL,

            session_time_spent_ms BIGINT UNSIGNED NULL,

            event_count INT UNSIGNED NOT NULL DEFAULT 0,

            page_view_count INT UNSIGNED NOT NULL DEFAULT 0,
            product_view_count INT UNSIGNED NOT NULL DEFAULT 0,

            click_count INT UNSIGNED NOT NULL DEFAULT 0,
            useful_click_count INT UNSIGNED NOT NULL DEFAULT 0,
            dead_click_count INT UNSIGNED NOT NULL DEFAULT 0,

            add_to_cart_count INT UNSIGNED NOT NULL DEFAULT 0,
            checkout_started_count INT UNSIGNED NOT NULL DEFAULT 0,
            scroll_count INT UNSIGNED NOT NULL DEFAULT 0,

            event_sequence JSON NULL,

            occurred_at DATETIME(6) NULL,

            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_session_id (session_id),
            KEY idx_actor_session (actor_id, session_start),
            KEY idx_visitor_session (visitor_id, session_start),
            KEY idx_session_start (session_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()
    _ensure_column_exists(cursor, connection, "intent_sessions", "occurred_at", "DATETIME(6) NULL")


_BEHAVIORAL_UPSERT_COLUMNS = (
    "event_id",
    "event_name",
    "actor_id",
    "client_id",
    "visitor_id",
    "session_id",
    "url",
    "referrer",
    "user_agent",
    "product_id",
    "variant_id",
    "product_title",
    "variant_title",
    "quantity",
    "price",
    "currency",
    "checkout_total",
    "scroll_percent",
    "data",
    "occurred_at",
)
# ingested_at is deliberately excluded from the UPDATE clause: it should
# reflect the event's *first* ingestion, not be reset by a later re-upsert
# of the same event_id (e.g. the watermark-overlap safety margin).
_BEHAVIORAL_UPDATE_COLUMNS = tuple(c for c in _BEHAVIORAL_UPSERT_COLUMNS if c != "event_id")


def _upsert_behavioral_events(cursor, connection, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    columns_sql = ", ".join(_BEHAVIORAL_UPSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_BEHAVIORAL_UPSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _BEHAVIORAL_UPDATE_COLUMNS)
    sql = (
        f"INSERT INTO behavioral_events ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    return executemany_chunked(cursor, connection, sql, rows)


_CLICK_EVENTS_UPSERT_COLUMNS = (
    "event_id",
    "event_name",
    "actor_id",
    "client_id",
    "visitor_id",
    "session_id",
    "url",
    "referrer",
    "user_agent",
    "click_x",
    "click_y",
    "click_tag",
    "click_element_id",
    "click_element_name",
    "click_element_type",
    "click_element_value",
    "click_href",
    "url_changed",
    "cart_changed",
    "ui_changed",
    "meaningful_scroll",
    "click_bucket",
    "data",
    "occurred_at",
)
_CLICK_EVENTS_UPDATE_COLUMNS = tuple(
    c for c in _CLICK_EVENTS_UPSERT_COLUMNS if c != "event_id"
)


def _upsert_click_events(cursor, connection, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    columns_sql = ", ".join(_CLICK_EVENTS_UPSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_CLICK_EVENTS_UPSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _CLICK_EVENTS_UPDATE_COLUMNS)
    sql = (
        f"INSERT INTO click_events ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    return executemany_chunked(cursor, connection, sql, rows)


_INTENT_SESSIONS_UPSERT_COLUMNS = (
    "session_id",
    "actor_id",
    "client_id",
    "visitor_id",
    "session_start",
    "session_end",
    "session_time_spent_ms",
    "event_count",
    "page_view_count",
    "product_view_count",
    "click_count",
    "useful_click_count",
    "dead_click_count",
    "add_to_cart_count",
    "checkout_started_count",
    "scroll_count",
    "event_sequence",
    "occurred_at",
)
_INTENT_SESSIONS_UPDATE_COLUMNS = tuple(
    c for c in _INTENT_SESSIONS_UPSERT_COLUMNS if c != "session_id"
)


def _upsert_intent_sessions(cursor, connection, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    # extract_row_fn's tuples end with updatedAt (for _sync_intent_collection's
    # watermark tracking only) - not an actual column here, since
    # created_at/updated_at are entirely DB-managed. Drop it before inserting.
    insert_rows = [row[:-1] for row in rows]

    columns_sql = ", ".join(_INTENT_SESSIONS_UPSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_INTENT_SESSIONS_UPSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _INTENT_SESSIONS_UPDATE_COLUMNS)
    sql = (
        f"INSERT INTO intent_sessions ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    return executemany_chunked(cursor, connection, sql, insert_rows)


def _delete_ingested_documents(collection, event_ids: List[str]) -> int:
    """
    Deletes documents by event_id, chunked at EXECUTEMANY_CHUNK_SIZE (same
    constant the MySQL side chunks on). Only ever called after upsert_fn has
    already returned successfully for these exact rows - see the
    delete_after_ingest usage in _sync_intent_collection below.
    """
    deleted = 0
    for i in range(0, len(event_ids), EXECUTEMANY_CHUNK_SIZE):
        chunk = event_ids[i : i + EXECUTEMANY_CHUNK_SIZE]
        result = collection.delete_many({"event_id": {"$in": chunk}})
        deleted += result.deleted_count
    return deleted


def _sync_intent_collection(
    brand_index: int,
    mongo_brand_id: str,
    brand_label: str,
    *,
    collection_resolver,
    ensure_table_fn,
    extract_row_fn,
    upsert_fn,
    metadata_key: str,
    event_label: str,
    watermark_field: str = "occurred_at",
    fallback_field: Optional[str] = None,
    default_lookback: timedelta = INTENT_DEFAULT_LOOKBACK,
    delete_after_ingest: bool = False,
    cursor=None,
    connection=None,
) -> None:
    """
    Shared sync flow for any Intent Mongo collection -> per-brand MySQL
    table: watermark fetch, one bounded query, extract+track-max, bulk
    upsert, advance watermark, log. sync_intent_events_for_brand,
    sync_click_events_for_brand, and sync_session_history_for_brand all
    call this with their collection/table/row-extractor-specific pieces,
    rather than duplicating the flow.

    watermark_field controls which document field the incremental query
    and watermark are based on - createdAt for the two immutable per-event
    streams (server-set, so unaffected by tracker clock/timezone skew in
    occurred_at), but updatedAt for session_history, whose documents get
    updated in place as a still-open session accumulates more events
    (createdAt alone would miss those updates). The watermark is read from
    doc[watermark_field].

    fallback_field (optional) covers documents that lack watermark_field
    entirely (a plain $gt would never match them): they're matched and
    watermarked on fallback_field instead, and row[-1] (which extract_row_fn
    ends with the fallback timestamp) is used for their watermark value.

    default_lookback is how far back the very first run (no stored
    watermark yet) looks.

    delete_after_ingest (default False - only sync_intent_events_for_brand
    and sync_click_events_for_brand opt in) deletes each row's source
    document from Mongo by event_id, but ONLY after upsert_fn has already
    returned successfully - i.e. the whole batch is durably committed to
    MySQL. Deliberately NOT used for session_history: those documents get
    updated in place as a session continues, so deleting one after a
    successful-but-mid-session ingest would silently truncate that
    session's eventual rollup. A delete failure is caught and logged, never
    re-raised - it must not undo or block a MySQL write that already
    succeeded; the document just stays in Mongo for a retry next run (or a
    TTL backstop, if one is configured).
    """
    client = None
    try:
        client, collection = collection_resolver()

        def _do_sync(c, conn):
            started_at = time.monotonic()
            log_prefix = f"[intent {event_label}] brand={brand_label} (mongo_brand_id={mongo_brand_id})"
            logger.info(f"{log_prefix}: starting ingestion")

            ensure_table_fn(c, conn)

            stored_watermark = get_pipeline_metadata_timestamp(c, metadata_key)
            watermark = stored_watermark or (datetime.now(IST) - default_lookback)
            query_lower_bound = (watermark - INTENT_OVERLAP).astimezone(timezone.utc)
            if stored_watermark is None:
                logger.info(
                    f"{log_prefix}: no stored watermark (first run) - looking back "
                    f"{default_lookback}"
                )
            logger.info(
                f"{log_prefix}: querying Mongo for {watermark_field} > "
                f"{query_lower_bound.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"(stored watermark {watermark.strftime('%Y-%m-%d %H:%M:%S')} IST "
                f"minus {int(INTENT_OVERLAP.total_seconds() // 60)}m overlap)"
            )

            query: Dict[str, Any] = {"brand_id": mongo_brand_id}
            if fallback_field:
                query["$or"] = [
                    {watermark_field: {"$gt": query_lower_bound}},
                    {
                        watermark_field: {"$exists": False},
                        fallback_field: {"$gt": query_lower_bound},
                    },
                ]
            else:
                query[watermark_field] = {"$gt": query_lower_bound}
            # Deliberately no server-side .sort(): order doesn't matter here
            # (upserts are idempotent, the watermark is a max over all rows),
            # and sorting on an unindexed field makes Mongo sort in memory,
            # which fails past 32MB (QueryExceededMemoryLimitNoDiskUseAllowed)
            # on large backlogs - and this cluster can't add indexes.
            docs = collection.find(query)

            rows: List[Tuple] = []
            max_watermark_value: Optional[datetime] = None
            docs_seen = 0
            skipped = 0
            for doc in docs:
                docs_seen += 1
                if docs_seen % INTENT_READ_PROGRESS_EVERY == 0:
                    logger.info(
                        f"{log_prefix}: read {docs_seen} document(s) from Mongo so far "
                        f"({time.monotonic() - started_at:.1f}s elapsed)"
                    )
                row = extract_row_fn(doc)
                if row is None:
                    skipped += 1
                    continue
                rows.append(row)
                watermark_value = doc.get(watermark_field)
                if not isinstance(watermark_value, datetime):
                    watermark_value = row[-1]
                if watermark_value.tzinfo is None:
                    watermark_value = watermark_value.replace(tzinfo=timezone.utc)
                if max_watermark_value is None or watermark_value > max_watermark_value:
                    max_watermark_value = watermark_value

            logger.info(
                f"{log_prefix}: read {docs_seen} document(s) from Mongo "
                f"({len(rows)} valid, {skipped} skipped as malformed) in "
                f"{time.monotonic() - started_at:.1f}s"
            )

            upsert_started_at = time.monotonic()
            if rows:
                logger.info(f"{log_prefix}: upserting {len(rows)} row(s) into MySQL")
            inserted = upsert_fn(c, conn, rows)
            if rows:
                logger.info(
                    f"{log_prefix}: MySQL upsert done in "
                    f"{time.monotonic() - upsert_started_at:.1f}s"
                )

            if max_watermark_value is not None:
                new_watermark = max_watermark_value.astimezone(IST)
                update_pipeline_metadata_timestamp(c, conn, metadata_key, new_watermark)
                logger.info(
                    f"{log_prefix}: watermark advanced to "
                    f"{new_watermark.strftime('%Y-%m-%d %H:%M:%S')} IST"
                )
            else:
                logger.info(f"{log_prefix}: no new documents, watermark unchanged")

            logger.info(
                f"Ingested intent {event_label} for brand={brand_label} "
                f"(mongo_brand_id={mongo_brand_id}): {inserted} row(s)"
            )

            if delete_after_ingest and INTENT_DELETE_INGESTED_DOCS and rows:
                try:
                    event_ids = [row[0] for row in rows]
                    logger.info(
                        f"{log_prefix}: deleting {len(event_ids)} ingested document(s) from Mongo"
                    )
                    deleted = _delete_ingested_documents(collection, event_ids)
                    logger.info(
                        f"Deleted {deleted} ingested {event_label} document(s) from Mongo "
                        f"for brand={brand_label} (mongo_brand_id={mongo_brand_id})"
                    )
                except Exception as e:
                    logger.error(
                        f"Error deleting ingested {event_label} documents from Mongo for "
                        f"brand={brand_label} (mongo_brand_id={mongo_brand_id}): {e}"
                    )

            logger.info(
                f"{log_prefix}: finished in {time.monotonic() - started_at:.1f}s"
            )

        if cursor is not None:
            _do_sync(cursor, connection)
        else:
            with get_db_cursor(brand_index) as (c, conn):
                _do_sync(c, conn)
    except Exception as e:
        logger.error(
            f"Error ingesting intent {event_label} for brand={brand_label} "
            f"(mongo_brand_id={mongo_brand_id}): {e}"
        )
    finally:
        if client is not None:
            client.close()


def sync_intent_events_for_brand(
    brand_index: int,
    mongo_brand_id: str,
    brand_label: str,
    cursor=None,
    connection=None,
) -> None:
    _sync_intent_collection(
        brand_index,
        mongo_brand_id,
        brand_label,
        collection_resolver=_resolve_intent_events_collection,
        ensure_table_fn=_ensure_behavioral_events_table,
        extract_row_fn=_extract_behavioral_event_row,
        upsert_fn=_upsert_behavioral_events,
        metadata_key=INTENT_METADATA_KEY,
        event_label="behavioral events",
        watermark_field="createdAt",
        fallback_field="occurred_at",
        default_lookback=INTENT_EVENT_STREAM_DEFAULT_LOOKBACK,
        delete_after_ingest=True,
        cursor=cursor,
        connection=connection,
    )


def sync_click_events_for_brand(
    brand_index: int,
    mongo_brand_id: str,
    brand_label: str,
    cursor=None,
    connection=None,
) -> None:
    _sync_intent_collection(
        brand_index,
        mongo_brand_id,
        brand_label,
        collection_resolver=_resolve_intent_click_events_collection,
        ensure_table_fn=_ensure_click_events_table,
        extract_row_fn=_extract_click_event_row,
        upsert_fn=_upsert_click_events,
        metadata_key=CLICK_EVENTS_METADATA_KEY,
        event_label="click events",
        watermark_field="createdAt",
        fallback_field="occurred_at",
        default_lookback=INTENT_EVENT_STREAM_DEFAULT_LOOKBACK,
        delete_after_ingest=True,
        cursor=cursor,
        connection=connection,
    )


def sync_session_history_for_brand(
    brand_index: int,
    mongo_brand_id: str,
    brand_label: str,
    cursor=None,
    connection=None,
) -> None:
    _sync_intent_collection(
        brand_index,
        mongo_brand_id,
        brand_label,
        collection_resolver=_resolve_intent_session_history_collection,
        ensure_table_fn=_ensure_intent_sessions_table,
        extract_row_fn=_extract_session_history_row,
        upsert_fn=_upsert_intent_sessions,
        metadata_key=INTENT_SESSION_HISTORY_METADATA_KEY,
        event_label="session history",
        watermark_field="updatedAt",
        cursor=cursor,
        connection=connection,
    )
