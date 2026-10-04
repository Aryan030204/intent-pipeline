"""
Batch writer for SQS intent messages. Row construction reuses the Mongo
extractors in pipeline/intent_events.py, and the event/click SQL mirrors the
existing upserts exactly (same columns, same ON DUPLICATE KEY UPDATE). The
only new SQL is the session upsert, which adds a source_updated_at version
guard so an older snapshot can never overwrite a newer one.

Nothing here commits per chunk: the whole batch for one brand commits once, or
not at all, so the worker can rely on rollback + SQS redelivery.
"""

from typing import Any, Dict, List

from pipeline.db import EXECUTEMANY_CHUNK_SIZE
from pipeline.intent_events import (
    _BEHAVIORAL_UPSERT_COLUMNS,
    _BEHAVIORAL_UPDATE_COLUMNS,
    _CLICK_EVENTS_UPSERT_COLUMNS,
    _CLICK_EVENTS_UPDATE_COLUMNS,
    _INTENT_SESSIONS_UPSERT_COLUMNS,
    _INTENT_SESSIONS_UPDATE_COLUMNS,
    _extract_behavioral_event_row,
    _extract_click_event_row,
    _extract_session_history_row,
)
from pipeline.intent_sqs_contract import (
    MalformedMessage,
    TYPE_CLICK,
    TYPE_EVENT,
    TYPE_SESSION_SNAPSHOT,
    collect_by_type,
    to_click_doc,
    to_event_doc,
    to_session_doc,
)

SESSION_VERSION_COLUMN = "source_updated_at"


def _upsert_sql(table: str, upsert_columns, update_columns) -> str:
    columns_sql = ", ".join(upsert_columns)
    placeholders = ", ".join(["%s"] * len(upsert_columns))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in update_columns)
    return (
        f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )


BEHAVIORAL_EVENTS_SQL = _upsert_sql(
    "behavioral_events", _BEHAVIORAL_UPSERT_COLUMNS, _BEHAVIORAL_UPDATE_COLUMNS
)
CLICK_EVENTS_SQL = _upsert_sql(
    "click_events", _CLICK_EVENTS_UPSERT_COLUMNS, _CLICK_EVENTS_UPDATE_COLUMNS
)


def _session_upsert_sql() -> str:
    """
    Guarded upsert. MySQL evaluates ON DUPLICATE KEY UPDATE assignments left to
    right, so source_updated_at is assigned LAST: every guard before it reads the
    stored version, and the version itself advances only when the guard passed.
    Equal versions are applied (idempotent re-delivery of the same snapshot).
    """
    insert_columns = list(_INTENT_SESSIONS_UPSERT_COLUMNS) + [SESSION_VERSION_COLUMN]
    columns_sql = ", ".join(insert_columns)
    placeholders = ", ".join(["%s"] * len(insert_columns))
    guard = (
        f"{SESSION_VERSION_COLUMN} IS NULL "
        f"OR {SESSION_VERSION_COLUMN} <= VALUES({SESSION_VERSION_COLUMN})"
    )
    assignments = [
        f"{c} = IF({guard}, VALUES({c}), {c})" for c in _INTENT_SESSIONS_UPDATE_COLUMNS
    ]
    assignments.append(
        f"{SESSION_VERSION_COLUMN} = IF({guard}, VALUES({SESSION_VERSION_COLUMN}), {SESSION_VERSION_COLUMN})"
    )
    return (
        f"INSERT INTO intent_sessions ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {', '.join(assignments)}"
    )


INTENT_SESSIONS_SQL = _session_upsert_sql()


def _require_row(row, message_key: str):
    if row is None:
        raise MalformedMessage(f"extractor rejected message {message_key}")
    return row


def build_rows(messages: List[Dict[str, Any]]) -> Dict[str, List[tuple]]:
    grouped = collect_by_type(messages)
    events = [
        _require_row(_extract_behavioral_event_row(to_event_doc(m)), m["message_key"])
        for m in grouped[TYPE_EVENT]
    ]
    clicks = [
        _require_row(_extract_click_event_row(to_click_doc(m)), m["message_key"])
        for m in grouped[TYPE_CLICK]
    ]
    sessions = []
    for m in grouped[TYPE_SESSION_SNAPSHOT]:
        # Extractor output = 18 intent_sessions columns + trailing version (updatedAt),
        # which is exactly the guarded insert's column order.
        row = _require_row(_extract_session_history_row(to_session_doc(m)), m["message_key"])
        sessions.append(tuple(row))
    return {"events": events, "clicks": clicks, "sessions": sessions}


def _assert_version_column_exists(cursor) -> None:
    cursor.execute("SHOW COLUMNS FROM intent_sessions LIKE %s", (SESSION_VERSION_COLUMN,))
    if cursor.fetchone() is None:
        raise RuntimeError(
            "intent_sessions.source_updated_at is missing; apply the approved schema change first"
        )


def _execute_chunks(cursor, sql: str, rows: List[tuple]) -> None:
    # Deliberately NOT executemany_chunked: that helper commits at the end of every
    # call, which would split one brand batch into several commits.
    for start in range(0, len(rows), EXECUTEMANY_CHUNK_SIZE):
        cursor.executemany(sql, rows[start : start + EXECUTEMANY_CHUNK_SIZE])


def apply_batch(cursor, connection, messages: List[Dict[str, Any]]) -> Dict[str, int]:
    """Apply one brand's messages in a single transaction. Any exception leaves
    the transaction uncommitted; the caller's get_db_cursor rolls it back."""
    rows = build_rows(messages)
    if rows["sessions"]:
        _assert_version_column_exists(cursor)

    _execute_chunks(cursor, BEHAVIORAL_EVENTS_SQL, rows["events"])
    _execute_chunks(cursor, CLICK_EVENTS_SQL, rows["clicks"])
    _execute_chunks(cursor, INTENT_SESSIONS_SQL, rows["sessions"])
    connection.commit()

    return {
        "events": len(rows["events"]),
        "clicks": len(rows["clicks"]),
        "sessions": len(rows["sessions"]),
    }
