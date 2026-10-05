"""
Batch writer for SQS intent messages. Row construction reuses the Mongo
extractors in pipeline/intent_events.py, and the event/click SQL mirrors the
existing upserts exactly (same columns, same ON DUPLICATE KEY UPDATE). The
only new SQL is the session upsert, which adds a source_updated_at version
guard so an older snapshot can never overwrite a newer one.

Nothing here commits per chunk: the whole batch for one brand commits once, or
not at all, so the worker can rely on rollback + SQS redelivery.
"""

import os
import uuid
from typing import Any, Dict, List

from pipeline.db import EXECUTEMANY_CHUNK_SIZE
from pipeline.intent_session_state import apply_event, atc_product_id, to_naive_utc
from pipeline.intent_sqs_store import MySqlIntentStore, verify_state_schema
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
    _parse_ts,
    collect_by_type,
    to_click_doc,
    to_event_doc,
    to_session_doc,
)

SESSION_VERSION_COLUMN = "source_updated_at"
DEFAULT_SESSION_TIMEOUT_S = 1800


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


def _execute_chunks(cursor, sql: str, rows: List[tuple]) -> None:
    # Deliberately NOT executemany_chunked: that helper commits at the end of every
    # call, which would split one brand batch into several commits.
    for start in range(0, len(rows), EXECUTEMANY_CHUNK_SIZE):
        cursor.executemany(sql, rows[start : start + EXECUTEMANY_CHUNK_SIZE])


def _session_timeout_seconds() -> int:
    raw = os.environ.get("SESSION_TIMEOUT", "").strip()
    try:
        return int(raw) if raw else DEFAULT_SESSION_TIMEOUT_S
    except ValueError:
        return DEFAULT_SESSION_TIMEOUT_S


def _event_doc_with_identity(message: Dict[str, Any], session_id: str, actor_id: str) -> Dict[str, Any]:
    doc = dict(message)
    doc["session_id"] = session_id
    doc["actor_id"] = actor_id
    return doc


def apply_messages(store, messages: List[Dict[str, Any]], timeout_s: int) -> Dict[str, int]:
    """State-aware apply. Events and clicks are grouped by actor (actor_id || client_id)
    and applied in occurred_at order. A message that is a duplicate, or an ATC already
    claimed for its session, changes nothing: no row, no cursor move, no close."""
    counts = {"events": 0, "clicks": 0, "sessions": 0, "duplicates": 0, "atc_deduped": 0, "closed": 0}

    grouped = collect_by_type(messages)
    snapshot_rows = []
    for m in grouped[TYPE_SESSION_SNAPSHOT]:
        row = _require_row(_extract_session_history_row(to_session_doc(m)), m["message_key"])
        snapshot_rows.append(tuple(row))
    if snapshot_rows:
        store.upsert_session_rows(INTENT_SESSIONS_SQL, snapshot_rows)
        counts["sessions"] += len(snapshot_rows)

    stateful = sorted(
        grouped[TYPE_EVENT] + grouped[TYPE_CLICK],
        key=lambda m: (_parse_ts(m["occurred_at"], "occurred_at"), str(m["event_id"])),
    )
    by_actor: Dict[str, List[Dict[str, Any]]] = {}
    one_off: List[Dict[str, Any]] = []
    for m in stateful:
        actor = m.get("actor_id") or m.get("client_id")
        if actor:
            by_actor.setdefault(actor, []).append(m)
        else:
            one_off.append(m)

    for m in one_off:
        session_id = str(uuid.uuid4())
        _insert_message(store, m, session_id, "", counts)

    # Deterministic global lock order: every consumer acquires actor row locks in
    # the same sorted sequence, so two consumers can never hold locks in opposite
    # orders (the classic deadlock). See lock_acquisition_order().
    for actor in lock_acquisition_order(by_actor.keys()):
        actor_messages = by_actor[actor]
        cursor_state = store.get_cursor_for_update(actor)
        changed = False
        for m in actor_messages:
            when = to_naive_utc(_parse_ts(m["occurred_at"], "occurred_at"))
            next_state, closed, session_id = apply_event(cursor_state, m, when, timeout_s, actor)

            pid = atc_product_id(m)
            if pid is not None and not store.claim_atc(session_id, pid):
                counts["atc_deduped"] += 1
                continue

            if not _insert_message(store, m, session_id, actor, counts):
                continue

            if closed is not None:
                store.upsert_session_rows(INTENT_SESSIONS_SQL, [_closed_session_row(closed)])
                counts["closed"] += 1
                counts["sessions"] += 1
            cursor_state = next_state
            changed = True

        if changed:
            store.save_cursor(actor, cursor_state)

    return counts


def lock_acquisition_order(actor_keys) -> List[str]:
    """Single source of truth for actor lock order: plain lexicographic sort of the
    actor key (actor_id || client_id). Changing it would break mixed-version consumers."""
    return sorted(actor_keys)


def _insert_message(store, message: Dict[str, Any], session_id: str, actor: str, counts: Dict[str, int]) -> bool:
    doc = _event_doc_with_identity(message, session_id, actor)
    if message["type"] == TYPE_EVENT:
        row = _require_row(_extract_behavioral_event_row(to_event_doc(doc)), message["message_key"])
        kind, key = "event", "events"
    else:
        row = _require_row(_extract_click_event_row(to_click_doc(doc)), message["message_key"])
        kind, key = "click", "clicks"
    if store.insert_event(kind, row):
        counts[key] += 1
        return True
    counts["duplicates"] += 1
    return False


def _closed_session_row(closed: Dict[str, Any]) -> tuple:
    row = _require_row(_extract_session_history_row(closed), closed["session_id"])
    return tuple(row)


def apply_batch(cursor, connection, messages: List[Dict[str, Any]]) -> Dict[str, int]:
    """One brand's messages in a single transaction. Any exception leaves the
    transaction uncommitted; the caller's get_db_cursor rolls it back, and the
    worker keeps the messages un-deleted for SQS redelivery."""
    verify_state_schema(cursor)
    store = MySqlIntentStore(cursor)
    counts = apply_messages(store, messages, _session_timeout_seconds())
    connection.commit()
    return counts
