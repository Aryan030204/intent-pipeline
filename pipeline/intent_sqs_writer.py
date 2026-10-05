"""
Batch writer for SQS intent messages.

Row construction reuses the extractors in pipeline/intent_events.py. The event and
click SQL mirrors the existing upserts (same columns). The session upsert adds a
source_updated_at version guard so an older snapshot cannot overwrite a newer one.

Transaction model (one brand, one batch):
- Each message is applied inside its own SAVEPOINT. A message that fails for a
  message-level reason (bad data, extractor rejection) is rolled back alone and
  reported in counts["failed_messages"]. Its healthy neighbours still commit.
- Batch-fatal errors (deadlock 1213, lock wait timeout 1205, lost connection
  2006/2013, actor lock timeout) abort the whole transaction, because InnoDB rolls
  back everything in those cases. They propagate, and the worker keeps all of the
  brand's messages for redelivery.
- Nothing commits per chunk. The brand's transaction commits once.
"""

import os
import uuid
from typing import Any, Callable, Dict, List

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
from pipeline.intent_session_state import (
    ORPHAN,
    apply_event,
    atc_product_id,
    decide_timing,
    to_naive_utc,
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
from pipeline.intent_sqs_store import ActorLockTimeout, MySqlIntentStore, verify_state_schema
from pipeline.state import logger

SESSION_VERSION_COLUMN = "source_updated_at"
DEFAULT_SESSION_TIMEOUT_S = 1800
BATCH_FATAL_MYSQL_ERRNOS = {1205, 1213, 2006, 2013, 2055}


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
    """Guarded upsert. MySQL evaluates ON DUPLICATE KEY UPDATE assignments left to
    right, so source_updated_at is assigned LAST: every guard reads the stored version,
    and the version advances only when the guard passed. Equal versions are applied."""
    insert_columns = list(_INTENT_SESSIONS_UPSERT_COLUMNS) + [SESSION_VERSION_COLUMN]
    columns_sql = ", ".join(insert_columns)
    placeholders = ", ".join(["%s"] * len(insert_columns))
    guard = (
        f"{SESSION_VERSION_COLUMN} IS NULL "
        f"OR {SESSION_VERSION_COLUMN} <= VALUES({SESSION_VERSION_COLUMN})"
    )
    assignments = [f"{c} = IF({guard}, VALUES({c}), {c})" for c in _INTENT_SESSIONS_UPDATE_COLUMNS]
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
        row = _require_row(_extract_session_history_row(to_session_doc(m)), m["message_key"])
        sessions.append(tuple(row))
    return {"events": events, "clicks": clicks, "sessions": sessions}


def _session_timeout_seconds() -> int:
    raw = os.environ.get("SESSION_TIMEOUT", "").strip()
    try:
        return int(raw) if raw else DEFAULT_SESSION_TIMEOUT_S
    except ValueError:
        return DEFAULT_SESSION_TIMEOUT_S


def _new_counts() -> Dict[str, int]:
    return {"events": 0, "clicks": 0, "sessions": 0, "duplicates": 0, "atc_deduped": 0,
            "closed": 0, "orphans": 0, "failed": 0}


def _merge(total: Dict[str, int], local: Dict[str, int]) -> None:
    for key, value in local.items():
        total[key] = total.get(key, 0) + value


def is_batch_fatal(exc: BaseException) -> bool:
    """True when InnoDB or the connection has taken the whole transaction with it, so
    no savepoint can salvage the rest of the batch."""
    if isinstance(exc, ActorLockTimeout):
        return True
    if getattr(exc, "errno", None) in BATCH_FATAL_MYSQL_ERRNOS:
        return True
    return isinstance(exc, (ConnectionError, TimeoutError))


def _run_isolated(store, message, action: Callable, counts, failed):
    """Run one message's writes inside its own savepoint. Returns the action's result,
    or None after rolling back just this message."""
    token = store.begin_group()
    local = _new_counts()
    try:
        result = action(local)
    except Exception as exc:
        if is_batch_fatal(exc):
            raise
        store.rollback_group(token)
        failed.append(message)
        counts["failed"] += 1
        logger.warning(
            f"[intent-sqs] category=message_failed event_id={message.get('event_id', '')} "
            f"message_key={message.get('message_key', '')} reason={type(exc).__name__}"
        )
        return None
    store.commit_group(token)
    _merge(counts, local)
    return result


def _insert_event_row(store, message: Dict[str, Any], session_id: str, actor: str, local: Dict[str, int]) -> bool:
    doc = dict(message)
    doc["session_id"] = session_id
    doc["actor_id"] = actor
    if message["type"] == TYPE_EVENT:
        row = _require_row(_extract_behavioral_event_row(to_event_doc(doc)), message["message_key"])
        kind, key = "event", "events"
    else:
        row = _require_row(_extract_click_event_row(to_click_doc(doc)), message["message_key"])
        kind, key = "click", "clicks"
    if store.insert_event(kind, row):
        local[key] += 1
        return True
    local["duplicates"] += 1
    return False


def _closed_session_row(closed: Dict[str, Any]) -> tuple:
    return tuple(_require_row(_extract_session_history_row(closed), closed["session_id"]))


def _apply_stateful(store, message, actor, cursor_state, timeout_s, local):
    when = to_naive_utc(_parse_ts(message["occurred_at"], "occurred_at"))
    if decide_timing(cursor_state, when, timeout_s) == ORPHAN:
        # Belongs to an earlier, already closed session. It is stored under its own
        # id and changes neither the cursor nor any session.
        _insert_event_row(store, message, str(uuid.uuid4()), actor, local)
        local["orphans"] += 1
        return cursor_state, False

    next_state, closed, session_id = apply_event(cursor_state, message, when, timeout_s, actor)

    pid = atc_product_id(message)
    if pid is not None and not store.claim_atc(session_id, pid):
        local["atc_deduped"] += 1
        return cursor_state, False

    if not _insert_event_row(store, message, session_id, actor, local):
        return cursor_state, False

    if closed is not None:
        store.upsert_session_rows(INTENT_SESSIONS_SQL, [_closed_session_row(closed)])
        local["closed"] += 1
        local["sessions"] += 1
    return next_state, True


def lock_acquisition_order(actor_keys) -> List[str]:
    """Single source of truth for actor lock order: sorted actor key (actor_id || client_id).
    Every consumer uses the same order, so two consumers cannot wait on each other."""
    return sorted(actor_keys)


def apply_messages(store, messages: List[Dict[str, Any]], timeout_s: int) -> Dict[str, Any]:
    """State-aware apply of one brand's messages. Returns counts plus
    counts["failed_messages"]: the message objects that were rolled back and must be
    redelivered. Healthy messages are not in that list."""
    counts = _new_counts()
    failed: List[Dict[str, Any]] = []
    grouped = collect_by_type(messages)

    for m in grouped[TYPE_SESSION_SNAPSHOT]:
        def _snapshot(local, m=m):
            row = _require_row(_extract_session_history_row(to_session_doc(m)), m["message_key"])
            store.upsert_session_rows(INTENT_SESSIONS_SQL, [tuple(row)])
            local["sessions"] += 1
        _run_isolated(store, m, _snapshot, counts, failed)

    stateful = sorted(
        grouped[TYPE_EVENT] + grouped[TYPE_CLICK],
        key=lambda m: (_parse_ts(m["occurred_at"], "occurred_at"), str(m["event_id"])),
    )
    by_actor: Dict[str, List[Dict[str, Any]]] = {}
    for m in stateful:
        actor = m.get("actor_id") or m.get("client_id")
        if actor:
            by_actor.setdefault(actor, []).append(m)
        else:
            # No actor: fresh session per event, no cursor, no actor lock.
            _run_isolated(
                store, m,
                lambda local, m=m: _insert_event_row(store, m, str(uuid.uuid4()), "", local),
                counts, failed,
            )

    for actor in lock_acquisition_order(by_actor.keys()):
        actor_messages = by_actor[actor]
        store.lock_actor(actor)
        try:
            cursor_state = store.get_cursor_for_update(actor)
        except Exception as exc:
            if is_batch_fatal(exc):
                raise
            # The actor's cursor cannot be read, so none of its messages can be applied.
            failed.extend(actor_messages)
            counts["failed"] += len(actor_messages)
            logger.warning(f"[intent-sqs] category=cursor_read_failed reason={type(exc).__name__}")
            continue

        changed = False
        for m in actor_messages:
            result = _run_isolated(
                store, m,
                lambda local, m=m, cs=cursor_state: _apply_stateful(store, m, actor, cs, timeout_s, local),
                counts, failed,
            )
            if result is not None:
                cursor_state, applied = result
                changed = changed or applied

        if changed:
            store.save_cursor(actor, cursor_state)

    counts["failed_messages"] = failed
    return counts


def apply_batch(cursor, connection, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """One brand's messages in one transaction. Batch-fatal errors propagate, so the
    caller's transaction rolls back and every message stays for redelivery."""
    verify_state_schema(cursor)
    store = MySqlIntentStore(cursor)
    try:
        counts = apply_messages(store, messages, _session_timeout_seconds())
    finally:
        store.unlock_all()
    connection.commit()
    return counts
