"""
Pure session/state logic for the SQS intent consumer. Mirrors the old producer
state machine (alerts-service services/intent/sessionState.js): same timeout,
same 30 s negative tolerance, same rollover/closing arithmetic, same
contiguous events_seq. No database handles here, so every rule is unit-testable.

Times are naive UTC datetimes internally. MySQL returns naive values, and the
rules compare instants, so one representation avoids tz-aware/naive mixing.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

NEGATIVE_TOLERANCE = timedelta(seconds=30)

NEW_SESSION = "new_session"
CONTINUE = "continue"


def to_naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def decide_timing(cursor: Optional[Dict[str, Any]], when: datetime, timeout_s: int) -> str:
    """Same predicate as sessionState.js:35-39. A missing cursor, a gap above the
    timeout, or an event more than 30 s before the last event starts a new session."""
    if cursor is None:
        return NEW_SESSION
    gap = when - cursor["last_event_at"]
    if gap > timedelta(seconds=timeout_s) or gap < -NEGATIVE_TOLERANCE:
        return NEW_SESSION
    return CONTINUE


def close_session_row(cursor: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
    """Session document in the shape _extract_session_history_row reads.
    session_end = last event time; session_time_spent = last event - session start,
    in ms. The timeout gap is excluded, as in sessionState.js:73-122."""
    last = cursor["last_event_at"]
    start = cursor["session_start"]
    spent_ms = int((last - start).total_seconds() * 1000)
    return {
        "session_id": cursor["session_id"],
        "actor_id": actor_id,
        "session_start": start.replace(tzinfo=timezone.utc),
        "session_end": last.replace(tzinfo=timezone.utc),
        "session_time_spent": spent_ms,
        "events_seq": cursor["events_seq"],
        "updatedAt": last.replace(tzinfo=timezone.utc),
    }


def _seq_entry(message: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_name": message.get("event_name"),
        "event_id": message.get("event_id"),
        "click_bucket": message.get("click_bucket"),
        "client_id": message.get("client_id"),
        "visitor_id": message.get("visitor_id"),
    }


def apply_event(
    cursor: Optional[Dict[str, Any]],
    message: Dict[str, Any],
    when: datetime,
    timeout_s: int,
    actor_id: str,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], str]:
    """Returns (next_cursor, closed_previous_session_or_None, session_id).

    Does not mutate `cursor`, so a caller can discard the result when the event
    turns out to be a duplicate or a deduped ATC."""
    decision = decide_timing(cursor, when, timeout_s)
    if decision == NEW_SESSION:
        closed = close_session_row(cursor, actor_id) if cursor else None
        session_id = str(uuid.uuid4())
        next_cursor = {
            "actor_id": actor_id,
            "session_id": session_id,
            "session_start": when,
            "last_event_at": when,
            "last_event_id": message.get("event_id"),
            "events_seq": {"1": _seq_entry(message)},
        }
        return next_cursor, closed, session_id

    seq = dict(cursor["events_seq"])
    seq[str(len(seq) + 1)] = _seq_entry(message)
    next_cursor = dict(cursor)
    next_cursor["events_seq"] = seq
    if when > cursor["last_event_at"]:
        next_cursor["last_event_at"] = when
        next_cursor["last_event_id"] = message.get("event_id")
    return next_cursor, None, cursor["session_id"]


def normalize_shopify_product_id(raw_id: str) -> str:
    """Same rule as alerts-service normalize.js normalizeShopifyId: a GID such as
    gid://shopify/Product/9 becomes "Product:9"; anything else is returned as-is.
    The producer already normalizes, so this is an idempotent guard."""
    if "/" in raw_id:
        parts = raw_id.split("/")
        return f"{parts[-2]}:{parts[-1]}"
    return raw_id


def atc_product_id(message: Dict[str, Any]) -> Optional[str]:
    """Product id used for ATC dedupe, matching the old Mongo path (ingest.js
    productId = resolveProductId). SYNTH: and FALLBACK: ids never dedupe, because
    the old path synthesized a per-event id for them."""
    if message.get("event_name") != "product_added_to_cart":
        return None
    raw = message.get("raw") or {}
    pid = raw.get("product_id") if isinstance(raw, dict) else None
    if pid is None:
        return None
    pid = normalize_shopify_product_id(str(pid).strip())
    if not pid or pid.startswith("SYNTH:") or pid.startswith("FALLBACK:"):
        return None
    return pid
