"""
Pure session/state logic for the SQS intent consumer. Mirrors the old producer
state machine (alerts-service services/intent/sessionState.js) for every ordered
case, and adds a deterministic policy for SQS standard reordering:

- Timeout, close and events_seq semantics are unchanged.
- An event up to 30 s before the actor's last event stays in the session and
  never moves last_event_at backwards (old behaviour).
- An event that is late but still inside the current session's span
  (session_start <= when < last_event_at - 30 s) stays in the session. The old
  code split the session here solely because SQS delivered the event late.
- An event before the current session's start belongs to an earlier, already
  closed session. It cannot be reattached without rewriting committed rows, so
  it is stored as an orphan with its own session id and changes no cursor and
  closes nothing.

Times are naive UTC datetimes internally (see Timestamps below).

Timestamps: the producer (alerts-service services/intent/sqsProducer.js) already
converts occurred_at to the brand's store-local wall clock
(services/intent/timezone.js toStoreLocalOccurredAt) and encodes that wall clock
with a trailing Z. The worker stores the parsed wall clock unchanged. Converting
again here would shift every event by the brand's offset. Consequence: for zones
with DST, the producer's display value does not carry the true instant, so gap
arithmetic is off by the DST shift at the two transitions a year. Brands in
fixed-offset zones (e.g. Asia/Kolkata) are unaffected.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

NEGATIVE_TOLERANCE = timedelta(seconds=30)

NEW_SESSION = "new_session"
CONTINUE = "continue"
CONTINUE_LATE = "continue_late"
ORPHAN = "orphan"


def to_naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def decide_timing(cursor: Optional[Dict[str, Any]], when: datetime, timeout_s: int) -> str:
    if cursor is None:
        return NEW_SESSION
    last = cursor["last_event_at"]
    start = cursor["session_start"]
    if when > last + timedelta(seconds=timeout_s):
        return NEW_SESSION
    if when < start - NEGATIVE_TOLERANCE:
        return ORPHAN
    if when >= last - NEGATIVE_TOLERANCE:
        return CONTINUE
    return CONTINUE_LATE


def close_session_row(cursor: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
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

    Does not mutate `cursor`. For ORPHAN the returned cursor is the input cursor
    unchanged and the session id is a fresh UUID that no cursor refers to."""
    decision = decide_timing(cursor, when, timeout_s)

    if decision == ORPHAN:
        return dict(cursor), None, str(uuid.uuid4())

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
    gid://shopify/Product/9 becomes "Product:9"; anything else is returned as-is."""
    if "/" in raw_id:
        parts = raw_id.split("/")
        return f"{parts[-2]}:{parts[-1]}"
    return raw_id


def atc_product_id(message: Dict[str, Any]) -> Optional[str]:
    """Product id used for ATC dedupe, matching the old Mongo path. SYNTH: and
    FALLBACK: ids never dedupe."""
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
