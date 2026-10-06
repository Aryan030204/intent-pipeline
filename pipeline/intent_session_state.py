"""
Per-actor session state machine for the Kafka intent consumer.

A line-for-line port of the producer-side state machine that /track ran before the
Kafka architecture (alerts-service services/intent/sessionState.js, removed in dashboard
commit 44b89911). The semantics are unchanged; only the transport and the storage moved.

- Identity: actor_id || client_id. With neither, every event is its own session and no
  cursor is kept (resolve_timing(None, ...) with actor None is handled by the writer).
- A new session starts when there is no cursor, when the gap to the cursor's last event
  exceeds the session timeout, or when the gap is more negative than the tolerance.
- A gap between -tolerance and 0 (small out-of-order arrival) stays in the session and
  never moves last_event_at backwards.
- A new session closes the previous one: session_end = previous last_event_at and
  session_time_spent = previous last_event_at - previous session_start (the timeout gap
  is excluded). Only closed sessions become intent_sessions rows.
- events_seq holds the open session's journey as "1".."N" and resets on a new session.

Times are naive datetimes in the store-local wall clock /track sent. Every gap is a
difference of two such values, so it is exact for fixed-offset zones (Asia/Kolkata). For
DST zones the gap is off by the DST shift at the two yearly transitions, a property of
the contract (the true instant is not sent), not of this module.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

DEFAULT_SESSION_TIMEOUT_S = 1800
NEGATIVE_GAP_TOLERANCE_S = 30


@dataclass(frozen=True)
class ActorCursor:
    actor_id: str
    session_id: str
    session_start: datetime
    last_event_at: datetime
    last_event_id: Optional[str]
    events_seq: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class Timing:
    session_id: str
    session_start: datetime
    is_new_session: bool


def new_session_id() -> str:
    return str(uuid.uuid4())


def resolve_timing(
    cursor: Optional[ActorCursor],
    when: datetime,
    timeout_s: int = DEFAULT_SESSION_TIMEOUT_S,
    tolerance_s: int = NEGATIVE_GAP_TOLERANCE_S,
) -> Timing:
    if cursor is None:
        return Timing(new_session_id(), when, True)
    gap = when - cursor.last_event_at
    if gap > timedelta(seconds=timeout_s) or gap < -timedelta(seconds=tolerance_s):
        return Timing(new_session_id(), when, True)
    return Timing(cursor.session_id, cursor.session_start, False)


def seq_entry(message) -> Dict[str, Any]:
    """The fields the session rollup reads from each events_seq step
    (pipeline/intent_events.py _extract_session_history_row)."""
    return {
        "event_name": message.event_name,
        "event_id": message.event_id,
        "click_bucket": message.click_bucket,
        "client_id": message.client_id,
        "visitor_id": message.visitor_id,
    }


def closed_session_doc(cursor: ActorCursor) -> Dict[str, Any]:
    """The session_history document for a session that a later event has closed, in the
    shape _extract_session_history_row reads."""
    return {
        "session_id": cursor.session_id,
        "actor_id": cursor.actor_id,
        "session_start": cursor.session_start,
        "session_end": cursor.last_event_at,
        "session_time_spent": int((cursor.last_event_at - cursor.session_start).total_seconds() * 1000),
        "events_seq": cursor.events_seq,
        "updatedAt": cursor.last_event_at,
    }


def commit_event(
    cursor: Optional[ActorCursor],
    timing: Timing,
    when: datetime,
    message,
    actor_id: str,
) -> Tuple[ActorCursor, Optional[Dict[str, Any]]]:
    """Cursor after a newly stored event, plus the closed previous session (or None).
    Call only after the event was inserted; a duplicate or deduped event changes nothing."""
    closed = closed_session_doc(cursor) if (timing.is_new_session and cursor is not None) else None

    if timing.is_new_session:
        seq = {"1": seq_entry(message)}
    else:
        seq = dict(cursor.events_seq)
        seq[str(len(seq) + 1)] = seq_entry(message)

    last_event_at, last_event_id = when, message.event_id
    if not timing.is_new_session and cursor.last_event_at > when:
        # A tolerated out-of-order event must not move the latest-event pointer backwards.
        last_event_at, last_event_id = cursor.last_event_at, cursor.last_event_id

    return (
        ActorCursor(
            actor_id=actor_id,
            session_id=timing.session_id,
            session_start=timing.session_start,
            last_event_at=last_event_at,
            last_event_id=last_event_id,
            events_seq=seq,
        ),
        closed,
    )


__all__ = [
    "ActorCursor",
    "Timing",
    "DEFAULT_SESSION_TIMEOUT_S",
    "NEGATIVE_GAP_TOLERANCE_S",
    "resolve_timing",
    "commit_event",
    "closed_session_doc",
    "seq_entry",
    "new_session_id",
]
