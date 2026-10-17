"""
Closes sessions whose visitor went quiet.

The session state machine (intent_session_state.py) only closes a session when the same actor's NEXT
event arrives more than the session timeout later. A visitor who never comes back therefore leaves an open
cursor forever and no intent_sessions row: single-page bounces and other static visits were invisible to
scoring and to every session count. This sweep closes them.

A swept session is exactly what the normal close would have written had the actor returned later: end =
the last event, time spent = last event - first event (so a one-page visit is 0 ms), the same row shape.
The cursor is then deleted, so the actor's next event starts a new session - the same outcome as the normal
path, where a gap above the timeout always starts one.

"Idle" is judged against the newest event this brand has ingested, not the wall clock. Event times are the
store-local clock /track sent, and the worker may be behind (a replay, an outage), so a wall-clock test
could close a session whose next event is still sitting in Kafka. Events are applied in send order, so
nothing still to come for an actor can be older than the newest ingested event minus the clock-skew margin.

The sweep runs inside the worker's run lock, so the worker is the only writer of intent_actor_cursors.
Scoring needs no change: a one-event, zero-duration session is already excluded there as "zero_signal".
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pipeline.intent_kafka_contract import InvalidMessage
from pipeline.intent_kafka_writer import _closed_session_row
from pipeline.intent_session_state import closed_session_doc
from pipeline.state import logger

SWEEP_MARGIN_S = 60        # slack on top of the session timeout, for clock skew and out-of-order arrival
SWEEP_CHUNK = 500          # sessions closed per MySQL transaction
SWEEP_MAX_PER_RUN = 20000  # per brand per cycle; a large backlog is worked off over several cycles


@dataclass
class SweepResult:
    closed: int = 0
    skipped: int = 0
    cutoff: Optional[datetime] = None


def sweep_cutoff(latest: Optional[datetime], timeout_s: int, margin_s: int = SWEEP_MARGIN_S) -> Optional[datetime]:
    """Open sessions whose last event is before this are finished. None when nothing has been ingested."""
    if latest is None:
        return None
    return latest - timedelta(seconds=timeout_s + margin_s)


def sweep_chunk(store, cutoff: datetime, limit: int = SWEEP_CHUNK) -> SweepResult:
    """Closes up to `limit` idle sessions inside the caller's transaction."""
    result = SweepResult(cutoff=cutoff)
    cursors = store.idle_cursors(cutoff, limit)
    rows, done = [], []
    for cursor in cursors:
        try:
            rows.append(_closed_session_row(closed_session_doc(cursor)))
        except InvalidMessage as exc:  # a cursor the normal close would also have rejected; left in place
            result.skipped += 1
            logger.error(f"[intent-kafka] category=sweep_skipped actor_id={cursor.actor_id} session_id={cursor.session_id} {exc}")
            continue
        done.append(cursor)
    if done:
        store.upsert_sessions(rows)
        store.delete_cursors(done)
    result.closed = len(done)
    return result


def sweep_brand(db, brand_index: int, timeout_s: int, margin_s: int = SWEEP_MARGIN_S,
                chunk: int = SWEEP_CHUNK, max_total: int = SWEEP_MAX_PER_RUN,
                should_stop=lambda: False) -> SweepResult:
    """Closes this brand's idle sessions, one transaction per chunk, until none are left, the per-run limit
    is reached, a chunk makes no progress, or should_stop() says so."""
    with db.transaction(brand_index) as store:
        cutoff = sweep_cutoff(store.latest_cursor_time(), timeout_s, margin_s)
    total = SweepResult(cutoff=cutoff)
    if cutoff is None:
        return total
    while total.closed < max_total and not should_stop():
        with db.transaction(brand_index) as store:
            part = sweep_chunk(store, cutoff, min(chunk, max_total - total.closed))
        total.closed += part.closed
        total.skipped += part.skipped
        if part.closed == 0:
            break
    return total
