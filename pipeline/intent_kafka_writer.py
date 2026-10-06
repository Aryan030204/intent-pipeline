"""
Applies one brand's batch of intent messages inside an already-open MySQL transaction.

Business logic is the old /track pipeline, order for order (sessionState.js + ingest.js):
  read the actor's cursor -> resolve session timing -> ATC dedupe -> insert the event or
  click -> only if it was newly inserted: close the previous session, write its
  intent_sessions row, commit the cursor.

Isolation. Each message runs in its own SAVEPOINT and each actor group in another, so a
message MySQL rejects for data reasons (too long, bad value) is rolled back alone and
reported in BrandResult.failed while its healthy neighbours still commit. An
infrastructure error (deadlock, lost connection, lock timeout, anything unclassified)
propagates instead: InnoDB may already have rolled the whole transaction back, and the
consumer must retry the batch without committing offsets.

Ordering. Messages are applied in the order given. The consumer passes them in Kafka
order within each partition (merged across partitions by occurred_at). Actors are locked
(SELECT ... FOR UPDATE on the cursor row) in sorted order, so two consumers overlapping
during a rebalance cannot deadlock each other.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Hashable, List, Optional, Sequence, Tuple

from pipeline.intent_events import (
    _extract_behavioral_event_row,
    _extract_click_event_row,
    _extract_session_history_row,
)
from pipeline.intent_kafka_contract import InvalidMessage, IntentMessage, TYPE_CLICK
from pipeline.intent_session_state import (
    DEFAULT_SESSION_TIMEOUT_S,
    ActorCursor,
    commit_event,
    new_session_id,
    resolve_timing,
)
from pipeline.state import logger

# MySQL errors that mean "this row's data is unacceptable", never "the server is unwell".
DATA_ERRNOS = {1048, 1253, 1264, 1265, 1292, 1366, 1406, 1451, 1452, 3140, 3143, 3150}


@dataclass
class WorkItem:
    ref: Hashable  # opaque to this module; the consumer uses (topic, partition, offset)
    message: IntentMessage


@dataclass
class BrandResult:
    applied: int = 0
    duplicates: int = 0
    atc_deduped: int = 0
    sessions_closed: int = 0
    failed: Dict[Hashable, str] = field(default_factory=dict)  # ref -> reason, data errors only

    def counts(self) -> Dict[str, int]:
        return {
            "applied": self.applied,
            "duplicates": self.duplicates,
            "atc_deduped": self.atc_deduped,
            "sessions_closed": self.sessions_closed,
            "failed": len(self.failed),
        }


def is_data_error(exc: BaseException) -> bool:
    """True only for errors that retrying can never fix. Everything else, including
    unknown exceptions, is treated as infrastructure so a bug blocks and alerts instead of
    quietly sending healthy events to the DLQ."""
    if isinstance(exc, InvalidMessage):
        return True
    errno = getattr(exc, "errno", None)
    if errno in DATA_ERRNOS:
        return True
    try:
        from mysql.connector import errors as mysql_errors

        return isinstance(exc, mysql_errors.DataError)
    except ImportError:  # pragma: no cover
        return False


def _require(row, what: str):
    if row is None:
        raise InvalidMessage(f"{what} rejected by the row extractor")
    return row


def _event_row(message: IntentMessage, session_id: str, actor_id: Optional[str]) -> Tuple[str, tuple]:
    doc = message.to_doc(session_id, actor_id)
    if message.type == TYPE_CLICK:
        return "click", _require(_extract_click_event_row(doc), "click")
    return "event", _require(_extract_behavioral_event_row(doc), "event")


def _closed_session_row(doc: Dict[str, Any]) -> tuple:
    row = _require(_extract_session_history_row(doc), "closed session")
    return tuple(row[:-1])  # the trailing updatedAt is not a column


def _apply_one(store, item: WorkItem, actor_id: Optional[str], cursor: Optional[ActorCursor],
               timeout_s: int, result: BrandResult) -> Optional[ActorCursor]:
    """Applies one message. Returns the actor's new cursor, or the unchanged one when the
    message was a duplicate or an ATC dedupe."""
    message = item.message
    when = message.occurred_at
    timing = resolve_timing(cursor, when, timeout_s) if actor_id else None
    session_id = timing.session_id if timing else new_session_id()

    product_id = message.product_id
    if product_id is not None and not store.claim_atc(session_id, product_id):
        result.atc_deduped += 1
        return cursor

    kind, row = _event_row(message, session_id, actor_id)
    if not store.insert_event(kind, row):
        result.duplicates += 1
        return cursor
    result.applied += 1

    if not actor_id:
        return cursor
    new_cursor, closed = commit_event(cursor, timing, when, message, actor_id)
    if closed is not None:
        store.upsert_session(_closed_session_row(closed))
        result.sessions_closed += 1
    return new_cursor


def _group_by_actor(items: Sequence[WorkItem]) -> Tuple[List[WorkItem], Dict[str, List[WorkItem]]]:
    actorless: List[WorkItem] = []
    by_actor: Dict[str, List[WorkItem]] = {}
    for item in items:
        identity = item.message.identity
        if identity:
            by_actor.setdefault(identity, []).append(item)
        else:
            actorless.append(item)
    return actorless, by_actor


def apply_batch(store, items: Sequence[WorkItem], timeout_s: int = DEFAULT_SESSION_TIMEOUT_S) -> BrandResult:
    """Applies the messages of one brand. Raises on infrastructure errors."""
    result = BrandResult()
    actorless, by_actor = _group_by_actor(items)

    def isolated(unit: Sequence[WorkItem], work: Callable[[], None]) -> bool:
        token = store.savepoint()
        try:
            work()
        except Exception as exc:
            if not is_data_error(exc):
                raise
            store.rollback_to(token)
            for item in unit:
                result.failed[item.ref] = f"{type(exc).__name__}: {exc}"
            logger.warning(
                f"[intent-kafka] category=message_rejected refs={[i.ref for i in unit][:3]} "
                f"reason={type(exc).__name__}: {str(exc)[:200]}"
            )
            return False
        store.release(token)
        return True

    for item in actorless:
        isolated([item], lambda item=item: _apply_one(store, item, None, None, timeout_s, result))

    for actor_id in sorted(by_actor):
        group = by_actor[actor_id]
        before = result.counts()

        def run_group(actor_id=actor_id, group=group) -> None:
            cursor = store.get_cursor_for_update(actor_id)
            changed = False
            for item in group:
                outcome: Dict[str, Any] = {}

                def one(item=item) -> None:
                    outcome["cursor"] = _apply_one(store, item, actor_id, cursor, timeout_s, result)

                if isolated([item], one):
                    changed = changed or outcome["cursor"] is not cursor
                    cursor = outcome["cursor"]
            if changed:
                store.save_cursor(cursor)

        # The actor-level savepoint exists for the cursor save, the only write outside a
        # per-message savepoint. If it is rejected the whole group is rolled back as one.
        if not isolated(group, run_group):
            # Counters recorded before the rollback no longer describe committed work.
            for key in ("applied", "duplicates", "atc_deduped", "sessions_closed"):
                setattr(result, key, before[key])
    return result
