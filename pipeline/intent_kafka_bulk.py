"""
Fast path for applying one brand's batch: a handful of multi-row statements instead of several
statements per event.

Why. The per-message writer (pipeline/intent_kafka_writer.apply_batch) sends a SAVEPOINT, an INSERT
and a RELEASE for every event and about five more statements per actor. Against a remote MySQL each
statement is a network round trip, so a batch of 1,724 events took 60 s (about 35 ms an event). This
module does the same work in memory and writes it with multi-row statements.

How it stays equivalent. Per message it does exactly what the per-message writer does, in the same
order and with the same session-id generation: resolve timing -> ATC claim (before the duplicate check,
as before) -> build the row -> duplicate check -> close the previous session -> advance the cursor.
Only the I/O differs: existing event ids, ATC claims and cursor rows are read up front in a few
statements, the decisions are made in memory, and the results are written in bulk. tests/
test_intent_kafka_fast_path.py runs both paths on randomised streams and requires identical state.

The old path is the fallback, untouched. Anything the fast path cannot do exactly (a row MySQL or the
extractor rejects, ids that MySQL's case-insensitive collation would treat as the same key) rolls the
batch back to a savepoint taken before it started and re-runs apply_batch, which isolates the bad
message as before. Infrastructure errors (deadlock, lost connection, anything unclassified) propagate
exactly as they do from apply_batch.

Assumes one writer at a time, which the worker's run lock guarantees; the unique keys and the
ON DUPLICATE KEY no-op statements remain the backstop if that is ever violated.
"""

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.intent_kafka_contract import IntentMessage
from pipeline.intent_kafka_writer import (
    BrandResult,
    WorkItem,
    _closed_session_row,
    _event_row,
    _group_by_actor,
    apply_batch,
    is_data_error,
)
from pipeline.intent_session_state import (
    DEFAULT_SESSION_TIMEOUT_S,
    ActorCursor,
    closed_session_doc,
    new_session_id,
    resolve_timing,
    seq_entry,
)
from pipeline.state import logger


class FastPathUnavailable(Exception):
    """The batch needs the exact per-message treatment (see module docstring)."""


class _Work:
    """An actor's cursor while the batch is applied. Mutated in place, so a long session is not
    copied once per event. Has the attributes resolve_timing reads."""

    __slots__ = ("actor_id", "session_id", "session_start", "last_event_at", "last_event_id", "seq", "changed")

    def __init__(self, actor_id: str, cursor: Optional[ActorCursor]) -> None:
        self.actor_id = actor_id
        self.changed = False
        if cursor is None:
            self.session_id = self.session_start = self.last_event_at = self.last_event_id = None
            self.seq: Dict[str, Dict[str, Any]] = {}
        else:
            self.session_id, self.session_start = cursor.session_id, cursor.session_start
            self.last_event_at, self.last_event_id = cursor.last_event_at, cursor.last_event_id
            self.seq = dict(cursor.events_seq)  # the one copy

    def to_cursor(self) -> ActorCursor:
        return ActorCursor(self.actor_id, self.session_id, self.session_start, self.last_event_at,
                           self.last_event_id, self.seq)


def _collision_check(items: Sequence[WorkItem]) -> None:
    """MySQL compares these keys case-insensitively; Python does not. Two ids that differ only in case
    would be one row in MySQL, so such a batch is left to the per-message path."""
    actors: Dict[str, str] = {}
    events: Dict[Tuple[str, str], str] = {}
    for item in items:
        message = item.message
        identity = message.identity
        if identity and actors.setdefault(identity.casefold(), identity) != identity:
            raise FastPathUnavailable("actor ids that differ only in case")
        kind = "click" if message.type == "click" else "event"
        event_id = message.event_id.strip()
        if events.setdefault((kind, event_id.casefold()), event_id) != event_id:
            raise FastPathUnavailable("event ids that differ only in case")


def _apply_bulk(store, items: Sequence[WorkItem], timeout_s: int) -> BrandResult:
    result = BrandResult(path="fast")
    _collision_check(items)
    actorless, by_actor = _group_by_actor(items)
    actor_ids = sorted(by_actor)

    # ---- reads: a few statements for the whole batch ----
    cursors = store.lock_cursors(actor_ids) if actor_ids else {}
    event_ids: Dict[str, Set[str]] = {"event": set(), "click": set()}
    for item in items:
        event_ids["click" if item.message.type == "click" else "event"].add(item.message.event_id.strip())
    existing = {kind: {i.casefold() for i in (store.existing_event_ids(kind, sorted(ids)) if ids else ())}
                for kind, ids in event_ids.items()}
    candidate_pairs = []
    for actor_id in actor_ids:
        cursor = cursors[actor_id.casefold()]
        if cursor is not None:
            candidate_pairs.extend((cursor.session_id, i.message.product_id)
                                   for i in by_actor[actor_id] if i.message.product_id is not None)
    existing_claims = {(s.casefold(), p.casefold()) for s, p in (store.existing_atc(candidate_pairs) if candidate_pairs else ())}

    # ---- decisions: in memory, message by message, in the per-message writer's order ----
    new_rows: Dict[str, List[tuple]] = {"event": [], "click": []}
    seen: Dict[str, Set[str]] = {"event": set(), "click": set()}
    new_claims: List[Tuple[str, str]] = []
    claim_keys: Set[Tuple[str, str]] = set()
    session_rows: List[tuple] = []

    def apply_one(message: IntentMessage, actor_id: Optional[str], work: Optional[_Work]) -> None:
        when = message.occurred_at
        has_session = work is not None and work.session_id is not None   # None means 'no cursor' to resolve_timing
        timing = resolve_timing(work if has_session else None, when, timeout_s) if actor_id else None
        session_id = timing.session_id if timing else new_session_id()

        product_id = message.product_id
        if product_id is not None:
            key = (session_id.casefold(), product_id.casefold())
            if key in existing_claims or key in claim_keys:
                result.atc_deduped += 1
                return
            claim_keys.add(key)
            new_claims.append((session_id, product_id))

        kind, row = _event_row(message, session_id, actor_id)
        row_key = row[0].casefold()
        if row_key in existing[kind] or row_key in seen[kind]:
            result.duplicates += 1
            return
        seen[kind].add(row_key)
        new_rows[kind].append(row)
        result.applied += 1

        if not actor_id:
            return
        if timing.is_new_session and has_session:
            session_rows.append(_closed_session_row(closed_session_doc(work.to_cursor())))
            result.sessions_closed += 1
        if timing.is_new_session:
            work.seq = {"1": seq_entry(message)}
        else:
            work.seq[str(len(work.seq) + 1)] = seq_entry(message)
        if timing.is_new_session or work.last_event_at <= when:
            work.last_event_at, work.last_event_id = when, message.event_id   # a tolerated late event never moves it back
        work.session_id, work.session_start = timing.session_id, timing.session_start
        work.changed = True

    for item in actorless:
        apply_one(item.message, None, None)

    changed: List[ActorCursor] = []
    for actor_id in actor_ids:
        cursor = cursors[actor_id.casefold()]
        work = _Work(actor_id, cursor)
        for item in by_actor[actor_id]:
            apply_one(item.message, actor_id, work)
        if work.changed:
            changed.append(work.to_cursor())

    # ---- writes: multi-row statements ----
    if new_claims:
        store.insert_claims(new_claims)
    for kind in ("event", "click"):
        if new_rows[kind]:
            store.insert_events(kind, new_rows[kind])
    if session_rows:
        store.upsert_sessions(session_rows)
    if changed:
        store.save_cursors(changed)
    return result


def apply_batch_fast(store, items: Sequence[WorkItem], timeout_s: int = DEFAULT_SESSION_TIMEOUT_S) -> BrandResult:
    """Applies the batch with the fast path; falls back to the per-message writer if it cannot."""
    token = store.savepoint()
    try:
        result = _apply_bulk(store, items, timeout_s)
    except FastPathUnavailable as exc:
        reason = str(exc)
    except Exception as exc:
        if not is_data_error(exc):
            raise                      # infrastructure: the caller rolls back and retries, as for apply_batch
        reason = f"{type(exc).__name__}: {str(exc)[:200]}"
    else:
        store.release(token)
        return result

    store.rollback_to(token)           # undo anything the fast path already wrote
    store.release(token)
    logger.warning(f"[intent-kafka] category=fast_path_fallback records={len(items)} reason={reason}")
    fallback = apply_batch(store, items, timeout_s)
    fallback.path = "fallback"
    return fallback
