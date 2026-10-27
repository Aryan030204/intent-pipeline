"""
MySQL access for the Kafka intent consumer, plus a schema check and an in-memory twin
used by unit tests.

Cursor discipline (the 'Unread result found' lesson): every SELECT here is read with
fetchall() before the next statement runs, and the consumer opens its cursors buffered.
Either rule alone prevents the error; both are kept so a change to one cannot bring it back.

Write semantics (verified against a real MySQL in tests/test_intent_kafka_mysql.py):
- Events and clicks use INSERT ... ON DUPLICATE KEY UPDATE event_id = event_id, a no-op on
  a duplicate, so a redelivered event never overwrites the stored row. rowcount is 1 for a
  new row and 0 for a duplicate. That only holds without the CLIENT_FOUND_ROWS flag, so
  assert_rowcount_semantics() refuses a connection that sets it.
- The ATC claim is the same pattern on intent_atc_dedupe's primary key.
- Closed sessions upsert on uq_session_id and overwrite, as the old Mongo sync did.
"""

import copy
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.intent_events import (
    _BEHAVIORAL_UPSERT_COLUMNS,
    _CLICK_EVENTS_UPSERT_COLUMNS,
    _INTENT_SESSIONS_UPDATE_COLUMNS,
    _INTENT_SESSIONS_UPSERT_COLUMNS,
)
from pipeline.intent_session_state import ActorCursor
from pipeline.state import logger
from pipeline.utm import UTM_COLUMNS

MIGRATION_FILE = "migrations/001_intent_kafka_state.sql"
# The utm_* columns are the last len(UTM_COLUMNS) of the event column tuples and row tuples. A brand database
# that does not have them yet (migration 003 not applied or deferred) is written without them (row[:-5]) so a
# pending migration never stops ingestion; they are not "required" and verify_schema only warns about them.
UTM_COUNT = len(UTM_COLUMNS)
# Store-local clocks are at most UTC+14. An event time further ahead than that is bad data (a client with a
# wrong date); the idle-session sweep must not treat it as the current time.
FUTURE_TOLERANCE_HOURS = 14
CURSOR_TABLE = "intent_actor_cursors"
ATC_TABLE = "intent_atc_dedupe"
CURSOR_COLUMNS = ("actor_id", "session_id", "session_start", "last_event_at", "last_event_id", "events_seq")

def _without_utm(columns: Sequence[str]) -> Tuple[str, ...]:
    assert tuple(columns[-UTM_COUNT:]) == tuple(UTM_COLUMNS), "the utm columns must be last"
    return tuple(columns[:-UTM_COUNT])


REQUIRED_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "behavioral_events": _without_utm(_BEHAVIORAL_UPSERT_COLUMNS),
    "click_events": _without_utm(_CLICK_EVENTS_UPSERT_COLUMNS),
    "intent_sessions": tuple(_INTENT_SESSIONS_UPSERT_COLUMNS),
    CURSOR_TABLE: CURSOR_COLUMNS,
    ATC_TABLE: ("session_id", "product_id"),
}
REQUIRED_UNIQUE_KEYS: Dict[str, Tuple[str, ...]] = {
    "behavioral_events": ("event_id",),
    "click_events": ("event_id",),
    "intent_sessions": ("session_id",),
}
REQUIRED_PRIMARY_KEYS: Dict[str, Tuple[str, ...]] = {
    CURSOR_TABLE: ("actor_id",),
    ATC_TABLE: ("session_id", "product_id"),
}


class SchemaMissing(RuntimeError):
    """A brand database lacks a table, column or key the consumer needs."""


def _insert_noop_on_duplicate(table: str, columns: Sequence[str], key: str) -> str:
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
        f"ON DUPLICATE KEY UPDATE {key} = {key}"
    )


EVENT_INSERT_SQL = _insert_noop_on_duplicate("behavioral_events", _BEHAVIORAL_UPSERT_COLUMNS, "event_id")
CLICK_INSERT_SQL = _insert_noop_on_duplicate("click_events", _CLICK_EVENTS_UPSERT_COLUMNS, "event_id")
# The same statements for a database without the utm_* columns.
EVENT_INSERT_SQL_NO_UTM = _insert_noop_on_duplicate("behavioral_events", _without_utm(_BEHAVIORAL_UPSERT_COLUMNS), "event_id")
CLICK_INSERT_SQL_NO_UTM = _insert_noop_on_duplicate("click_events", _without_utm(_CLICK_EVENTS_UPSERT_COLUMNS), "event_id")
ATC_CLAIM_SQL = _insert_noop_on_duplicate(ATC_TABLE, ("session_id", "product_id"), "session_id")
SESSION_UPSERT_SQL = (
    f"INSERT INTO intent_sessions ({', '.join(_INTENT_SESSIONS_UPSERT_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INTENT_SESSIONS_UPSERT_COLUMNS))}) "
    f"ON DUPLICATE KEY UPDATE {', '.join(f'{c} = VALUES({c})' for c in _INTENT_SESSIONS_UPDATE_COLUMNS)}"
)
# Claims the actor's cursor row before it is locked. SELECT ... FOR UPDATE on a row that does not
# exist yet takes a gap lock under REPEATABLE READ, and two consumers that each meet a new actor
# then deadlock on their following INSERTs (error 1213). Inserting a placeholder first means the
# lock below is a plain record lock on an existing row: distinct actors never block each other,
# and two consumers on the same new actor queue on this INSERT instead of overwriting each other.
# A placeholder has session_id '' and is read back as "no cursor".
CURSOR_CLAIM_SQL = (
    f"INSERT INTO {CURSOR_TABLE} ({', '.join(CURSOR_COLUMNS)}) "
    f"VALUES (%s, '', '1970-01-01 00:00:01', '1970-01-01 00:00:01', NULL, '{{}}') "
    f"ON DUPLICATE KEY UPDATE actor_id = actor_id"
)
CURSOR_SELECT_SQL = f"SELECT {', '.join(CURSOR_COLUMNS)} FROM {CURSOR_TABLE} WHERE actor_id = %s FOR UPDATE"
CURSOR_UPSERT_SQL = (
    f"INSERT INTO {CURSOR_TABLE} ({', '.join(CURSOR_COLUMNS)}) VALUES ({', '.join(['%s'] * len(CURSOR_COLUMNS))}) "
    f"ON DUPLICATE KEY UPDATE "
    + ", ".join(f"{c} = VALUES({c})" for c in CURSOR_COLUMNS if c != "actor_id")
)

_FOUND_ROWS_FLAG = 2  # mysql.connector.constants.ClientFlag.FOUND_ROWS


def assert_rowcount_semantics(connection) -> None:
    flags = getattr(connection, "client_flags", 0) or 0
    if isinstance(flags, int) and flags & _FOUND_ROWS_FLAG:
        raise SchemaMissing(
            "connection uses CLIENT_FOUND_ROWS, so a duplicate insert would report one affected "
            "row and every redelivered event would be applied twice"
        )


def _cell(row, index: int, name: str):
    return row.get(name) if isinstance(row, dict) else row[index]


def verify_schema(cursor) -> None:
    """Read-only. Raises SchemaMissing for the first missing table, column, unique key or
    primary key. Never issues DDL."""
    tables = sorted(REQUIRED_COLUMNS)
    marks = ", ".join(["%s"] * len(tables))

    cursor.execute(
        f"SELECT table_name AS name FROM information_schema.tables "
        f"WHERE table_schema = DATABASE() AND table_name IN ({marks})",
        tables,
    )
    present = {_cell(r, 0, "name") for r in cursor.fetchall()}
    for table in tables:
        if table not in present:
            raise SchemaMissing(f"missing table {table}; apply {MIGRATION_FILE} first")

    cursor.execute(
        f"SELECT table_name AS tbl, column_name AS col FROM information_schema.columns "
        f"WHERE table_schema = DATABASE() AND table_name IN ({marks})",
        tables,
    )
    have: Dict[str, Set[str]] = {t: set() for t in tables}
    for r in cursor.fetchall():
        have[_cell(r, 0, "tbl")].add(_cell(r, 1, "col"))
    for table, columns in REQUIRED_COLUMNS.items():
        for column in columns:
            if column not in have[table]:
                raise SchemaMissing(f"{table}.{column} is missing")
    # Optional: without the utm_* columns events are stored without UTM values until migration 003 is applied.
    for table in ("behavioral_events", "click_events"):
        absent = [c for c in UTM_COLUMNS if c not in have[table]]
        if absent:
            logger.warning(
                f"[intent-kafka] category=utm_columns_missing table={table} missing={absent}; events are stored "
                f"without UTM values until migrations/003_event_utm_columns.sql is applied"
            )

    cursor.execute(
        f"SELECT table_name AS tbl, index_name AS idx, non_unique AS nu, column_name AS col "
        f"FROM information_schema.statistics WHERE table_schema = DATABASE() "
        f"AND table_name IN ({marks}) ORDER BY table_name, index_name, seq_in_index",
        tables,
    )
    index_cols: Dict[Tuple[str, str], List[str]] = {}
    index_unique: Dict[Tuple[str, str], bool] = {}
    for r in cursor.fetchall():
        key = (_cell(r, 0, "tbl"), _cell(r, 1, "idx"))
        index_cols.setdefault(key, []).append(_cell(r, 3, "col"))
        index_unique[key] = int(_cell(r, 2, "nu")) == 0

    for table, expected in REQUIRED_PRIMARY_KEYS.items():
        actual = tuple(index_cols.get((table, "PRIMARY"), ()))
        if actual != expected:
            raise SchemaMissing(f"{table} primary key is {actual}, expected {expected}")
    for table, expected in REQUIRED_UNIQUE_KEYS.items():
        if not any(
            index_unique.get(k) and tuple(v) == expected for k, v in index_cols.items() if k[0] == table
        ):
            raise SchemaMissing(f"{table} has no unique key on {expected}; duplicates would not be detected")


def utm_columns_present(cursor) -> bool:
    """True when both event tables of the connected database have all the utm_* columns."""
    marks = ", ".join(["%s"] * len(UTM_COLUMNS))
    cursor.execute(
        f"SELECT table_name AS tbl, COUNT(*) AS n FROM information_schema.columns "
        f"WHERE table_schema = DATABASE() AND table_name IN ('behavioral_events', 'click_events') "
        f"AND column_name IN ({marks}) GROUP BY table_name",
        list(UTM_COLUMNS),
    )
    counts = {_cell(r, 0, "tbl"): int(_cell(r, 1, "n")) for r in cursor.fetchall()}
    return counts.get("behavioral_events") == UTM_COUNT and counts.get("click_events") == UTM_COUNT


def migration_statements() -> List[str]:
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), MIGRATION_FILE)
    with open(path, encoding="utf-8") as handle:
        body = "\n".join(line for line in handle.read().splitlines() if not line.strip().startswith("--"))
    return [part.strip() for part in body.split(";") if part.strip()]


def apply_migration(cursor, connection) -> None:
    """Applies migrations/001_intent_kafka_state.sql. For tests and operators; the worker
    itself never calls this."""
    for statement in migration_statements():
        cursor.execute(statement)
    connection.commit()


def _load_cursor(row) -> ActorCursor:
    get = row.get if isinstance(row, dict) else dict(zip(CURSOR_COLUMNS, row)).get
    seq = get("events_seq")
    if isinstance(seq, (bytes, bytearray)):
        seq = seq.decode("utf-8")
    return ActorCursor(
        actor_id=get("actor_id"),
        session_id=get("session_id"),
        session_start=get("session_start"),
        last_event_at=get("last_event_at"),
        last_event_id=get("last_event_id"),
        events_seq=json.loads(seq) if isinstance(seq, str) else dict(seq or {}),
    )


# ---------------- bulk statements (the fast path, pipeline/intent_kafka_bulk.py) ----------------
# One statement carries many rows, so a batch costs a handful of round trips instead of several per
# event. Statements are bounded in rows and bytes so they stay far below max_allowed_packet.
BULK_MAX_ROWS = 200
BULK_MAX_BYTES = 1_000_000
LOOKUP_IDS_PER_QUERY = 1000
LOOKUP_PAIRS_PER_QUERY = 500
LOCK_ACTORS_PER_QUERY = 500

EVENT_TAIL = "ON DUPLICATE KEY UPDATE event_id = event_id"
ATC_TAIL = "ON DUPLICATE KEY UPDATE session_id = session_id"
SESSION_TAIL = "ON DUPLICATE KEY UPDATE " + ", ".join(f"{c} = VALUES({c})" for c in _INTENT_SESSIONS_UPDATE_COLUMNS)
CURSOR_TAIL = "ON DUPLICATE KEY UPDATE " + ", ".join(f"{c} = VALUES({c})" for c in CURSOR_COLUMNS if c != "actor_id")
CURSOR_CLAIM_TAIL = "ON DUPLICATE KEY UPDATE actor_id = actor_id"
# Same placeholder row as CURSOR_CLAIM_SQL: only the actor id is a parameter.
CURSOR_CLAIM_ROW = "(%s, '', '1970-01-01 00:00:01', '1970-01-01 00:00:01', NULL, '{}')"


def _row_bytes(row) -> int:
    return sum(len(v) if isinstance(v, (str, bytes)) else 8 for v in row)


def bulk_chunks(rows):
    """Splits rows into statements of at most BULK_MAX_ROWS rows and about BULK_MAX_BYTES of data."""
    chunk, size = [], 0
    for row in rows:
        row_size = _row_bytes(row)
        if chunk and (len(chunk) >= BULK_MAX_ROWS or size + row_size > BULK_MAX_BYTES):
            yield chunk
            chunk, size = [], 0
        chunk.append(row)
        size += row_size
    if chunk:
        yield chunk


def bulk_insert_sql(table: str, columns: Sequence[str], tail: str, rows: int, row_sql: Optional[str] = None) -> str:
    one = row_sql or "(" + ", ".join(["%s"] * len(columns)) + ")"
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {', '.join([one] * rows)} {tail}"


def _flatten(rows) -> tuple:
    return tuple(value for row in rows for value in row)


_EVENT_TABLES = {"event": ("behavioral_events", _BEHAVIORAL_UPSERT_COLUMNS),
                 "click": ("click_events", _CLICK_EVENTS_UPSERT_COLUMNS)}


class MySqlIntentStore:
    """One brand database inside one open transaction. The caller commits or rolls back."""

    def __init__(self, cursor, utm: bool = True) -> None:
        self.cursor = cursor
        self.utm = utm            # False: this database has no utm_* columns yet; rows are written without them
        self._savepoints = 0

    def savepoint(self) -> str:
        self._savepoints += 1
        name = f"sp_{self._savepoints}"
        self.cursor.execute(f"SAVEPOINT {name}")
        return name

    def release(self, name: str) -> None:
        self.cursor.execute(f"RELEASE SAVEPOINT {name}")

    def rollback_to(self, name: str) -> None:
        self.cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")

    def get_cursor_for_update(self, actor_id: str) -> Optional[ActorCursor]:
        self.cursor.execute(CURSOR_CLAIM_SQL, (actor_id,))
        self.cursor.execute(CURSOR_SELECT_SQL, (actor_id,))
        rows = self.cursor.fetchall()  # fully consumed before any later statement
        if not rows:  # cannot happen after the claim; fail loudly rather than start a second session
            raise RuntimeError(f"cursor row for {actor_id!r} vanished after it was claimed")
        cursor = _load_cursor(rows[0])
        return cursor if cursor.session_id else None  # '' = placeholder, the actor has no session yet

    def save_cursor(self, cursor_state: ActorCursor) -> None:
        self.cursor.execute(
            CURSOR_UPSERT_SQL,
            (
                cursor_state.actor_id,
                cursor_state.session_id,
                cursor_state.session_start,
                cursor_state.last_event_at,
                cursor_state.last_event_id,
                json.dumps(cursor_state.events_seq),
            ),
        )

    def insert_event(self, kind: str, row: tuple) -> bool:
        if self.utm:
            self.cursor.execute(EVENT_INSERT_SQL if kind == "event" else CLICK_INSERT_SQL, row)
        else:
            self.cursor.execute(EVENT_INSERT_SQL_NO_UTM if kind == "event" else CLICK_INSERT_SQL_NO_UTM, row[:-UTM_COUNT])
        return self.cursor.rowcount == 1

    def claim_atc(self, session_id: str, product_id: str) -> bool:
        self.cursor.execute(ATC_CLAIM_SQL, (session_id, product_id))
        return self.cursor.rowcount == 1

    def upsert_session(self, row: tuple) -> None:
        self.cursor.execute(SESSION_UPSERT_SQL, row)

    # ---- bulk operations used by the fast path ----

    def lock_cursors(self, actor_ids: Sequence[str]) -> Dict[str, Optional[ActorCursor]]:
        """Claims (placeholder rows, see CURSOR_CLAIM_SQL) and locks the cursor rows of all the actors, in
        sorted order, in two statements per 500 actors. Keyed by the actor id lower-cased, because MySQL's
        collation matches keys case-insensitively. A placeholder reads back as None (no session yet)."""
        out: Dict[str, Optional[ActorCursor]] = {}
        ids = sorted(actor_ids)
        columns = ", ".join(CURSOR_COLUMNS)
        for start in range(0, len(ids), LOCK_ACTORS_PER_QUERY):
            part = ids[start:start + LOCK_ACTORS_PER_QUERY]
            self.cursor.execute(
                bulk_insert_sql(CURSOR_TABLE, CURSOR_COLUMNS, CURSOR_CLAIM_TAIL, len(part), CURSOR_CLAIM_ROW), tuple(part))
            self.cursor.execute(
                f"SELECT {columns} FROM {CURSOR_TABLE} WHERE actor_id IN ({', '.join(['%s'] * len(part))}) "
                f"ORDER BY actor_id FOR UPDATE", tuple(part))
            for row in self.cursor.fetchall():  # fully consumed before the next statement
                cursor = _load_cursor(row)
                out[cursor.actor_id.casefold()] = cursor if cursor.session_id else None
        missing = [a for a in ids if a.casefold() not in out]
        if missing:  # cannot happen after the claim; fail loudly rather than start a second session
            raise RuntimeError(f"cursor rows vanished after they were claimed: {missing[:3]}")
        return out

    def existing_event_ids(self, kind: str, event_ids: Sequence[str]) -> Set[str]:
        table, _columns = _EVENT_TABLES[kind]
        found: Set[str] = set()
        ids = list(event_ids)
        for start in range(0, len(ids), LOOKUP_IDS_PER_QUERY):
            part = ids[start:start + LOOKUP_IDS_PER_QUERY]
            self.cursor.execute(
                f"SELECT event_id FROM {table} WHERE event_id IN ({', '.join(['%s'] * len(part))})", tuple(part))
            found.update(_cell(row, 0, "event_id") for row in self.cursor.fetchall())
        return found

    def existing_atc(self, pairs: Sequence[Tuple[str, str]]) -> Set[Tuple[str, str]]:
        found: Set[Tuple[str, str]] = set()
        items = list(pairs)
        for start in range(0, len(items), LOOKUP_PAIRS_PER_QUERY):
            part = items[start:start + LOOKUP_PAIRS_PER_QUERY]
            self.cursor.execute(
                f"SELECT session_id, product_id FROM {ATC_TABLE} "
                f"WHERE (session_id, product_id) IN ({', '.join(['(%s, %s)'] * len(part))})", _flatten(part))
            found.update((_cell(r, 0, "session_id"), _cell(r, 1, "product_id")) for r in self.cursor.fetchall())
        return found

    def insert_events(self, kind: str, rows: Sequence[tuple]) -> None:
        table, columns = _EVENT_TABLES[kind]
        if not self.utm:
            columns = _without_utm(columns)
            rows = [row[:-UTM_COUNT] for row in rows]
        for chunk in bulk_chunks(rows):
            self.cursor.execute(bulk_insert_sql(table, columns, EVENT_TAIL, len(chunk)), _flatten(chunk))

    def insert_claims(self, pairs: Sequence[Tuple[str, str]]) -> None:
        for chunk in bulk_chunks(pairs):
            self.cursor.execute(bulk_insert_sql(ATC_TABLE, ("session_id", "product_id"), ATC_TAIL, len(chunk)), _flatten(chunk))

    def upsert_sessions(self, rows: Sequence[tuple]) -> None:
        for chunk in bulk_chunks(rows):
            self.cursor.execute(
                bulk_insert_sql("intent_sessions", _INTENT_SESSIONS_UPSERT_COLUMNS, SESSION_TAIL, len(chunk)), _flatten(chunk))

    def save_cursors(self, cursors: Sequence[ActorCursor]) -> None:
        rows = [(c.actor_id, c.session_id, c.session_start, c.last_event_at, c.last_event_id, json.dumps(c.events_seq))
                for c in cursors]
        for chunk in bulk_chunks(rows):
            self.cursor.execute(bulk_insert_sql(CURSOR_TABLE, CURSOR_COLUMNS, CURSOR_TAIL, len(chunk)), _flatten(chunk))

    # ---- idle-session sweep (pipeline/intent_session_sweep.py) ----

    def latest_cursor_time(self) -> Optional[datetime]:
        """The newest last_event_at of any open session: how far this brand's ingestion has got, in the
        store-local clock the events carry. Placeholder rows (session_id '') do not count, and neither does a
        time that no store clock can have reached (more than FUTURE_TOLERANCE_HOURS ahead of UTC): one bogus
        future timestamp would otherwise become 'now' and make every live session look idle."""
        self.cursor.execute(
            f"SELECT MAX(last_event_at) AS latest FROM {CURSOR_TABLE} "
            f"WHERE session_id <> '' AND last_event_at <= UTC_TIMESTAMP() + INTERVAL {FUTURE_TOLERANCE_HOURS} HOUR")
        rows = self.cursor.fetchall()
        return _cell(rows[0], 0, "latest") if rows else None

    def idle_cursors(self, before: datetime, limit: int) -> List[ActorCursor]:
        """Open sessions whose last event is older than `before`, oldest first. A plain read, no row locks:
        the run lock makes the worker the only writer of this table, and the delete below re-checks the row."""
        self.cursor.execute(
            f"SELECT {', '.join(CURSOR_COLUMNS)} FROM {CURSOR_TABLE} "
            f"WHERE session_id <> '' AND last_event_at < %s ORDER BY last_event_at LIMIT %s", (before, limit))
        return [_load_cursor(row) for row in self.cursor.fetchall()]

    def delete_cursors(self, cursors: Sequence[ActorCursor]) -> None:
        """Removes exactly these open sessions (matched on actor and session id, so a cursor that has moved
        on to a newer session is left alone). The actor's next event then starts a new session."""
        for start in range(0, len(cursors), LOOKUP_PAIRS_PER_QUERY):
            part = cursors[start:start + LOOKUP_PAIRS_PER_QUERY]
            self.cursor.execute(
                f"DELETE FROM {CURSOR_TABLE} WHERE (actor_id, session_id) IN ({', '.join(['(%s, %s)'] * len(part))})",
                _flatten([(c.actor_id, c.session_id) for c in part]))


class InMemoryIntentStore:
    """Same unique keys, savepoint rollback and lock-order record as MySqlIntentStore, for
    fast unit tests. Real behaviour is covered by tests/test_intent_kafka_mysql.py."""

    def __init__(self) -> None:
        self.events: Dict[str, tuple] = {}
        self.clicks: Dict[str, tuple] = {}
        self.atc: Set[Tuple[str, str]] = set()
        self.cursors: Dict[str, ActorCursor] = {}
        self.sessions: List[tuple] = []
        self.locked_actors: List[str] = []
        self.fail_on: Dict[str, Exception] = {}  # event_id -> error raised on insert

    def savepoint(self):
        return copy.deepcopy((self.events, self.clicks, self.atc, self.cursors, self.sessions))

    def release(self, token) -> None:
        pass

    def rollback_to(self, token) -> None:
        self.events, self.clicks, self.atc, self.cursors, self.sessions = copy.deepcopy(token)

    def get_cursor_for_update(self, actor_id: str) -> Optional[ActorCursor]:
        self.locked_actors.append(actor_id)
        return self.cursors.get(actor_id)

    def save_cursor(self, cursor_state: ActorCursor) -> None:
        self.cursors[cursor_state.actor_id] = cursor_state

    def insert_event(self, kind: str, row: tuple) -> bool:
        if row[0] in self.fail_on:
            raise self.fail_on[row[0]]
        target = self.events if kind == "event" else self.clicks
        if row[0] in target:
            return False
        target[row[0]] = row
        return True

    def claim_atc(self, session_id: str, product_id: str) -> bool:
        if (session_id, product_id) in self.atc:
            return False
        self.atc.add((session_id, product_id))
        return True

    def upsert_session(self, row: tuple) -> None:
        self.sessions = [r for r in self.sessions if r[0] != row[0]] + [row]

    # ---- bulk operations used by the fast path ----

    def lock_cursors(self, actor_ids: Sequence[str]) -> Dict[str, Optional[ActorCursor]]:
        out: Dict[str, Optional[ActorCursor]] = {}
        for actor_id in sorted(actor_ids):
            self.locked_actors.append(actor_id)
            out[actor_id.casefold()] = self.cursors.get(actor_id)
        return out

    def existing_event_ids(self, kind: str, event_ids: Sequence[str]) -> Set[str]:
        target = self.events if kind == "event" else self.clicks
        return {i for i in event_ids if i in target}

    def existing_atc(self, pairs: Sequence[Tuple[str, str]]) -> Set[Tuple[str, str]]:
        return {p for p in pairs if p in self.atc}

    def insert_events(self, kind: str, rows: Sequence[tuple]) -> None:
        target = self.events if kind == "event" else self.clicks
        for row in rows:
            if row[0] in self.fail_on:
                raise self.fail_on[row[0]]
            target.setdefault(row[0], row)

    def insert_claims(self, pairs: Sequence[Tuple[str, str]]) -> None:
        self.atc.update(pairs)

    def upsert_sessions(self, rows: Sequence[tuple]) -> None:
        for row in rows:
            self.upsert_session(row)

    def save_cursors(self, cursors: Sequence[ActorCursor]) -> None:
        for cursor in cursors:
            self.cursors[cursor.actor_id] = cursor

    # ---- idle-session sweep ----

    def latest_cursor_time(self) -> Optional[datetime]:
        limit = datetime.utcnow() + timedelta(hours=FUTURE_TOLERANCE_HOURS)
        return max((c.last_event_at for c in self.cursors.values() if c.last_event_at <= limit), default=None)

    def idle_cursors(self, before: datetime, limit: int) -> List[ActorCursor]:
        idle = [c for c in self.cursors.values() if c.last_event_at < before]
        return sorted(idle, key=lambda c: c.last_event_at)[:limit]

    def delete_cursors(self, cursors: Sequence[ActorCursor]) -> None:
        for cursor in cursors:
            current = self.cursors.get(cursor.actor_id)
            if current is not None and current.session_id == cursor.session_id:
                del self.cursors[cursor.actor_id]
