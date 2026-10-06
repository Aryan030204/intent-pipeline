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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.intent_events import (
    _BEHAVIORAL_UPSERT_COLUMNS,
    _CLICK_EVENTS_UPSERT_COLUMNS,
    _INTENT_SESSIONS_UPDATE_COLUMNS,
    _INTENT_SESSIONS_UPSERT_COLUMNS,
)
from pipeline.intent_session_state import ActorCursor

MIGRATION_FILE = "migrations/001_intent_kafka_state.sql"
CURSOR_TABLE = "intent_actor_cursors"
ATC_TABLE = "intent_atc_dedupe"
CURSOR_COLUMNS = ("actor_id", "session_id", "session_start", "last_event_at", "last_event_id", "events_seq")

REQUIRED_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "behavioral_events": tuple(_BEHAVIORAL_UPSERT_COLUMNS),
    "click_events": tuple(_CLICK_EVENTS_UPSERT_COLUMNS),
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


class MySqlIntentStore:
    """One brand database inside one open transaction. The caller commits or rolls back."""

    def __init__(self, cursor) -> None:
        self.cursor = cursor
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
        self.cursor.execute(EVENT_INSERT_SQL if kind == "event" else CLICK_INSERT_SQL, row)
        return self.cursor.rowcount == 1

    def claim_atc(self, session_id: str, product_id: str) -> bool:
        self.cursor.execute(ATC_CLAIM_SQL, (session_id, product_id))
        return self.cursor.rowcount == 1

    def upsert_session(self, row: tuple) -> None:
        self.cursor.execute(SESSION_UPSERT_SQL, row)


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
