"""
Storage for the SQS intent consumer. Two implementations share one interface:

- MySqlIntentStore: production. Runs inside the caller's transaction.
- InMemoryIntentStore: tests. Enforces the same unique keys, savepoint rollback
  and actor locking, so duplicate, dedupe and failure-isolation behaviour is
  exercised without MySQL or AWS.

Concurrency: each actor is serialised by a server-side named lock (GET_LOCK)
taken before the cursor row is read. This does not depend on InnoDB isolation
level or gap-lock behaviour, so the first event of an actor is race-free under
any isolation level. Actors are locked in sorted order, so named locks cannot
form a cycle between workers. The row lock (SELECT ... FOR UPDATE) is kept.

Schema is not created here. DDL lives in migrations/003_intent_sqs_state.sql and
is applied explicitly per brand database. At runtime the consumer only verifies,
read-only and once per database per process, that tables, columns, unique keys
and primary keys exist.

This module never reads or writes a document store.
"""

import copy
import hashlib
import json
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.intent_events import (
    _BEHAVIORAL_UPSERT_COLUMNS,
    _CLICK_EVENTS_UPSERT_COLUMNS,
    _INTENT_SESSIONS_UPSERT_COLUMNS,
)
from pipeline.intent_session_state import to_naive_utc

CURSOR_TABLE = "intent_actor_cursors"
ATC_TABLE = "intent_atc_dedupe"
REQUIRED_SESSION_COLUMN = "source_updated_at"
REQUIRED_SESSION_COLUMN_TYPE = "datetime(6)"
MIGRATION_FILE = "migrations/003_intent_sqs_state.sql"

CURSOR_COLUMNS = ("actor_id", "session_id", "session_start", "last_event_at", "last_event_id", "events_seq")
ATC_COLUMNS = ("session_id", "product_id", "created_at")

REQUIRED_PRIMARY_KEYS = {
    CURSOR_TABLE: ("actor_id",),
    ATC_TABLE: ("session_id", "product_id"),
}
REQUIRED_UNIQUE_KEYS = {
    "behavioral_events": ("event_id",),
    "click_events": ("event_id",),
    "intent_sessions": ("session_id",),
}
REQUIRED_COLUMNS = {
    "behavioral_events": tuple(_BEHAVIORAL_UPSERT_COLUMNS),
    "click_events": tuple(_CLICK_EVENTS_UPSERT_COLUMNS),
    "intent_sessions": tuple(_INTENT_SESSIONS_UPSERT_COLUMNS) + (REQUIRED_SESSION_COLUMN,),
    CURSOR_TABLE: CURSOR_COLUMNS,
    ATC_TABLE: ATC_COLUMNS,
}

ACTOR_LOCK_TIMEOUT_S = 30
MAX_ACTOR_LOCK_NAME = 64

_verified_databases: Set[str] = set()


class SchemaMissing(RuntimeError):
    pass


class ActorLockTimeout(RuntimeError):
    pass


def _noop_insert_sql(table: str, columns: Sequence[str]) -> str:
    columns_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    # `event_id = event_id` changes nothing, so rowcount is 1 for a new row and 0
    # for a duplicate. Duplicates must NOT overwrite the stored first delivery.
    return (
        f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE event_id = event_id"
    )


_EVENT_INSERT_SQL = _noop_insert_sql("behavioral_events", _BEHAVIORAL_UPSERT_COLUMNS)
_CLICK_INSERT_SQL = _noop_insert_sql("click_events", _CLICK_EVENTS_UPSERT_COLUMNS)
_ATC_CLAIM_SQL = (
    f"INSERT INTO {ATC_TABLE} (session_id, product_id) VALUES (%s, %s) "
    f"ON DUPLICATE KEY UPDATE session_id = session_id"
)
_CURSOR_UPSERT_SQL = (
    f"INSERT INTO {CURSOR_TABLE} "
    f"(actor_id, session_id, session_start, last_event_at, last_event_id, events_seq) "
    f"VALUES (%s, %s, %s, %s, %s, %s) "
    f"ON DUPLICATE KEY UPDATE session_id = VALUES(session_id), "
    f"session_start = VALUES(session_start), last_event_at = VALUES(last_event_at), "
    f"last_event_id = VALUES(last_event_id), events_seq = VALUES(events_seq)"
)


def _cell(row, index: int, name: str):
    return row.get(name) if isinstance(row, dict) else row[index]


def verify_state_schema(cursor) -> None:
    """Read-only. Raises SchemaMissing with the first problem found: a missing table,
    column, unique key or primary key, or a wrong source_updated_at type. Runs once
    per database per process and never issues DDL."""
    cursor.execute("SELECT DATABASE() AS db")
    row = cursor.fetchone()
    db = _cell(row, 0, "db") if row else None
    if db in _verified_databases:
        return

    tables = sorted(set(REQUIRED_COLUMNS) | set(REQUIRED_UNIQUE_KEYS))
    placeholders = ", ".join(["%s"] * len(tables))

    cursor.execute(
        f"SELECT table_name AS name FROM information_schema.tables "
        f"WHERE table_schema = DATABASE() AND table_name IN ({placeholders})",
        tables,
    )
    present = {_cell(r, 0, "name") for r in cursor.fetchall()}
    for table in tables:
        if table not in present:
            raise SchemaMissing(f"missing table {table}; apply {MIGRATION_FILE} first")

    cursor.execute(
        f"SELECT table_name AS tbl, column_name AS col, column_type AS ctype "
        f"FROM information_schema.columns WHERE table_schema = DATABASE() "
        f"AND table_name IN ({placeholders})",
        tables,
    )
    have_cols: Dict[str, Dict[str, str]] = {t: {} for t in tables}
    for r in cursor.fetchall():
        have_cols[_cell(r, 0, "tbl")][_cell(r, 1, "col")] = str(_cell(r, 2, "ctype")).lower()
    for table, columns in REQUIRED_COLUMNS.items():
        for column in columns:
            if column not in have_cols[table]:
                raise SchemaMissing(f"{table}.{column} is missing; apply {MIGRATION_FILE} first")
    actual_type = have_cols["intent_sessions"][REQUIRED_SESSION_COLUMN]
    if actual_type != REQUIRED_SESSION_COLUMN_TYPE:
        raise SchemaMissing(
            f"intent_sessions.{REQUIRED_SESSION_COLUMN} is {actual_type}, expected {REQUIRED_SESSION_COLUMN_TYPE}"
        )

    cursor.execute(
        f"SELECT table_name AS tbl, index_name AS idx, non_unique AS nu, column_name AS col "
        f"FROM information_schema.statistics WHERE table_schema = DATABASE() "
        f"AND table_name IN ({placeholders}) ORDER BY table_name, index_name, seq_in_index",
        tables,
    )
    index_cols: Dict[Tuple[str, str], List[str]] = {}
    index_unique: Dict[Tuple[str, str], bool] = {}
    for r in cursor.fetchall():
        key = (_cell(r, 0, "tbl"), _cell(r, 1, "idx"))
        index_cols.setdefault(key, []).append(_cell(r, 3, "col"))
        index_unique[key] = int(_cell(r, 2, "nu")) == 0

    for table, expected in REQUIRED_PRIMARY_KEYS.items():
        actual = tuple(index_cols.get((table, "PRIMARY"), []))
        if actual != expected:
            raise SchemaMissing(f"{table} primary key is {actual}, expected {expected}; refusing to write")
    for table, expected in REQUIRED_UNIQUE_KEYS.items():
        found = any(
            index_unique.get(k) and tuple(v) == expected
            for k, v in index_cols.items() if k[0] == table
        )
        if not found:
            raise SchemaMissing(f"{table} has no unique key on {expected}; refusing to write")

    _verified_databases.add(db)


def reset_schema_verification() -> None:
    """Test hook: forget verified databases."""
    _verified_databases.clear()


def _lock_name(db: str, actor: str) -> str:
    digest = hashlib.sha256(f"{db}|{actor}".encode("utf-8")).hexdigest()
    return ("ia:" + digest)[:MAX_ACTOR_LOCK_NAME]


class MySqlIntentStore:
    def __init__(self, cursor) -> None:
        self.cursor = cursor
        self._db: Optional[str] = None
        self._held_locks: List[str] = []
        self._savepoint_seq = 0

    def _database(self) -> str:
        if self._db is None:
            self.cursor.execute("SELECT DATABASE() AS db")
            row = self.cursor.fetchone()
            self._db = _cell(row, 0, "db") or ""
        return self._db

    def lock_actor(self, actor_id: str) -> None:
        name = _lock_name(self._database(), actor_id)
        self.cursor.execute("SELECT GET_LOCK(%s, %s) AS got", (name, ACTOR_LOCK_TIMEOUT_S))
        if _cell(self.cursor.fetchone(), 0, "got") != 1:
            raise ActorLockTimeout(f"could not acquire actor lock within {ACTOR_LOCK_TIMEOUT_S}s")
        self._held_locks.append(name)

    def unlock_all(self) -> None:
        while self._held_locks:
            self.cursor.execute("SELECT RELEASE_LOCK(%s)", (self._held_locks.pop(),))
            # The connector keeps a SELECT result unread until it is fetched, and the next
            # execute raises 'Unread result found'. Consume it before the next release.
            self.cursor.fetchone()

    def begin_group(self):
        self._savepoint_seq += 1
        name = f"sp_{self._savepoint_seq}"
        self.cursor.execute(f"SAVEPOINT {name}")
        return name

    def commit_group(self, token) -> None:
        self.cursor.execute(f"RELEASE SAVEPOINT {token}")

    def rollback_group(self, token) -> None:
        self.cursor.execute(f"ROLLBACK TO SAVEPOINT {token}")

    def get_cursor_for_update(self, actor_id: str) -> Optional[Dict[str, Any]]:
        self.cursor.execute(
            f"SELECT {', '.join(CURSOR_COLUMNS)} FROM {CURSOR_TABLE} WHERE actor_id = %s FOR UPDATE",
            (actor_id,),
        )
        row = self.cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            get = row.get
        else:
            get = dict(zip(CURSOR_COLUMNS, row)).get
        seq = get("events_seq")
        return {
            "actor_id": get("actor_id"),
            "session_id": get("session_id"),
            "session_start": to_naive_utc(get("session_start")),
            "last_event_at": to_naive_utc(get("last_event_at")),
            "last_event_id": get("last_event_id"),
            "events_seq": json.loads(seq) if isinstance(seq, str) else dict(seq or {}),
        }

    def save_cursor(self, actor_id: str, cursor_state: Dict[str, Any]) -> None:
        self.cursor.execute(
            _CURSOR_UPSERT_SQL,
            (
                actor_id,
                cursor_state["session_id"],
                cursor_state["session_start"],
                cursor_state["last_event_at"],
                cursor_state.get("last_event_id"),
                json.dumps(cursor_state["events_seq"]),
            ),
        )

    def insert_event(self, kind: str, row: tuple) -> bool:
        sql = _EVENT_INSERT_SQL if kind == "event" else _CLICK_INSERT_SQL
        self.cursor.execute(sql, row)
        return self.cursor.rowcount == 1

    def claim_atc(self, session_id: str, product_id: str) -> bool:
        self.cursor.execute(_ATC_CLAIM_SQL, (session_id, product_id))
        return self.cursor.rowcount == 1

    def upsert_session_rows(self, sql: str, rows: List[tuple]) -> None:
        if rows:
            self.cursor.executemany(sql, rows)


class InMemoryIntentStore:
    """Mirrors the MySQL unique keys, savepoint rollback and actor locking."""

    def __init__(self) -> None:
        self.events: Dict[str, tuple] = {}
        self.clicks: Dict[str, tuple] = {}
        self.atc: Set[Tuple[str, str]] = set()
        self.cursors: Dict[str, Dict[str, Any]] = {}
        self.sessions: List[tuple] = []
        self.locked_actors: List[str] = []
        self.fail_on_actor: Set[str] = set()

    def lock_actor(self, actor_id: str) -> None:
        self.locked_actors.append(actor_id)

    def unlock_all(self) -> None:
        pass

    def _snapshot(self):
        return {
            "events": copy.deepcopy(self.events),
            "clicks": copy.deepcopy(self.clicks),
            "atc": set(self.atc),
            "cursors": copy.deepcopy(self.cursors),
            "sessions": list(self.sessions),
        }

    def begin_group(self):
        return self._snapshot()

    def commit_group(self, token) -> None:
        pass

    def rollback_group(self, token) -> None:
        self.events = token["events"]
        self.clicks = token["clicks"]
        self.atc = token["atc"]
        self.cursors = token["cursors"]
        self.sessions = token["sessions"]

    def get_cursor_for_update(self, actor_id: str) -> Optional[Dict[str, Any]]:
        if actor_id in self.fail_on_actor:
            raise RuntimeError(f"injected failure for actor {actor_id}")
        stored = self.cursors.get(actor_id)
        return None if stored is None else _copy_cursor(stored)

    def save_cursor(self, actor_id: str, cursor_state: Dict[str, Any]) -> None:
        self.cursors[actor_id] = _copy_cursor(cursor_state)

    def insert_event(self, kind: str, row: tuple) -> bool:
        target = self.events if kind == "event" else self.clicks
        event_id = row[0]
        if event_id in target:
            return False
        target[event_id] = row
        return True

    def claim_atc(self, session_id: str, product_id: str) -> bool:
        key = (session_id, product_id)
        if key in self.atc:
            return False
        self.atc.add(key)
        return True

    def upsert_session_rows(self, sql: str, rows: List[tuple]) -> None:
        self.sessions.extend(rows)


def _copy_cursor(state: Dict[str, Any]) -> Dict[str, Any]:
    copied = dict(state)
    copied["events_seq"] = {k: dict(v) for k, v in state["events_seq"].items()}
    return copied
