"""
Storage for the SQS intent consumer. Two implementations share one interface:

- MySqlIntentStore: production. Runs inside the caller's transaction, so nothing
  is visible or acknowledged until the brand's batch commits.
- InMemoryIntentStore: tests. Enforces the same unique keys, so duplicate and
  deduplication behaviour is exercised without MySQL or AWS.

Neither implementation reads or writes any document store.

Schema is not created here. DDL lives in migrations/003_intent_sqs_state.sql and
is applied explicitly per brand database. At runtime the consumer only verifies,
read-only and once per database per process, that the tables exist.
"""

import json
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pipeline.intent_events import (
    _BEHAVIORAL_UPSERT_COLUMNS,
    _CLICK_EVENTS_UPSERT_COLUMNS,
)
from pipeline.intent_session_state import to_naive_utc

CURSOR_TABLE = "intent_actor_cursors"
ATC_TABLE = "intent_atc_dedupe"
REQUIRED_TABLES = (CURSOR_TABLE, ATC_TABLE)
REQUIRED_SESSION_COLUMN = "source_updated_at"
MIGRATION_FILE = "migrations/003_intent_sqs_state.sql"

_verified_databases: Set[str] = set()


class SchemaMissing(RuntimeError):
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


REQUIRED_PRIMARY_KEYS = {
    CURSOR_TABLE: ("actor_id",),
    ATC_TABLE: ("session_id", "product_id"),
}
REQUIRED_SESSION_COLUMN_TYPE = "datetime(6)"


def verify_state_schema(cursor) -> None:
    """Read-only. Raises SchemaMissing when a required table is absent, a required
    primary key differs from the migration, or intent_sessions.source_updated_at is
    missing or not DATETIME(6). Runs once per database per process; never issues DDL."""
    cursor.execute("SELECT DATABASE() AS db")
    row = cursor.fetchone()
    db = (row.get("db") if isinstance(row, dict) else row[0]) if row else None
    if db in _verified_databases:
        return

    cursor.execute(
        "SELECT table_name AS name FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name IN (%s, %s)",
        REQUIRED_TABLES,
    )
    present = {(r.get("name") if isinstance(r, dict) else r[0]) for r in cursor.fetchall()}
    missing = [t for t in REQUIRED_TABLES if t not in present]
    if missing:
        raise SchemaMissing(f"missing tables {missing}; apply {MIGRATION_FILE} first")

    cursor.execute(
        "SELECT table_name AS tbl, column_name AS col FROM information_schema.statistics "
        "WHERE table_schema = DATABASE() AND index_name = 'PRIMARY' AND table_name IN (%s, %s) "
        "ORDER BY table_name, seq_in_index",
        REQUIRED_TABLES,
    )
    actual_pk: Dict[str, List[str]] = {t: [] for t in REQUIRED_TABLES}
    for r in cursor.fetchall():
        tbl = r.get("tbl") if isinstance(r, dict) else r[0]
        col = r.get("col") if isinstance(r, dict) else r[1]
        actual_pk.setdefault(tbl, []).append(col)
    for table, expected in REQUIRED_PRIMARY_KEYS.items():
        if tuple(actual_pk.get(table, [])) != expected:
            raise SchemaMissing(
                f"{table} primary key is {tuple(actual_pk.get(table, []))}, expected {expected}; "
                f"refusing to write (see {MIGRATION_FILE})"
            )

    cursor.execute(
        "SELECT column_type AS ctype FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = 'intent_sessions' AND column_name = %s",
        (REQUIRED_SESSION_COLUMN,),
    )
    rows = cursor.fetchall()
    if not rows:
        raise SchemaMissing(
            f"intent_sessions.{REQUIRED_SESSION_COLUMN} is missing; apply {MIGRATION_FILE} first"
        )
    ctype = rows[0].get("ctype") if isinstance(rows[0], dict) else rows[0][0]
    if str(ctype).lower() != REQUIRED_SESSION_COLUMN_TYPE:
        raise SchemaMissing(
            f"intent_sessions.{REQUIRED_SESSION_COLUMN} is {ctype}, expected {REQUIRED_SESSION_COLUMN_TYPE}"
        )

    _verified_databases.add(db)


def reset_schema_verification() -> None:
    """Test hook: forget verified databases."""
    _verified_databases.clear()


class MySqlIntentStore:
    def __init__(self, cursor) -> None:
        self.cursor = cursor

    def get_cursor_for_update(self, actor_id: str) -> Optional[Dict[str, Any]]:
        # FOR UPDATE serialises two consumers working on the same actor.
        self.cursor.execute(
            f"SELECT actor_id, session_id, session_start, last_event_at, last_event_id, events_seq "
            f"FROM {CURSOR_TABLE} WHERE actor_id = %s FOR UPDATE",
            (actor_id,),
        )
        row = self.cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            get = row.get
        else:
            names = ("actor_id", "session_id", "session_start", "last_event_at", "last_event_id", "events_seq")
            get = dict(zip(names, row)).get
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
    """Mirrors the MySQL unique keys: event_id per table, (session_id, product_id)
    for ATC, actor_id for cursors."""

    def __init__(self) -> None:
        self.events: Dict[str, tuple] = {}
        self.clicks: Dict[str, tuple] = {}
        self.atc: Set[Tuple[str, str]] = set()
        self.cursors: Dict[str, Dict[str, Any]] = {}
        self.sessions: List[tuple] = []
        self.locked_actors: List[str] = []

    def get_cursor_for_update(self, actor_id: str) -> Optional[Dict[str, Any]]:
        self.locked_actors.append(actor_id)
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
