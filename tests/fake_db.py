"""
Shared fake for the read-only schema queries and the locking/savepoint statements
the intent consumer issues. Describes one brand database. Tests choose what is
wrong with it through SchemaState; everything else behaves like a healthy schema.
"""

from typing import Dict, List, Optional, Tuple

from pipeline.intent_sqs_store import (
    CURSOR_TABLE,
    REQUIRED_COLUMNS,
    REQUIRED_PRIMARY_KEYS,
    REQUIRED_SESSION_COLUMN,
    REQUIRED_UNIQUE_KEYS,
)


class FatalDbError(Exception):
    """Stands in for a MySQL error that takes the whole transaction with it (lost connection)."""

    errno = 2013


class SchemaState:
    def __init__(
        self,
        missing_tables=(),
        missing_columns=(),
        pk_override: Optional[Dict[str, Tuple[str, ...]]] = None,
        missing_unique=(),
        source_type: str = "datetime(6)",
    ) -> None:
        self.missing_tables = set(missing_tables)
        self.missing_columns = set(missing_columns)  # entries "table.column"
        self.pk_override = pk_override or {}
        self.missing_unique = set(missing_unique)  # table names whose unique key is absent
        self.source_type = source_type


def schema_rows(state: SchemaState, sql: str) -> Optional[List[tuple]]:
    """Rows for a schema query, or None if the SQL is not a schema query."""
    tables = sorted(set(REQUIRED_COLUMNS) | set(REQUIRED_UNIQUE_KEYS))
    if "information_schema.tables" in sql:
        return [(t,) for t in tables if t not in state.missing_tables]
    if "information_schema.columns" in sql:
        rows = []
        for table, columns in REQUIRED_COLUMNS.items():
            if table in state.missing_tables:
                continue
            for column in columns:
                if f"{table}.{column}" in state.missing_columns:
                    continue
                ctype = state.source_type if column == REQUIRED_SESSION_COLUMN else "varchar(100)"
                rows.append((table, column, ctype))
        return rows
    if "information_schema.statistics" in sql:
        rows = []
        for table in tables:
            if table in state.missing_tables:
                continue
            pk = state.pk_override.get(table, REQUIRED_PRIMARY_KEYS.get(table))
            if pk:
                for col in pk:
                    rows.append((table, "PRIMARY", 0, col))
            for col in REQUIRED_UNIQUE_KEYS.get(table, ()):
                if table in state.missing_unique:
                    continue
                rows.append((table, f"uq_{col}", 0, col))
        return rows
    return None


class FakeDbCursor:
    """Cursor for schema checks, actor locks and savepoints. Subclasses add write behaviour."""

    def __init__(self, state: Optional[SchemaState] = None) -> None:
        self.state = state or SchemaState()
        self.sql: List[str] = []
        self.rowcount = 0
        self._last = ""
        self._rows: List[tuple] = []
        self.seen_keys: Dict[tuple, bool] = {}
        self.executemany_calls: List[tuple] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self._last = sql
        self.rowcount = 0
        self._rows = []
        if "SELECT DATABASE()" in sql:
            self._rows = [("testdb",)]
            return
        schema = schema_rows(self.state, sql)
        if schema is not None:
            self._rows = schema
            return
        if "GET_LOCK" in sql:
            self._rows = [(1,)]
            return
        if "RELEASE_LOCK" in sql:
            self._rows = [(1,)]
            return
        if "FOR UPDATE" in sql:
            self._rows = []
            return
        self._record_insert(sql, params)

    def _record_insert(self, sql, params):
        for table in ("behavioral_events", "click_events"):
            if f"INSERT INTO {table} " in sql and "ON DUPLICATE KEY UPDATE event_id" in sql:
                key = (table, params[0])
                self.rowcount = 0 if key in self.seen_keys else 1
                self.seen_keys[key] = True
        if "INSERT INTO intent_atc_dedupe" in sql:
            key = ("atc", params[0], params[1])
            self.rowcount = 0 if key in self.seen_keys else 1
            self.seen_keys[key] = True

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.in_transaction = True

    def commit(self):
        self.commits += 1
        self.in_transaction = False

    def rollback(self):
        self.rollbacks += 1
        self.in_transaction = False
