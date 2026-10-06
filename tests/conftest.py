import contextlib
import os
import sys
import uuid

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Integration tests run only when a real MySQL / Kafka is named in the environment, so the
# default `pytest` run needs neither:
#   INTENT_TEST_MYSQL_HOST, _PORT (3306), _USER (root), _PASSWORD
#   INTENT_TEST_KAFKA_BOOTSTRAP   (e.g. localhost:29092)
# Every test database is named intent_test_<random> and is dropped afterwards; the fixture
# refuses to drop any other name.

TEST_TABLES = (
    "behavioral_events", "click_events", "intent_sessions", "intent_actor_cursors", "intent_atc_dedupe",
)


def mysql_settings():
    host = os.environ.get("INTENT_TEST_MYSQL_HOST", "").strip()
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.environ.get("INTENT_TEST_MYSQL_PORT", "3306")),
        "user": os.environ.get("INTENT_TEST_MYSQL_USER", "root"),
        "password": os.environ.get("INTENT_TEST_MYSQL_PASSWORD", ""),
    }


requires_mysql = pytest.mark.skipif(mysql_settings() is None, reason="INTENT_TEST_MYSQL_HOST not set")
requires_kafka = pytest.mark.skipif(
    not os.environ.get("INTENT_TEST_KAFKA_BOOTSTRAP") or mysql_settings() is None,
    reason="INTENT_TEST_KAFKA_BOOTSTRAP and INTENT_TEST_MYSQL_HOST not set",
)


class MysqlTestDb:
    def __init__(self, settings, name):
        self.settings, self.name = settings, name

    def connect(self, **extra):
        import mysql.connector

        return mysql.connector.connect(database=self.name, **self.settings, **extra)

    @contextlib.contextmanager
    def factory(self, _brand_index=None):
        """Stands in for pipeline.db.get_db_connection: yields an open connection."""
        connection = self.connect()
        try:
            yield connection
        finally:
            try:
                if connection.in_transaction:
                    connection.rollback()
            finally:
                connection.close()

    def rows(self, sql, params=()):
        connection = self.connect()
        try:
            cursor = connection.cursor(dictionary=True, buffered=True)
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            connection.close()

    def execute(self, sql, params=()):
        connection = self.connect()
        try:
            cursor = connection.cursor(buffered=True)
            cursor.execute(sql, params)
            connection.commit()
        finally:
            connection.close()

    def create_schema(self, with_state_tables=True):
        from pipeline.intent_events import (
            _ensure_behavioral_events_table,
            _ensure_click_events_table,
            _ensure_intent_sessions_table,
        )
        from pipeline.intent_kafka_store import apply_migration

        connection = self.connect()
        try:
            cursor = connection.cursor(buffered=True)
            _ensure_behavioral_events_table(cursor, connection)
            _ensure_click_events_table(cursor, connection)
            _ensure_intent_sessions_table(cursor, connection)
            if with_state_tables:
                apply_migration(cursor, connection)
        finally:
            connection.close()

    def truncate_all(self):
        connection = self.connect()
        try:
            cursor = connection.cursor(buffered=True)
            for table in TEST_TABLES:
                cursor.execute(f"TRUNCATE TABLE {table}")
            connection.commit()
        finally:
            connection.close()


@pytest.fixture(scope="module")
def mysql_db():
    settings = mysql_settings()
    if settings is None:
        pytest.skip("INTENT_TEST_MYSQL_HOST not set")
    import mysql.connector

    name = f"intent_test_{uuid.uuid4().hex[:8]}"
    admin = mysql.connector.connect(**settings)
    admin.cursor().execute(f"CREATE DATABASE {name} CHARACTER SET utf8mb4")
    handle = MysqlTestDb(settings, name)
    handle.create_schema()
    try:
        yield handle
    finally:
        assert name.startswith("intent_test_")
        admin.cursor().execute(f"DROP DATABASE {name}")
        admin.close()


@pytest.fixture
def clean_db(mysql_db):
    mysql_db.truncate_all()
    return mysql_db
