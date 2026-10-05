"""Regression: mysql-connector raises InternalError 'Unread result found' when a statement
runs on a cursor whose previous SELECT result was not fully fetched (buffered=False, the
default that pipeline/db.py uses). The fake below behaves that way, so a missed fetch
fails the test the same way it fails in production."""

from mysql.connector.errors import InternalError

from pipeline.intent_sqs_store import reset_schema_verification
from pipeline.intent_sqs_writer import apply_batch
from tests.fake_db import FakeConnection, FakeDbCursor
from tests.test_intent_session_state import event, parse


class UnbufferedCursor(FakeDbCursor):
    """Any SELECT leaves a result set unread until fetchone/fetchall consumes it."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unread = False

    def execute(self, sql, params=None):
        if self.unread:
            raise InternalError("Unread result found")
        super().execute(sql, params)
        self.unread = sql.lstrip().upper().startswith("SELECT")

    def fetchone(self):
        self.unread = False
        return super().fetchone()

    def fetchall(self):
        self.unread = False
        return super().fetchall()


def _run(messages):
    reset_schema_verification()
    cursor, connection = UnbufferedCursor(), FakeConnection()
    apply_batch(cursor, connection, parse(messages))
    return cursor, connection


def test_single_actor_batch_commits_on_unbuffered_cursor():
    _, connection = _run([event("e-1", 0), event("e-2", 30)])
    assert connection.commits == 1


def test_two_actor_batch_commits_on_unbuffered_cursor():
    # Two locks held means two RELEASE_LOCK statements in unlock_all. Each SELECT must be
    # fetched before the next statement; before the fix the second raised 'Unread result found'.
    _, connection = _run([event("e-a", 0, actor="actor-a"), event("e-b", 10, actor="actor-b")])
    assert connection.commits == 1


def test_every_actor_lock_is_released_after_a_multi_actor_batch():
    cursor, _ = _run([
        event("e-a", 0, actor="actor-a"),
        event("e-b", 10, actor="actor-b"),
        event("e-c", 20, actor="actor-c"),
    ])
    release_statements = [s for s in cursor.sql if "RELEASE_LOCK" in s]
    assert len(release_statements) == 3
