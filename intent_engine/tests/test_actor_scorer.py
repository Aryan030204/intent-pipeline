import datetime

from intent_engine.actor_scorer import recompute_actors


class _FakeConnection:
    def __init__(self):
        self.committed = False

    def commit(self):
        self.committed = True


def test_actor_with_multiple_sessions_uses_max_score():
    """actor with sessions low/medium/high -> actor bucket is high (best, not average/last)."""
    rows = [
        {"actor_id": "a1", "session_id": "s3", "predictive_score": 80.0, "intent_bucket": "high"},
        {"actor_id": "a1", "session_id": "s1", "predictive_score": 10.0, "intent_bucket": "low"},
        {"actor_id": "a1", "session_id": "s2", "predictive_score": 40.0, "intent_bucket": "medium"},
    ]

    class _Cursor:
        def __init__(self):
            self.executemany_calls = []

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return rows

        def executemany(self, sql, params_list):
            self.executemany_calls.append((sql, params_list))

    cursor = _Cursor()
    connection = _FakeConnection()
    recompute_actors(cursor, connection, ["a1"], datetime.datetime(2026, 9, 28))

    assert len(cursor.executemany_calls) == 1
    _, params_list = cursor.executemany_calls[0]
    assert len(params_list) == 1
    actor_id, predictive_score, intent_bucket, best_session_id, best_session_score, session_count, _, _ = params_list[0]
    assert actor_id == "a1"
    assert predictive_score == 80.0
    assert intent_bucket == "high"
    assert best_session_id == "s3"
    assert session_count == 3


def test_all_excluded_actor_gets_null_score_not_low():
    class _Cursor:
        def __init__(self):
            self.executemany_calls = []

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return []  # no scored sessions at all for this actor

        def executemany(self, sql, params_list):
            self.executemany_calls.append((sql, params_list))

    cursor = _Cursor()
    connection = _FakeConnection()
    recompute_actors(cursor, connection, ["a_excluded"], datetime.datetime(2026, 9, 28))

    _, params_list = cursor.executemany_calls[0]
    actor_id, predictive_score, intent_bucket, best_session_id, best_session_score, session_count, _, _ = params_list[0]
    assert actor_id == "a_excluded"
    assert predictive_score is None
    assert intent_bucket is None
    assert session_count == 0


def test_empty_actor_ids_is_noop():
    class _Cursor:
        def execute(self, sql, params=None):
            raise AssertionError("should not query when actor_ids is empty")

    cursor = _Cursor()
    connection = _FakeConnection()
    result = recompute_actors(cursor, connection, [], datetime.datetime(2026, 9, 28))
    assert result == 0
