"""score_sessions_isolated: one session that cannot be scored must not stop the others."""

import logging
from datetime import datetime

import pytest

from intent_engine import session_scorer

NOW = datetime(2026, 10, 15, 11, 0, 0)


class Db:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


def sessions(n):
    return [{"session_id": f"s{i}", "actor_id": f"a{i}"} for i in range(n)]


class Fake:
    """A stand-in for score_sessions: raises if any session of the group is poisoned, otherwise 'writes' them."""

    def __init__(self, poison=(), error=None):
        self.poison, self.error, self.calls, self.written = set(poison), error, [], {}

    def __call__(self, cursor, connection, group, now, read_cursor=None, label="", stats=None, **_):
        ids = [s["session_id"] for s in group]
        self.calls.append(ids)
        if self.poison & set(ids):
            raise self.error or ValueError(f"cannot score {sorted(self.poison & set(ids))}")
        for s in group:
            self.written[s["session_id"]] = self.written.get(s["session_id"], 0) + 1     # an UPDATE by id, never an INSERT
        if stats is not None:
            stats["scored"] = stats.get("scored", 0) + len(group)
        return {s["actor_id"] for s in group}


def run(monkeypatch, fake, n, db=None, stats=None):
    monkeypatch.setattr(session_scorer, "score_sessions", fake)
    return session_scorer.score_sessions_isolated(object(), db or Db(), sessions(n), NOW, stats=stats)


def test_a_clean_batch_is_scored_in_one_go(monkeypatch):
    fake, stats = Fake(), {}
    actors, failed = run(monkeypatch, fake, 8, stats=stats)
    assert failed == [] and len(fake.calls) == 1 and actors == {f"a{i}" for i in range(8)}
    assert stats == {"scored": 8, "excluded": 0, "failed": 0}


def test_one_bad_session_is_isolated_and_all_the_others_are_scored(monkeypatch, caplog):
    fake, stats, db = Fake(poison={"s5"}), {}, Db()
    with caplog.at_level(logging.ERROR):
        actors, failed = run(monkeypatch, fake, 8, db=db, stats=stats)
    assert failed == ["s5"]
    assert set(fake.written) == {f"s{i}" for i in range(8)} - {"s5"}           # everyone else was written
    assert "a5" not in actors and {f"a{i}" for i in range(8)} - {"a5"} <= actors
    assert stats["scored"] == 7 and stats["failed"] == 1
    assert db.rollbacks >= 1                                                    # the failed attempt was rolled back
    assert any("scoring_session_failed session_id=s5" in r.getMessage() for r in caplog.records)
    assert len(fake.calls) < 2 * 8                                              # bisection, not one call per session each time


def test_several_bad_sessions_are_all_found(monkeypatch):
    fake, stats = Fake(poison={"s1", "s6"}), {}
    _actors, failed = run(monkeypatch, fake, 8, stats=stats)
    assert sorted(failed) == ["s1", "s6"] and stats["scored"] == 6 and stats["failed"] == 2


def test_a_batch_where_every_session_fails_returns_them_all_without_raising(monkeypatch):
    _actors, failed = run(monkeypatch, Fake(poison={f"s{i}" for i in range(4)}), 4)
    assert sorted(failed) == ["s0", "s1", "s2", "s3"]


def test_a_lost_connection_is_raised_not_retried_session_by_session(monkeypatch):
    class Gone(Exception):
        errno = 2013

    fake = Fake(poison={"s2"}, error=Gone("Lost connection to MySQL server during query"))
    with pytest.raises(Gone):
        run(monkeypatch, fake, 8)
    assert len(fake.calls) == 1                                                 # no per-session retries against a dead connection


def test_scoring_the_same_sessions_again_is_safe_and_changes_nothing_but_the_write_count(monkeypatch):
    fake = Fake()
    run(monkeypatch, fake, 5)
    first = set(fake.written)
    run(monkeypatch, fake, 5)                                                    # a retry / an overlapping window
    assert set(fake.written) == first and all(count == 2 for count in fake.written.values())   # rows updated twice, none added


def test_an_empty_list_does_nothing(monkeypatch):
    fake = Fake()
    actors, failed = run(monkeypatch, fake, 0)
    assert actors == set() and failed == []


def test_a_failure_in_the_windowed_pass_page_does_not_stop_the_window(monkeypatch):
    # score_window_for_brand scores each page through the isolating function
    pages = [sessions(3), []]
    monkeypatch.setattr(session_scorer, "fetch_eligible_sessions", lambda *a, **k: pages.pop(0) if pages else [])
    fake = Fake(poison={"s1"})
    monkeypatch.setattr(session_scorer, "score_sessions", fake)
    touched = session_scorer.score_window_for_brand(object(), Db(), NOW, NOW, NOW)
    assert touched == {"a0", "a2"}                                               # s1 left pending, the others scored
