"""score_window_for_brand reads and scores the window in pages; the outcome equals a single pass."""
from datetime import datetime, timedelta
from unittest import mock

from intent_engine import repository, session_scorer


def _sessions(n):
    base = datetime(2026, 8, 17, 10, 0, 0)
    return [{"session_id": f"s{i:05d}", "updated_at": base + timedelta(seconds=i // 3)} for i in range(n)]


class _PagedSource:
    """Serves fetch_eligible_sessions(reader, ws, we, limit, after) from a fixed list, like the SQL does."""

    def __init__(self, rows):
        self.rows = sorted(rows, key=lambda r: (r["updated_at"], r["session_id"]))
        self.calls = []

    def __call__(self, reader, ws, we, limit=None, after=None):
        self.calls.append((limit, after))
        rows = self.rows
        if limit is None:
            return list(rows)
        if after is not None:
            rows = [r for r in rows if (r["updated_at"], r["session_id"]) > after]
        return rows[:limit]


def _run(monkeypatch, n, page):
    source = _PagedSource(_sessions(n))
    scored_batches = []

    def fake_score(cursor, connection, sessions, now, read_cursor=None, label="", started_at=None):
        scored_batches.append([s["session_id"] for s in sessions])
        return {f"actor-{s['session_id']}" for s in sessions}

    monkeypatch.setattr(session_scorer, "fetch_eligible_sessions", source)
    monkeypatch.setattr(session_scorer, "score_sessions", fake_score)
    monkeypatch.setenv("INTENT_SCORING_WINDOW_CHUNK", str(page))
    touched = session_scorer.score_window_for_brand(
        mock.Mock(), mock.Mock(), datetime(2026, 8, 17), datetime(2026, 8, 18), datetime(2026, 8, 18)
    )
    return touched, scored_batches, source


def test_every_session_is_scored_exactly_once_across_pages(monkeypatch):
    touched, batches, source = _run(monkeypatch, 450, 100)

    flat = [sid for b in batches for sid in b]
    assert len(flat) == 450 and len(set(flat)) == 450
    assert max(len(b) for b in batches) <= 100
    assert len(batches) == 5
    assert touched == {f"actor-s{i:05d}" for i in range(450)}


def test_exact_multiple_of_page_size_terminates(monkeypatch):
    touched, batches, _ = _run(monkeypatch, 300, 100)

    assert [len(b) for b in batches] == [100, 100, 100]
    assert len(touched) == 300


def test_empty_window_scores_nothing(monkeypatch):
    touched, batches, source = _run(monkeypatch, 0, 100)

    assert touched == set() and batches == [] and len(source.calls) == 1


def test_default_without_limit_still_returns_everything():
    cursor = mock.Mock()
    cursor.fetchall.return_value = ["row"]
    rows = repository.fetch_eligible_sessions(cursor, datetime(2026, 8, 17), datetime(2026, 8, 18))

    sql, params = cursor.execute.call_args.args
    assert rows == ["row"]
    assert "LIMIT" not in sql and len(params) == 2


def test_paged_query_uses_keyset_after_the_last_row():
    cursor = mock.Mock()
    cursor.fetchall.return_value = []
    after = (datetime(2026, 8, 17, 10), "s00007")
    repository.fetch_eligible_sessions(cursor, datetime(2026, 8, 17), datetime(2026, 8, 18), 100, after)

    sql, params = cursor.execute.call_args.args
    assert "ORDER BY updated_at, session_id LIMIT %s" in sql
    assert "session_id > %s" in sql
    assert params[-1] == 100 and params[-2] == "s00007"
