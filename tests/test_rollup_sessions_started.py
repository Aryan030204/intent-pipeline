"""
sessions_started: one canonical definition of "a session" for every session figure.

A session is a row of intent_sessions (closed, or closed by the idle-session sweep) or, until it closes, the visitor's
open session record in intent_actor_cursors. The rollup's sessions_started is those two added together per day. It
no longer counts distinct session ids in behavioral_events, a population that misses sessions made only of clicks.
"""

import logging
from datetime import date, datetime

from pipeline import rollups
from tests.test_intent_phase2_phase3 import Conn, ScriptCursor

DAY = date(2026, 10, 28)
YESTERDAY = date(2026, 10, 27)


def agg(day, sessions, **kw):
    row = {"d": day, "sessions": sessions, "unique_actors": sessions, "total_events": sessions * 3, "page_views": sessions,
           "product_views": 0, "clicks": 0, "useful_clicks": 0, "dead_clicks": 0, "add_to_carts": 0, "checkout_starts": 0,
           "scroll_events": 0, "avg_session_time_ms": 1000, "avg_events_per_session": 3, "avg_product_views_per_session": 0,
           "avg_clicks_per_session": 0}
    row.update(kw)
    return row


def run(closed_rows, open_rows, dates=(DAY,), fail_open=False):
    fail = {"FROM intent_actor_cursors": RuntimeError("no such table")} if fail_open else {}
    cursor = ScriptCursor(
        fail_on=fail,
        rows_for={"FROM intent_sessions": closed_rows, "FROM intent_actor_cursors": open_rows},
    )
    rollups._rollup_intent_daily_summary(cursor, Conn(), list(dates))
    return cursor


def stored(cursor):
    """{date: (sessions, sessions_started)} as written to intent_daily_summary."""
    out = {}
    for sql, rows in cursor.many_calls:
        if "INSERT INTO intent_daily_summary" in sql:
            for row in rows:
                out[row[0]] = (row[1], row[-1])
    return out


def test_sessions_started_is_closed_plus_open_sessions():
    cursor = run([agg(DAY, 1200)], [{"d": DAY, "open_sessions": 35}])
    assert stored(cursor) == {DAY: (1200, 1235)}


def test_it_counts_each_day_of_the_window_separately():
    cursor = run([agg(YESTERDAY, 900), agg(DAY, 100)], [{"d": YESTERDAY, "open_sessions": 4}, {"d": DAY, "open_sessions": 20}],
                 dates=(YESTERDAY, DAY))
    assert stored(cursor) == {YESTERDAY: (900, 904), DAY: (100, 120)}


def test_a_day_with_only_open_sessions_still_gets_its_count():
    # typical for the first minutes of a day: nothing has closed yet, visitors are mid-session
    cursor = run([], [{"d": DAY, "open_sessions": 12}])
    assert stored(cursor) == {DAY: (0, 12)}


def test_a_day_with_no_sessions_at_all_gets_no_row():
    assert stored(run([], [])) == {}


def test_a_click_only_session_is_a_session():
    # Its events are all in click_events, so behavioral_events has nothing for it. It is a row of intent_sessions
    # (the session builder counts clicks too), so it is counted, which the old events-based count could not do.
    cursor = run([agg(DAY, 1, page_views=0, clicks=5)], [])
    assert stored(cursor) == {DAY: (1, 1)}


def test_bots_are_inside_the_canonical_count():
    # the rollup counts every session row; bot sessions are identified at scoring and are left out of the intent
    # distribution as a documented, reported exclusion, never silently here
    assert stored(run([agg(DAY, 500)], [])) == {DAY: (500, 500)}


def test_the_count_no_longer_reads_the_events_table():
    cursor = run([agg(DAY, 10)], [{"d": DAY, "open_sessions": 1}])
    sql = " ".join(call[0] for call in cursor.calls)
    assert "FROM behavioral_events" not in sql and "COUNT(DISTINCT session_id)" not in sql


def test_open_sessions_are_read_by_range_on_the_indexed_last_event_time():
    cursor = run([agg(DAY, 10)], [])
    sql, params = next(c for c in cursor.calls if "FROM intent_actor_cursors" in c[0])
    assert "session_id <> ''" in sql                          # placeholder rows are not sessions
    assert "last_event_at >= %s" in sql and "session_start >= %s AND session_start < %s" in sql
    assert params[0] == params[1] and params[2] > params[1]    # window start (twice) and end


def test_an_unreadable_cursor_table_falls_back_to_closed_sessions_and_says_so(caplog):
    with caplog.at_level(logging.WARNING):
        cursor = run([agg(DAY, 700)], [], fail_open=True)
    assert stored(cursor) == {DAY: (700, 700)}
    assert any("open sessions not counted" in r.getMessage() for r in caplog.records)


def test_the_closed_count_in_the_row_and_the_started_count_agree_when_nothing_is_open():
    (sessions, started), = stored(run([agg(DAY, 321)], [])).values()
    assert sessions == started == 321
