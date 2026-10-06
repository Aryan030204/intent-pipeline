"""The pure session rules, one test per behaviour of the old sessionState.js."""

from datetime import datetime, timedelta

from pipeline.intent_kafka_contract import parse_record
from pipeline.intent_session_state import (
    NEGATIVE_GAP_TOLERANCE_S,
    ActorCursor,
    closed_session_doc,
    commit_event,
    resolve_timing,
)
from tests.helpers import encode, value

T0 = datetime(2026, 10, 5, 10, 30, 0)
TIMEOUT = 1800


def cursor(last=T0, start=T0, seq=None):
    return ActorCursor("a", "s-1", start, last, "e-last", seq or {"1": {"event_id": "e-last"}})


def message(event_id="e-new"):
    return parse_record(encode(value(event_id)))


def test_no_cursor_starts_a_new_session_at_the_event_time():
    timing = resolve_timing(None, T0, TIMEOUT)
    assert timing.is_new_session and timing.session_start == T0


def test_gap_exactly_at_the_timeout_continues_and_one_microsecond_over_starts_a_new_session():
    assert not resolve_timing(cursor(), T0 + timedelta(seconds=TIMEOUT), TIMEOUT).is_new_session
    assert resolve_timing(cursor(), T0 + timedelta(seconds=TIMEOUT, microseconds=1), TIMEOUT).is_new_session


def test_small_negative_gap_stays_in_the_session_and_never_moves_last_event_back():
    for seconds in (-1, -NEGATIVE_GAP_TOLERANCE_S):
        when = T0 + timedelta(seconds=seconds)
        timing = resolve_timing(cursor(), when, TIMEOUT)
        assert not timing.is_new_session and timing.session_id == "s-1"
        after, closed = commit_event(cursor(), timing, when, message(), "a")
        assert closed is None
        assert after.last_event_at == T0 and after.last_event_id == "e-last"


def test_negative_gap_beyond_the_tolerance_starts_a_new_session():
    timing = resolve_timing(cursor(), T0 - timedelta(seconds=NEGATIVE_GAP_TOLERANCE_S, microseconds=1), TIMEOUT)
    assert timing.is_new_session


def test_continuing_event_appends_the_next_step_and_advances_last_event():
    when = T0 + timedelta(seconds=60)
    timing = resolve_timing(cursor(), when, TIMEOUT)
    after, closed = commit_event(cursor(), timing, when, message("e-2"), "a")
    assert closed is None
    assert list(after.events_seq) == ["1", "2"]
    assert after.last_event_at == when and after.last_event_id == "e-2"
    assert after.session_id == "s-1" and after.session_start == T0


def test_rollover_closes_the_previous_session_with_end_and_time_spent():
    open_cursor = cursor(last=T0 + timedelta(seconds=100), seq={"1": {"event_id": "a"}, "2": {"event_id": "b"}})
    when = T0 + timedelta(seconds=100 + TIMEOUT + 1)
    timing = resolve_timing(open_cursor, when, TIMEOUT)
    after, closed = commit_event(open_cursor, timing, when, message("e-new"), "a")
    assert closed["session_id"] == "s-1"
    assert closed["session_end"] == T0 + timedelta(seconds=100)       # the previous last event
    assert closed["session_time_spent"] == 100_000                    # ms, timeout gap excluded
    assert list(closed["events_seq"]) == ["1", "2"]
    assert after.session_id != "s-1" and after.session_start == when
    assert list(after.events_seq) == ["1"]                             # sequence resets


def test_closed_session_doc_matches_what_the_extractor_reads():
    doc = closed_session_doc(cursor(last=T0 + timedelta(seconds=5)))
    assert set(doc) == {"session_id", "actor_id", "session_start", "session_end",
                        "session_time_spent", "events_seq", "updatedAt"}


def test_commit_does_not_mutate_the_input_cursor():
    original = cursor()
    when = T0 + timedelta(seconds=10)
    commit_event(original, resolve_timing(original, when, TIMEOUT), when, message(), "a")
    assert list(original.events_seq) == ["1"]
