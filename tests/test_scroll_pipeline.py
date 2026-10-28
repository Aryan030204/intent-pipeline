"""
The scroll path, end to end inside this repo: a `scroll_depth` event from /track becomes a behavioral_events row, is
counted in the session's scroll_count when the session closes, and reaches the scoring features.

These tests pin the contract the pipeline expects. They cannot tell whether the pixel actually sends the event, or
under which name: the event census (scripts/diagnose_intent.py section 19) answers that against real data. If the
pixel names the event anything but `scroll_depth`, scroll_count stays 0 for every session, which is exactly what the
"unknown event type" test shows.
"""

import pytest

from intent_engine.config import SCROLLS_CAP, SCROLLS_WEIGHT
from intent_engine.feature_calculator import extract_features
from intent_engine.score_calculator import compute_raw_score
from pipeline.intent_events import _BEHAVIORAL_UPSERT_COLUMNS, _INTENT_SESSIONS_UPSERT_COLUMNS
from pipeline.intent_kafka_contract import InvalidMessage, parse_record
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch

from tests.helpers import encode, items, value

GAP = 1900   # seconds: more than the 1800 s session timeout, so the next event closes the session


def scroll(event_id, at, percent, actor="cid-1", name="scroll_depth", raw="default"):
    return value(event_id, at, actor=actor, name=name, raw={"percent": percent} if raw == "default" else raw)


def column(row, name):
    return row[_INTENT_SESSIONS_UPSERT_COLUMNS.index(name)]


def close_session(*messages):
    """Applies the messages for one visitor, then an event after the timeout that closes their session.
    Returns (the closed session row, the store)."""
    store = InMemoryIntentStore()
    apply_batch(store, items(*messages, value("later", 100 + GAP, actor=messages[0]["actor_id"])))
    assert len(store.sessions) == 1, "the later event should have closed exactly one session"
    return store.sessions[0], store


def test_a_scroll_event_at_25_percent_is_stored_with_its_percent_and_counted():
    row, store = close_session(value("p1", 0), scroll("s1", 5, 25))
    assert column(row, "scroll_count") == 1
    stored = dict(zip(_BEHAVIORAL_UPSERT_COLUMNS, store.events["s1"]))
    assert stored["event_name"] == "scroll_depth" and stored["scroll_percent"] == 25.0


def test_a_scroll_event_at_75_percent_is_counted():
    row, store = close_session(value("p1", 0), scroll("s1", 5, 75))
    assert column(row, "scroll_count") == 1
    assert dict(zip(_BEHAVIORAL_UPSERT_COLUMNS, store.events["s1"]))["scroll_percent"] == 75.0


def test_several_milestones_are_each_one_scroll_event():
    row, _ = close_session(value("p1", 0), scroll("s25", 4, 25), scroll("s50", 8, 50), scroll("s75", 12, 75), scroll("s100", 16, 100))
    assert column(row, "scroll_count") == 4


def test_the_same_scroll_event_delivered_twice_counts_once():
    # a Kafka redelivery or a replay: the event id is unique, the second copy is a duplicate and changes nothing
    store = InMemoryIntentStore()
    messages = [value("p1", 0), scroll("s1", 5, 25), scroll("s1", 5, 25), value("later", 100 + GAP)]
    apply_batch(store, items(*messages))
    assert column(store.sessions[0], "scroll_count") == 1


def test_a_session_with_a_scroll_and_no_click_has_scroll_count_above_zero():
    row, _ = close_session(value("p1", 0), scroll("s1", 6, 50))
    assert column(row, "scroll_count") == 1 and column(row, "click_count") == 0


def test_a_session_with_a_scroll_and_a_click_counts_both():
    row, _ = close_session(value("p1", 0), scroll("s1", 4, 25), value("c1", 8, click=True))
    assert column(row, "scroll_count") == 1 and column(row, "click_count") == 1
    assert column(row, "event_count") == 3


def test_a_scroll_event_without_a_usable_percent_is_still_one_scroll_but_has_no_percent():
    # the session builder counts scroll events by name; a missing or non-numeric percent only leaves the column NULL
    row, store = close_session(value("p1", 0), scroll("s1", 4, None, raw={}), scroll("s2", 8, "not-a-number", raw={"percent": "not-a-number"}))
    assert column(row, "scroll_count") == 2
    assert dict(zip(_BEHAVIORAL_UPSERT_COLUMNS, store.events["s1"]))["scroll_percent"] is None
    assert dict(zip(_BEHAVIORAL_UPSERT_COLUMNS, store.events["s2"]))["scroll_percent"] is None


def test_an_unparseable_scroll_message_is_rejected_not_counted():
    with pytest.raises(InvalidMessage):
        parse_record(b"{not json")
    broken = scroll("bad", 3, 25)
    broken["occurred_at"] = "yesterday-ish"
    with pytest.raises(InvalidMessage):
        parse_record(encode(broken))


def test_an_event_with_another_name_is_not_a_scroll():
    # THE failure mode to rule out upstream: if the pixel sends 'scroll' / 'scrolled' / 'scroll_depth_reached', nothing
    # here recognises it, every session keeps scroll_count = 0, and the scroll features stay dead.
    for name in ("scroll", "scrolled", "scroll_depth_reached", "scrollDepth", "SCROLL_DEPTH"):
        row, _ = close_session(value("p1", 0, actor=f"cid-{name}"), scroll("s1", 5, 50, actor=f"cid-{name}", name=name))
        assert column(row, "scroll_count") == 0, name
        assert column(row, "event_count") == 2                                   # stored as an event, just not a scroll


def test_scroll_events_do_not_become_clicks_or_page_views():
    row, _ = close_session(value("p1", 0), scroll("s1", 4, 25))
    assert (column(row, "page_view_count"), column(row, "click_count"), column(row, "product_view_count")) == (1, 0, 0)


# ---- the scoring engine receives the signal ----

def features_for(scroll_count):
    return extract_features({"session_time_spent_ms": 60000, "scroll_count": scroll_count, "product_view_count": 0,
                             "useful_click_count": 0, "page_view_count": 1})


def test_the_scroll_count_reaches_the_scoring_features_and_the_score():
    assert features_for(3).scrolls == 3
    assert features_for(None).scrolls == 0
    assert compute_raw_score(features_for(3)) - compute_raw_score(features_for(0)) == pytest.approx(3 * SCROLLS_WEIGHT)


def test_one_physical_scroll_cannot_earn_unlimited_points():
    # the existing cap: past SCROLLS_CAP scroll events a session earns nothing more for scrolling
    assert compute_raw_score(features_for(SCROLLS_CAP)) == compute_raw_score(features_for(SCROLLS_CAP * 10))
    assert compute_raw_score(features_for(SCROLLS_CAP)) - compute_raw_score(features_for(0)) == pytest.approx(SCROLLS_CAP * SCROLLS_WEIGHT)
