"""Future-dated client timestamps must not drive sessionization or poison the idle-session sweep.

The incident: a visitor's device clock was hours ahead, the actor cursor written for it carried that future time, the sweep took
the newest cursor time as "now", every live cursor looked idle, and the next event of each visitor started a new session."""

from datetime import datetime, timedelta

import pytest

from pipeline.intent_kafka_contract import parse_record
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch
from pipeline.intent_session_sweep import SWEEP_MARGIN_S, sweep_brand
from pipeline.intent_time_policy import FUTURE_TOLERANCE_S, effective_latest, normalize_occurred_at, store_local_now

from tests.helpers import BASE, FakeDb, encode, items, stamp, value

TIMEOUT = 1800


def _db(store):
    return FakeDb([], {1: store})


def _ingest(store, *messages):
    apply_batch(store, items(*messages), TIMEOUT)


def _sessions_of(store, actor):
    return {s[0] for s in store.sessions if s[1] == actor} | ({store.cursors[actor].session_id} if actor in store.cursors else set())


# ---------------- the two rules in isolation ----------------

def test_a_small_future_skew_is_accepted_as_is_and_a_larger_one_takes_the_receipt_time():
    received = BASE
    assert normalize_occurred_at(received + timedelta(seconds=FUTURE_TOLERANCE_S), received) == (received + timedelta(seconds=FUTURE_TOLERANCE_S), False)
    assert normalize_occurred_at(received + timedelta(seconds=FUTURE_TOLERANCE_S + 1), received) == (received, True)
    assert normalize_occurred_at(received - timedelta(hours=3), received) == (received - timedelta(hours=3), False)   # late events are not touched
    assert normalize_occurred_at(datetime(2040, 1, 1, 1, 5), received) == (received, True)
    assert normalize_occurred_at(datetime(2040, 1, 1, 1, 5), None) == (datetime(2040, 1, 1, 1, 5), False)            # no receipt time: nothing to judge


def test_the_sweep_reference_is_never_later_than_store_local_now():
    now = BASE
    assert effective_latest(now + timedelta(hours=4), now) == now
    assert effective_latest(now - timedelta(minutes=2), now) == now - timedelta(minutes=2)
    assert effective_latest(None, now) is None
    assert effective_latest(now, None) == now


def test_store_local_now_is_naive_in_the_brand_zone(monkeypatch):
    from zoneinfo import ZoneInfo
    from pipeline import intent_time_policy as policy

    monkeypatch.setitem(policy.brand_timezones, 77, ZoneInfo("Asia/Kolkata"))
    instant = datetime(2026, 11, 17, 9, 50, tzinfo=ZoneInfo("UTC"))
    assert store_local_now(77, clock=lambda: instant) == datetime(2026, 11, 17, 15, 20)
    assert store_local_now(78, clock=lambda: instant) == datetime(2026, 11, 17, 15, 20)       # unknown brand falls back to IST


# ---------------- the contract applies the rule and keeps the raw time ----------------

def _message(at_seconds, received_seconds=0, **extra):
    m = value("e1", at_seconds)
    m["received_at"] = stamp(received_seconds)
    m.update(extra)
    return parse_record(encode(m))


def test_a_far_future_client_clock_is_replaced_by_the_receipt_time_and_the_raw_time_is_kept():
    parsed = _message(4 * 3600, 0)
    assert parsed.occurred_at == BASE and parsed.client_occurred_at == BASE + timedelta(hours=4) and parsed.received_at == BASE


def test_the_year_2040_event_cannot_reach_the_cursor():
    m = value("e1", 0)
    m["occurred_at"] = "2040-01-01T01:05:35.922Z"
    m["received_at"] = stamp(0)
    parsed = parse_record(encode(m))
    assert parsed.occurred_at == BASE and parsed.client_occurred_at == datetime(2040, 1, 1, 1, 5, 35, 922000)
    store = InMemoryIntentStore()
    apply_batch(store, items(m), TIMEOUT)
    assert store.cursors["cid-1"].last_event_at == BASE


def test_slight_skew_and_messages_without_a_receipt_time_are_unchanged():
    assert _message(FUTURE_TOLERANCE_S - 1, 0).occurred_at == BASE + timedelta(seconds=FUTURE_TOLERANCE_S - 1)
    assert _message(FUTURE_TOLERANCE_S - 1, 0).client_occurred_at is None
    plain = parse_record(encode(value("e2", 600)))
    assert plain.occurred_at == BASE + timedelta(seconds=600) and plain.received_at is None and plain.client_occurred_at is None


def test_an_invalid_diagnostic_field_never_poisons_the_record():
    m = value("e3", 0)
    m["received_at"] = "not a time"
    m["client_occurred_at"] = 12345
    parsed = parse_record(encode(m))
    assert parsed.occurred_at == BASE and parsed.received_at is None and parsed.client_occurred_at is None


# ---------------- ordinary sessionization is unchanged ----------------

def test_five_minutes_apart_is_one_session_and_thirty_one_minutes_is_two():
    store = InMemoryIntentStore()
    _ingest(store, value("a", 0, actor="x"), value("b", 300, actor="x"))
    assert len(_sessions_of(store, "x")) == 1
    _ingest(store, value("c", 300 + 31 * 60, actor="x"))
    assert len(_sessions_of(store, "x")) == 2


def test_an_event_slightly_out_of_order_stays_in_the_session_and_a_client_two_minutes_fast_is_one_session():
    store = InMemoryIntentStore()
    _ingest(store, value("a", 100, actor="x"), value("b", 80, actor="x"))          # 20 s out of order: tolerated (30 s)
    assert len(_sessions_of(store, "x")) == 1
    m = value("c", 130, actor="x")
    m["received_at"] = stamp(10)                                                    # +120 s ahead of the receipt time, inside the tolerance
    _ingest(store, m)
    assert len(_sessions_of(store, "x")) == 1


# ---------------- the sweep cannot be poisoned ----------------

def _with_future_cursor(store, hours):
    """A visitor whose device clock is `hours` ahead: their cursor is in the future of the real store time (BASE + 2 min)."""
    _ingest(store, value("f1", 0, actor="future"))
    c = store.cursors["future"]
    store.cursors["future"] = c.__class__(actor_id="future", session_id=c.session_id, session_start=BASE + timedelta(hours=hours),
                                          last_event_at=BASE + timedelta(hours=hours), last_event_id="f1", events_seq=c.events_seq)


def test_a_future_cursor_does_not_close_live_sessions__and_without_the_cap_it_does():
    store = InMemoryIntentStore()
    _ingest(store, value("l1", 60, actor="live"))
    _with_future_cursor(store, 4)                                   # inside the 14 h guard: the dangerous case
    real_now = BASE + timedelta(minutes=2)

    assert sweep_brand(_db(store), 1, TIMEOUT, now_fn=lambda: real_now).closed == 0
    assert "live" in store.cursors and "future" in store.cursors

    # control: the previous behaviour (no cap) treats the live visitor as idle and deletes their cursor
    assert sweep_brand(_db(store), 1, TIMEOUT).closed == 1
    assert "live" not in store.cursors


def test_a_future_actor_never_closes_another_actors_session_and_repeated_sweeps_destroy_nothing():
    store = InMemoryIntentStore()
    _ingest(store, value("a1", 60, actor="a"), value("b1", 90, actor="b"), value("c1", 120, actor="c"))
    _with_future_cursor(store, 6)
    for minute in range(2, 12):                                      # ten cycles, five minutes apart in real time
        now = BASE + timedelta(minutes=minute * 5 // 5)
        result = sweep_brand(_db(store), 1, TIMEOUT, now_fn=lambda now=now: now)
        assert result.closed == 0 and result.effective_latest <= now
    assert {"a", "b", "c", "future"} <= set(store.cursors)


def test_the_cap_is_reported_and_idle_sessions_still_close_once_real_time_passes():
    store = InMemoryIntentStore()
    _ingest(store, value("a1", 0, actor="a"))
    _with_future_cursor(store, 3)
    later = BASE + timedelta(seconds=TIMEOUT + SWEEP_MARGIN_S + 10)  # a genuinely idle visitor
    result = sweep_brand(_db(store), 1, TIMEOUT, now_fn=lambda: later)
    assert result.latest == BASE + timedelta(hours=3) and result.effective_latest == later
    assert result.closed == 1 and "a" not in store.cursors and "future" in store.cursors


def test_a_normalised_future_event_does_not_even_create_the_future_cursor():
    store = InMemoryIntentStore()
    _ingest(store, value("l1", 60, actor="live"))
    m = value("f1", 4 * 3600, actor="bad")
    m["received_at"] = stamp(70)
    _ingest(store, m)
    assert store.cursors["bad"].last_event_at == BASE + timedelta(seconds=70)
    assert sweep_brand(_db(store), 1, TIMEOUT, now_fn=lambda: BASE + timedelta(seconds=75)).closed == 0


def test_actors_in_one_batch_resolve_independently_and_in_sorted_lock_order():
    store = InMemoryIntentStore()
    m = value("f1", 4 * 3600, actor="b-future")
    m["received_at"] = stamp(5)
    _ingest(store, value("a1", 1, actor="a-live"), m, value("a2", 40, actor="a-live"))
    assert len(_sessions_of(store, "a-live")) == 1 and len(_sessions_of(store, "b-future")) == 1
    assert store.cursors["b-future"].last_event_at == BASE + timedelta(seconds=5)


# ---------------- the year-2040 cursor, explicitly ----------------

def _store_with_2040_cursor_and_live_actors():
    store = InMemoryIntentStore()
    _ingest(store, value("l1", 60, actor="live-1"), value("l2", 90, actor="live-2"), value("l3", 100, actor="live-3"))
    _ingest(store, value("x", 0, actor="clock-2040"))
    c = store.cursors["clock-2040"]
    store.cursors["clock-2040"] = c.__class__(actor_id="clock-2040", session_id=c.session_id, session_start=datetime(2040, 1, 1, 1, 5, 35),
                                              last_event_at=datetime(2040, 1, 1, 1, 5, 35), last_event_id="x", events_seq=c.events_seq)
    return store


def test_a_year_2040_cursor_cannot_influence_idle_before_even_if_the_store_reports_it_as_latest():
    """The 14 h guard in the store excludes 2040 today, but it is evaluated against UTC at sweep time and ADMITS the cursor once UTC gets
    within 14 h of it. The cap must hold on its own: force the store to hand the 2040 time to the sweep."""
    store = _store_with_2040_cursor_and_live_actors()
    store.latest_cursor_time = lambda: datetime(2040, 1, 1, 1, 5, 35)         # the guard failed / admitted it
    real_now = BASE + timedelta(minutes=3)

    result = sweep_brand(_db(store), 1, TIMEOUT, now_fn=lambda: real_now)

    assert result.latest == datetime(2040, 1, 1, 1, 5, 35)
    assert result.effective_latest == real_now
    assert result.cutoff == real_now - timedelta(seconds=TIMEOUT + SWEEP_MARGIN_S)        # idle_before follows the REAL clock
    assert result.closed == 0
    assert {"live-1", "live-2", "live-3", "clock-2040"} <= set(store.cursors)             # no other actor's cursor was deleted


def test_the_2040_cursor_is_admitted_by_the_store_guard_in_2039_and_the_cap_still_protects_live_sessions(monkeypatch):
    import pipeline.intent_kafka_store as store_module

    class Fake2039(datetime):
        @classmethod
        def utcnow(cls):
            return cls(2039, 12, 31, 18, 0, 0)               # UTC is now within 14 h of 2040-01-01 01:05: the guard admits it

    store = InMemoryIntentStore()
    live = []
    for i, actor in enumerate(("live-1", "live-2", "live-3")):
        m = value(f"l{i}", 0, actor=actor)
        m["occurred_at"] = f"2039-12-31T23:3{i + 1}:00.000Z"                     # visitors active in the last minutes of store-local 2039
        live.append(m)
    _ingest(store, *live)
    _ingest(store, value("x", 0, actor="clock-2040"))
    c = store.cursors["clock-2040"]
    store.cursors["clock-2040"] = c.__class__(actor_id="clock-2040", session_id=c.session_id, session_start=datetime(2040, 1, 1, 1, 5, 35),
                                              last_event_at=datetime(2040, 1, 1, 1, 5, 35), last_event_id="x", events_seq=c.events_seq)
    monkeypatch.setattr(store_module, "datetime", Fake2039)
    assert store.latest_cursor_time() == datetime(2040, 1, 1, 1, 5, 35)                  # the old sweep would have used this
    result = sweep_brand(_db(store), 1, TIMEOUT, now_fn=lambda: datetime(2039, 12, 31, 23, 35))   # store-local now (IST)
    assert result.effective_latest == datetime(2039, 12, 31, 23, 35) and result.closed == 0
    assert {"live-1", "live-2", "live-3"} <= set(store.cursors)


def test_without_the_cap_the_admitted_2040_cursor_deletes_every_live_cursor_the_regression_control(monkeypatch):
    store = _store_with_2040_cursor_and_live_actors()
    store.latest_cursor_time = lambda: datetime(2040, 1, 1, 1, 5, 35)
    assert sweep_brand(_db(store), 1, TIMEOUT).closed == 3                                 # old behaviour: all three live visitors closed
    assert not ({"live-1", "live-2", "live-3"} & set(store.cursors))


# ---------------- monitoring signals the consumer emits ----------------

def test_the_consumer_counts_normalised_future_timestamps_per_cycle():
    from tests.test_intent_kafka_cycle import OTHER, Rig

    rig = Rig()
    ok = value("ok", 0)
    ok["received_at"] = stamp(0)
    future = value("fut", 4 * 3600, actor="bad", client="bad")
    future["received_at"] = stamp(5)
    rig.put(OTHER, 0, ok)
    rig.put(OTHER, 0, future)
    rig.ingestor.run_cycle("schedule")
    assert rig.ingestor.stats["future_timestamp_normalized"] == 1                 # only the one whose client clock was ahead
    assert rig.store.cursors["bad"].last_event_at == BASE + timedelta(seconds=5)


def test_sweep_clock_capped_is_counted_only_when_a_cursor_is_ahead_by_more_than_the_tolerance(monkeypatch):
    from pipeline import intent_kafka_consumer as consumer
    from tests.test_intent_kafka_cycle import OTHER, Rig

    rig = Rig()
    rig.ingestor.sweep_brand_indices = (1,)
    rig.put(OTHER, 0, value("a", 0, actor="a", client="a"))
    rig.ingestor.run_cycle("schedule")
    monkeypatch.setattr(consumer, "store_local_now", lambda index: BASE + timedelta(seconds=60))

    rig.ingestor._sweep()
    assert rig.ingestor.stats["sweep_clock_capped"] == 0                           # latest is BASE: not ahead of the store clock

    cursor = rig.store.cursors["a"]
    rig.store.cursors["a"] = cursor.__class__(actor_id="a", session_id=cursor.session_id, session_start=cursor.session_start,
                                              last_event_at=BASE + timedelta(seconds=60 + 120), last_event_id="a", events_seq=cursor.events_seq)
    rig.ingestor._sweep()
    assert rig.ingestor.stats["sweep_clock_capped"] == 0                           # +120 s is ordinary clock skew, inside the 300 s tolerance

    rig.store.cursors["a"] = rig.store.cursors["a"].__class__(actor_id="a", session_id=cursor.session_id, session_start=cursor.session_start,
                                                              last_event_at=BASE + timedelta(hours=4), last_event_id="a", events_seq=cursor.events_seq)
    rig.ingestor._sweep()
    assert rig.ingestor.stats["sweep_clock_capped"] == 1 and "a" in rig.store.cursors


def test_a_message_in_the_new_format_is_accepted_unchanged_by_the_previous_consumer_contract(tmp_path):
    """Deploy order safety in the other direction: the extra fields must not break a consumer that does not know them."""
    import importlib.util, subprocess
    old_source = subprocess.run(["git", "show", "HEAD:pipeline/intent_kafka_contract.py"], capture_output=True, text=True, check=True).stdout
    path = tmp_path / "old_contract.py"
    path.write_text(old_source, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("old_contract", path)
    old = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old)
    message = value("n1", 0)
    message["received_at"] = stamp(1)
    message["client_occurred_at"] = stamp(4 * 3600)
    parsed = old.parse_record(encode(message))
    assert parsed.event_id == "n1" and parsed.occurred_at == BASE
