"""The idle-session sweep: closes sessions whose visitor went quiet, exactly as the normal close would."""

from pipeline.intent_kafka_consumer import ConsumerConfig, IntentIngestor
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch
from pipeline.intent_session_sweep import SWEEP_MARGIN_S, sweep_brand, sweep_chunk, sweep_cutoff

from tests.helpers import BASE, FakeDb, items, value

TIMEOUT = 1800
GAP = TIMEOUT + SWEEP_MARGIN_S  # the sweep only closes sessions idle for longer than this


def _ingest(store, *messages):
    apply_batch(store, items(*messages), TIMEOUT)


def _db(store):
    return FakeDb([], {1: store})


def test_bounce_gets_a_zero_length_session_and_its_cursor_is_removed():
    store = InMemoryIntentStore()
    _ingest(store, value("bounce-1", 0, actor="a"), value("other-1", GAP + 5, actor="b"))
    assert store.sessions == []  # nothing closes on its own: nobody came back

    result = sweep_brand(_db(store), 1, TIMEOUT)

    assert result.closed == 1
    assert [row[0] for row in store.sessions] != []
    session = store.sessions[0]
    assert session[1] == "a"                      # actor
    assert session[4] == BASE and session[5] == BASE   # start == end: a single page view
    assert session[6] == 0                        # time spent, ms
    assert session[7] == 1                        # event count
    assert "a" not in store.cursors and "b" in store.cursors  # only the idle actor is swept


def test_multi_event_session_is_first_to_last_event():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("e2", 150, actor="a"), value("e3", 250, actor="a"),
            value("late", GAP + 400, actor="b"))

    sweep_brand(_db(store), 1, TIMEOUT)

    session = store.sessions[0]
    assert session[6] == 250_000 and session[7] == 3   # 250 s, not the idle time after it


def test_the_swept_row_equals_the_one_the_normal_close_writes():
    swept, normal = InMemoryIntentStore(), InMemoryIntentStore()
    _ingest(swept, value("e1", 0, actor="a"), value("e2", 60, actor="a"), value("x", GAP + 70, actor="b"))
    sweep_brand(_db(swept), 1, TIMEOUT)
    _ingest(normal, value("e1", 0, actor="a"), value("e2", 60, actor="a"), value("back", GAP + 70, actor="a"))

    ignore_session_id = lambda row: row[1:]
    assert ignore_session_id(swept.sessions[0]) == ignore_session_id(normal.sessions[0])


def test_returning_visitor_after_the_sweep_starts_a_new_session_without_a_second_close():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("x", GAP + 5, actor="b"))
    old_session = store.cursors["a"].session_id
    sweep_brand(_db(store), 1, TIMEOUT)

    _ingest(store, value("back", GAP + 100, actor="a"))

    assert store.cursors["a"].session_id != old_session
    assert len(store.sessions) == 1               # the old one was closed once, by the sweep
    assert store.sessions[0][0] == old_session


def test_recent_sessions_are_left_open():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("e2", 600, actor="b"))  # a is only 10 minutes behind the newest event

    assert sweep_brand(_db(store), 1, TIMEOUT).closed == 0
    assert set(store.cursors) == {"a", "b"} and store.sessions == []


def test_one_bogus_future_timestamp_does_not_make_live_sessions_look_idle():
    from datetime import datetime

    store = InMemoryIntentStore()
    _ingest(store, value("live-1", 0, actor="live"), value("future-1", 5, actor="bogus"))
    # an event stamped years ahead, as a client with a wrong date would send
    store.cursors["bogus"] = store.cursors["bogus"].__class__(
        actor_id="bogus", session_id=store.cursors["bogus"].session_id, session_start=datetime(2040, 1, 1, 1, 5),
        last_event_at=datetime(2040, 1, 1, 1, 5), last_event_id="x", events_seq={"1": {}})

    # the newest believable time is the live visitor's, so nothing is idle and nothing is closed early
    assert store.latest_cursor_time() == store.cursors["live"].last_event_at
    assert sweep_brand(_db(store), 1, TIMEOUT).closed == 0
    assert "live" in store.cursors


def test_nothing_ingested_means_nothing_to_sweep():
    assert sweep_brand(_db(InMemoryIntentStore()), 1, TIMEOUT).closed == 0
    assert sweep_cutoff(None, TIMEOUT) is None


def test_second_sweep_is_a_no_op():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("x", GAP + 5, actor="b"))
    assert sweep_brand(_db(store), 1, TIMEOUT).closed == 1
    assert sweep_brand(_db(store), 1, TIMEOUT).closed == 0


def test_chunks_and_the_per_run_limit():
    store = InMemoryIntentStore()
    _ingest(store, *[value(f"e{i}", i, actor=f"a{i}") for i in range(7)], value("x", GAP + 50, actor="z"))

    assert sweep_brand(_db(store), 1, TIMEOUT, chunk=3, max_total=5).closed == 5   # limit respected across chunks
    assert sweep_brand(_db(store), 1, TIMEOUT, chunk=3).closed == 2                # the rest on the next run
    assert set(store.cursors) == {"z"}


def test_sweep_chunk_returns_the_count_it_closed():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("x", GAP + 5, actor="b"))
    cutoff = sweep_cutoff(store.latest_cursor_time(), TIMEOUT)
    assert sweep_chunk(store, cutoff).closed == 1


def _ingestor(db, **config):
    return IntentIngestor(None, None, db, {"bbb_shop": 1}.get, ConsumerConfig(**config), sweep_brand_indices=[1])


def test_ingestor_sweep_runs_by_default_and_counts():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("x", GAP + 5, actor="b"))
    ingestor = _ingestor(_db(store))
    ingestor._sweep()
    assert ingestor.stats["sessions_swept"] == 1 and len(store.sessions) == 1


def test_ingestor_sweep_can_be_switched_off():
    store = InMemoryIntentStore()
    _ingest(store, value("e1", 0, actor="a"), value("x", GAP + 5, actor="b"))
    ingestor = _ingestor(_db(store), sweep_idle_sessions=False)
    ingestor._sweep()
    assert store.sessions == [] and "a" in store.cursors


def test_a_failing_sweep_is_logged_and_never_raised():
    store = InMemoryIntentStore()
    db = _db(store)
    db.fail_brand[1] = RuntimeError("connection lost")
    ingestor = _ingestor(db)
    ingestor._sweep()                                    # must not raise
    assert ingestor.stats["sweep_failures"] == 1
