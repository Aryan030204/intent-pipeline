"""The bulk (fast) writer against the per-message writer.

The fast path is only acceptable if it is indistinguishable from the old one, so the main test runs both
on the same randomised streams and requires identical stored state and identical counters. The rest cover
the fallbacks, the generated SQL and the choice of path. (Real-MySQL parity is in test_intent_kafka_mysql.py.)"""

import itertools
import json
import random
from datetime import datetime

import pytest

import pipeline.intent_kafka_bulk as bulk
import pipeline.intent_kafka_writer as writer
import pipeline.intent_session_state as state
from pipeline.intent_kafka_consumer import ConsumerConfig, IntentIngestor
from pipeline.intent_kafka_contract import InvalidMessage
from pipeline.intent_kafka_store import (
    BULK_MAX_ROWS,
    InMemoryIntentStore,
    MySqlIntentStore,
    bulk_chunks,
    bulk_insert_sql,
)
from tests.helpers import BRANDS, FakeBroker, FakeDb, FakeDlq, atc, item, record, value

TIMEOUT = 1800


class DataTooLong(Exception):
    errno = 1406


class Deadlock(Exception):
    errno = 1213


def deterministic_session_ids(monkeypatch):
    """Both writers must draw session ids in the same order, so give each run its own counter."""
    counter = itertools.count(1)
    make = lambda: f"sess-{next(counter)}"
    for module in (state, writer, bulk):
        monkeypatch.setattr(module, "new_session_id", make)


def snapshot(store):
    return {
        "events": dict(store.events), "clicks": dict(store.clicks), "atc": set(store.atc),
        "cursors": dict(store.cursors), "sessions": sorted(store.sessions, key=lambda r: r[0]),
    }


def canonical(snap):
    """The same state with session ids renamed by order of first appearance. After a fallback the abandoned
    fast attempt has already drawn some ids, so literal ids differ (in production they are random UUIDs)."""
    names = {}

    def rename(session_id):
        return names.setdefault(session_id, f"S{len(names)}")

    for kind in ("events", "clicks"):
        for event_id in sorted(snap[kind]):
            rename(snap[kind][event_id][5])
    events = {e: r[:5] + (rename(r[5]),) + r[6:] for e, r in snap["events"].items()}
    clicks = {e: r[:5] + (rename(r[5]),) + r[6:] for e, r in snap["clicks"].items()}
    cursors = {a: c.__class__(c.actor_id, rename(c.session_id), c.session_start, c.last_event_at, c.last_event_id, c.events_seq)
               for a, c in snap["cursors"].items()}
    sessions = sorted(((rename(r[0]),) + r[1:] for r in snap["sessions"]), key=lambda r: r[0])
    atc = {(rename(sid), product) for sid, product in snap["atc"]}
    return {"events": events, "clicks": clicks, "cursors": cursors, "sessions": sessions, "atc": atc}


# ---------------- randomised streams ----------------

GAPS = [1, 5, 20, 40, 300, 1900, -10, -25, -100]       # seconds: continue, rollover, tolerated and beyond-tolerance negatives
ACTORS = ["a1", "a2", "a3", "a4", None]                # None: no actor at all (an actorless event)


def random_batches(seed, batches=3, size=25):
    rng = random.Random(seed)
    clock = {a: 0 for a in ACTORS}
    sent, counter = [], itertools.count()
    out = []
    for _ in range(batches):
        batch = []
        for _ in range(size):
            if sent and rng.random() < 0.12:
                batch.append(rng.choice(sent))                      # the same event delivered again
                continue
            actor = rng.choice(ACTORS)
            clock[actor] += rng.choice(GAPS)
            event_id = f"e{next(counter):04d}"
            kind = rng.random()
            kw = dict(actor=actor, client=actor, visitor="v")
            if kind < 0.15:
                message = value(event_id, clock[actor], click=True, bucket=rng.choice(["useful_click", "dead_click"]), **kw)
            elif kind < 0.45:
                message = atc(event_id, clock[actor], product=rng.choice(["Product:1", "Product:2", "SYNTH:" + event_id]), **kw)
            elif kind < 0.55:
                message = value(event_id, clock[actor], name="checkout_started", **kw)
            else:
                message = value(event_id, clock[actor], name=rng.choice(["page_viewed", "product_viewed", "scroll_depth"]), **kw)
            sent.append(message)
            batch.append(message)
        out.append(batch)
    return out


def run_stream(apply, batches, monkeypatch):
    deterministic_session_ids(monkeypatch)
    store = InMemoryIntentStore()
    counts, paths = [], []
    for number, batch in enumerate(batches):
        result = apply(store, [item(m, ("t", 0, f"{number}-{i}")) for i, m in enumerate(batch)], TIMEOUT)
        counts.append(result.counts())
        paths.append(result.path)
    return snapshot(store), counts, paths


@pytest.mark.parametrize("seed", range(60))
def test_the_fast_path_leaves_exactly_the_state_the_per_message_writer_leaves(seed, monkeypatch):
    batches = random_batches(seed)
    safe_state, safe_counts, _ = run_stream(writer.apply_batch, batches, monkeypatch)
    fast_state, fast_counts, paths = run_stream(bulk.apply_batch_fast, batches, monkeypatch)
    assert fast_state == safe_state
    assert fast_counts == safe_counts
    assert set(paths) == {"fast"}                      # none of these streams may silently take the fallback


def test_the_random_streams_really_exercise_rollovers_duplicates_atc_dedupe_and_actorless_events(monkeypatch):
    totals = {"sessions_closed": 0, "duplicates": 0, "atc_deduped": 0, "applied": 0}
    actorless = 0
    for seed in range(60):
        _, counts, _ = run_stream(bulk.apply_batch_fast, random_batches(seed), monkeypatch)
        for batch_counts in counts:
            for key in totals:
                totals[key] += batch_counts[key]
    assert all(value > 20 for value in totals.values()), totals


# ---------------- fallbacks ----------------

def safe_reference(batches, monkeypatch, prepare=None):
    deterministic_session_ids(monkeypatch)
    store = InMemoryIntentStore()
    if prepare:
        prepare(store)
    results = [writer.apply_batch(store, [item(m, ("t", 0, i)) for i, m in enumerate(b)], TIMEOUT) for b in batches]
    return snapshot(store), results


def test_a_row_mysql_rejects_makes_the_fast_path_fall_back_and_the_poison_is_isolated_as_before(monkeypatch):
    batches = [[value("ok1", 0), value("bad", 10), value("ok2", 20), value("other", 5, actor="a2", client="a2")]]
    prepare = lambda store: store.fail_on.update(bad=DataTooLong("Data too long for column"))
    expected_state, expected = safe_reference(batches, monkeypatch, prepare)

    deterministic_session_ids(monkeypatch)
    store = InMemoryIntentStore()
    prepare(store)
    result = bulk.apply_batch_fast(store, [item(m, ("t", 0, i)) for i, m in enumerate(batches[0])], TIMEOUT)
    assert result.path == "fallback"
    assert list(result.failed) == list(expected[0].failed) == [("t", 0, 1)]
    assert canonical(snapshot(store)) == canonical(expected_state)   # healthy neighbours committed, nothing half-written
    assert "bad" not in store.events and {"ok1", "ok2", "other"} <= set(store.events)


def test_a_row_the_extractor_rejects_also_falls_back(monkeypatch):
    batches = [[value("ok1", 0), value("odd", 10), value("ok2", 20)]]
    expected_state, _ = safe_reference(batches, monkeypatch)

    real = writer._event_row

    def picky(message, session_id, actor_id):
        if message.event_id == "odd":
            raise InvalidMessage("rejected by the extractor")
        return real(message, session_id, actor_id)

    monkeypatch.setattr(bulk, "_event_row", picky)              # only the fast path sees the failure
    deterministic_session_ids(monkeypatch)
    store = InMemoryIntentStore()
    result = bulk.apply_batch_fast(store, [item(m, ("t", 0, i)) for i, m in enumerate(batches[0])], TIMEOUT)
    assert result.path == "fallback" and result.failed == {}
    assert canonical(snapshot(store)) == canonical(expected_state)


def test_ids_that_mysql_would_treat_as_the_same_key_take_the_per_message_path(monkeypatch):
    batch = [value("e1", 0, actor="AbC", client="AbC"), value("e2", 5, actor="abc", client="abc"),
             value("EV-1", 6, actor="z", client="z"), value("ev-1", 7, actor="y", client="y")]
    expected_state, _ = safe_reference([batch], monkeypatch)
    deterministic_session_ids(monkeypatch)
    store = InMemoryIntentStore()
    result = bulk.apply_batch_fast(store, [item(m, ("t", 0, i)) for i, m in enumerate(batch)], TIMEOUT)
    assert result.path == "fallback" and canonical(snapshot(store)) == canonical(expected_state)


def test_infrastructure_errors_are_not_swallowed_by_the_fallback():
    for error in (Deadlock("deadlock"), ConnectionError("lost"), RuntimeError("unclassified")):
        store = InMemoryIntentStore()
        store.fail_on["e1"] = error
        with pytest.raises(type(error)):
            bulk.apply_batch_fast(store, [item(value("e1", 0), ("t", 0, 0))], TIMEOUT)


def test_a_fast_path_that_fails_part_way_leaves_nothing_behind_before_the_fallback_runs(monkeypatch):
    class FailsOnSessions(InMemoryIntentStore):
        calls = 0

        def upsert_sessions(self, rows):
            FailsOnSessions.calls += 1
            if FailsOnSessions.calls == 1:
                raise DataTooLong("closed session rejected")     # after claims and events were already written
            super().upsert_sessions(rows)

    batch = [value("e1", 0), value("e2", 2000)]
    FailsOnSessions.calls = 0
    deterministic_session_ids(monkeypatch)
    store = FailsOnSessions()
    result = bulk.apply_batch_fast(store, [item(m, ("t", 0, i)) for i, m in enumerate(batch)], TIMEOUT)
    assert result.path == "fallback" and FailsOnSessions.calls >= 1
    assert set(store.events) == {"e1", "e2"} and len(store.sessions) == 1   # exactly once, not twice


# ---------------- the SQL it generates ----------------

class RecordingCursor:
    """A cursor that records statements and answers the few reads the writers make."""

    def __init__(self):
        self.calls, self.rowcount, self._rows = [], 1, []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self._rows = []
        if "FROM intent_actor_cursors" in sql:
            self._rows = [{"actor_id": a, "session_id": "", "session_start": datetime(1970, 1, 1), "last_event_at": datetime(1970, 1, 1),
                           "last_event_id": None, "events_seq": "{}"} for a in (params or ())]

    def fetchall(self):
        return self._rows


def many_events(count, actors):
    return [item(value(f"e{i:04d}", i, actor=f"u{i % actors}", client=f"u{i % actors}"), ("t", 0, i)) for i in range(count)]


def test_statements_per_batch_stay_flat_while_the_per_message_path_grows_with_the_events():
    fast_counts, safe_counts = [], []
    for events in (100, 400):
        cursor = RecordingCursor()
        bulk.apply_batch_fast(MySqlIntentStore(cursor), many_events(events, 40), TIMEOUT)
        fast_counts.append(len(cursor.calls))
        cursor = RecordingCursor()
        writer.apply_batch(MySqlIntentStore(cursor), many_events(events, 40), TIMEOUT)
        safe_counts.append(len(cursor.calls))
    assert fast_counts[1] - fast_counts[0] <= 4                    # 4x the events adds a few chunked inserts, not hundreds
    assert fast_counts[1] < 25
    assert safe_counts[1] >= 3 * 400                               # the old path: 3 statements per event
    assert safe_counts[1] > 40 * fast_counts[1]


def test_bulk_insert_sql_has_one_placeholder_group_per_row_and_the_no_op_duplicate_tail():
    sql = bulk_insert_sql("t", ("a", "b"), "ON DUPLICATE KEY UPDATE a = a", 3)
    assert sql == "INSERT INTO t (a, b) VALUES (%s, %s), (%s, %s), (%s, %s) ON DUPLICATE KEY UPDATE a = a"


def test_rows_are_split_into_statements_by_count_and_by_size():
    assert [len(c) for c in bulk_chunks([("x",)] * (BULK_MAX_ROWS * 2 + 5))] == [BULK_MAX_ROWS, BULK_MAX_ROWS, 5]
    assert [len(c) for c in bulk_chunks([("y" * 600_000,)] * 3)] == [1, 1, 1]       # big rows travel alone, never past max_allowed_packet


def test_event_inserts_use_the_same_columns_and_no_op_duplicate_handling_as_the_per_message_path():
    cursor = RecordingCursor()
    rows = [("id%03d" % i,) + (None,) * 19 for i in range(450)]
    MySqlIntentStore(cursor).insert_events("event", rows)
    assert [len(params) // 20 for _sql, params in cursor.calls] == [200, 200, 50]
    assert all(sql.startswith("INSERT INTO behavioral_events") and sql.endswith("ON DUPLICATE KEY UPDATE event_id = event_id")
               for sql, _ in cursor.calls)


def test_cursors_are_claimed_then_locked_in_sorted_order_in_two_statements():
    cursor = RecordingCursor()
    locked = MySqlIntentStore(cursor).lock_cursors(["b", "A", "c"])
    claim, select = cursor.calls
    assert claim[1] == ("A", "b", "c") and "ON DUPLICATE KEY UPDATE actor_id = actor_id" in claim[0]
    assert select[0].endswith("ORDER BY actor_id FOR UPDATE") and select[1] == ("A", "b", "c")
    assert locked == {"a": None, "b": None, "c": None}             # placeholders read back as "no session yet", keyed by lower case


# ---------------- choosing the path ----------------

def build(**cfg):
    log = []
    broker = FakeBroker(log=log)
    ingestor = IntentIngestor(broker.factory, FakeDlq(log), FakeDb(log), BRANDS.get, ConsumerConfig(**cfg), clock_ms=lambda: 10 ** 12)
    ingestor.kafka = broker.factory()
    return ingestor


def feed(ingestor, brand="bbb_shop"):
    ingestor.process_batch([record("intent.other", 0, 0, value("e1", 0, brand=brand)),
                            record("intent.other", 0, 1, value("e2", 5, brand=brand))])


def test_safe_is_the_default_and_batch_mode_switches_the_writer():
    safe, fast = build(), build(write_mode="batch")
    feed(safe)
    feed(fast)
    assert safe.stats["path_safe"] == 1 and "path_fast" not in safe.stats
    assert fast.stats["path_fast"] == 1 and fast.stats["applied"] == 2


def test_batch_mode_can_be_limited_to_some_brands():
    ingestor = build(write_mode="batch", fast_path_brands=("bbb_shop",))
    feed(ingestor, "bbb_shop")
    feed(ingestor, "pts_shop")
    assert ingestor.stats["path_fast"] == 1 and ingestor.stats["path_safe"] == 1


def test_the_mode_comes_from_the_environment_and_is_validated(monkeypatch):
    import workers.intent_kafka_worker as worker

    monkeypatch.delenv("INTENT_WRITE_MODE", raising=False)
    monkeypatch.delenv("INTENT_FAST_PATH_BRANDS", raising=False)
    assert worker.config_from_env().write_mode == "safe" and worker.config_from_env().fast_path_brands == ()
    monkeypatch.setenv("INTENT_WRITE_MODE", "batch")
    monkeypatch.setenv("INTENT_FAST_PATH_BRANDS", "bbb_shop, pts_shop")
    config = worker.config_from_env()
    assert config.write_mode == "batch" and config.fast_path_brands == ("bbb_shop", "pts_shop")
    monkeypatch.setenv("INTENT_WRITE_MODE", "turbo")
    with pytest.raises(SystemExit, match="safe or batch"):
        worker.config_from_env()
