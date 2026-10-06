"""Cross-topic ordering against real Kafka and real MySQL (skipped unless both are named in the
environment; see conftest.py).

An actor's events sit on different topics. These tests make one topic lag and check the one thing
that matters: the session state machine sees the events in the order /track handled them, so the
session is not split. Kafka timestamps (CreateTime) are set explicitly, as /track's producer sets
them at send time, so a lagging topic can be simulated exactly."""

import json
import os
import subprocess
import time

import pytest

from tests.conftest import requires_kafka
from tests.helpers import value
from tests.test_intent_kafka_e2e import (
    KafkaEnv, PARTITIONS, Running, bootstrap, count, cursor_seq, wait_for,
)

pytestmark = requires_kafka
KEY = "bbb_shop:cid-1"


@pytest.fixture
def env():
    return KafkaEnv()


def now_ms():
    return int(time.time() * 1000)


def stored_sessions(db):
    return db.rows("SELECT session_id, event_count, session_start, session_end FROM intent_sessions ORDER BY id")


# ---------------- the bug, and the fix ----------------

def test_a_lagging_topic_is_waited_for_so_the_session_is_not_split(env, clean_db):
    """e1 and e3 are on `other`; e2 (a click, sent between them) reaches the click topic 4 s late.
    Pixel times are 0 s, 20 s and 70 s: applying e2 after e3 would be 50 s 'behind' the cursor."""
    t0 = now_ms()
    env.send("intent.other", KEY, value("e1", 0), timestamp=t0)
    env.send("intent.other", KEY, value("e3", 70), timestamp=t0 + 2000)
    with Running(env, clean_db, order_slack_s=8) as run:
        time.sleep(4)
        assert count(clean_db, "behavioral_events") == 0                 # held: click might still hold something older
        env.send("intent.click", KEY, value("e2", 20, click=True), timestamp=t0 + 1000)   # the lagging topic delivers
        wait_for(lambda: count(clean_db, "behavioral_events") == 2 and count(clean_db, "click_events") == 1, "rows")
        assert run.consumers[0].stats["order_violations"] == 0
    assert cursor_seq(clean_db, "cid-1") == ["e1", "e2", "e3"]           # /track arrival order
    assert stored_sessions(clean_db) == []                               # one open session, nothing closed
    assert len({r["session_id"] for r in clean_db.rows("SELECT session_id FROM behavioral_events")}
               | {r["session_id"] for r in clean_db.rows("SELECT session_id FROM click_events")}) == 1


def test_without_the_slack_a_topic_that_is_late_beyond_it_splits_the_session_and_is_reported(env, clean_db):
    """The control: the same events with no slack and a click topic that is 5 s late. This is the
    failure the slack exists to prevent. It is detected and logged (order_violation), and the late
    event is still stored, never dropped."""
    t0 = now_ms()
    env.send("intent.other", KEY, value("e1", 0), timestamp=t0)
    env.send("intent.other", KEY, value("e3", 70), timestamp=t0 + 2000)
    with Running(env, clean_db, order_slack_s=0) as run:
        wait_for(lambda: count(clean_db, "behavioral_events") == 2, "e1 and e3 applied without waiting")
        time.sleep(1)
        env.send("intent.click", KEY, value("e2", 20, click=True), timestamp=t0 + 1000)
        wait_for(lambda: count(clean_db, "click_events") == 1, "the late click")
        assert run.consumers[0].stats["order_violations"] == 1
    assert len(stored_sessions(clean_db)) == 1                           # e1+e3 were closed when e2 arrived "behind" them
    assert cursor_seq(clean_db, "cid-1") == ["e2"]


def test_a_backlog_is_applied_in_send_order_across_topics_and_memory_stays_bounded(env, clean_db):
    """60 events, two topics, all waiting in Kafka before the consumer starts, so it reads one topic
    far ahead of the other. The buffer limit is 10 records, so the partition that is ahead is paused."""
    t0 = now_ms() - 120_000                          # old enough that the slack has long passed
    for i in range(30):
        env.send("intent.other", KEY, value(f"o{i:02d}", i * 2), timestamp=t0 + i * 200)
    for i in range(30):
        env.send("intent.click", KEY, value(f"c{i:02d}", i * 2 + 1, click=True), timestamp=t0 + i * 200 + 100)
    with Running(env, clean_db, order_buffer_max=10) as run:
        wait_for(lambda: count(clean_db, "behavioral_events") + count(clean_db, "click_events") == 60, "all 60")
        stats = run.consumers[0].stats
    expected = [x for i in range(30) for x in (f"o{i:02d}", f"c{i:02d}")]
    assert cursor_seq(clean_db, "cid-1") == expected                     # interleaved exactly as sent
    assert stored_sessions(clean_db) == [] and stats["order_violations"] == 0
    assert stats["ordering_buffer_high_water"] <= 10 + 50                # cap + one poll's worth (batch_size 50)
    assert stats["ordering_buffer_high_water"] < 60                      # not "everything buffered"


def test_events_on_all_four_topics_are_applied_in_send_order_whatever_order_the_topics_are_read(env, clean_db):
    t0 = now_ms() - 120_000
    plan = [("intent.other", "page_viewed"), ("intent.checkout", "checkout_started"), ("intent.atc", "product_added_to_cart"),
            ("intent.click", "click")] * 3
    expected = []
    for i, (topic, name) in enumerate(plan):
        event_id = f"x{i:02d}"
        expected.append(event_id)
        body = (value(event_id, i, click=True) if name == "click" else
                value(event_id, i, name=name, raw={"product_id": f"Product:{i}"} if "cart" in name else None))
        env.send(topic, KEY, body, timestamp=t0 + i * 100)
    # topic-major arrival: the topics are produced in the order other, checkout, atc, click above, so
    # whatever the consumer reads first, the sequence must still follow the timestamps.
    with Running(env, clean_db) as run:
        wait_for(lambda: count(clean_db, "behavioral_events") + count(clean_db, "click_events") == 12, "all 12")
        assert run.consumers[0].stats["order_violations"] == 0
    assert cursor_seq(clean_db, "cid-1") == expected
    assert stored_sessions(clean_db) == []


def test_many_actors_across_topics_each_keep_their_own_send_order(env, clean_db):
    t0 = now_ms() - 120_000
    actors = [f"a{i}" for i in range(6)]
    expected = {a: [] for a in actors}
    n = 0
    for step in range(6):
        for actor in actors:
            topic = ["intent.other", "intent.click", "intent.atc", "intent.checkout"][(step + len(actor)) % 4]
            event_id = f"{actor}-{step}"
            is_click = topic == "intent.click"
            body = (value(event_id, step, actor=actor, client=actor, click=True) if is_click else
                    value(event_id, step, actor=actor, client=actor,
                          name={"intent.atc": "product_added_to_cart", "intent.checkout": "checkout_started"}.get(topic, "page_viewed"),
                          raw={"product_id": f"Product:{actor}{step}"} if topic == "intent.atc" else None))
            env.send(topic, f"bbb_shop:{actor}", body, timestamp=t0 + n * 50)
            expected[actor].append(event_id)
            n += 1
    with Running(env, clean_db):
        wait_for(lambda: count(clean_db, "behavioral_events") + count(clean_db, "click_events") == 36, "all 36")
    for actor in actors:
        assert cursor_seq(clean_db, actor) == expected[actor]
    assert stored_sessions(clean_db) == []


# ---------------- the ordering domain under a real rebalance ----------------

def test_two_consumers_on_uneven_topics_refuse_to_process_until_only_one_remains(env, clean_db):
    """2/2/3/3 partitions cannot be co-partitioned, so a second consumer would split an actor's topics
    between two processes. Both must stand down (and say so) instead of applying events out of order."""
    with Running(env, clean_db, count=2, autostart=False) as run:
        run.start_one(0)
        wait_for(lambda: run.consumers[0]._domain_ok, "the first consumer owns every partition")
        run.start_one(1)
        wait_for(lambda: not run.consumers[0]._domain_ok and not run.consumers[1]._domain_ok, "the rebalance splits the topics")
        for i in range(5):
            env.send("intent.other", KEY, value(f"d{i}", i), timestamp=now_ms() - 60_000 + i)
        time.sleep(6)
        assert count(clean_db, "behavioral_events") == 0              # nothing applied while the domain is split
        run.stop_one(1)                                               # the extra consumer leaves the group
        wait_for(lambda: count(clean_db, "behavioral_events") == 5, "the remaining consumer takes over")
    assert cursor_seq(clean_db, "cid-1") == [f"d{i}" for i in range(5)]


# ---------------- the assumption the whole design rests on ----------------

ALERTS_DIR = os.environ.get("INTENT_TEST_ALERTS_SERVICE_DIR", "")


@pytest.mark.skipif(not ALERTS_DIR, reason="INTENT_TEST_ALERTS_SERVICE_DIR not set")
def test_the_real_track_publisher_stamps_each_record_with_its_send_time(env):
    """Ordering by Kafka timestamp only reproduces /track's arrival order if the real producer sets
    CreateTime at send time. Publishes through the actual alerts-service publisher and reads it back."""
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    script = os.path.join(os.path.dirname(__file__), "fixtures", "produce_with_track_publisher.js")
    out = subprocess.run(["node", script, ALERTS_DIR, bootstrap(), env.topic_for("intent.other"), "bbb_shop:probe"],
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    sent = json.loads(out.stdout.strip().splitlines()[-1])
    (msg,) = env.read_all(env.topic_for("intent.other"), 1)
    kind, ts = msg.timestamp()
    assert kind == TIMESTAMP_CREATE_TIME
    assert abs(ts - sent["sentAt"]) < 3000                              # stamped when it was sent
