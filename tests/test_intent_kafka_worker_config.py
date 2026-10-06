import threading

import pytest

import workers.intent_kafka_worker as worker
from pipeline.intent_kafka_consumer import ConsumerConfig


def test_defaults_match_the_documented_deployment(monkeypatch):
    for name in ("KAFKA_BOOTSTRAP_SERVERS", "INTENT_KAFKA_GROUP_ID", "INTENT_KAFKA_TOPICS", "INTENT_DLQ_TOPIC",
                 "INTENT_KAFKA_BATCH_SIZE", "INTENT_KAFKA_BATCH_WAIT_S", "INTENT_KAFKA_MAX_ATTEMPTS", "SESSION_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)
    config = worker.config_from_env()
    assert config.bootstrap_servers == "kafka-service:9092"          # service name, never an IP
    assert config.group_id == "intent-pipeline-workers"
    assert config.topics == ("intent.checkout", "intent.atc", "intent.click", "intent.other")
    assert config.dlq_topic == "intent.dlq"
    assert (config.batch_size, config.batch_wait_s, config.max_record_attempts, config.session_timeout_s) == (200, 1.0, 3, 1800)


def test_environment_overrides(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-a:9092")
    monkeypatch.setenv("INTENT_KAFKA_GROUP_ID", "intent-group-2")
    monkeypatch.setenv("INTENT_KAFKA_TOPICS", "intent.click, intent.other")
    monkeypatch.setenv("INTENT_KAFKA_BATCH_SIZE", "50")
    monkeypatch.setenv("SESSION_TIMEOUT", "900")
    config = worker.config_from_env()
    assert (config.bootstrap_servers, config.group_id, config.topics) == ("broker-a:9092", "intent-group-2", ("intent.click", "intent.other"))
    assert config.batch_size == 50 and config.session_timeout_s == 900


@pytest.mark.parametrize("name, value", [("INTENT_KAFKA_BATCH_SIZE", "abc"), ("INTENT_KAFKA_BATCH_SIZE", "0"),
                                         ("INTENT_KAFKA_MAX_ATTEMPTS", "-1"), ("INTENT_KAFKA_BATCH_WAIT_S", "soon")])
def test_invalid_numbers_stop_the_worker_at_startup(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(SystemExit):
        worker.config_from_env()


def test_one_consumer_group_for_every_topic():
    """A single group id for the whole stream: Kafka splits partitions between members, so
    consumers are parallel and never duplicate."""
    assert worker.DEFAULT_GROUP_ID == "intent-pipeline-workers"
    settings = ConsumerConfig().kafka_settings("c0")
    assert settings["group.id"] == worker.DEFAULT_GROUP_ID


def test_thread_count_is_bounded_by_the_partition_total():
    assert worker.DEFAULT_THREADS <= worker.MAX_THREADS == 10


def test_a_dying_consumer_stops_the_others_and_fails_the_process():
    stop = threading.Event()

    class Dies:
        name = "dies"
        def run(self): raise RuntimeError("fatal")

    class Waits:
        name = "waits"
        def run(self): stop.wait(5)

    assert worker.run_threads([Dies(), Waits()], stop) == 1
    assert stop.is_set()


def test_clean_exit_returns_zero():
    class Quick:
        name = "quick"
        def run(self): pass

    assert worker.run_threads([Quick(), Quick()], threading.Event()) == 0


# ---------------- ordering settings ----------------

def test_ordering_defaults_are_one_consumer_a_ten_second_slack_and_a_bounded_buffer(monkeypatch):
    for name in ("INTENT_KAFKA_ORDERING_DOMAIN", "INTENT_KAFKA_ORDER_SLACK_S", "INTENT_KAFKA_ORDER_BUFFER_MAX",
                 "INTENT_KAFKA_CONSUMER_THREADS"):
        monkeypatch.delenv(name, raising=False)
    config = worker.config_from_env()
    assert (config.ordering_domain, config.order_slack_s, config.order_buffer_max) == ("single", 10.0, 2000)
    assert worker.DEFAULT_THREADS == 1


def test_ordering_overrides_and_validation(monkeypatch):
    monkeypatch.setenv("INTENT_KAFKA_ORDERING_DOMAIN", "copartitioned")
    monkeypatch.setenv("INTENT_KAFKA_ORDER_SLACK_S", "4")
    monkeypatch.setenv("INTENT_KAFKA_ORDER_BUFFER_MAX", "500")
    config = worker.config_from_env()
    assert (config.ordering_domain, config.order_slack_s, config.order_buffer_max) == ("copartitioned", 4.0, 500)
    monkeypatch.setenv("INTENT_KAFKA_ORDERING_DOMAIN", "anything")
    with pytest.raises(SystemExit, match="single or copartitioned"):
        worker.config_from_env()


# ---------------- INTENT_DB_MAP validation ----------------

DATABASES = {"bbb": 2, "pts": 1, "shylenew": 8}
resolve = lambda name: DATABASES.get(name.lower())
PRODUCTION_MAP = {"bbb_shop": "BBB", "pts_shop": "PTS", "shyle_shop": "SHYLENEW"}


def test_the_production_mapping_resolves_every_brand_the_producer_allows():
    assert worker.validate_db_map(PRODUCTION_MAP, resolve) == {"bbb_shop": 2, "pts_shop": 1, "shyle_shop": 8}


@pytest.mark.parametrize("db_map, message", [
    ({}, "empty"),
    ({"bbb_shop": "NOPE"}, "matches no active brand database"),
    ({"BBB Shop": "BBB"}, "not a valid brand id"),
    ({"bbb_shop": ""}, "non-empty string"),
    ({"bbb_shop": "BBB", "bbb_two": "BBB"}, "same database"),
])
def test_an_invalid_db_map_stops_the_worker_at_startup_with_every_problem_named(db_map, message):
    with pytest.raises(SystemExit, match=message):
        worker.validate_db_map(db_map, resolve)


def test_the_env_file_example_maps_exactly_the_production_brands():
    import json, os, re

    text = open(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env.example"), encoding="utf-8").read()
    mapping = json.loads(re.search(r"^INTENT_DB_MAP=(.+)$", text, re.MULTILINE).group(1))
    assert mapping == PRODUCTION_MAP


def test_threads_above_one_are_refused_in_single_mode(monkeypatch):
    monkeypatch.setenv("INTENT_KAFKA_CONSUMER_THREADS", "2")
    monkeypatch.delenv("INTENT_KAFKA_ORDERING_DOMAIN", raising=False)
    monkeypatch.setattr(worker, "resolve_brands", lambda: pytest.fail("must refuse before touching brands"))
    with pytest.raises(SystemExit, match="ordering guarantee"):
        worker.main()


# ---------------- --preflight ----------------

def _preflight_with(monkeypatch, *, brands, schema_ok, topics_ok):
    import pipeline.intent_kafka_db as kdb

    monkeypatch.setattr(worker, "resolve_brands", brands)
    monkeypatch.setattr(kdb, "verify_brand_schemas",
                        lambda idx: None if schema_ok(idx[0]) else (_ for _ in ()).throw(RuntimeError("missing table intent_actor_cursors")))
    monkeypatch.setattr(worker, "check_topics", lambda cfg: None if topics_ok else (_ for _ in ()).throw(SystemExit("Kafka topics missing: ['intent.dlq']")))
    monkeypatch.delenv("INTENT_KAFKA_CONSUMER_THREADS", raising=False)
    monkeypatch.delenv("INTENT_KAFKA_ORDERING_DOMAIN", raising=False)
    return worker.preflight()


def test_preflight_passes_when_everything_is_ready(monkeypatch):
    assert _preflight_with(monkeypatch, brands=lambda: {"bbb_shop": 2, "pts_shop": 1}, schema_ok=lambda i: True, topics_ok=True) == 0


def test_preflight_reports_every_failure_not_just_the_first(monkeypatch, caplog):
    import logging

    caplog.set_level(logging.INFO)
    code = _preflight_with(monkeypatch, brands=lambda: {"bbb_shop": 2, "pts_shop": 1, "shyle_shop": 8},
                           schema_ok=lambda i: i == 2, topics_ok=False)
    text = caplog.text
    assert code == 1
    assert "PASS schema ready for bbb_shop" in text
    assert "FAIL schema ready for pts_shop" in text and "FAIL schema ready for shyle_shop" in text
    assert "FAIL Kafka topics" in text and "NOT READY" in text


def test_preflight_fails_on_an_invalid_db_map_without_starting_anything(monkeypatch):
    def bad():
        raise SystemExit("INTENT_DB_MAP is invalid: x")

    assert _preflight_with(monkeypatch, brands=bad, schema_ok=lambda i: True, topics_ok=True) == 1


def test_preflight_flag_exits_with_its_result_code(monkeypatch):
    monkeypatch.setattr(worker, "preflight", lambda: 3)
    monkeypatch.setattr("sys.argv", ["intent_kafka_worker.py", "--preflight"])
    with pytest.raises(SystemExit) as exit_info:
        worker.main()
    assert exit_info.value.code == 3
