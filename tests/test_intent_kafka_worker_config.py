import pytest

import workers.intent_kafka_worker as worker
from pipeline.intent_kafka_consumer import ConsumerConfig

ENV = ("KAFKA_BOOTSTRAP_SERVERS", "INTENT_KAFKA_GROUP_ID", "INTENT_KAFKA_TOPICS", "INTENT_DLQ_TOPIC", "INTENT_EVENTS_BATCH",
       "INTENT_KAFKA_RUN_EVERY_MINUTES", "INTENT_EVENTS_CHECK_INTERVAL_S", "INTENT_KAFKA_MAX_ATTEMPTS", "SESSION_TIMEOUT",
       "INTENT_KAFKA_ORDER_SLACK_S", "INTENT_KAFKA_ORDER_BUFFER_MAX")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ENV:                       # a developer's .env may set any of these
        monkeypatch.delenv(name, raising=False)


def test_defaults_match_the_documented_deployment():
    config = worker.config_from_env()
    assert config.bootstrap_servers == "kafka-service:9092"          # service name, never an IP
    assert config.topics == ("intent.checkout", "intent.atc", "intent.click", "intent.other")
    assert config.dlq_topic == "intent.dlq"
    assert (config.events_batch, config.run_every_minutes, config.check_interval_s) == (500, 25.0, 30.0)
    assert (config.max_record_attempts, config.session_timeout_s, config.order_slack_s) == (3, 1800, 10.0)


def test_environment_overrides(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-a:9092")
    monkeypatch.setenv("INTENT_KAFKA_TOPICS", "intent.click, intent.other")
    monkeypatch.setenv("INTENT_EVENTS_BATCH", "2000")
    monkeypatch.setenv("INTENT_KAFKA_RUN_EVERY_MINUTES", "10")
    monkeypatch.setenv("INTENT_EVENTS_CHECK_INTERVAL_S", "5")
    monkeypatch.setenv("SESSION_TIMEOUT", "900")
    config = worker.config_from_env()
    assert (config.bootstrap_servers, config.topics) == ("broker-a:9092", ("intent.click", "intent.other"))
    assert (config.events_batch, config.run_every_minutes, config.check_interval_s, config.session_timeout_s) == (2000, 10.0, 5.0, 900)


@pytest.mark.parametrize("name, value", [("INTENT_EVENTS_BATCH", "abc"), ("INTENT_EVENTS_BATCH", "0"),
                                         ("INTENT_KAFKA_MAX_ATTEMPTS", "-1"), ("INTENT_KAFKA_RUN_EVERY_MINUTES", "soon"),
                                         ("INTENT_KAFKA_RUN_EVERY_MINUTES", "0"), ("INTENT_EVENTS_CHECK_INTERVAL_S", "0")])
def test_invalid_numbers_stop_the_worker_at_startup(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(SystemExit):
        worker.config_from_env()


def test_offsets_are_committed_under_one_group_name_and_there_is_one_consumer_setup_for_every_topic():
    assert worker.DEFAULT_GROUP_ID == "intent-pipeline-workers"
    assert ConsumerConfig().kafka_settings("c0")["group.id"] == worker.DEFAULT_GROUP_ID


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


# ---------------- --preflight ----------------

def _preflight_with(monkeypatch, *, brands, schema_ok, topics_ok):
    import pipeline.intent_kafka_db as kdb

    monkeypatch.setattr(worker, "resolve_brands", brands)
    monkeypatch.setattr(kdb, "verify_brand_schemas",
                        lambda idx: None if schema_ok(idx[0]) else (_ for _ in ()).throw(RuntimeError("missing table intent_actor_cursors")))
    monkeypatch.setattr(worker, "check_topics", lambda cfg: None if topics_ok else (_ for _ in ()).throw(SystemExit("Kafka topics missing: ['intent.dlq']")))
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
