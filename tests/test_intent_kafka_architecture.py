"""Static guarantees about the architecture: Kafka -> consumer -> MySQL, and nothing else."""

import os
import re
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER_FILES = (
    "workers/intent_kafka_worker.py",
    "pipeline/intent_kafka_consumer.py",
    "pipeline/intent_kafka_contract.py",
    "pipeline/intent_kafka_db.py",
    "pipeline/intent_kafka_store.py",
    "pipeline/intent_kafka_writer.py",
    "pipeline/intent_session_state.py",
)


def source(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as handle:
        return handle.read()


@pytest.mark.parametrize("rel", WORKER_FILES)
def test_consumer_code_has_no_mongo_sqs_or_outbox_reference(rel):
    text = source(rel)
    for banned in (r"pymongo", r"MongoClient", r"INTENT_MONGO_URI", r"intent_outbox", r"\bsqs\b", r"boto3",
                   r"receive_message", r"delete_message", r"change_message_visibility", r"SQS_", r"/collect",
                   r"intent_sessions\.(events|click_events|actor_cursors|session_history|slug_cache)"):
        assert not re.search(banned, text, re.IGNORECASE), f"{rel} mentions {banned}"


def test_importing_the_consumer_and_writer_does_not_load_pymongo_or_boto3():
    code = (
        "import sys\n"
        "import pipeline.intent_kafka_consumer, pipeline.intent_kafka_writer, pipeline.intent_kafka_db, "
        "pipeline.intent_kafka_store, workers.intent_kafka_worker\n"
        "print('LOADED=' + ','.join(m for m in ('pymongo', 'boto3', 'botocore') if m in sys.modules))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT)
    assert "LOADED=\n" in out.stdout + "\n", out.stdout + out.stderr


def test_the_aggregation_job_no_longer_reads_mongo():
    text = source("pipeline/orchestration.py")
    assert "sync_intent_events_for_brand" not in text and "sync_click_events_for_brand" not in text
    assert "sync_session_history_for_brand" not in text and "pymongo" not in text.lower()
    assert "run_rollups_for_brand" in text   # the MySQL rollups still run


def test_no_module_calls_the_legacy_mongo_sync_functions():
    for root, _dirs, files in os.walk(ROOT):
        if any(part in root for part in (".git", "__pycache__", "venv", "node_modules")):
            continue
        for name in files:
            if not name.endswith(".py") or name == "intent_events.py" or root.endswith("tests"):
                continue
            text = open(os.path.join(root, name), encoding="utf-8").read()
            assert not re.search(r"sync_(intent_events|click_events|session_history)_for_brand\(", text), (root, name)


def test_the_kafka_worker_is_wired_in_compose_with_a_graceful_stop_and_pipeline_net():
    text = source("docker-compose.yml")
    block = text[text.index("intent-kafka-worker:"):]
    assert "workers/intent_kafka_worker.py" in block
    assert "pipeline-net" in block and "stop_grace_period: 60s" in block and "restart: always" in block
    assert "ports:" not in block                                   # Kafka and the worker are not published
    assert "kafka-service" in text                                 # reached by service name
    assert not re.search(r"\d+\.\d+\.\d+\.\d+", block)             # never an EC2 IP


def test_no_sqs_files_or_settings_exist_in_the_repo():
    for rel in ("workers/intent_sqs_worker.py", "pipeline/intent_sqs_writer.py", "pipeline/intent_sqs_store.py",
                "pipeline/intent_sqs_contract.py", "migrations/003_intent_sqs_state.sql", "send_sqs_test_messages.sh"):
        assert not os.path.exists(os.path.join(ROOT, rel)), rel
    assert "boto3" not in source("requirements.txt")
    assert "SQS_" not in source("docker-compose.yml")
    assert "confluent-kafka" in source("requirements.txt")


def test_the_state_tables_come_from_one_migration_without_sqs_columns():
    from pipeline.intent_kafka_store import migration_statements

    statements = migration_statements()          # executable statements only, comments stripped
    assert len(statements) == 2 and all(s.upper().startswith("CREATE TABLE IF NOT EXISTS") for s in statements)
    joined = " ".join(statements)
    assert "intent_actor_cursors" in joined and "intent_atc_dedupe" in joined
    assert "source_updated_at" not in joined and "ALTER" not in joined.upper()
