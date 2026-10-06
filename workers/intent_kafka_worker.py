"""
Kafka -> MySQL intent worker (replaces the Mongo ingestion of the intent pipeline).

    intent.checkout, intent.atc, intent.click, intent.other      (one consumer group)
        -> IntentConsumer batches -> one MySQL transaction per brand -> COMMIT
        -> manual Kafka offset commit
    poison records -> intent.dlq

One service, one consumer group (INTENT_KAFKA_GROUP_ID, default intent-pipeline-workers)
subscribed to all four topics. Do not give a second service the same topics under a
different group id: every event would then be processed twice.

Ordering. An actor's events sit on different topics, which Kafka does not order against each
other, so the consumer releases records in Kafka-timestamp order behind a watermark (see
pipeline/intent_kafka_ordering.py). That holds only while ONE consumer owns every partition
an actor can land on:
  INTENT_KAFKA_ORDERING_DOMAIN=single (default)       exactly one consumer in the group
  INTENT_KAFKA_ORDERING_DOMAIN=copartitioned          all topics have the same partition count,
                                                      so several consumers can share the load
A consumer whose assignment breaks the domain pauses and logs ordering_domain_violated.

Environment: see .env.example (Kafka section).
"""

import logging
import os
import re
import signal
import socket
import sys
import threading

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipeline.intent_kafka_consumer import (  # noqa: E402
    DEFAULT_DLQ_TOPIC,
    DEFAULT_GROUP_ID,
    ConsumerConfig,
    IntentConsumer,
)
from pipeline.intent_kafka_contract import INTENT_TOPICS  # noqa: E402
from pipeline.state import logger  # noqa: E402

DEFAULT_THREADS = 1  # one consumer owns every partition, which the ordering guarantee needs (domain single)
MAX_THREADS = 10


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer, got {raw!r}")
    if value < minimum:
        raise SystemExit(f"{name} must be at least {minimum}, got {value}")
    return value


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a number, got {raw!r}")


def config_from_env() -> ConsumerConfig:
    topics_raw = os.environ.get("INTENT_KAFKA_TOPICS", "").strip()
    topics = tuple(t.strip() for t in topics_raw.split(",") if t.strip()) or INTENT_TOPICS
    return ConsumerConfig(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-service:9092").strip(),
        group_id=os.environ.get("INTENT_KAFKA_GROUP_ID", DEFAULT_GROUP_ID).strip() or DEFAULT_GROUP_ID,
        topics=topics,
        dlq_topic=os.environ.get("INTENT_DLQ_TOPIC", DEFAULT_DLQ_TOPIC).strip() or DEFAULT_DLQ_TOPIC,
        batch_size=_env_int("INTENT_KAFKA_BATCH_SIZE", 200),
        batch_wait_s=_env_float("INTENT_KAFKA_BATCH_WAIT_S", 1.0),
        max_record_attempts=_env_int("INTENT_KAFKA_MAX_ATTEMPTS", 3),
        session_timeout_s=_env_int("SESSION_TIMEOUT", 1800),
        stats_interval_s=_env_float("INTENT_KAFKA_STATS_INTERVAL_S", 60.0),
        ordering_domain=_env_domain(),
        order_slack_s=_env_float("INTENT_KAFKA_ORDER_SLACK_S", 10.0),
        order_buffer_max=_env_int("INTENT_KAFKA_ORDER_BUFFER_MAX", 2000),
    )


def _env_domain() -> str:
    value = os.environ.get("INTENT_KAFKA_ORDERING_DOMAIN", "single").strip().lower() or "single"
    if value not in ("single", "copartitioned"):
        raise SystemExit(f"INTENT_KAFKA_ORDERING_DOMAIN must be single or copartitioned, got {value!r}")
    return value


def check_topics(config: ConsumerConfig) -> None:
    """Refuses to start if a consumed topic or the DLQ topic does not exist. Topics are
    created only by kafka-service/topics.conf; this worker never creates them."""
    from confluent_kafka.admin import AdminClient

    metadata = AdminClient({"bootstrap.servers": config.bootstrap_servers}).list_topics(timeout=15)
    existing = set(metadata.topics)
    missing = [t for t in (*config.topics, config.dlq_topic) if t not in existing]
    if missing:
        raise SystemExit(
            f"Kafka topics missing: {missing}. Add them to kafka-service/topics.conf and re-run kafka-init "
            f"(the DLQ topic {config.dlq_topic} is required before any poison record can be quarantined)."
        )


BRAND_ID_PATTERN = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")  # the pattern /track enforces for brand ids


def validate_db_map(db_map, resolve_database):
    """Returns {brand_id: brand_index} or raises SystemExit naming every problem. A brand that
    is mapped but does not resolve to an active brand database is fatal: every record of that
    brand would be quarantined, so the mistake must stop the deploy, not fill the DLQ.
    `resolve_database(database_name)` returns the brand index or None."""
    problems = []
    if not db_map:
        problems.append("INTENT_DB_MAP is empty or not a JSON object of brand_id -> database name")
    resolved = {}
    for brand_id, database in db_map.items():
        if not BRAND_ID_PATTERN.match(brand_id):
            problems.append(f"brand_id {brand_id!r} is not a valid brand id (lowercase letters, digits, underscores)")
        if not isinstance(database, str) or not database.strip():
            problems.append(f"{brand_id}: database name must be a non-empty string")
            continue
        index = resolve_database(database)
        if index is None:
            problems.append(f"{brand_id} -> {database!r} matches no active brand database")
        else:
            resolved[brand_id] = index
    by_index = {}
    for brand_id, index in resolved.items():
        by_index.setdefault(index, []).append(brand_id)
    for index, brands in by_index.items():
        if len(brands) > 1:
            problems.append(f"brands {sorted(brands)} all map to the same database (brand_index {index})")
    if problems:
        raise SystemExit("INTENT_DB_MAP is invalid: " + "; ".join(problems))
    return resolved


def resolve_brands():
    """INTENT_DB_MAP brand_id -> brand database index, resolved once at startup. A brand
    added to INTENT_DB_MAP needs a worker restart. Every brand /track can send
    (INTENT_BRANDS_ALLOWLIST in alerts-service) must be listed; one that is not is
    quarantined to the DLQ."""
    import aws_background
    from pipeline.intent_events import _parse_intent_db_map
    from pipeline.orchestration import _resolve_brand_index_by_db_database
    from pipeline.state import active_brand_indices

    active_brand_indices.clear()
    aws_background.initialize_brand_configs()
    resolved = validate_db_map(_parse_intent_db_map(), _resolve_brand_index_by_db_database)
    logger.info(f"[intent-kafka] category=brands_resolved brands={sorted(resolved)}")
    return resolved


def run_threads(consumers, stop_event: threading.Event) -> int:
    """Runs each consumer in its own thread. Returns the process exit code."""
    failures = []

    def target(consumer):
        try:
            consumer.run()
        except BaseException as exc:  # a dead consumer must stop the whole worker
            failures.append(exc)
            logger.error(f"[intent-kafka] category=consumer_died name={consumer.name} error={type(exc).__name__}: {exc}")
            stop_event.set()

    threads = [threading.Thread(target=target, args=(c,), name=c.name) for c in consumers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return 1 if failures else 0


def preflight() -> int:
    """`python workers/intent_kafka_worker.py --preflight`: runs every startup check on its own and
    reports ALL failures, without consuming or writing anything (information_schema SELECTs and a
    Kafka metadata request only). Exit code 0 means the worker can start."""
    from pipeline.intent_kafka_db import verify_brand_schemas

    failures = []

    def check(label, fn):
        try:
            result = fn()
            logger.info(f"[preflight] PASS {label}")
            return result
        except SystemExit as exc:
            failures.append(label)
            logger.error(f"[preflight] FAIL {label}: {exc}")
        except Exception as exc:
            failures.append(label)
            logger.error(f"[preflight] FAIL {label}: {type(exc).__name__}: {exc}")
        return None

    config = check("environment settings", config_from_env)
    if config is None:
        return 1
    brands = check("INTENT_DB_MAP resolves to active brand databases", resolve_brands)
    for brand_id, index in sorted((brands or {}).items()):
        check(f"schema ready for {brand_id} (brand_index {index})", lambda index=index: verify_brand_schemas([index]))
    check(f"Kafka topics {list(config.topics)} and DLQ {config.dlq_topic} exist", lambda: check_topics(config))
    threads = _env_int("INTENT_KAFKA_CONSUMER_THREADS", DEFAULT_THREADS)
    if threads > 1 and config.ordering_domain == "single":
        failures.append("consumer threads")
        logger.error(f"[preflight] FAIL consumer threads: {threads} consumers break ordering domain 'single'")
    logger.info("[preflight] ready to start" if not failures else f"[preflight] NOT READY: {failures}")
    return 1 if failures else 0


def main() -> None:
    if "--preflight" in sys.argv:
        sys.exit(preflight())

    from confluent_kafka import Consumer, Producer

    from pipeline.intent_kafka_db import BrandConnections, verify_brand_schemas

    config = config_from_env()
    thread_count = min(_env_int("INTENT_KAFKA_CONSUMER_THREADS", DEFAULT_THREADS), MAX_THREADS)
    if thread_count > 1 and config.ordering_domain == "single":
        raise SystemExit(
            f"INTENT_KAFKA_CONSUMER_THREADS={thread_count} would split the partitions between consumers and break "
            f"the cross-topic ordering guarantee. Use 1, or set INTENT_KAFKA_ORDERING_DOMAIN=copartitioned after "
            f"giving all four topics the same partition count."
        )

    brands = resolve_brands()
    resolver = brands.get
    verify_brand_schemas(sorted(set(brands.values())))
    check_topics(config)

    stop_event = threading.Event()
    host = socket.gethostname()
    dlq = Producer(
        {"bootstrap.servers": config.bootstrap_servers, "client.id": f"{host}-intent-dlq", "acks": "all",
         "enable.idempotence": True}
    )
    consumers = [
        IntentConsumer(
            Consumer(config.kafka_settings(f"{host}-intent-{n}")),
            dlq,
            BrandConnections(),
            resolver,
            config,
            stop_event,
            name=f"consumer-{n}",
        )
        for n in range(thread_count)
    ]

    # Installed after the imports above: pipeline.db registers handlers on import that
    # raise SystemExit, which would abort a batch between MySQL COMMIT and the offset commit.
    def request_stop(signum, _frame):
        logger.info(f"[intent-kafka] received signal {signum}; finishing the current batch, then exiting")
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    logger.info(
        f"[intent-kafka] starting threads={thread_count} group={config.group_id} "
        f"brokers={config.bootstrap_servers} topics={list(config.topics)} dlq={config.dlq_topic}"
    )
    code = run_threads(consumers, stop_event)
    dlq.flush(30)
    logger.info("[intent-kafka] stopped cleanly" if code == 0 else "[intent-kafka] stopped after a consumer failure")
    sys.exit(code)


if __name__ == "__main__":
    main()
