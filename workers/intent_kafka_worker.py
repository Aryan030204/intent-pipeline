"""
Kafka -> MySQL intent worker (replaces the Mongo ingestion of the intent pipeline).

    intent.checkout, intent.atc, intent.click, intent.other
        -> ingest cycle: read what is waiting, in /track send order
        -> one MySQL transaction per brand -> COMMIT -> manual Kafka offset commit
    poison records -> intent.dlq

It does not poll continuously. A cycle runs when either trigger fires:
  * batch completion: the events waiting across ALL topics reach INTENT_EVENTS_BATCH. The count is read
    from Kafka offsets every INTENT_EVENTS_CHECK_INTERVAL_S seconds (end offset - committed offset);
    no records are consumed to take it.
  * schedule: every INTENT_KAFKA_RUN_EVERY_MINUTES (default 25), whatever is waiting, and once at startup.
Cycles never overlap, in this process (one at a time) or across containers (a MySQL named lock: a second
worker skips its cycle and logs it). Run one worker.

Environment: see .env.example (Kafka section).
"""

import logging
import os
import re
import signal
import socket
import sys
import threading
from datetime import datetime
from typing import Optional

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
    CycleResult,
    IntentIngestor,
)
from pipeline.intent_kafka_contract import INTENT_TOPICS  # noqa: E402
from pipeline.state import logger  # noqa: E402

SCHEDULER_TIMEZONE = "Asia/Kolkata"
SHUTDOWN_WAIT_S = 50


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


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a number, got {raw!r}")
    if value < minimum:
        raise SystemExit(f"{name} must be at least {minimum}, got {value}")
    return value


def config_from_env() -> ConsumerConfig:
    topics_raw = os.environ.get("INTENT_KAFKA_TOPICS", "").strip()
    topics = tuple(t.strip() for t in topics_raw.split(",") if t.strip()) or INTENT_TOPICS
    return ConsumerConfig(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-service:9092").strip(),
        group_id=os.environ.get("INTENT_KAFKA_GROUP_ID", DEFAULT_GROUP_ID).strip() or DEFAULT_GROUP_ID,
        topics=topics,
        dlq_topic=os.environ.get("INTENT_DLQ_TOPIC", DEFAULT_DLQ_TOPIC).strip() or DEFAULT_DLQ_TOPIC,
        events_batch=_env_int("INTENT_EVENTS_BATCH", 500),
        run_every_minutes=_env_float("INTENT_KAFKA_RUN_EVERY_MINUTES", 25.0, minimum=1.0),
        check_interval_s=_env_float("INTENT_EVENTS_CHECK_INTERVAL_S", 30.0, minimum=1.0),
        max_record_attempts=_env_int("INTENT_KAFKA_MAX_ATTEMPTS", 3),
        session_timeout_s=_env_int("SESSION_TIMEOUT", 1800),
        order_slack_s=_env_float("INTENT_KAFKA_ORDER_SLACK_S", 10.0),
        order_buffer_max=_env_int("INTENT_KAFKA_ORDER_BUFFER_MAX", 2000),
    )


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


class CycleController:
    """Runs ingest cycles for the two triggers, one at a time."""

    def __init__(self, ingestor: IntentIngestor, config: ConsumerConfig, stop_event: threading.Event) -> None:
        self.ingestor, self.cfg, self.stop_event = ingestor, config, stop_event
        self._busy = threading.Lock()

    def run(self, reason: str) -> Optional[CycleResult]:
        """Trigger 2 (schedule) and the body of trigger 1. Skipped if a cycle is already running."""
        if self.stop_event.is_set():
            return None
        if not self._busy.acquire(blocking=False):
            logger.info(f"[intent-kafka] category=cycle_skipped trigger={reason} a cycle is already running")
            return None
        try:
            return self.ingestor.run_cycle(reason)
        except Exception as exc:  # a failed cycle must never stop the scheduler
            logger.exception(f"[intent-kafka] category=cycle_crashed trigger={reason} {type(exc).__name__}: {exc}")
            return None
        finally:
            self._busy.release()

    def check_batch(self) -> Optional[CycleResult]:
        """Trigger 1: runs a cycle when the events waiting reach INTENT_EVENTS_BATCH."""
        if self.stop_event.is_set() or self._busy.locked():
            return None
        try:
            pending = self.ingestor.pending_events()
        except Exception as exc:
            logger.warning(f"[intent-kafka] category=pending_check_failed {type(exc).__name__}: {exc}")
            return None
        if pending < self.cfg.events_batch:
            return None
        logger.info(f"[intent-kafka] category=batch_trigger pending={pending} batch={self.cfg.events_batch}")
        return self.run("batch")

    def drain(self, timeout: float = SHUTDOWN_WAIT_S) -> bool:
        """Waits for a running cycle to finish (it stops between batches once stop_event is set)."""
        if self._busy.acquire(timeout=timeout):
            self._busy.release()
            return True
        return False


def build_scheduler(controller: CycleController, config: ConsumerConfig):
    """The schedule trigger (every run_every_minutes, and once now) and the batch trigger's check
    (every check_interval_s). max_instances=1 and coalesce keep each job from piling up."""
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = BlockingScheduler(timezone=SCHEDULER_TIMEZONE)
    scheduler.add_job(
        controller.run, IntervalTrigger(seconds=int(config.run_every_minutes * 60), timezone=SCHEDULER_TIMEZONE),
        args=["schedule"], id="intent_kafka_schedule", max_instances=1, coalesce=True, misfire_grace_time=300,
        next_run_time=datetime.now(scheduler.timezone),
    )
    scheduler.add_job(
        controller.check_batch, IntervalTrigger(seconds=int(config.check_interval_s), timezone=SCHEDULER_TIMEZONE),
        id="intent_kafka_batch_check", max_instances=1, coalesce=True, misfire_grace_time=30,
    )
    return scheduler


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
    logger.info("[preflight] ready to start" if not failures else f"[preflight] NOT READY: {failures}")
    return 1 if failures else 0


def main() -> None:
    if "--preflight" in sys.argv:
        sys.exit(preflight())

    from confluent_kafka import Consumer, Producer

    from pipeline.db import get_db_connection
    from pipeline.intent_kafka_db import BrandConnections, RunLock, verify_brand_schemas

    config = config_from_env()
    brands = resolve_brands()
    verify_brand_schemas(sorted(set(brands.values())))
    check_topics(config)

    stop_event = threading.Event()
    host = socket.gethostname()
    dlq = Producer(
        {"bootstrap.servers": config.bootstrap_servers, "client.id": f"{host}-intent-dlq", "acks": "all",
         "enable.idempotence": True}
    )
    ingestor = IntentIngestor(
        lambda: Consumer(config.kafka_settings(f"{host}-intent")),
        dlq,
        BrandConnections(),
        brands.get,
        config,
        stop_event,
        run_lock=RunLock(get_db_connection, min(brands.values()), f"intent-kafka:{config.group_id}"),
    )
    controller = CycleController(ingestor, config, stop_event)
    scheduler = build_scheduler(controller, config)

    # Installed after the imports above: pipeline.db registers handlers on import that
    # raise SystemExit, which would abort a batch between MySQL COMMIT and the offset commit.
    def request_stop(signum, _frame):
        logger.info(f"[intent-kafka] received signal {signum}; finishing the current batch, then exiting")
        stop_event.set()
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    logger.info(
        f"[intent-kafka] starting brokers={config.bootstrap_servers} topics={list(config.topics)} dlq={config.dlq_topic} "
        f"batch={config.events_batch} schedule=every {config.run_every_minutes:g} min "
        f"batch_check=every {config.check_interval_s:g}s slack={config.order_slack_s:g}s"
    )
    try:
        scheduler.start()                       # blocks until request_stop
    finally:
        stop_event.set()
        finished = controller.drain()
        ingestor.close()
        dlq.flush(30)
    logger.info("[intent-kafka] stopped cleanly" if finished else "[intent-kafka] stopped; a cycle was still finishing")
    sys.exit(0 if finished else 1)


if __name__ == "__main__":
    main()
