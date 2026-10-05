"""
SQS consumer for the intent message contract (schema_version 1, produced by
alerts-service/services/intent/messageContract.js).

Modes (INTENT_SQS_WRITE_MODE):
  dry_run        (default) parse, validate and build rows; no MySQL writes and
                 NO deletes, so messages are simply redelivered. Safe to point
                 at intent-events-shadow.
  authoritative  write to each brand's production MySQL in one transaction per
                 brand per batch, then delete only what committed. Refuses to
                 start unless INTENT_SQS_ALLOW_PRODUCTION_WRITES=true. Not used
                 until cutover; the Mongo pipeline stays authoritative.

Delete rule: a message is deleted only after its brand's transaction has
committed. Malformed messages, unknown brands and failed transactions are left
un-deleted so SQS redelivers them, and they reach the DLQ after maxReceiveCount.
"""

import json
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipeline.intent_sqs_contract import MalformedMessage, parse_message  # noqa: E402
from pipeline.state import logger  # noqa: E402

WRITE_MODES = ("dry_run", "authoritative")
MAX_SQS_BATCH = 10
DELETE_CHUNK = 10


@dataclass
class ProcessResult:
    committed: List[Dict[str, str]] = field(default_factory=list)
    retained: List[str] = field(default_factory=list)
    malformed: List[str] = field(default_factory=list)


class VisibilityHeartbeat:
    """Extends visibility for receipt handles still being processed. Starts only
    after one interval, so a batch that finishes quickly makes no extra API calls.
    Stopped before any delete, so it never touches a message it is about to delete."""

    def __init__(self, sqs, queue_url: str, timeout_s: int, interval_s: Optional[float] = None) -> None:
        self.sqs = sqs
        self.queue_url = queue_url
        self.timeout_s = int(timeout_s)
        self.interval_s = interval_s if interval_s is not None else max(1.0, timeout_s / 3)
        self._handles: List[str] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def track(self, handles: List[str]) -> None:
        with self._lock:
            self._handles = list(handles)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="intent-sqs-heartbeat", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            with self._lock:
                handles = list(self._handles)
            for handle in handles:
                try:
                    self.sqs.change_message_visibility(
                        QueueUrl=self.queue_url, ReceiptHandle=handle, VisibilityTimeout=self.timeout_s
                    )
                except Exception as exc:
                    logger.error(
                        f"[intent-sqs] category=visibility_heartbeat_failed reason={type(exc).__name__}: {exc}"
                    )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)


class IntentSqsWorker:
    def __init__(
        self,
        sqs,
        queue_url: str,
        *,
        write_mode: str = "dry_run",
        wait_time_seconds: int = 20,
        batch_size: int = MAX_SQS_BATCH,
        visibility_timeout_seconds: int = 300,
        brand_resolver: Optional[Callable[[str], Optional[int]]] = None,
        transaction_factory: Optional[Callable[[int], Any]] = None,
        apply_fn: Optional[Callable[..., Dict[str, int]]] = None,
        clock: Callable[[], float] = time.monotonic,
        heartbeat_interval_s: Optional[float] = None,
    ) -> None:
        if write_mode not in WRITE_MODES:
            raise ValueError(f"unknown write mode {write_mode!r}")
        if write_mode == "authoritative" and (brand_resolver is None or transaction_factory is None or apply_fn is None):
            raise ValueError("authoritative mode requires brand_resolver, transaction_factory and apply_fn")
        self.sqs = sqs
        self.queue_url = queue_url
        self.write_mode = write_mode
        self.wait_time_seconds = min(max(int(wait_time_seconds), 0), 20)
        self.batch_size = min(max(int(batch_size), 1), MAX_SQS_BATCH)
        self.visibility_timeout_seconds = int(visibility_timeout_seconds)
        self.brand_resolver = brand_resolver
        self.transaction_factory = transaction_factory
        self.apply_fn = apply_fn
        self.clock = clock
        self.heartbeat_interval_s = heartbeat_interval_s

    def receive(self) -> List[Dict[str, Any]]:
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.batch_size,
            WaitTimeSeconds=self.wait_time_seconds,
            AttributeNames=["ApproximateReceiveCount"],
        )
        return response.get("Messages", []) or []

    def process(self, raw_messages: List[Dict[str, Any]]) -> ProcessResult:
        started = self.clock()
        result = ProcessResult()
        by_brand: Dict[str, List[tuple]] = {}

        for raw in raw_messages:
            message_id = raw.get("MessageId", "")
            receive_count = (raw.get("Attributes") or {}).get("ApproximateReceiveCount", "?")
            try:
                parsed = parse_message(raw.get("Body", ""))
            except MalformedMessage as exc:
                result.malformed.append(message_id)
                logger.warning(
                    f"[intent-sqs] malformed message left for redelivery "
                    f"message_id={message_id} receive_count={receive_count} reason={exc}"
                )
                continue
            by_brand.setdefault(parsed["brand_id"], []).append((raw, parsed))

        if self.write_mode == "dry_run":
            for brand, items in by_brand.items():
                kinds = {}
                for _, msg in items:
                    kinds[msg["type"]] = kinds.get(msg["type"], 0) + 1
                logger.info(f"[intent-sqs] dry-run brand={brand} messages={len(items)} types={kinds} (no writes, no deletes)")
            result.retained.extend(raw.get("MessageId", "") for raw in raw_messages if raw.get("MessageId") not in result.malformed)
        else:
            heartbeat = VisibilityHeartbeat(
                self.sqs, self.queue_url, self.visibility_timeout_seconds, self.heartbeat_interval_s
            )
            heartbeat.track([raw.get("ReceiptHandle", "") for raw in raw_messages])
            heartbeat.start()
            try:
                for brand, items in by_brand.items():
                    brand_index = self.brand_resolver(brand)
                    if brand_index is None:
                        result.retained.extend(raw.get("MessageId", "") for raw, _ in items)
                        logger.warning(
                            f"[intent-sqs] category=unknown_brand brand_id={brand} "
                            f"messages={len(items)} left for redelivery (not in INTENT_DB_MAP)"
                        )
                        continue
                    try:
                        with self.transaction_factory(brand_index) as (cursor, connection):
                            counts = self.apply_fn(cursor, connection, [msg for _, msg in items])
                    except Exception as exc:
                        result.retained.extend(raw.get("MessageId", "") for raw, _ in items)
                        logger.error(
                            f"[intent-sqs] category=mysql_transaction_failed brand={brand} "
                            f"messages={len(items)} left for redelivery: {type(exc).__name__}: {exc}"
                        )
                        continue
                    failed_ids = {id(m) for m in counts.pop("failed_messages", [])}
                    for raw, msg in items:
                        if id(msg) in failed_ids:
                            result.retained.append(raw.get("MessageId", ""))
                        else:
                            result.committed.append(
                                {"MessageId": raw.get("MessageId", ""), "ReceiptHandle": raw["ReceiptHandle"]}
                            )
                    logger.info(
                        f"[intent-sqs] category=committed brand={brand} committed={len(items) - len(failed_ids & {id(m) for _, m in items})} "
                        f"retained={len(failed_ids & {id(m) for _, m in items})} counts={counts}"
                    )
            finally:
                heartbeat.stop()

        self._delete_committed(result.committed)

        elapsed = self.clock() - started
        if elapsed > self.visibility_timeout_seconds / 2:
            logger.warning(
                f"[intent-sqs] batch took {elapsed:.1f}s, over half the {self.visibility_timeout_seconds}s "
                f"visibility timeout; raise the timeout or reduce batch size"
            )
        return result

    def _delete_committed(self, entries: List[Dict[str, str]]) -> None:
        for start in range(0, len(entries), DELETE_CHUNK):
            chunk = entries[start : start + DELETE_CHUNK]
            batch = [{"Id": str(i), "ReceiptHandle": e["ReceiptHandle"]} for i, e in enumerate(chunk)]
            response = self.sqs.delete_message_batch(QueueUrl=self.queue_url, Entries=batch)
            for failed in response.get("Failed", []) or []:
                logger.error(f"[intent-sqs] delete failed id={failed.get('Id')} code={failed.get('Code')} (will redeliver; dedupe handles it)")

    def run_once(self) -> ProcessResult:
        return self.process(self.receive())

    def run_forever(self, stop_event: threading.Event) -> None:
        logger.info(
            f"[intent-sqs] consumer started mode={self.write_mode} queue={self.queue_url} "
            f"batch={self.batch_size} wait={self.wait_time_seconds}s"
        )
        while not stop_event.is_set():
            try:
                self.run_once()
            except Exception as exc:
                logger.error(f"[intent-sqs] receive/process error, backing off: {type(exc).__name__}: {exc}")
                stop_event.wait(5)
        logger.info("[intent-sqs] consumer stopped after finishing the current batch")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    try:
        return int(raw) if raw.strip() else default
    except ValueError:
        raise SystemExit(f"{name} must be an integer, got {raw!r}")


def build_from_env(sqs_client=None) -> IntentSqsWorker:
    queue_url = os.environ.get("SQS_INTENT_QUEUE_URL", "").strip()
    if not queue_url:
        raise SystemExit("SQS_INTENT_QUEUE_URL is required (set to intent-events or intent-events-shadow)")

    write_mode = os.environ.get("INTENT_SQS_WRITE_MODE", "dry_run").strip().lower()
    if write_mode not in WRITE_MODES:
        raise SystemExit(f"INTENT_SQS_WRITE_MODE must be one of {WRITE_MODES}")

    region = os.environ.get("AWS_REGION", "ap-south-1").strip() or "ap-south-1"
    if sqs_client is None:
        import boto3  # lazy: tests and dry_run do not need the SDK

        sqs_client = boto3.client("sqs", region_name=region)

    common = dict(
        write_mode=write_mode,
        wait_time_seconds=_env_int("SQS_WAIT_TIME_SECONDS", 20),
        batch_size=_env_int("SQS_BATCH_SIZE", MAX_SQS_BATCH),
        visibility_timeout_seconds=_env_int("SQS_VISIBILITY_TIMEOUT_SECONDS", 300),
    )

    if write_mode == "dry_run":
        return IntentSqsWorker(sqs_client, queue_url, **common)

    if os.environ.get("INTENT_SQS_ALLOW_PRODUCTION_WRITES", "").strip().lower() != "true":
        raise SystemExit("authoritative mode requires INTENT_SQS_ALLOW_PRODUCTION_WRITES=true")

    import aws_background  # noqa: F401  (loads brand configs and INTENT_DB_MAP routing)
    from pipeline.db import get_db_cursor
    from pipeline.intent_events import _parse_intent_db_map
    from pipeline.intent_sqs_writer import apply_batch
    from pipeline.orchestration import _resolve_brand_index_by_db_database

    aws_background.active_brand_indices.clear()
    aws_background.initialize_brand_configs()
    db_map = _parse_intent_db_map()

    def brand_resolver(brand_id: str) -> Optional[int]:
        db_database = db_map.get(brand_id)
        if not db_database:
            return None
        return _resolve_brand_index_by_db_database(db_database)

    return IntentSqsWorker(
        sqs_client,
        queue_url,
        brand_resolver=brand_resolver,
        transaction_factory=get_db_cursor,
        apply_fn=apply_batch,
        **common,
    )


def main() -> None:
    worker = build_from_env()
    stop = threading.Event()

    def _request_stop(signum, _frame):
        logger.info(f"[intent-sqs] received signal {signum}; finishing current batch")
        stop.set()

    # Installed after imports: pipeline.db registers its own handlers on import
    # that raise SystemExit, which would abort a batch mid-transaction.
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    worker.run_forever(stop)


if __name__ == "__main__":
    main()
