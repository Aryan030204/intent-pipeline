"""Shared test helpers: messages shaped like the real /track output, and fakes for the
Kafka consumer, DLQ producer and database that record one shared, ordered event log."""

import contextlib
import copy
import json
import os
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipeline.intent_kafka_contract import parse_record
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import WorkItem

FIXTURE_FILE = os.path.join(os.path.dirname(__file__), "fixtures", "track_kafka_messages.json")
BASE = datetime(2026, 10, 5, 10, 30, 0)  # store-local wall clock, as /track sends it


def fixtures() -> Dict[str, Dict[str, Any]]:
    """Messages captured from the real alerts-service producer code
    (controllers/trackIntent.js buildIntentMessage + topicRouting.js)."""
    with open(FIXTURE_FILE, encoding="utf-8") as handle:
        return json.load(handle)


def stamp(seconds: float = 0, base: datetime = BASE) -> str:
    return (base + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def value(event_id: str, at: float = 0, actor: Optional[str] = "cid-1", name: str = "page_viewed",
          raw: Optional[dict] = None, brand: str = "bbb_shop", click: bool = False,
          bucket: str = "useful_click", client: Optional[str] = "cid-1",
          visitor: Optional[str] = "vis-1") -> Dict[str, Any]:
    """A canonical message dict built from the real fixtures, with the fields varied."""
    template = fixtures()["click_useful" if click else "page_viewed"]["value"]
    message = copy.deepcopy(template)
    message.update(
        brand_id=brand, event_id=event_id, message_key=event_id, actor_id=actor, client_id=client,
        visitor_id=visitor, occurred_at=stamp(at),
    )
    if click:
        message["click_bucket"] = bucket
    else:
        message["event_name"] = name
        message["raw"] = raw if raw is not None else message.get("raw")
    return message


def atc(event_id: str, at: float = 0, product: str = "Product:42", **kw) -> Dict[str, Any]:
    return value(event_id, at, name="product_added_to_cart", raw={"product_id": product, "quantity": 1}, **kw)


def encode(message: Dict[str, Any]) -> bytes:
    return json.dumps(message).encode("utf-8")


def item(message: Dict[str, Any], ref=None) -> WorkItem:
    parsed = parse_record(encode(message))
    return WorkItem(ref if ref is not None else ("intent.other", 0, message["event_id"]), parsed)


def items(*messages: Dict[str, Any]) -> List[WorkItem]:
    return [item(m, ("intent.other", 0, i)) for i, m in enumerate(messages)]


def seq_ids(store: InMemoryIntentStore, actor: str) -> List[str]:
    return [step["event_id"] for _, step in sorted(store.cursors[actor].events_seq.items(), key=lambda kv: int(kv[0]))]


# ---------------- fakes for the consumer tests ----------------

class FakeError:
    def __init__(self, text: str, fatal: bool = False) -> None:
        self._text, self._fatal = text, fatal

    def fatal(self) -> bool:
        return self._fatal

    def __str__(self) -> str:
        return self._text


class FakeMsg:
    def __init__(self, topic: str, partition: int, offset: int, body: Any, key: bytes = b"k", error=None,
                 ts: int = 1000) -> None:
        self._t, self._p, self._o, self._v, self._k, self._e, self._ts = topic, partition, offset, body, key, error, ts

    def timestamp(self): return (1, self._ts)   # (CREATE_TIME, ms): how /track stamps a record

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v
    def key(self): return self._k
    def error(self): return self._e


def record(topic: str, partition: int, offset: int, message: Any, key: bytes = b"bbb_shop:cid-1",
           ts: int = 1000) -> FakeMsg:
    body = message if isinstance(message, (bytes, type(None))) else encode(message)
    return FakeMsg(topic, partition, offset, body, key, ts=ts)


PARTITION_COUNTS = {"intent.checkout": 2, "intent.atc": 2, "intent.click": 3, "intent.other": 3}


class FakeBroker:
    """Per-partition logs and committed offsets shared by the fake consumers, like a real broker."""

    def __init__(self, counts=None, log=None):
        self.counts = dict(counts or PARTITION_COUNTS)
        self.logs = {(t, p): [] for t, n in self.counts.items() for p in range(n)}
        self.committed = {}
        self.log = log if log is not None else []
        self.created = []          # every consumer the factory handed out
        self.delay = {}            # (topic, partition) -> consume() calls before it delivers: a lagging topic
        self.fail_commit = False
        self.read_limit = None     # records one consume() may return, like a fetch that is smaller than the backlog

    def put(self, topic, partition, body, ts=1000, key=b"bbb_shop:cid-1"):
        """Appends a record (a message dict, bytes, or None) and returns its offset."""
        log = self.logs[(topic, partition)]
        payload = body if isinstance(body, (bytes, type(None))) else encode(body)
        log.append(FakeMsg(topic, partition, len(log), payload, key, ts=ts))
        return len(log) - 1

    def factory(self):
        consumer = FakeKafka(self)
        self.created.append(consumer)
        return consumer


class FakeKafka:
    """confluent_kafka.Consumer stand-in for a cycle: never subscribed, partitions assigned by hand."""

    def __init__(self, broker: FakeBroker) -> None:
        from confluent_kafka import TopicPartition

        self._TP = TopicPartition
        self.broker = broker
        self.log = broker.log
        self.positions = {}
        self.paused = set()
        self.pause_calls, self.resume_calls, self.commits = [], [], []
        self.delay = dict(broker.delay)
        self.closed = False
        self.assigned = []

    # metadata and offsets
    def list_topics(self, timeout=None):
        return types.SimpleNamespace(topics={t: types.SimpleNamespace(partitions={i: None for i in range(n)}, error=None)
                                             for t, n in self.broker.counts.items()})

    def committed(self, tps, timeout=None):
        return [self._TP(tp.topic, tp.partition, self.broker.committed.get((tp.topic, tp.partition), -1001)) for tp in tps]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return (0, len(self.broker.logs[(tp.topic, tp.partition)]))

    def offsets_for_times(self, tps, timeout=None):
        out = []
        for tp in tps:
            hit = next((m.offset() for m in self.broker.logs[(tp.topic, tp.partition)] if m._ts >= tp.offset), -1)
            out.append(self._TP(tp.topic, tp.partition, hit))
        return out

    # reading
    def assign(self, tps):
        self.assigned = [(tp.topic, tp.partition) for tp in tps]
        self.positions = {(tp.topic, tp.partition): tp.offset for tp in tps}

    def position(self, tps):
        return [self._TP(tp.topic, tp.partition, self.positions.get((tp.topic, tp.partition), -1001)) for tp in tps]

    def consume(self, num_messages, timeout):
        out = []
        if self.broker.read_limit:
            num_messages = min(num_messages, self.broker.read_limit)
        for key in self.assigned:
            if key in self.paused:
                continue
            if self.delay.get(key, 0) > 0:
                self.delay[key] -= 1
                continue
            log = self.broker.logs[key]
            while self.positions[key] < len(log) and len(out) < num_messages:
                out.append(log[self.positions[key]])
                self.positions[key] += 1
        return out

    def pause(self, tps):
        self.pause_calls.append([(t.topic, t.partition) for t in tps])
        self.paused |= {(t.topic, t.partition) for t in tps}

    def resume(self, tps):
        self.resume_calls.append([(t.topic, t.partition) for t in tps])
        self.paused -= {(t.topic, t.partition) for t in tps}

    # committing
    def commit(self, offsets, asynchronous):
        assert asynchronous is False, "offset commits must be synchronous"
        if self.broker.fail_commit:
            raise RuntimeError("commit failed")
        snapshot = {(tp.topic, tp.partition): tp.offset for tp in offsets}
        self.commits.append(snapshot)
        self.broker.committed.update(snapshot)
        self.log.append(("offset_commit", snapshot))

    def close(self):
        self.closed = True


class FakeDlq:
    def __init__(self, log: List[tuple]) -> None:
        self.log = log
        self.sent: List[Dict] = []
        self.fail = False
        self._pending = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if self.fail == "raise":
            raise BufferError("queue full")
        self._pending.append((topic, key, value, dict(headers or []), on_delivery))

    def flush(self, timeout):
        for topic, key, value, headers, cb in self._pending:
            if self.fail:
                cb(RuntimeError("not acknowledged"), None)
            else:
                self.sent.append({"topic": topic, "key": key, "value": value, "headers": {k: v.decode() for k, v in headers.items()}})
                self.log.append(("dlq", headers["dlq_source_offset"].decode()))
                cb(None, None)
        self._pending = []
        return 0


class FakeDb:
    """One InMemoryIntentStore per brand index. transaction() commits on a clean exit and
    restores the store on an exception, like MySQL."""

    def __init__(self, log: List[tuple], stores: Optional[Dict[int, InMemoryIntentStore]] = None) -> None:
        self.log = log
        self.stores = stores or {1: InMemoryIntentStore(), 2: InMemoryIntentStore()}
        self.fail_brand: Dict[int, Exception] = {}
        self.fail_commit: Dict[int, Exception] = {}
        self.on_commit = None          # called after each MySQL commit (tests use it to stop a cycle mid-way)
        self.closed = False

    @contextlib.contextmanager
    def transaction(self, brand_index):
        store = self.stores[brand_index]
        token = store.savepoint()
        try:
            if brand_index in self.fail_brand:
                raise self.fail_brand[brand_index]
            yield store
            if brand_index in self.fail_commit:
                raise self.fail_commit[brand_index]
        except BaseException:
            store.rollback_to(token)
            self.log.append(("rollback", brand_index))
            raise
        self.log.append(("mysql_commit", brand_index))
        if self.on_commit:
            self.on_commit()

    def close(self):
        self.closed = True


BRANDS = {"bbb_shop": 1, "pts_shop": 2}
