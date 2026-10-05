"""Worker lifecycle tests with fakes: heartbeat, delete-after-commit, delete failure,
unknown brand, poison isolation and repeated redelivery.

These run the real IntentSqsWorker.process() and the real apply_messages() state logic
against per-brand in-memory stores that roll back on failure. Nothing here talks to AWS
or MySQL. The DLQ itself lives in the queue's RedrivePolicy, so the repeated-poison test
only proves the worker never deletes a poison message; it does not prove AWS moves it."""

import contextlib
import threading
import time

from pipeline.intent_sqs_store import InMemoryIntentStore
from pipeline.intent_sqs_writer import apply_messages
from workers.intent_sqs_worker import IntentSqsWorker

QUEUE = "https://sqs.ap-south-1.amazonaws.com/000000000000/intent-events"
BRANDS = {"bbb_shop": 1, "pts_shop": 2}
HEARTBEAT_THREAD = "intent-sqs-heartbeat"


def body(event_id, brand="bbb_shop", actor="actor-1", when="2026-10-05T12:34:56.123Z",
         name="page_viewed", raw=None):
    # Same shape as alerts-service buildEventMessage (messageContract.js).
    return {
        "schema_version": 1, "type": "event", "message_key": event_id, "brand_id": brand,
        "event_id": event_id, "event_name": name, "actor_id": actor, "client_id": actor,
        "visitor_id": "visitor-1", "session_id": None, "occurred_at": when,
        "url": "https://shop.example/", "referrer": None, "user_agent": "UA",
        "session_start": None, "session_end": None, "session_time_spent": None, "raw": raw,
    }


def raw(message, message_id, receipt, receive_count="1"):
    import json

    return {
        "MessageId": message_id, "ReceiptHandle": receipt, "Body": json.dumps(message),
        "Attributes": {"ApproximateReceiveCount": receive_count},
    }


class RecordingSqs:
    def __init__(self):
        self.events = []  # ("visibility"|"delete", receipt) in call order
        self.visibility_calls = []
        self.deleted = []
        self.fail_visibility = False
        self.fail_delete = set()

    def receive_message(self, **kwargs):
        return {"Messages": []}

    def change_message_visibility(self, QueueUrl, ReceiptHandle, VisibilityTimeout):
        self.events.append(("visibility", ReceiptHandle))
        self.visibility_calls.append((ReceiptHandle, VisibilityTimeout))
        if self.fail_visibility:
            raise RuntimeError("ThrottlingException")

    def delete_message_batch(self, QueueUrl, Entries):
        ok, failed = [], []
        for entry in Entries:
            receipt = entry["ReceiptHandle"]
            self.events.append(("delete", receipt))
            if receipt in self.fail_delete:
                failed.append({"Id": entry["Id"], "Code": "InternalError"})
            else:
                ok.append({"Id": entry["Id"]})
                self.deleted.append(receipt)
        return {"Successful": ok, "Failed": failed}


class Databases:
    """One in-memory store per brand database. A transaction rolls its store back when the
    block raises, which is how MySQL behaves for a batch-fatal error."""

    def __init__(self, slow_s=0.0, poison_ids=()):
        self.stores = {index: PoisonStore(poison_ids) if poison_ids else InMemoryIntentStore()
                       for index in BRANDS.values()}
        self.opened = []
        self.slow_s = slow_s

    @contextlib.contextmanager
    def transaction(self, brand_index):
        self.opened.append(brand_index)
        store = self.stores[brand_index]
        snapshot = store.begin_group()
        try:
            yield store, None
        except Exception:
            store.rollback_group(snapshot)
            raise
        else:
            store.commit_group(snapshot)

    def apply(self, store, connection, messages):
        if self.slow_s:
            time.sleep(self.slow_s)
        return apply_messages(store, messages, 1800)


class PoisonStore(InMemoryIntentStore):
    """Rejects one event_id with a message-level error (bad data), not a batch-fatal one."""

    def __init__(self, poison_ids):
        super().__init__()
        self.poison_ids = set(poison_ids)

    def insert_event(self, kind, row):
        if row[0] in self.poison_ids:
            raise ValueError("Data too long for column 'event_id'")
        return super().insert_event(kind, row)


def make_worker(sqs, db, heartbeat_s=0.02, visibility_s=300):
    return IntentSqsWorker(
        sqs, QUEUE, write_mode="authoritative", visibility_timeout_seconds=visibility_s,
        brand_resolver=lambda brand: BRANDS.get(brand),
        transaction_factory=db.transaction, apply_fn=db.apply, heartbeat_interval_s=heartbeat_s,
    )


def heartbeat_threads_alive():
    return [t for t in threading.enumerate() if t.name == HEARTBEAT_THREAD and t.is_alive()]


# ---------- A. visibility heartbeat ----------

def test_A1_heartbeat_extends_visibility_while_processing():
    sqs, db = RecordingSqs(), Databases(slow_s=0.15)
    make_worker(sqs, db).process([raw(body("e-1"), "m-1", "r-1")])
    assert len(sqs.visibility_calls) >= 2
    assert all(handle == "r-1" and timeout == 300 for handle, timeout in sqs.visibility_calls)


def test_A2_heartbeat_stops_when_processing_completes():
    sqs, db = RecordingSqs(), Databases(slow_s=0.1)
    make_worker(sqs, db).process([raw(body("e-1"), "m-1", "r-1")])
    settled = len(sqs.visibility_calls)
    time.sleep(0.1)
    assert len(sqs.visibility_calls) == settled
    assert heartbeat_threads_alive() == []


def test_A3_heartbeat_stops_before_any_delete():
    sqs, db = RecordingSqs(), Databases(slow_s=0.1)
    make_worker(sqs, db).process([raw(body("e-1"), "m-1", "r-1")])
    first_delete = next(i for i, (kind, _) in enumerate(sqs.events) if kind == "delete")
    assert all(kind != "visibility" for kind, _ in sqs.events[first_delete:])


def test_A4_heartbeat_failure_does_not_fail_the_batch_or_leave_a_thread_running():
    sqs, db = RecordingSqs(), Databases(slow_s=0.1)
    sqs.fail_visibility = True
    result = make_worker(sqs, db).process([raw(body("e-1"), "m-1", "r-1")])
    assert [c["ReceiptHandle"] for c in result.committed] == ["r-1"]
    assert sqs.deleted == ["r-1"]
    assert "e-1" in db.stores[1].events
    assert heartbeat_threads_alive() == []


# ---------- B. delete failure after MySQL commit ----------

def test_B1_delete_failure_after_commit_is_safe_on_redelivery():
    sqs, db = RecordingSqs(), Databases()
    sqs.fail_delete = {"r-1"}
    worker = make_worker(sqs, db)
    worker.process([raw(body("e-1"), "m-1", "r-1")])
    store = db.stores[1]
    assert sqs.deleted == []  # SQS still holds the message
    assert "e-1" in store.events

    cursor_before = dict(store.cursors["actor-1"])
    events_seq_before = dict(cursor_before["events_seq"])

    sqs.fail_delete = set()
    worker.process([raw(body("e-1"), "m-1", "r-1", receive_count="2")])
    assert len(store.events) == 1
    assert store.cursors["actor-1"]["events_seq"] == events_seq_before
    assert store.cursors["actor-1"]["last_event_id"] == cursor_before["last_event_id"]
    assert sqs.deleted == ["r-1"]


def test_B2_atc_redelivery_after_delete_failure_does_not_double_count():
    sqs, db = RecordingSqs(), Databases()
    atc = body("a-1", name="product_added_to_cart", raw={"product_id": "Product:9"})
    sqs.fail_delete = {"r-1"}
    worker = make_worker(sqs, db)
    worker.process([raw(atc, "m-1", "r-1")])
    sqs.fail_delete = set()
    worker.process([raw(atc, "m-1", "r-1", receive_count="2")])
    store = db.stores[1]
    assert len(store.events) == 1
    assert len(store.atc) == 1
    assert len(store.cursors["actor-1"]["events_seq"]) == 1


# ---------- C. unknown brand ----------

def test_C1_unknown_brand_is_not_written_and_is_retained_for_redelivery():
    sqs, db = RecordingSqs(), Databases()
    healthy = raw(body("e-bbb", brand="bbb_shop"), "m-b", "r-b")
    unknown = raw(body("e-shy", brand="shyle_shop"), "m-s", "r-s")
    result = make_worker(sqs, db).process([healthy, unknown])

    assert "m-s" in result.retained  # not silently dropped
    assert "r-s" not in sqs.deleted  # not deleted, so SQS redrives it to the DLQ
    assert db.opened == [1]  # no transaction opened for the unmapped brand
    assert "e-shy" not in db.stores[1].events
    assert all("e-shy" not in store.events for store in db.stores.values())
    assert "e-bbb" in db.stores[1].events  # healthy neighbour unaffected
    assert sqs.deleted == ["r-b"]


# ---------- D. poison messages ----------

def test_D1_poison_message_is_isolated_from_healthy_messages_in_the_same_batch():
    sqs = RecordingSqs()
    db = Databases(poison_ids={"e-poison"})
    batch = [
        raw(body("e-ok-1", when="2026-10-05T12:00:00.000Z"), "m-ok-1", "r-ok-1"),
        raw(body("e-poison", when="2026-10-05T12:00:10.000Z"), "m-poison", "r-poison"),
        raw(body("e-ok-2", when="2026-10-05T12:00:20.000Z"), "m-ok-2", "r-ok-2"),
    ]
    result = make_worker(sqs, db).process(batch)
    store = db.stores[1]
    assert "m-poison" in result.retained
    assert "r-poison" not in sqs.deleted
    assert sorted(sqs.deleted) == ["r-ok-1", "r-ok-2"]
    assert "e-ok-1" in store.events and "e-ok-2" in store.events
    assert "e-poison" not in store.events


def test_D2_repeated_poison_is_never_deleted_and_healthy_messages_still_are():
    sqs = RecordingSqs()
    db = Databases(poison_ids={"e-poison"})
    worker = make_worker(sqs, db)
    seen_counts = []
    for receive_count in range(1, 6):
        healthy_id = f"e-ok-{receive_count}"
        healthy = raw(body(healthy_id, when=f"2026-10-05T12:0{receive_count}:00.000Z"),
                      f"m-ok-{receive_count}", f"r-ok-{receive_count}")
        poison = raw(body("e-poison", when="2026-10-05T11:00:00.000Z"), "m-poison", "r-poison",
                     receive_count=str(receive_count))
        result = worker.process([poison, healthy])
        seen_counts.append(receive_count)
        assert "m-poison" in result.retained
        assert f"r-ok-{receive_count}" in sqs.deleted
    assert seen_counts == [1, 2, 3, 4, 5]
    assert "r-poison" not in sqs.deleted  # only the queue's redrive policy can remove it
    assert "e-ok-5" in db.stores[1].events
