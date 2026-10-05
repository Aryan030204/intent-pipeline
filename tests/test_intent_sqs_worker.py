import contextlib
import copy
import json
import os
import threading
from datetime import datetime, timezone

import pytest

from pipeline.intent_sqs_contract import (
    MalformedMessage,
    parse_message,
    to_click_doc,
    to_event_doc,
    to_session_doc,
)
from pipeline.intent_sqs_writer import (
    BEHAVIORAL_EVENTS_SQL,
    CLICK_EVENTS_SQL,
    INTENT_SESSIONS_SQL,
    apply_batch,
    build_rows,
)
from pipeline.intent_events import (
    _BEHAVIORAL_UPSERT_COLUMNS,
    _CLICK_EVENTS_UPSERT_COLUMNS,
    _INTENT_SESSIONS_UPSERT_COLUMNS,
)
from workers.intent_sqs_worker import IntentSqsWorker, build_from_env

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def fixture(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return json.load(f)


def raw(message, message_id="m-1", receipt="r-1", receive_count="1"):
    body = message if isinstance(message, str) else json.dumps(message)
    return {
        "MessageId": message_id,
        "ReceiptHandle": receipt,
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": receive_count},
    }


class FakeCursor:
    """Models the MySQL behaviour the consumer relies on: a unique key makes an
    INSERT ... ON DUPLICATE KEY UPDATE report rowcount 1 when new and 0 when it
    is a duplicate. Cursor-state lookups return no row (fresh actors)."""

    def __init__(self, source_column_present=True):
        from pipeline.intent_sqs_store import reset_schema_verification

        reset_schema_verification()
        self.executemany_calls = []
        self.execute_calls = []
        self._source_column_present = source_column_present
        self._last = None
        self.rowcount = 0
        self.seen_keys = {}

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        self._last = sql
        self.rowcount = 0
        for table in ("behavioral_events", "click_events"):
            if f"INSERT INTO {table} " in sql and "ON DUPLICATE KEY UPDATE event_id" in sql:
                key = (table, params[0])
                self.rowcount = 0 if key in self.seen_keys else 1
                self.seen_keys[key] = True
        if "INSERT INTO intent_atc_dedupe" in sql:
            key = ("atc", params[0], params[1])
            self.rowcount = 0 if key in self.seen_keys else 1
            self.seen_keys[key] = True

    def fetchone(self):
        if self._last and "SELECT DATABASE()" in self._last:
            return ("testdb",)
        if self._last and "FOR UPDATE" in self._last:
            return None
        return None

    def fetchall(self):
        last = self._last or ""
        if "information_schema.tables" in last:
            return [("intent_actor_cursors",), ("intent_atc_dedupe",)]
        if "information_schema.columns" in last:
            return [("source_updated_at",)] if self._source_column_present else []
        return []

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.in_transaction = True

    def commit(self):
        self.commits += 1
        self.in_transaction = False

    def rollback(self):
        self.rollbacks += 1
        self.in_transaction = False


def make_transaction_factory(cursor, connection, log):
    @contextlib.contextmanager
    def factory(brand_index):
        log.append(("open", brand_index))
        try:
            yield cursor, connection
        except Exception:
            connection.rollback()
            log.append(("rollback", brand_index))
            raise

    return factory


class FakeSqs:
    def __init__(self, batches=None):
        self.batches = list(batches or [])
        self.deleted = []
        self.receive_calls = 0

    def receive_message(self, **kwargs):
        self.receive_calls += 1
        if self.batches:
            return {"Messages": self.batches.pop(0)}
        return {"Messages": []}

    def delete_message_batch(self, QueueUrl, Entries):
        self.deleted.extend(e["ReceiptHandle"] for e in Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}


# ---------- A/B/C/D: row mapping from the producer's real messages ----------

def test_A_event_message_maps_to_behavioral_events_row():
    msg = parse_message(json.dumps(fixture("event_v1.json")))
    row = build_rows([msg])["events"][0]
    values = dict(zip(_BEHAVIORAL_UPSERT_COLUMNS, row))

    assert values["event_id"] == "sh-1"
    assert values["event_name"] == "product_viewed"
    assert values["actor_id"] == "actor-1"
    assert values["client_id"] == "cid-1"
    assert values["visitor_id"] == "vid-1"
    assert values["session_id"] == "sess-1"
    assert values["url"] == "https://blabliblulife.com/products/x"
    assert values["user_agent"] == "UA"
    assert values["product_id"] == "gid://shopify/Product/9"
    assert values["variant_id"] == "77"
    assert values["product_title"] == "Oud"
    assert values["variant_title"] == "100ml"
    assert values["quantity"] == 2
    assert float(values["price"]) == 499
    assert values["currency"] == "INR"
    assert values["occurred_at"] == datetime(2026, 10, 4, 6, 7, 53, 497000, tzinfo=timezone.utc)
    assert json.loads(values["data"])["product_id"] == "gid://shopify/Product/9"


def test_B_click_message_maps_to_click_events_row():
    msg = parse_message(json.dumps(fixture("click_v1.json")))
    row = build_rows([msg])["clicks"][0]
    values = dict(zip(_CLICK_EVENTS_UPSERT_COLUMNS, row))

    assert values["event_id"] == "shu-1"
    assert values["event_name"] == "click"
    assert values["click_x"] == 266
    assert values["click_y"] == 598
    assert values["click_tag"] == "BUTTON"
    assert values["click_element_id"] == "add"
    assert values["cart_changed"] is True
    assert values["url_changed"] is False
    assert values["click_bucket"] == "useful_click"
    data = json.loads(values["data"])
    assert set(data) == {"click", "signals"}
    assert data["signals"]["cart_changed"] is True


def test_C_session_snapshot_maps_to_intent_sessions_row():
    msg = parse_message(json.dumps(fixture("session_snapshot_v1.json")))
    row = build_rows([msg])["sessions"][0]
    values = dict(zip(_INTENT_SESSIONS_UPSERT_COLUMNS + ("source_updated_at",), row))

    assert values["session_id"] == "sess-9"
    assert values["actor_id"] == "actor-9"
    assert values["session_start"] == datetime(2026, 10, 4, 6, 0, tzinfo=timezone.utc)
    assert values["session_end"] == datetime(2026, 10, 4, 6, 10, tzinfo=timezone.utc)
    assert values["session_time_spent_ms"] == 600000
    assert values["source_updated_at"] == datetime(2026, 10, 4, 6, 10, 0, 1000, tzinfo=timezone.utc)


def test_D_session_snapshot_derived_counters_and_sequence_match_extractor_rules():
    msg = parse_message(json.dumps(fixture("session_snapshot_v1.json")))
    row = build_rows([msg])["sessions"][0]
    values = dict(zip(_INTENT_SESSIONS_UPSERT_COLUMNS + ("source_updated_at",), row))

    assert values["event_count"] == 7
    assert values["page_view_count"] == 1
    assert values["product_view_count"] == 1
    assert values["click_count"] == 2
    assert values["useful_click_count"] == 1
    assert values["dead_click_count"] == 1
    assert values["add_to_cart_count"] == 1
    assert values["checkout_started_count"] == 1
    assert values["scroll_count"] == 1
    sequence = json.loads(values["event_sequence"])
    assert sequence["1"] == {"page_viewed": "p1"}
    assert sequence["3"] == {"click": "k-useful"}
    assert list(sequence) == [str(i) for i in range(1, 8)]


# ---------- E–I: version guard and duplicate safety ----------

def _apply_guard(state, incoming):
    """Pure model of the guarded upsert written in INTENT_SESSIONS_SQL. It proves
    the rule, not MySQL itself; real MySQL verification is a separate step."""
    stored_version = state.get("source_updated_at")
    if stored_version is None or stored_version <= incoming["source_updated_at"]:
        state.update(incoming)
    return state


def test_E_newer_snapshot_overwrites_older():
    old = {"session_id": "s", "event_count": 3, "source_updated_at": datetime(2026, 10, 4, 6, 0, tzinfo=timezone.utc)}
    new = {"session_id": "s", "event_count": 5, "source_updated_at": datetime(2026, 10, 4, 6, 9, tzinfo=timezone.utc)}
    state = _apply_guard(dict(old), new)
    assert state["event_count"] == 5


def test_F_older_snapshot_does_not_overwrite_newer():
    new = {"session_id": "s", "event_count": 5, "source_updated_at": datetime(2026, 10, 4, 6, 9, tzinfo=timezone.utc)}
    old = {"session_id": "s", "event_count": 3, "source_updated_at": datetime(2026, 10, 4, 6, 0, tzinfo=timezone.utc)}
    state = _apply_guard(dict(new), old)
    assert state["event_count"] == 5
    assert state["source_updated_at"] == new["source_updated_at"]


def test_I_duplicate_snapshot_is_idempotent():
    snap = {"session_id": "s", "event_count": 5, "source_updated_at": datetime(2026, 10, 4, 6, 9, tzinfo=timezone.utc)}
    once = _apply_guard({}, dict(snap))
    twice = _apply_guard(dict(once), dict(snap))
    assert once == twice


def test_guard_sql_assigns_version_last_and_guards_every_column():
    from pipeline.intent_events import _INTENT_SESSIONS_UPDATE_COLUMNS

    guard = "IF(source_updated_at IS NULL OR source_updated_at <= VALUES(source_updated_at)"
    assert INTENT_SESSIONS_SQL.count(guard) == len(_INTENT_SESSIONS_UPDATE_COLUMNS) + 1
    last_assignment = INTENT_SESSIONS_SQL.rfind(" = IF(")
    assert INTENT_SESSIONS_SQL[:last_assignment].rsplit(", ", 1)[-1] == "source_updated_at"
    assert "session_id = IF(" not in INTENT_SESSIONS_SQL, "session_id is the key and must not be reassigned"


def test_G_duplicate_event_is_harmless_by_unique_event_id():
    assert "ON DUPLICATE KEY UPDATE" in BEHAVIORAL_EVENTS_SQL
    assert "event_id = VALUES(event_id)" not in BEHAVIORAL_EVENTS_SQL
    msg = parse_message(json.dumps(fixture("event_v1.json")))
    rows = build_rows([msg, copy.deepcopy(msg)])["events"]
    assert rows[0] == rows[1]


def test_H_duplicate_click_is_harmless_by_unique_event_id():
    assert "ON DUPLICATE KEY UPDATE" in CLICK_EVENTS_SQL
    msg = parse_message(json.dumps(fixture("click_v1.json")))
    rows = build_rows([msg, copy.deepcopy(msg)])["clicks"]
    assert rows[0] == rows[1]


# ---------- J–N: worker transaction, delete rule, routing, shutdown ----------

def test_J_mysql_failure_rolls_back_and_does_not_delete():
    class BoomCursor(FakeCursor):
        def execute(self, sql, params=None):
            if "INSERT INTO behavioral_events" in sql:
                raise RuntimeError("mysql down")
            super().execute(sql, params)

    log = []
    cursor, connection = BoomCursor(), FakeConnection()
    sqs = FakeSqs()
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="authoritative",
        brand_resolver=lambda brand: 1,
        transaction_factory=make_transaction_factory(cursor, connection, log),
        apply_fn=apply_batch,
    )
    msg = raw(fixture("event_v1.json"), receipt="rh-1")
    result = worker.process([msg])

    assert ("rollback", 1) in log
    assert sqs.deleted == []
    assert result.retained == ["m-1"]
    assert connection.commits == 0


def test_K_successful_commit_then_delete():
    log = []
    cursor, connection = FakeCursor(), FakeConnection()
    sqs = FakeSqs()
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="authoritative",
        brand_resolver=lambda brand: 7,
        transaction_factory=make_transaction_factory(cursor, connection, log),
        apply_fn=apply_batch,
    )
    worker.process([raw(fixture("event_v1.json"), receipt="rh-1"), raw(fixture("click_v1.json"), message_id="m-2", receipt="rh-2")])

    assert connection.commits == 1
    assert sorted(sqs.deleted) == ["rh-1", "rh-2"]


def test_batch_is_one_commit_across_tables_and_a_later_table_failure_commits_nothing():
    class ClickFailsCursor(FakeCursor):
        def execute(self, sql, params=None):
            if "INSERT INTO click_events" in sql:
                raise RuntimeError("click table locked")
            super().execute(sql, params)

    cursor, connection = ClickFailsCursor(), FakeConnection()
    messages = [
        parse_message(json.dumps(fixture("event_v1.json"))),
        parse_message(json.dumps(fixture("click_v1.json"))),
    ]
    with pytest.raises(RuntimeError):
        apply_batch(cursor, connection, messages)
    assert connection.commits == 0

    ok_cursor, ok_connection = FakeCursor(), FakeConnection()
    apply_batch(ok_cursor, ok_connection, messages)
    assert ok_connection.commits == 1


def test_L_malformed_messages_are_retained_not_deleted_or_dropped():
    bad_version = dict(fixture("event_v1.json"), schema_version=2)
    bad_type = dict(fixture("event_v1.json"), type="mystery")
    missing_source = {k: v for k, v in fixture("session_snapshot_v1.json").items() if k != "source_updated_at"}
    cases = [
        raw(bad_version, message_id="v", receipt="rv"),
        raw(bad_type, message_id="t", receipt="rt"),
        raw(missing_source, message_id="s", receipt="rs"),
        raw("not json", message_id="j", receipt="rj"),
    ]
    sqs = FakeSqs()
    worker = IntentSqsWorker(sqs, "https://q", write_mode="dry_run")
    result = worker.process(cases)

    assert sorted(result.malformed) == ["j", "s", "t", "v"]
    assert sqs.deleted == []
    with pytest.raises(MalformedMessage):
        parse_message(json.dumps(bad_version))


def test_M_batches_are_grouped_and_committed_per_brand():
    log = []
    cursor, connection = FakeCursor(), FakeConnection()
    sqs = FakeSqs()
    other = dict(fixture("event_v1.json"), brand_id="pts_shop", event_id="sh-2")
    indices = {"bbb_shop": 2, "pts_shop": 5}
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="authoritative",
        brand_resolver=lambda brand: indices[brand],
        transaction_factory=make_transaction_factory(cursor, connection, log),
        apply_fn=apply_batch,
    )
    worker.process([raw(fixture("event_v1.json"), message_id="a", receipt="ra"), raw(other, message_id="b", receipt="rb")])

    assert [entry for entry in log if entry[0] == "open"] == [("open", 2), ("open", 5)]
    assert connection.commits == 2
    assert sorted(sqs.deleted) == ["ra", "rb"]


def test_unmapped_brand_is_left_for_redelivery():
    sqs = FakeSqs()
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="authoritative",
        brand_resolver=lambda brand: None,
        transaction_factory=make_transaction_factory(FakeCursor(), FakeConnection(), []),
        apply_fn=apply_batch,
    )
    result = worker.process([raw(fixture("event_v1.json"), receipt="rx")])
    assert sqs.deleted == []
    assert result.retained == ["m-1"]


def test_missing_version_column_fails_the_batch_without_commit():
    cursor = FakeCursor(source_column_present=False)
    connection = FakeConnection()
    log = []
    sqs = FakeSqs()
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="authoritative",
        brand_resolver=lambda brand: 1,
        transaction_factory=make_transaction_factory(cursor, connection, log),
        apply_fn=apply_batch,
    )
    worker.process([raw(fixture("session_snapshot_v1.json"), receipt="rs")])
    assert connection.commits == 0
    assert sqs.deleted == []


def test_dry_run_never_opens_a_transaction_or_deletes():
    log = []
    sqs = FakeSqs()
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="dry_run",
        transaction_factory=make_transaction_factory(FakeCursor(), FakeConnection(), log),
        apply_fn=apply_batch,
    )
    result = worker.process([raw(fixture("event_v1.json"), receipt="rd")])
    assert log == []
    assert sqs.deleted == []
    assert result.retained == ["m-1"]


def test_N_graceful_shutdown_finishes_current_batch_before_exit():
    stop = threading.Event()
    log = []
    cursor, connection = FakeCursor(), FakeConnection()
    sqs = FakeSqs(batches=[[raw(fixture("event_v1.json"), receipt="rg")]])

    original_receive = sqs.receive_message

    def receive_then_request_stop(**kwargs):
        response = original_receive(**kwargs)
        stop.set()
        return response

    sqs.receive_message = receive_then_request_stop
    worker = IntentSqsWorker(
        sqs,
        "https://q",
        write_mode="authoritative",
        brand_resolver=lambda brand: 1,
        transaction_factory=make_transaction_factory(cursor, connection, log),
        apply_fn=apply_batch,
    )
    worker.run_forever(stop)

    assert sqs.receive_calls == 1
    assert connection.commits == 1
    assert sqs.deleted == ["rg"]


# ---------- configuration and guard rails ----------

def test_authoritative_mode_requires_explicit_double_opt_in(monkeypatch):
    monkeypatch.setenv("SQS_INTENT_QUEUE_URL", "https://sqs/intent-events")
    monkeypatch.setenv("INTENT_SQS_WRITE_MODE", "authoritative")
    monkeypatch.delenv("INTENT_SQS_ALLOW_PRODUCTION_WRITES", raising=False)
    with pytest.raises(SystemExit, match="INTENT_SQS_ALLOW_PRODUCTION_WRITES"):
        build_from_env(sqs_client=FakeSqs())


def test_queue_url_is_required_and_selects_shadow_by_config(monkeypatch):
    monkeypatch.delenv("SQS_INTENT_QUEUE_URL", raising=False)
    with pytest.raises(SystemExit, match="SQS_INTENT_QUEUE_URL"):
        build_from_env(sqs_client=FakeSqs())

    monkeypatch.setenv("SQS_INTENT_QUEUE_URL", "https://sqs/intent-events-shadow")
    monkeypatch.setenv("INTENT_SQS_WRITE_MODE", "dry_run")
    worker = build_from_env(sqs_client=FakeSqs())
    assert worker.queue_url.endswith("intent-events-shadow")
    assert worker.write_mode == "dry_run"


def test_unknown_write_mode_is_rejected(monkeypatch):
    monkeypatch.setenv("SQS_INTENT_QUEUE_URL", "https://sqs/x")
    monkeypatch.setenv("INTENT_SQS_WRITE_MODE", "dual")
    with pytest.raises(SystemExit, match="INTENT_SQS_WRITE_MODE"):
        build_from_env(sqs_client=FakeSqs())


def test_no_aws_access_keys_are_read_by_the_worker_or_writer():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for rel in ("workers/intent_sqs_worker.py", "pipeline/intent_sqs_writer.py", "pipeline/intent_sqs_contract.py"):
        with open(os.path.join(here, rel), encoding="utf-8") as f:
            source = f.read()
        assert "AWS_ACCESS_KEY_ID" not in source
        assert "AWS_SECRET_ACCESS_KEY" not in source
