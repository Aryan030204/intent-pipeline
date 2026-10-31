"""The utm_* columns on the write path: the row tuples, the MySQL store, and a database that lacks the columns."""

import pytest

from pipeline.intent_events import _BEHAVIORAL_UPSERT_COLUMNS, _CLICK_EVENTS_UPSERT_COLUMNS
from pipeline.intent_kafka_contract import parse_record
from pipeline.intent_kafka_db import UTM_RECHECK_S, BrandConnections
from pipeline import intent_kafka_store as store_mod
from pipeline.intent_kafka_store import (
    CLICK_INSERT_SQL,
    CLICK_INSERT_SQL_NO_UTM,
    EVENT_INSERT_SQL,
    EVENT_INSERT_SQL_NO_UTM,
    REQUIRED_COLUMNS,
    InMemoryIntentStore,
    MySqlIntentStore,
    utm_columns_present,
)
from pipeline.intent_kafka_writer import _event_row, apply_batch
from pipeline.utm import UTM_COLUMNS

from tests.helpers import encode, items, value

LANDING = "https://shop.com/?utm_source=Google&utm_medium=cpc&utm_campaign=diwali&utm_term=serum&utm_content=b1"
WITH = ("Google", "cpc", "diwali", "serum", "b1")
NONE5 = (None, None, None, None, None)


def message(event_id, url, click=False, at=0):
    raw = value(event_id, at, click=click)
    raw["url"] = url
    return parse_record(encode(raw))


# ---- the row tuples ----

def test_event_rows_end_with_the_five_utm_values_and_match_the_column_tuple():
    kind, row = _event_row(message("e1", LANDING), "s1", "a1")
    assert kind == "event" and len(row) == len(_BEHAVIORAL_UPSERT_COLUMNS)
    assert tuple(_BEHAVIORAL_UPSERT_COLUMNS[-5:]) == UTM_COLUMNS and row[-5:] == WITH


def test_click_rows_end_with_the_five_utm_values_and_match_the_column_tuple():
    kind, row = _event_row(message("c1", LANDING, click=True), "s1", "a1")
    assert kind == "click" and len(row) == len(_CLICK_EVENTS_UPSERT_COLUMNS)
    assert tuple(_CLICK_EVENTS_UPSERT_COLUMNS[-5:]) == UTM_COLUMNS and row[-5:] == WITH


def test_a_url_without_utm_gives_nulls_not_an_error():
    assert _event_row(message("e2", "https://shop.com/products/x"), "s", "a")[1][-5:] == NONE5
    assert _event_row(message("c2", "https://shop.com/", click=True), "s", "a")[1][-5:] == NONE5


def test_the_full_url_is_still_stored_unchanged():
    row = _event_row(message("e3", LANDING), "s", "a")[1]
    assert row[_BEHAVIORAL_UPSERT_COLUMNS.index("url")] == LANDING


def test_the_utm_columns_are_optional_for_the_startup_schema_check():
    assert not set(UTM_COLUMNS) & set(REQUIRED_COLUMNS["behavioral_events"])
    assert not set(UTM_COLUMNS) & set(REQUIRED_COLUMNS["click_events"])
    assert "url" in REQUIRED_COLUMNS["behavioral_events"] and "occurred_at" in REQUIRED_COLUMNS["click_events"]


def test_the_in_memory_store_keeps_them_through_the_writer():
    store = InMemoryIntentStore()
    apply_batch(store, items(value("m1", 0), {**value("m2", 5), "url": LANDING}))
    assert store.events["m2"][-5:] == WITH and store.events["m1"][-5:] != WITH


# ---- the MySQL store, with and without the columns ----

class Cursor:
    def __init__(self, rows=None):
        self.calls, self.rowcount, self._rows = [], 1, rows or []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self._rows


def sample(kind):
    return _event_row(message("x", LANDING, click=(kind == "click")), "s", "a")


@pytest.mark.parametrize("kind,full,legacy", [("event", EVENT_INSERT_SQL, EVENT_INSERT_SQL_NO_UTM),
                                              ("click", CLICK_INSERT_SQL, CLICK_INSERT_SQL_NO_UTM)])
def test_one_event_insert_includes_the_utm_columns_when_the_database_has_them(kind, full, legacy):
    cursor = Cursor()
    _kind, row = sample(kind)
    assert MySqlIntentStore(cursor, utm=True).insert_event(kind, row) is True
    sql, params = cursor.calls[0]
    assert sql == full and all(c in sql for c in UTM_COLUMNS) and params == row


@pytest.mark.parametrize("kind,full,legacy", [("event", EVENT_INSERT_SQL, EVENT_INSERT_SQL_NO_UTM),
                                              ("click", CLICK_INSERT_SQL, CLICK_INSERT_SQL_NO_UTM)])
def test_one_event_insert_leaves_them_out_when_the_database_does_not(kind, full, legacy):
    cursor = Cursor()
    _kind, row = sample(kind)
    MySqlIntentStore(cursor, utm=False).insert_event(kind, row)
    sql, params = cursor.calls[0]
    assert sql == legacy and not any(c in sql for c in UTM_COLUMNS)
    assert params == row[:-5] and sql.count("%s") == len(params)


@pytest.mark.parametrize("kind,table", [("event", "behavioral_events"), ("click", "click_events")])
def test_bulk_inserts_follow_the_same_rule(kind, table):
    _k, row = sample(kind)
    with_utm, without = Cursor(), Cursor()
    MySqlIntentStore(with_utm, utm=True).insert_events(kind, [row, row])
    MySqlIntentStore(without, utm=False).insert_events(kind, [row, row])
    sql, params = with_utm.calls[0]
    assert "utm_source" in sql and len(params) == 2 * len(row)
    sql, params = without.calls[0]
    assert "utm_source" not in sql and len(params) == 2 * (len(row) - 5) and sql.count("%s") == len(params)
    assert f"INSERT INTO {table}" in sql


def test_the_store_defaults_to_writing_the_utm_columns():
    cursor = Cursor()
    MySqlIntentStore(cursor).insert_event("event", sample("event")[1])
    assert "utm_source" in cursor.calls[0][0]


def test_utm_columns_present_needs_all_five_on_both_tables():
    ok = [{"tbl": "behavioral_events", "n": 5}, {"tbl": "click_events", "n": 5}]
    assert utm_columns_present(Cursor(ok)) is True
    assert utm_columns_present(Cursor([{"tbl": "behavioral_events", "n": 5}])) is False
    assert utm_columns_present(Cursor([{"tbl": "behavioral_events", "n": 5}, {"tbl": "click_events", "n": 3}])) is False
    assert utm_columns_present(Cursor([])) is False


# ---- BrandConnections decides per brand, and notices when the migration arrives ----

class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class CheckCursor(Cursor):
    def __init__(self, present, fail=False):
        super().__init__()
        self.present, self.fail = present, fail

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if self.fail:
            raise RuntimeError("information_schema unavailable")
        self._rows = ([{"tbl": "behavioral_events", "n": 5}, {"tbl": "click_events", "n": 5}] if self.present else [])


def test_a_database_with_the_columns_is_checked_once():
    brands, cursor = BrandConnections(clock=Clock()), CheckCursor(True)
    assert brands._has_utm(1, cursor) is True and brands._has_utm(1, cursor) is True
    assert len(cursor.calls) == 1


def test_a_database_without_them_is_asked_again_after_the_recheck_interval_and_then_switches_on():
    clock, brands = Clock(), BrandConnections(clock=Clock())
    brands._clock = clock
    cursor = CheckCursor(False)
    assert brands._has_utm(1, cursor) is False
    clock.now += UTM_RECHECK_S - 1
    assert brands._has_utm(1, cursor) is False and len(cursor.calls) == 1       # not asked yet
    clock.now += 2
    cursor.present = True                                                         # the migration was applied meanwhile
    assert brands._has_utm(1, cursor) is True and len(cursor.calls) == 2


def test_a_failed_check_means_write_without_utm_and_try_again_later():
    clock, brands = Clock(), BrandConnections()
    brands._clock = clock
    cursor = CheckCursor(True, fail=True)
    assert brands._has_utm(1, cursor) is False
    clock.now += UTM_RECHECK_S + 1
    cursor.fail = False
    assert brands._has_utm(1, cursor) is True


def test_each_brand_is_decided_separately():
    brands = BrandConnections(clock=Clock())
    assert brands._has_utm(1, CheckCursor(True)) is True and brands._has_utm(2, CheckCursor(False)) is False


def test_the_verify_schema_warning_names_the_missing_columns(monkeypatch):
    warnings = []
    monkeypatch.setattr(store_mod.logger, "warning", lambda message: warnings.append(message))

    class SchemaCursor(Cursor):
        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            lowered = sql.lower()
            if "information_schema.tables" in lowered:
                self._rows = [{"name": t} for t in sorted(REQUIRED_COLUMNS)]
            elif "information_schema.columns" in lowered:
                self._rows = [{"tbl": t, "col": c} for t, cols in REQUIRED_COLUMNS.items() for c in cols]
                self._rows.append({"tbl": "intent_sessions", "col": "first_utm_source"})  # present: only the utm_* are missing
            else:
                self._rows = ([{"tbl": "behavioral_events", "idx": "uq", "nu": 0, "col": "event_id"},
                               {"tbl": "click_events", "idx": "uq", "nu": 0, "col": "event_id"},
                               {"tbl": "intent_sessions", "idx": "uq", "nu": 0, "col": "session_id"},
                               {"tbl": "intent_actor_cursors", "idx": "PRIMARY", "nu": 0, "col": "actor_id"},
                               {"tbl": "intent_atc_dedupe", "idx": "PRIMARY", "nu": 0, "col": "session_id"},
                               {"tbl": "intent_atc_dedupe", "idx": "PRIMARY", "nu": 0, "col": "product_id"}])

    store_mod.verify_schema(SchemaCursor())          # does not raise: the columns are optional
    assert len(warnings) == 2 and all("utm_source" in w and "003_event_utm_columns" in w for w in warnings)
