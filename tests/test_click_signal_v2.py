"""
Click Signal V2 inside this repo. /track (alerts-service) decides whether a click is useful or dead, from the four
signal booleans and the pixel's response_signals, and sends the result as `click_bucket` plus the cleaned
`signals`. This repo only counts the bucket and keeps the extra signal fields in the click row's `data` JSON.

These tests pin that contract: new fields are stored without a new column, old messages store exactly what they
always did, a click is one click however many response signals it has, and a scroll response does not touch the
session's scroll count (which only `scroll_depth` events feed).
"""

import json

from pipeline.intent_events import _CLICK_EVENTS_UPSERT_COLUMNS, _extract_click_event_row, _INTENT_SESSIONS_UPSERT_COLUMNS
from pipeline.intent_kafka_contract import parse_record
from pipeline.intent_kafka_store import InMemoryIntentStore
from pipeline.intent_kafka_writer import apply_batch
from scripts import diagnose_intent as d

from tests.helpers import encode, items, value

GAP = 1900   # seconds: more than the 1800 s session timeout, so the next event closes the session

NONE = {"url_changed": False, "cart_changed": False, "ui_changed": False, "meaningful_scroll": False}


def response(name, category="other"):
    return {"event_name": name, "category": category, "occurred_at": "2026-10-05T10:30:01.000Z"}


def v2_click(event_id, at, signals, bucket, **kw):
    message = value(event_id, at, click=True, bucket=bucket, **kw)
    message["signals"] = signals
    return message


def v2_signals(responses, **flags):
    categories = []
    for r in responses:
        if r["category"] not in categories:
            categories.append(r["category"])
    useful = bool(responses) or any(flags.values())
    return {**NONE, **flags, "response_signals": responses, "response_categories": categories,
            "useful_click": useful, "dead_click": not useful, "contract_version": 2}


def row_data(message):
    doc = parse_record(encode(message)).to_doc("sess-1", message["actor_id"])
    row = _extract_click_event_row(doc)
    return dict(zip(_CLICK_EVENTS_UPSERT_COLUMNS, row)), json.loads(dict(zip(_CLICK_EVENTS_UPSERT_COLUMNS, row))["data"])


def session_row(*messages):
    store = InMemoryIntentStore()
    apply_batch(store, items(*messages, value("later", 100 + GAP, actor=messages[0]["actor_id"])))
    assert len(store.sessions) == 1
    return store.sessions[0]


def column(row, name):
    return row[_INTENT_SESSIONS_UPSERT_COLUMNS.index(name)]


# ---------------- storage ----------------

def test_the_new_signal_fields_are_kept_in_the_data_json_without_a_new_column():
    message = v2_click("c1", 0, v2_signals([response("product_viewed", "product"), response("page_viewed", "navigation")]), "useful_click")
    columns, data = row_data(message)
    assert columns["click_bucket"] == "useful_click"
    signals = data["signals"]
    assert [r["event_name"] for r in signals["response_signals"]] == ["product_viewed", "page_viewed"]
    assert signals["response_categories"] == ["product", "navigation"]
    assert signals["useful_click"] is True and signals["dead_click"] is False and signals["contract_version"] == 2
    assert set(columns) == set(_CLICK_EVENTS_UPSERT_COLUMNS)    # same columns as before: nothing was added


def test_a_legacy_click_message_stores_exactly_what_it_stored_before():
    message = value("c1", 0, click=True, bucket="dead_click")      # the captured fixture: four booleans only
    assert "response_signals" not in message["signals"]
    columns, data = row_data(message)
    assert columns["click_bucket"] == "dead_click"
    assert set(data["signals"]) == {"url_changed", "cart_changed", "ui_changed", "meaningful_scroll"}


def test_response_signals_that_are_not_a_list_are_not_stored():
    message = v2_click("c1", 0, {**NONE, "response_signals": "oops", "response_categories": {"a": 1}}, "dead_click")
    _, data = row_data(message)
    assert "response_signals" not in data["signals"] and "response_categories" not in data["signals"]


def test_the_bucket_is_taken_from_track_not_recomputed_from_the_signals():
    # the pipeline trusts /track: a click /track called dead stays dead even if the stored signals look useful
    message = v2_click("c1", 0, v2_signals([response("product_viewed", "product")]), "dead_click")
    columns, _ = row_data(message)
    assert columns["click_bucket"] == "dead_click"


# ---------------- counting ----------------

def test_a_click_with_many_response_signals_is_one_useful_click():
    many = v2_signals([response("product_viewed", "product"), response("collection_viewed", "collection"), response("page_viewed", "navigation")])
    row = session_row(value("p1", 0), v2_click("c1", 4, many, "useful_click"))
    assert (column(row, "click_count"), column(row, "useful_click_count"), column(row, "dead_click_count")) == (1, 1, 0)


def test_useful_and_dead_clicks_count_by_the_bucket_track_sent():
    row = session_row(
        value("p1", 0),
        v2_click("c1", 3, v2_signals([response("cart_viewed", "cart")]), "useful_click"),
        v2_click("c2", 6, v2_signals([]), "dead_click"),
        value("c3", 9, click=True, bucket="dead_click"),
    )
    assert (column(row, "click_count"), column(row, "useful_click_count"), column(row, "dead_click_count")) == (3, 1, 2)


def test_a_scroll_response_on_a_click_does_not_change_the_sessions_scroll_count():
    scrolled = v2_signals([response("scroll_depth", "scroll")], meaningful_scroll=True)
    row = session_row(value("p1", 0), v2_click("c1", 4, scrolled, "useful_click"))
    assert column(row, "scroll_count") == 0 and column(row, "click_count") == 1


def test_only_scroll_depth_events_feed_the_scroll_count_even_next_to_a_scroll_response():
    scrolled = v2_signals([response("scroll_depth", "scroll")], meaningful_scroll=True)
    scroll_event = value("s1", 5, name="scroll_depth", raw={"percent": 25})
    row = session_row(value("p1", 0), v2_click("c1", 4, scrolled, "useful_click"), scroll_event)
    assert column(row, "scroll_count") == 1          # the one scroll_depth event, not that plus the click's response


# ---------------- diagnostics helpers ----------------

def test_response_signal_counts_are_grouped_as_0_1_2_3plus():
    assert [d.response_count_bucket(n) for n in (None, 0, 1, 2, 3, 4, 9)] == ["0", "0", "1", "2", "3+", "3+", "3+"]


def test_the_contract_label_tells_legacy_rows_from_new_ones():
    assert d.contract_label(None) == "legacy (no marker)"
    assert d.contract_label(1).startswith("v1")
    assert d.contract_label(2).startswith("v2")


def test_the_dead_click_summary_has_every_requested_percentile():
    summary = d.dead_click_summary([i / 100 for i in range(1, 101)])
    assert all(key in summary for key in ("P50", "P75", "P80", "P85", "P90", "P95", "P99"))
    assert summary["P50"] <= summary["P75"] <= summary["P80"] <= summary["P85"] <= summary["P90"] <= summary["P95"] <= summary["P99"]


def test_the_struggle_report_says_whether_any_session_is_above_the_stored_p90():
    # the existing rule is strictly greater than: nothing can sit above a P90 of exactly 1.0
    stuck = d.struggle_trigger_report([0.2, 0.9, 1.0, 1.0], 1.0)
    assert stuck == {"stored_p90": 1.0, "sessions": 4, "above_stored_p90": 0, "can_fire": False}
    live = d.struggle_trigger_report([0.2, 0.9, 1.0, 1.0], 0.5)
    assert live["above_stored_p90"] == 3 and live["can_fire"] is True
    assert d.struggle_trigger_report([0.5], None)["can_fire"] is None


def test_section_22_can_be_selected_and_the_script_lists_it():
    assert d.parse_args(["--brand", "BBB", "--date", "2026-11-04", "--only", "20,22", "--deep"]).only_sections == {20, 22}
    assert callable(d.click_signal_v2)
