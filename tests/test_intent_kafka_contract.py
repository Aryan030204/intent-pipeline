import copy
import json

import pytest

from pipeline.intent_kafka_contract import (
    INTENT_TOPICS,
    InvalidMessage,
    expected_topic,
    parse_record,
)
from tests.helpers import atc, encode, fixtures, value


@pytest.mark.parametrize("name", sorted(fixtures()))
def test_every_message_the_real_producer_emits_is_accepted(name):
    parse_record(encode(fixtures()[name]["value"]))


@pytest.mark.parametrize("name", sorted(fixtures()))
def test_topic_in_the_fixture_matches_the_workers_routing_table(name):
    case = fixtures()[name]
    assert case["topic"] in INTENT_TOPICS
    assert expected_topic(case["value"]["event_name"]) == case["topic"]


def test_the_documented_routes():
    for name in ("checkout_started", "checkout_completed"):
        assert expected_topic(name) == "intent.checkout"
    for name in ("product_added_to_cart", "add_to_cart", "cart_item_added_to_cart", "quick_add_to_cart_clicked"):
        assert expected_topic(name) == "intent.atc"                  # any name containing add_to_cart / added_to_cart
    assert expected_topic("click") == "intent.click"
    for name in ("page_viewed", "product_viewed", "scroll_depth", "cart_viewed", "anything_new", ""):
        assert expected_topic(name) == "intent.other"
    assert expected_topic("CHECKOUT_COMPLETED") == "intent.checkout"   # names are compared case-insensitively


def test_event_id_is_used_exactly_as_supplied_and_never_generated():
    message = parse_record(encode(value("sh-0B94-aBc")))
    assert message.event_id == "sh-0B94-aBc"
    for missing in (None, "", "   "):
        with pytest.raises(InvalidMessage, match="event_id"):
            parse_record(encode({**value("x"), "event_id": missing}))
    broken = value("x")
    del broken["event_id"]
    with pytest.raises(InvalidMessage, match="event_id"):
        parse_record(encode(broken))


def test_actor_identity_is_actor_id_then_client_id_and_never_visitor_id():
    assert parse_record(encode(value("a", actor="actor-1", client="cid-1"))).identity == "actor-1"
    assert parse_record(encode(value("b", actor=None, client="cid-1"))).identity == "cid-1"
    assert parse_record(encode(value("c", actor=None, client=None, visitor="vis-only"))).identity is None


def test_occurred_at_is_the_store_local_wall_clock_unchanged():
    message = parse_record(encode({**value("a"), "occurred_at": "2026-10-05T10:30:00.123Z"}))
    assert message.occurred_at.isoformat() == "2026-10-05T10:30:00.123000"
    assert message.occurred_at.tzinfo is None  # no conversion, nothing to shift


@pytest.mark.parametrize("bad", [
    "2026-10-05T10:30:00.000+05:30", "2026-10-05T10:30:00.000", "not a date", "", None, 1759660200,
])
def test_occurred_at_must_end_in_z(bad):
    with pytest.raises(InvalidMessage, match="occurred_at"):
        parse_record(encode({**value("a"), "occurred_at": bad}))


def test_id_length_limits_follow_the_varchar_100_columns():
    for field in ("event_id", "actor_id", "client_id", "visitor_id"):
        parse_record(encode({**value("a"), field: "x" * 100}))
        with pytest.raises(InvalidMessage, match=field):
            parse_record(encode({**value("a"), field: "x" * 101}))


@pytest.mark.parametrize("payload, reason", [
    (b"not json", "JSON"),
    (b"[1, 2]", "object"),
    (b"\xff\xfe", "JSON"),
    (None, "JSON"),
])
def test_undecodable_payloads_are_invalid(payload, reason):
    with pytest.raises(InvalidMessage, match=reason):
        parse_record(payload)


@pytest.mark.parametrize("field, bad, match", [
    ("schema_version", 2, "schema_version"),
    ("schema_version", True, "schema_version"),
    ("type", "session_snapshot", "type"),
    ("brand_id", "", "brand_id"),
    ("event_name", None, "event_name"),
    ("actor_id", 123, "actor_id"),
    ("raw", "text", "raw"),
])
def test_structural_violations_are_invalid(field, bad, match):
    message = {**value("a"), field: bad}
    with pytest.raises(InvalidMessage, match=match):
        parse_record(encode(message))


def test_click_must_be_type_click_with_a_click_object_and_a_valid_bucket():
    good = value("c", click=True)
    assert parse_record(encode(good)).click_bucket == "useful_click"
    with pytest.raises(InvalidMessage, match="invalid click_bucket"):
        parse_record(encode({**good, "click_bucket": "loud_click"}))
    with pytest.raises(InvalidMessage, match="click payload"):
        parse_record(encode({**good, "click": None}))
    with pytest.raises(InvalidMessage, match="together"):
        parse_record(encode({**good, "type": "event"}))
    with pytest.raises(InvalidMessage, match="together"):
        parse_record(encode({**value("p"), "type": "click"}))


def test_atc_requires_a_product_id_and_it_arrives_normalized():
    ok = parse_record(encode(fixtures()["product_added_to_cart"]["value"]))
    assert ok.is_atc and ok.product_id == "Product:42"
    synth = parse_record(encode(fixtures()["atc_synth"]["value"]))
    assert synth.product_id.startswith("SYNTH:")
    no_product = atc("a")
    no_product["raw"] = {"quantity": 1}
    with pytest.raises(InvalidMessage, match="product_id"):
        parse_record(encode(no_product))
    with pytest.raises(InvalidMessage, match="product_id"):
        parse_record(encode(atc("a", product="p" * 101)))


def test_the_add_to_cart_alias_is_not_an_atc_dedupe_event():
    """The old pipeline deduped only product_added_to_cart. Kept as is (see report)."""
    assert not parse_record(encode(fixtures()["add_to_cart_alias"]["value"])).is_atc


def test_session_fields_sent_by_the_producer_are_ignored_and_the_consumer_assigns_its_own():
    raw = copy.deepcopy(fixtures()["page_viewed"]["value"])
    raw.update(session_id="from-producer", session_start="2026-10-05T10:00:00.000Z")
    message = parse_record(json.dumps(raw))
    assert not hasattr(message, "session_id")
    assert message.to_doc("assigned-by-consumer", "cid-1")["session_id"] == "assigned-by-consumer"
