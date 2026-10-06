"""
Contract check for intent messages consumed from Kafka (schema_version 1).

/track (alerts-service controllers/trackIntent.js + services/intent/messageContract.js)
has already validated and normalized every message: event_id is the pixel's id, actor_id
is actor_id || client_id, ATC product ids are normalized, clicks are bucketed, occurred_at
is the brand's store-local wall clock encoded with a trailing Z. This module therefore
only verifies that a record has the canonical shape the consumer relies on. It never
normalizes, generates ids or reinterprets pixel payloads.

A record that fails here can never succeed on retry, so it is a poison record
(InvalidMessage) and goes to the DLQ instead of blocking its partition.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

SCHEMA_VERSION = 1
TYPE_EVENT = "event"
TYPE_CLICK = "click"
VALID_CLICK_BUCKETS = ("useful_click", "dead_click")

# Every id lands in a VARCHAR(100) column (behavioral_events, click_events,
# intent_actor_cursors, intent_atc_dedupe).
MAX_ID_LENGTH = 100
ATC_EVENT_NAME = "product_added_to_cart"

# Topic each event name is routed to by /track. Used only to log a mismatch; the consumer handles a
# record by its content, not its topic. The rules:
#   checkout_started, checkout_completed        -> intent.checkout
#   a name containing add_to_cart / added_to_cart -> intent.atc
#   click                                       -> intent.click
#   everything else                             -> intent.other
TOPIC_CHECKOUT = "intent.checkout"
TOPIC_ATC = "intent.atc"
TOPIC_CLICK = "intent.click"
TOPIC_OTHER = "intent.other"
INTENT_TOPICS = (TOPIC_CHECKOUT, TOPIC_ATC, TOPIC_CLICK, TOPIC_OTHER)
CHECKOUT_EVENTS = ("checkout_started", "checkout_completed")
ATC_NAME_PARTS = ("add_to_cart", "added_to_cart")


class InvalidMessage(ValueError):
    """The record is structurally invalid and will never be accepted."""


@dataclass(frozen=True)
class IntentMessage:
    type: str
    brand_id: str
    event_id: str
    event_name: str
    actor_id: Optional[str]
    client_id: Optional[str]
    visitor_id: Optional[str]
    occurred_at: datetime  # naive: the store-local wall clock exactly as /track sent it
    url: Optional[str]
    referrer: Optional[str]
    user_agent: Optional[str]
    raw: Optional[Dict[str, Any]]
    click: Optional[Dict[str, Any]]
    signals: Optional[Dict[str, Any]]
    click_bucket: Optional[str]

    @property
    def identity(self) -> Optional[str]:
        """Actor identity, the existing rule: actor_id || client_id. visitor_id never counts."""
        return self.actor_id or self.client_id

    @property
    def is_atc(self) -> bool:
        return self.type == TYPE_EVENT and self.event_name == ATC_EVENT_NAME

    @property
    def product_id(self) -> Optional[str]:
        return (self.raw or {}).get("product_id") if self.is_atc else None

    def to_doc(self, session_id: str, actor_id: Optional[str]) -> Dict[str, Any]:
        """The document shape pipeline/intent_events.py's row extractors read (the shape
        of the old Mongo documents), with the session assigned by the consumer."""
        doc: Dict[str, Any] = {
            "event_id": self.event_id,
            "event_name": self.event_name,
            "actor_id": actor_id,
            "client_id": self.client_id,
            "visitor_id": self.visitor_id,
            "session_id": session_id,
            "url": self.url,
            "referrer": self.referrer,
            "user_agent": self.user_agent,
            "occurred_at": self.occurred_at,
            "raw": self.raw,
        }
        if self.type == TYPE_CLICK:
            doc.update(click=self.click, signals=self.signals, click_bucket=self.click_bucket)
        return doc


def expected_topic(event_name: str) -> str:
    name = (event_name or "").lower()
    if name in CHECKOUT_EVENTS:
        return TOPIC_CHECKOUT
    if any(part in name for part in ATC_NAME_PARTS):
        return TOPIC_ATC
    if name == "click":
        return TOPIC_CLICK
    return TOPIC_OTHER


def _text(
    payload: Dict[str, Any], field: str, *, required: bool = False, id_field: bool = True
) -> Optional[str]:
    value = payload.get(field)
    if value is None:
        if required:
            raise InvalidMessage(f"{field} is required")
        return None
    if not isinstance(value, str):
        raise InvalidMessage(f"{field} must be a string")
    if not value.strip():
        if required:
            raise InvalidMessage(f"{field} must not be blank")
        return None
    if id_field and len(value) > MAX_ID_LENGTH:
        raise InvalidMessage(f"{field} is longer than {MAX_ID_LENGTH} characters")
    return value


def _parse_occurred_at(value: Any) -> datetime:
    # The producer always ends the store-local wall clock with Z. Any other form was not
    # produced by /track, and reading an offset here would shift the event.
    if not isinstance(value, str) or not value.endswith("Z"):
        raise InvalidMessage("occurred_at must be an ISO timestamp ending in Z")
    try:
        return datetime.fromisoformat(value[:-1]).replace(tzinfo=None)
    except ValueError as exc:
        raise InvalidMessage("occurred_at is not a valid ISO timestamp") from exc


def _object(payload: Dict[str, Any], field: str) -> Optional[Dict[str, Any]]:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise InvalidMessage(f"{field} must be an object")
    return value


def parse_record(value: Any) -> IntentMessage:
    """Decode and validate one Kafka record value. Raises InvalidMessage."""
    try:
        text = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value
        payload = json.loads(text)
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise InvalidMessage(f"payload is not valid JSON: {type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise InvalidMessage("payload must be a JSON object")

    if payload.get("schema_version") != SCHEMA_VERSION or isinstance(payload.get("schema_version"), bool):
        raise InvalidMessage(f"unsupported schema_version {payload.get('schema_version')!r}")
    msg_type = payload.get("type")
    if msg_type not in (TYPE_EVENT, TYPE_CLICK):
        raise InvalidMessage(f"unsupported type {msg_type!r}")

    brand_id = _text(payload, "brand_id", required=True)
    event_id = _text(payload, "event_id", required=True)
    event_name = _text(payload, "event_name", required=True)
    if (msg_type == TYPE_CLICK) != (event_name == "click"):
        raise InvalidMessage("type click and event_name click must go together")

    message = IntentMessage(
        type=msg_type,
        brand_id=brand_id,
        event_id=event_id,
        event_name=event_name,
        actor_id=_text(payload, "actor_id"),
        client_id=_text(payload, "client_id"),
        visitor_id=_text(payload, "visitor_id"),
        occurred_at=_parse_occurred_at(payload.get("occurred_at")),
        url=_text(payload, "url", id_field=False),
        referrer=_text(payload, "referrer", id_field=False),
        user_agent=_text(payload, "user_agent", id_field=False),
        raw=_object(payload, "raw"),
        click=_object(payload, "click"),
        signals=_object(payload, "signals"),
        click_bucket=payload.get("click_bucket"),
    )

    if msg_type == TYPE_CLICK:
        if message.click is None:
            raise InvalidMessage("click payload is required for a click")
        if message.click_bucket is not None and message.click_bucket not in VALID_CLICK_BUCKETS:
            raise InvalidMessage(f"invalid click_bucket {message.click_bucket!r}")
    if message.is_atc:
        product_id = (message.raw or {}).get("product_id")
        if not isinstance(product_id, str) or not product_id.strip():
            raise InvalidMessage("product_added_to_cart requires raw.product_id")
        if len(product_id) > MAX_ID_LENGTH:
            raise InvalidMessage(f"raw.product_id is longer than {MAX_ID_LENGTH} characters")
    return message
