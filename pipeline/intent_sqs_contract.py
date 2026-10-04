"""
Consumer side of the intent message contract (schema_version 1) produced by
alerts-service/services/intent/messageContract.js. The producer is the source
of truth; this module only validates and converts messages into the document
shapes that the existing Mongo extractors in pipeline/intent_events.py already
read, so MySQL rows are built by the same code as the Mongo pipeline.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

SCHEMA_VERSION = 1
TYPE_EVENT = "event"
TYPE_CLICK = "click"
TYPE_SESSION_SNAPSHOT = "session_snapshot"
_KNOWN_TYPES = {TYPE_EVENT, TYPE_CLICK, TYPE_SESSION_SNAPSHOT}
_VALID_CLICK_BUCKETS = {"useful_click", "dead_click"}


class MalformedMessage(ValueError):
    """Permanently invalid message. The worker leaves it un-deleted so SQS
    redelivers it and it eventually reaches the DLQ; it is never dropped."""


def _parse_ts(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise MalformedMessage(f"{field} must be an ISO timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MalformedMessage(f"{field} is not a valid ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_ts(value: Any, field: str):
    if value is None:
        return None
    return _parse_ts(value, field)


def _require_str(message: Dict[str, Any], field: str) -> str:
    value = message.get(field)
    if not isinstance(value, str) or not value.strip():
        raise MalformedMessage(f"{field} required")
    return value


def parse_message(body: str) -> Dict[str, Any]:
    try:
        message = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise MalformedMessage("body is not valid JSON") from exc
    if not isinstance(message, dict):
        raise MalformedMessage("message must be a JSON object")

    version = message.get("schema_version")
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise MalformedMessage(f"unsupported schema_version {version!r}")

    msg_type = message.get("type")
    if msg_type not in _KNOWN_TYPES:
        raise MalformedMessage(f"unknown type {msg_type!r}")

    _require_str(message, "brand_id")
    _require_str(message, "message_key")

    if msg_type in (TYPE_EVENT, TYPE_CLICK):
        _require_str(message, "event_id")
        _require_str(message, "event_name")
        _parse_ts(message.get("occurred_at"), "occurred_at")
        if msg_type == TYPE_CLICK:
            bucket = message.get("click_bucket")
            if bucket is not None and bucket not in _VALID_CLICK_BUCKETS:
                raise MalformedMessage(f"invalid click_bucket {bucket!r}")
    else:
        _require_str(message, "session_id")
        _require_str(message, "actor_id")
        _parse_ts(message.get("session_start"), "session_start")
        _parse_ts(message.get("source_updated_at"), "source_updated_at")
        if not isinstance(message.get("events_seq"), dict):
            raise MalformedMessage("events_seq must be an object")

    return message


def to_event_doc(message: Dict[str, Any]) -> Dict[str, Any]:
    """Shape read by _extract_behavioral_event_row (mirrors intent_sessions.events)."""
    return {
        "event_id": message["event_id"],
        "event_name": message["event_name"],
        "actor_id": message.get("actor_id"),
        "client_id": message.get("client_id"),
        "visitor_id": message.get("visitor_id"),
        "session_id": message.get("session_id"),
        "url": message.get("url"),
        "referrer": message.get("referrer"),
        "user_agent": message.get("user_agent"),
        "occurred_at": _parse_ts(message["occurred_at"], "occurred_at"),
        "raw": message.get("raw") if isinstance(message.get("raw"), dict) else {},
    }


def to_click_doc(message: Dict[str, Any]) -> Dict[str, Any]:
    """Shape read by _extract_click_event_row (mirrors intent_sessions.click_events)."""
    return {
        "event_id": message["event_id"],
        "event_name": message["event_name"],
        "actor_id": message.get("actor_id"),
        "client_id": message.get("client_id"),
        "visitor_id": message.get("visitor_id"),
        "session_id": message.get("session_id"),
        "url": message.get("url"),
        "referrer": message.get("referrer"),
        "user_agent": message.get("user_agent"),
        "occurred_at": _parse_ts(message["occurred_at"], "occurred_at"),
        "click": message.get("click") if isinstance(message.get("click"), dict) else {},
        "signals": message.get("signals") if isinstance(message.get("signals"), dict) else {},
        "click_bucket": message.get("click_bucket"),
        "raw": message.get("raw"),
    }


def to_session_doc(message: Dict[str, Any]) -> Dict[str, Any]:
    """Shape read by _extract_session_history_row (mirrors intent_sessions.session_history).
    source_updated_at is passed as updatedAt because the extractor uses it as the row's
    trailing version value, which the worker's guarded upsert then compares."""
    return {
        "session_id": message["session_id"],
        "actor_id": message["actor_id"],
        "session_start": _parse_ts(message["session_start"], "session_start"),
        "session_end": _optional_ts(message.get("session_end"), "session_end"),
        "session_time_spent": message.get("session_time_spent"),
        "events_seq": message["events_seq"],
        "updatedAt": _parse_ts(message["source_updated_at"], "source_updated_at"),
    }


def collect_by_type(messages: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped = {TYPE_EVENT: [], TYPE_CLICK: [], TYPE_SESSION_SNAPSHOT: []}
    for message in messages:
        grouped[message["type"]].append(message)
    return grouped
