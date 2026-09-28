"""
V1 traffic quality filter - exactly two exclusion checks: bot/crawler and
true zero-signal session. Deliberately does NOT implement theme-preview,
password-only, or post-purchase/thank-you exclusion - out of scope for V1.

Both checks are only ever meant to be called on CLOSED sessions (i.e. after
the caller has already filtered to session_time_spent_ms IS NOT NULL) -
this module has no opinion on that filtering itself, it just documents the
precondition.
"""

import re
from typing import Any, Dict, Optional

from intent_engine.config import (
    BOT_UA_SIGNATURES,
    BOT_GENERIC_PATTERN,
    ZERO_SIGNAL_MAX_EVENT_COUNT,
    ZERO_SIGNAL_MAX_DURATION_MS,
)

_BOT_GENERIC_RE = re.compile(BOT_GENERIC_PATTERN, re.IGNORECASE)


def is_bot(user_agent: Optional[str]) -> bool:
    if not user_agent:
        return False
    ua_lower = user_agent.lower()
    if any(sig in ua_lower for sig in BOT_UA_SIGNATURES):
        return True
    return bool(_BOT_GENERIC_RE.search(ua_lower))


def is_zero_signal(session_row: Dict[str, Any]) -> bool:
    """
    All four conditions must hold. session_row is expected to have a
    non-NULL session_time_spent_ms (callers only evaluate this on closed
    sessions) - a session must never be excluded on duration alone.
    """
    event_count = session_row.get("event_count") or 0
    duration_ms = session_row.get("session_time_spent_ms")
    click_count = session_row.get("click_count") or 0
    scroll_count = session_row.get("scroll_count") or 0

    if duration_ms is None:
        return False

    return (
        event_count <= ZERO_SIGNAL_MAX_EVENT_COUNT
        and duration_ms < ZERO_SIGNAL_MAX_DURATION_MS
        and click_count == 0
        and scroll_count == 0
    )
