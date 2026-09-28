"""
Extracts the five raw scoring features from an intent_sessions row. Only
ever called on sessions already known to have a non-NULL
session_time_spent_ms (the eligibility query filters this upstream), so
there is no NULL-handling branch here - a session with an unknown duration
is never scored at all (stays intent_status='pending'), never scored with a
substituted zero.
"""

from typing import Any, Dict, NamedTuple


class SessionFeatures(NamedTuple):
    product_views: int
    useful_clicks: int
    minutes: float
    scrolls: int
    page_views: int


def extract_features(session_row: Dict[str, Any]) -> SessionFeatures:
    duration_ms = session_row["session_time_spent_ms"]
    if duration_ms is None:
        raise ValueError(
            "extract_features called on a session with NULL "
            "session_time_spent_ms - caller must filter these out upstream"
        )

    return SessionFeatures(
        product_views=session_row.get("product_view_count") or 0,
        useful_clicks=session_row.get("useful_click_count") or 0,
        minutes=duration_ms / 60000,
        scrolls=session_row.get("scroll_count") or 0,
        page_views=session_row.get("page_view_count") or 0,
    )
