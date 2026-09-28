"""
Page-type score MODIFIER (multiplier applied to the raw+penalty score),
based on the highest-intent page type reached during the session - distinct
from entry_page_type (overrides.py), which is about the literal first page
visited, not the "best" page reached.
"""

from typing import Iterable, Optional

from intent_engine.config import PAGE_TYPE_MODIFIERS, PAGE_TYPE_INTENT_RANK


def highest_intent_page_type(seen_types: Iterable[str]) -> str:
    """
    Returns the highest-ranked page type present in seen_types, per
    PAGE_TYPE_INTENT_RANK (checkout > pdp > collection > home > other).
    Empty input (a session with no page_viewed events at all) defaults to
    "other" - undefined by spec, documented assumption.
    """
    seen = set(seen_types)
    for page_type in PAGE_TYPE_INTENT_RANK:
        if page_type in seen:
            return page_type
    return "other"


def modifier_for(page_type: Optional[str]) -> float:
    return PAGE_TYPE_MODIFIERS.get(page_type or "other", PAGE_TYPE_MODIFIERS["other"])
