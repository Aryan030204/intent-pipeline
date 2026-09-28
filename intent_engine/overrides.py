"""
V1 hard overrides: facts beat predictive score. Exactly three independent
conditions (ORed), any one of which forces operational_bucket to "high"
regardless of the predictive intent_bucket. These change ONLY
operational_bucket - they never alter or inflate predictive_score.
"""

from typing import Optional


def apply_hard_overrides(
    intent_bucket: Optional[str],
    entry_page_type: Optional[str],
    checkout_started_count: int,
    add_to_cart_count: int,
) -> Optional[str]:
    if (
        entry_page_type == "checkout"
        or (checkout_started_count or 0) > 0
        or (add_to_cart_count or 0) > 0
    ):
        return "high"
    return intent_bucket
