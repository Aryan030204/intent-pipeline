"""
V1 page taxonomy - exactly five types, deterministic ordered rules over an
already-normalized page path (see url_normalizer.py). Intentionally does
NOT implement cart/blog/account/policy/search/bundle_builder/gamified/
static/thankyou/password - those are out of scope for V1 per the spec.
"""

from typing import Optional

PAGE_TYPES = ("home", "collection", "pdp", "checkout", "other")


def classify_page_type(normalized_path: Optional[str]) -> str:
    """
    normalized_path is expected to already be run through
    url_normalizer.normalize_page_path (lowercased, query/fragment
    stripped, leading slash, dynamic segments collapsed to :id).
    """
    if not normalized_path or normalized_path in ("(unknown)", "(invalid)"):
        return "other"

    path = normalized_path

    if path == "/":
        return "home"
    if path.startswith("/products/"):
        return "pdp"
    if path.startswith("/collections/"):
        return "collection"
    if path.startswith("/checkout"):
        return "checkout"

    return "other"
