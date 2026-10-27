"""
UTM parameters of an event's page URL, for the utm_* columns of behavioral_events and click_events.

Pure and forgiving: it never raises. A missing, empty or malformed URL, or a URL with no UTM parameters,
gives five Nones. Parameter names are matched case-insensitively, values are URL-decoded, the first value
of a repeated parameter wins, and a value is stored as sent (trimmed, not lower-cased) up to the column
width.
"""

from typing import Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

UTM_COLUMNS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
UTM_MAX_LEN = 255   # the column width: VARCHAR(255)

UtmValues = Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]
_EMPTY: UtmValues = (None, None, None, None, None)


def _pairs(url: str):
    """The query parameters of the URL, from the query string and, for single-page apps that put it there,
    from a query that sits after the '#'."""
    parts = urlsplit(url)
    yield from parse_qsl(parts.query, keep_blank_values=True)
    fragment = parts.fragment
    if "?" in fragment:
        yield from parse_qsl(fragment.split("?", 1)[1], keep_blank_values=True)


def extract_utm(url) -> UtmValues:
    """(utm_source, utm_medium, utm_campaign, utm_term, utm_content) found in `url`."""
    if not isinstance(url, str) or "utm_" not in url.lower():
        return _EMPTY
    try:
        found = {}
        for name, value in _pairs(url.strip()):
            key = name.strip().lower()
            if key in UTM_COLUMNS and key not in found:
                cleaned = value.strip()
                found[key] = cleaned[:UTM_MAX_LEN] if cleaned else None
        return tuple(found.get(column) for column in UTM_COLUMNS)  # type: ignore[return-value]
    except Exception:  # a URL the parser rejects is simply "no UTM"
        return _EMPTY
