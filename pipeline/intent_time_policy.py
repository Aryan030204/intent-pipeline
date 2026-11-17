"""
Time rules that keep a client's clock from driving sessionization.

Why this exists. Event times are the store-local wall clock /track derived from the visitor's own device clock. A device
whose clock is hours ahead (BBB sees 100-180 such events a day, and a year-2040 one) writes an actor cursor whose
last_event_at is in the future. The idle-session sweep used the newest cursor time as "now", so one such cursor made every
live session look idle and the sweep deleted the live cursors: the visitor's next event then started a new session
(see docs/INTENT_SESSION_FRAGMENTATION.md).

Two rules, both applied here and nowhere else:

1. normalize_occurred_at: an event more than FUTURE_TOLERANCE_S ahead of the time the server received it takes the receipt
   time instead. The client's own value is never discarded: IntentMessage.client_occurred_at keeps it.
2. effective_latest: the sweep's reference time is capped at the brand's store-local "now".
"""

import logging
from datetime import datetime
from typing import Callable, Optional, Tuple

from pipeline.state import IST, brand_timezones

logger = logging.getLogger(__name__)

# Chosen from BBB 2026-11-12..14 (see the measurement in the report): honest events never run ahead of the server, the
# P99.9 of "client ahead of the write time" is +110..150 s, and 62% of the events that do run ahead are within +5 min
# (a device clock a few minutes fast is harmless: all of an actor's events share the same offset). Beyond 5 minutes the
# tail is a different population (100-180 events a day beyond 30 min, up to 26 h, one in 2040).
FUTURE_TOLERANCE_S = 300


def normalize_occurred_at(
    occurred_at: datetime, received_at: Optional[datetime], tolerance_s: int = FUTURE_TOLERANCE_S
) -> Tuple[datetime, bool]:
    """(time to sessionize with, whether it was replaced). Both datetimes are naive store-local wall clocks.

    Without a receipt time (a message from an older /track) nothing can be judged and the event time is kept; the sweep
    cap below still protects the sessionizer from such an event."""
    if received_at is None:
        return occurred_at, False
    if (occurred_at - received_at).total_seconds() > tolerance_s:
        return received_at, True
    return occurred_at, False


def store_local_now(brand_index: int, clock: Optional[Callable[[], datetime]] = None) -> datetime:
    """The brand's current store-local wall clock as a naive datetime (the convention of every cursor time).

    A brand without a known timezone falls back to IST, the pipeline's own zone, and logs it: refusing to sweep would
    leave sessions open forever, and a wrong zone is still far safer than trusting a cursor time."""
    zone = brand_timezones.get(brand_index)
    if zone is None:
        logger.warning("[intent-kafka] category=sweep_clock_zone_unknown brand_index=%s using IST", brand_index)
        zone = IST
    now = clock() if clock is not None else datetime.now(zone)
    if now.tzinfo is not None:
        now = now.astimezone(zone)
    return now.replace(tzinfo=None)


def effective_latest(latest: Optional[datetime], now: Optional[datetime]) -> Optional[datetime]:
    """The sweep's reference time: the newest cursor time, but never later than real store-local time."""
    if latest is None:
        return None
    if now is None:
        return latest
    return min(latest, now)
