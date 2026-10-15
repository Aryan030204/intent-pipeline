"""
Translates the scoring window from the IST wall clock into the database's own clock.

The scoring job builds its window from `datetime.now(IST)`. The MySQL driver sends only the
wall-clock digits of a datetime, never its timezone. `intent_sessions.updated_at` is stamped
by the database server (`DEFAULT CURRENT_TIMESTAMP`), which on RDS runs in UTC. Compared as
sent, the IST window sits 5h30m ahead of the data it should cover: it contains only times
that have not happened yet in the database's clock, so every run reported "no closed eligible
sessions" and new sessions stayed `pending`.

The offset is measured against the server at run time (`SELECT NOW()`), not hard-coded, so the
job keeps working if the server is ever set to IST or another zone.
"""

from datetime import datetime, timedelta
from typing import Optional

# Real offsets are whole multiples of 15 minutes. Rounding absorbs the few milliseconds
# between reading the IST clock and the server's.
OFFSET_ROUNDING_S = 15 * 60


def db_clock_offset(db_now: datetime, reference_now: datetime) -> timedelta:
    """db_now: the server's NOW() (naive). reference_now: the same instant on the clock the
    window is expressed in (aware or naive; only its wall-clock digits count).
    Returns how far the database's clock is from that clock, rounded to 15 minutes."""
    delta = db_now.replace(tzinfo=None) - reference_now.replace(tzinfo=None)
    rounded = round(delta.total_seconds() / OFFSET_ROUNDING_S) * OFFSET_ROUNDING_S
    return timedelta(seconds=rounded)


def to_db_clock(value: datetime, offset: timedelta) -> datetime:
    """The wall-clock digits the database uses for the same instant `value` denotes."""
    return value.replace(tzinfo=None) + offset


def read_db_now(cursor) -> Optional[datetime]:
    """The server's current NOW() as a naive datetime."""
    cursor.execute("SELECT NOW() AS db_now")
    row = cursor.fetchone()
    if not row:
        return None
    value = row["db_now"] if isinstance(row, dict) else row[0]
    return value.replace(tzinfo=None) if isinstance(value, datetime) else None
