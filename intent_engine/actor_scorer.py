"""
Best-session actor recompute, shared identically by the 2-hour scoring job
(actors touched by newly-scored sessions) and the daily calibration job
(actors with any scored session on the calibrated date, since calibration
can reclassify existing sessions' buckets without any new session activity).
One implementation, two callers - never a running max, always a full
recompute from intent_sessions.
"""

from datetime import datetime
from typing import List, Sequence

from intent_engine.config import SCORE_VERSION
from intent_engine.repository import fetch_scored_sessions_for_actors, bulk_upsert_actors


def recompute_actors(cursor, connection, actor_ids: Sequence[str], scored_at: datetime) -> int:
    """
    For each actor_id in actor_ids: picks the scored session with the
    highest predictive_score as "best" (predictive_score is monotonic with
    intent_bucket, so max score = best bucket), counts their scored
    sessions, and bulk-upserts intent_actors. An actor with zero scored
    sessions (all excluded, or none found) is upserted with
    predictive_score=NULL, intent_bucket=NULL - never defaulted to "low".
    """
    unique_actor_ids = sorted(set(a for a in actor_ids if a))
    if not unique_actor_ids:
        return 0

    session_rows = fetch_scored_sessions_for_actors(cursor, unique_actor_ids)

    best_by_actor = {}
    count_by_actor = {}
    for row in session_rows:
        actor_id = row["actor_id"]
        count_by_actor[actor_id] = count_by_actor.get(actor_id, 0) + 1
        # session_rows is ORDER BY actor_id, predictive_score DESC, so the
        # first row seen per actor_id is already their best session.
        if actor_id not in best_by_actor:
            best_by_actor[actor_id] = row

    rows: List[tuple] = []
    for actor_id in unique_actor_ids:
        best = best_by_actor.get(actor_id)
        session_count = count_by_actor.get(actor_id, 0)
        if best is None:
            rows.append((
                actor_id, None, None, None, None, session_count, scored_at, SCORE_VERSION,
            ))
        else:
            rows.append((
                actor_id,
                best["predictive_score"],
                best["intent_bucket"],
                best["session_id"],
                best["predictive_score"],
                session_count,
                scored_at,
                SCORE_VERSION,
            ))

    return bulk_upsert_actors(cursor, connection, rows)
