"""
2-hour scoring job orchestration: load closed eligible sessions, enrich with
page/entry-page/user-agent info via one batched join, apply traffic-quality
filters, score, resolve that session's date's already-persisted thresholds
(never recompute them here), bucket, bulk-write, and return the affected
actor_ids for the caller to recompute.
"""

import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set

from pipeline.state import logger
from intent_engine.config import SCORE_VERSION
from intent_engine.url_normalizer import normalize_page_path
from intent_engine.page_classifier import classify_page_type
from intent_engine.traffic_quality import is_bot, is_zero_signal
from intent_engine.feature_calculator import extract_features
from intent_engine.penalties import struggle_penalty, bounce_penalty
from intent_engine.page_modifier import highest_intent_page_type, modifier_for
from intent_engine.score_calculator import compute_predictive_score, assign_bucket
from intent_engine.overrides import apply_hard_overrides
from intent_engine.threshold_resolution import get_effective_thresholds
from intent_engine.repository import (
    fetch_eligible_sessions,
    fetch_page_viewed_enrichment,
    fetch_click_user_agents,
    bulk_update_scored_sessions,
    bulk_update_excluded_sessions,
)


class _SessionEnrichment:
    __slots__ = ("entry_page_type", "page_types_seen", "user_agent")

    def __init__(self, entry_page_type: str, page_types_seen: Set[str], user_agent: Any) -> None:
        self.entry_page_type = entry_page_type
        self.page_types_seen = page_types_seen
        self.user_agent = user_agent


def _build_enrichment(cursor, session_ids: List[str]) -> Dict[str, _SessionEnrichment]:
    page_rows = fetch_page_viewed_enrichment(cursor, session_ids)

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in page_rows:
        grouped[row["session_id"]].append(row)

    enrichment: Dict[str, _SessionEnrichment] = {}
    missing_ua_session_ids: List[str] = []

    for session_id in session_ids:
        rows = grouped.get(session_id)
        if not rows:
            enrichment[session_id] = _SessionEnrichment("other", set(), None)
            missing_ua_session_ids.append(session_id)
            continue

        rows_sorted = sorted(rows, key=lambda r: r["occurred_at"])
        entry_url = rows_sorted[0]["url"]
        entry_page_type = classify_page_type(normalize_page_path(entry_url))

        page_types_seen = {
            classify_page_type(normalize_page_path(r["url"])) for r in rows
        }
        user_agent = next((r["user_agent"] for r in rows if r["user_agent"]), None)
        if user_agent is None:
            missing_ua_session_ids.append(session_id)

        enrichment[session_id] = _SessionEnrichment(entry_page_type, page_types_seen, user_agent)

    if missing_ua_session_ids:
        click_uas = fetch_click_user_agents(cursor, missing_ua_session_ids)
        for session_id, ua in click_uas.items():
            enrichment[session_id].user_agent = ua

    return enrichment


def score_window_for_brand(
    cursor, connection, window_start: datetime, window_end: datetime, now: datetime,
    read_cursor=None,
) -> Set[str]:
    """
    Runs the full 2-hour scoring pass for one brand's already-open
    cursor/connection over [window_start, window_end). Returns the set of
    actor_ids touched (for the caller to pass to actor_scorer.recompute_actors).

    `read_cursor` (optional) serves the pure reads: the eligible sessions, their event
    enrichment and the thresholds. It may be a read replica. Writes always use `cursor`.
    Without it every query uses `cursor`, as before.
    """
    started_at = time.monotonic()
    reader = read_cursor if read_cursor is not None else cursor
    page_size = _window_page_size()

    # The window is read and scored one page at a time (oldest first) instead of loading every
    # eligible session of the window into memory. Scoring is per session and the touched actors are
    # recomputed afterwards by the caller, so the result is the same as one big pass.
    touched: Set[str] = set()
    after = None
    total = 0
    while True:
        sessions = fetch_eligible_sessions(reader, window_start, window_end, page_size, after)
        if not sessions:
            break
        total += len(sessions)
        touched |= score_sessions(
            cursor, connection, sessions, now, read_cursor=read_cursor,
            label=f"range={window_start}..{window_end} page_end={sessions[-1].get('updated_at')}",
            started_at=time.monotonic(),
        )
        if len(sessions) < page_size:
            break
        last = sessions[-1]
        after = (last["updated_at"], last["session_id"])

    if not total:
        logger.info(
            "[intent scoring] range=%s..%s: no closed eligible sessions", window_start, window_end
        )
    return touched


def _window_page_size() -> int:
    """INTENT_SCORING_WINDOW_CHUNK: sessions read and scored per page of the scoring window."""
    try:
        return max(100, int(os.environ.get("INTENT_SCORING_WINDOW_CHUNK", "2000")))
    except ValueError:
        return 2000


def score_sessions(
    cursor, connection, sessions: List[Dict[str, Any]], now: datetime, read_cursor=None,
    label: str = "", started_at: float = None,
) -> Set[str]:
    """
    Scores an explicit list of eligible sessions (rows shaped like fetch_eligible_sessions
    returns) and writes the results through `cursor`. Used by the windowed scoring pass and
    by the startup catch-up of pending sessions. Pure reads use `read_cursor` when given.
    Returns the actor_ids touched, for actor_scorer.recompute_actors.
    """
    started_at = started_at if started_at is not None else time.monotonic()
    reader = read_cursor if read_cursor is not None else cursor
    session_ids = [s["session_id"] for s in sessions]
    enrichment = _build_enrichment(reader, session_ids)

    excluded_rows = []
    scorable_sessions = []
    for session in sessions:
        session_id = session["session_id"]
        enr = enrichment[session_id]

        if is_bot(enr.user_agent):
            excluded_rows.append(("bot", now, SCORE_VERSION, session_id))
            continue
        if is_zero_signal(session):
            excluded_rows.append(("zero_signal", now, SCORE_VERSION, session_id))
            continue

        scorable_sessions.append(session)

    session_dates = list({s["session_start"].date() for s in scorable_sessions})
    effective_thresholds = get_effective_thresholds(reader, session_dates)

    scored_rows = []
    affected_actor_ids: Set[str] = set()

    for session in scorable_sessions:
        session_id = session["session_id"]
        enr = enrichment[session_id]
        session_date = session["session_start"].date()
        thresholds = effective_thresholds[session_date]

        features = extract_features(session)
        struggle = struggle_penalty(session, thresholds.p90_dead_click_rate)
        bounce = bounce_penalty(session)
        page_type_for_modifier = highest_intent_page_type(enr.page_types_seen)
        modifier = modifier_for(page_type_for_modifier)

        predictive_score = compute_predictive_score(features, struggle, bounce, modifier)
        predictive_bucket = assign_bucket(predictive_score, thresholds.p35, thresholds.p72)
        operational_bucket = apply_hard_overrides(
            predictive_bucket, enr.entry_page_type,
            session.get("checkout_started_count") or 0,
            session.get("add_to_cart_count") or 0,
        )

        scored_rows.append((
            round(predictive_score, 2), predictive_bucket, operational_bucket,
            enr.entry_page_type, "scored", now, SCORE_VERSION, session_id,
        ))
        affected_actor_ids.add(session["actor_id"])

    # excluded_rows are (reason, now, version, session_id) tuples with no
    # actor_id carried - recover it from the original session list so an
    # excluded session (possibly an actor's only session) still triggers
    # that actor's recompute to NULL/None.
    excluded_session_ids = {row[3] for row in excluded_rows}
    for session in sessions:
        if session["session_id"] in excluded_session_ids:
            affected_actor_ids.add(session["actor_id"])

    upserted = bulk_update_scored_sessions(cursor, connection, scored_rows)
    excluded_count = bulk_update_excluded_sessions(cursor, connection, excluded_rows)

    duration = time.monotonic() - started_at
    logger.info(
        "[intent scoring] %s duration=%.2fs sessions_loaded=%s "
        "scored=%s excluded=%s low=%s medium=%s high=%s score_version=%s",
        label, duration, len(sessions), upserted, excluded_count,
        sum(1 for r in scored_rows if r[1] == "low"),
        sum(1 for r in scored_rows if r[1] == "medium"),
        sum(1 for r in scored_rows if r[1] == "high"),
        SCORE_VERSION,
    )

    return affected_actor_ids
