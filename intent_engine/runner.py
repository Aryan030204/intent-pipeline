"""
The two APScheduler entrypoints registered in aws_background.py:

    run_intent_scoring_pipeline()      - every 2 hours
    run_intent_calibration_pipeline()  - once daily

Both re-discover brands the same way the existing ingestion job does, then
iterate INTENT_DB_MAP-mapped brands via a ThreadPoolExecutor capped at
MAX_CONCURRENT_BRANDS, each brand independently try/excepted - the same
per-brand-isolation shape already used by pipeline/orchestration.py's
run_data_pipeline and pipeline/rollups.py's run_rollups_for_brand.

Neither function is called anywhere inside pipeline/orchestration.py's
15-minute ingestion tick - both run on their own independent schedule,
which is why this engine needs no hook there at all.
"""

import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from pipeline.state import IST, logger, active_brand_indices
from pipeline.db import (
    get_db_cursor, get_db_read_cursor, get_pipeline_metadata_timestamp, update_pipeline_metadata_timestamp,
)
from pipeline.intent_events import _parse_intent_db_map
from pipeline.orchestration import _resolve_brand_index_by_db_database

from intent_engine.config import (
    SCORING_METADATA_KEY, SCORING_OVERLAP, SCORING_DEFAULT_LOOKBACK,
    CALIBRATION_LOOKBACK_DAYS,
)
from intent_engine.db_clock import db_clock_offset, read_db_now, to_db_clock
from intent_engine.repository import (
    _ensure_scoring_schema, fetch_last_processed_updated_at, fetch_pending_sessions,
)
from intent_engine.session_scorer import score_sessions, score_window_for_brand
from intent_engine.threshold_calibration import calibrate_date_for_brand
from intent_engine.actor_scorer import recompute_actors

# aws_background owns brand-config discovery (initialize_brand_configs) and
# MAX_CONCURRENT_BRANDS-adjacent settings. Imported at module level is safe:
# attribute access only happens when these functions are actually CALLED,
# during real execution, long after aws_background.py has fully loaded -
# same pattern pipeline/orchestration.py and pipeline/rollups.py already use.
import aws_background


def _mapped_brands():
    """
    Yields (brand_index, brand_label) for every INTENT_DB_MAP entry that
    resolves to a currently-active brand - the same set the ingestion
    workers and the rollup layer operate over.
    """
    intent_db_map = _parse_intent_db_map()
    for mongo_brand_id, db_database_value in intent_db_map.items():
        brand_index = _resolve_brand_index_by_db_database(db_database_value)
        if brand_index is None:
            continue
        yield brand_index, db_database_value


def _run_scoring_for_brand(brand_index: int, brand_label: str) -> None:
    with get_db_cursor(brand_index) as (cursor, connection):
        _ensure_scoring_schema(brand_index, cursor, connection)

        now = datetime.now(IST)
        stored_watermark = get_pipeline_metadata_timestamp(cursor, SCORING_METADATA_KEY)
        window_start = (
            stored_watermark - SCORING_OVERLAP if stored_watermark is not None
            else now - SCORING_DEFAULT_LOOKBACK
        )

        # `window_start`/`now` are IST wall-clock times, but intent_sessions.updated_at is
        # stamped by the database server's own clock (UTC on RDS). Express the window in that
        # clock, or it lands hours in the future and matches nothing (see db_clock.py).
        db_now = read_db_now(cursor)
        offset = db_clock_offset(db_now, now) if db_now is not None else timedelta(0)
        db_window_start, db_window_end = to_db_clock(window_start, offset), to_db_clock(now, offset)
        logger.info(
            "[intent scoring] brand=%s window ist=%s..%s db_clock=%s..%s (db offset %s)",
            brand_label, window_start.strftime("%Y-%m-%d %H:%M:%S"), now.strftime("%Y-%m-%d %H:%M:%S"),
            db_window_start, db_window_end, offset,
        )

        # The pure reads (eligible sessions, enrichment, thresholds) go to the read replica when
        # DB_READ_REPLICA_HOST is set; every write stays on the primary `cursor`. A session the
        # replica has not received yet is picked up by the next run (SCORING_OVERLAP).
        with get_db_read_cursor(brand_index) as read_cursor:
            affected_actor_ids = score_window_for_brand(
                cursor, connection, window_start=db_window_start, window_end=db_window_end, now=now,
                read_cursor=read_cursor,
            )

        if affected_actor_ids:
            recompute_actors(cursor, connection, affected_actor_ids, now)

        update_pipeline_metadata_timestamp(cursor, connection, SCORING_METADATA_KEY, now)


def run_intent_scoring_pipeline() -> None:
    run_started_at = time.monotonic()

    active_brand_indices.clear()
    aws_background.initialize_brand_configs()

    brands = list(_mapped_brands())
    if not brands:
        logger.info("[intent scoring] no INTENT_DB_MAP-mapped brands found. Nothing to do.")
        return

    max_concurrent_brands = int(os.environ.get("MAX_CONCURRENT_BRANDS", "3"))
    max_workers = max(1, min(len(brands), max_concurrent_brands))
    logger.info(
        "[intent scoring] starting run for %s brand(s) with %s parallel worker(s)",
        len(brands), max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_scoring_for_brand, brand_index, brand_label): brand_label
            for brand_index, brand_label in brands
        }
        for fut in as_completed(futures):
            brand_label = futures[fut]
            try:
                fut.result()
            except Exception as e:
                logger.error("[intent scoring] brand=%s failed: %s", brand_label, e)
                traceback.print_exc()

    logger.info(
        "[intent scoring] run complete in %.2fs", time.monotonic() - run_started_at
    )


def _catchup_chunk_size() -> int:
    raw = os.environ.get("INTENT_SCORING_CATCHUP_CHUNK", "").strip()
    try:
        return max(1, int(raw)) if raw else 2000
    except ValueError:
        return 2000


def _catch_up_brand(brand_index: int, brand_label: str) -> int:
    """Scores the sessions still `pending` that were closed after the last session the engine
    processed. Works in chunks of INTENT_SCORING_CATCHUP_CHUNK sessions, oldest first, and
    recomputes the touched actors after each chunk, so a restart part-way loses nothing.
    Returns the number of sessions it scored or excluded."""
    chunk_size = _catchup_chunk_size()
    with get_db_cursor(brand_index) as (cursor, connection):
        _ensure_scoring_schema(brand_index, cursor, connection)

        now = datetime.now(IST)
        db_now = read_db_now(cursor)
        offset = db_clock_offset(db_now, now) if db_now is not None else timedelta(0)
        window_end = to_db_clock(now, offset)

        last_processed = fetch_last_processed_updated_at(cursor)
        if last_processed is not None:
            window_start = last_processed - SCORING_OVERLAP
        else:
            window_start = window_end - SCORING_DEFAULT_LOOKBACK
        logger.info(
            "[intent catchup] brand=%s last processed session updated_at=%s; scoring pending sessions in %s..%s "
            "(database clock, chunk=%s)",
            brand_label, last_processed, window_start, window_end, chunk_size,
        )

        total = 0
        after = None
        with get_db_read_cursor(brand_index) as read_cursor:
            while True:
                sessions = fetch_pending_sessions(read_cursor, window_start, window_end, chunk_size, after)
                if not sessions:
                    break
                affected_actor_ids = score_sessions(
                    cursor, connection, sessions, now, read_cursor=read_cursor,
                    label=f"catchup brand={brand_label} chunk_end={sessions[-1]['updated_at']}",
                )
                if affected_actor_ids:
                    recompute_actors(cursor, connection, affected_actor_ids, now)
                total += len(sessions)
                after = (sessions[-1]["updated_at"], sessions[-1]["session_id"])
                logger.info("[intent catchup] brand=%s processed %s pending sessions so far", brand_label, total)
                if len(sessions) < chunk_size:
                    break

        logger.info("[intent catchup] brand=%s done: %s pending sessions processed", brand_label, total)
        return total


def run_intent_scoring_catchup() -> None:
    """Runs once when the scoring worker starts. For each brand: finds the last session already
    scored (or excluded) in the database, then scores every pending session closed after it.
    Afterwards the worker runs its normal windowed pass and its cron schedule. Set
    INTENT_SCORING_STARTUP_CATCHUP=false to skip it."""
    if os.environ.get("INTENT_SCORING_STARTUP_CATCHUP", "true").strip().lower() == "false":
        logger.info("[intent catchup] disabled by INTENT_SCORING_STARTUP_CATCHUP=false")
        return

    run_started_at = time.monotonic()
    active_brand_indices.clear()
    aws_background.initialize_brand_configs()

    brands = list(_mapped_brands())
    if not brands:
        logger.info("[intent catchup] no INTENT_DB_MAP-mapped brands found. Nothing to do.")
        return

    max_concurrent_brands = int(os.environ.get("MAX_CONCURRENT_BRANDS", "3"))
    max_workers = max(1, min(len(brands), max_concurrent_brands))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_catch_up_brand, brand_index, brand_label): brand_label
            for brand_index, brand_label in brands
        }
        for fut in as_completed(futures):
            brand_label = futures[fut]
            try:
                fut.result()
            except Exception as e:
                logger.error("[intent catchup] brand=%s failed: %s", brand_label, e)
                traceback.print_exc()

    logger.info("[intent catchup] complete in %.2fs", time.monotonic() - run_started_at)


def _run_calibration_for_brand(brand_index: int, brand_label: str, target_date) -> None:
    with get_db_cursor(brand_index) as (cursor, connection):
        _ensure_scoring_schema(brand_index, cursor, connection)
        now = datetime.now(IST)
        with get_db_read_cursor(brand_index) as read_cursor:
            affected_actor_ids = calibrate_date_for_brand(
                cursor, connection, target_date, now, read_cursor=read_cursor,
            )
        if affected_actor_ids:
            recompute_actors(cursor, connection, affected_actor_ids, now)


def run_intent_calibration_pipeline() -> None:
    """
    Always targets a single date: yesterday, IST, relative to when this
    fires. Never revisits older dates - see threshold_calibration.py and
    the plan's "calibrate once, the day after, then read-only" policy.
    """
    run_started_at = time.monotonic()

    active_brand_indices.clear()
    aws_background.initialize_brand_configs()

    brands = list(_mapped_brands())
    if not brands:
        logger.info("[intent calibration] no INTENT_DB_MAP-mapped brands found. Nothing to do.")
        return

    target_date = (datetime.now(IST) - timedelta(days=CALIBRATION_LOOKBACK_DAYS)).date()
    max_concurrent_brands = int(os.environ.get("MAX_CONCURRENT_BRANDS", "3"))
    max_workers = max(1, min(len(brands), max_concurrent_brands))
    logger.info(
        "[intent calibration] starting run for date=%s across %s brand(s) with %s parallel worker(s)",
        target_date, len(brands), max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_calibration_for_brand, brand_index, brand_label, target_date): brand_label
            for brand_index, brand_label in brands
        }
        for fut in as_completed(futures):
            brand_label = futures[fut]
            try:
                fut.result()
            except Exception as e:
                logger.error("[intent calibration] brand=%s failed: %s", brand_label, e)
                traceback.print_exc()

    logger.info(
        "[intent calibration] run complete in %.2fs", time.monotonic() - run_started_at
    )
