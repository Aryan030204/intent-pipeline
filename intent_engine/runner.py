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

import contextlib
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
    CALIBRATION_LOOKBACK_DAYS, SCORING_PENDING_LOOKBACK_DAYS, SCORING_DRAIN_BUDGET_S, SCORING_PENDING_COUNT_CAP,
)
from intent_engine.db_clock import db_clock_offset, read_db_now, to_db_clock
from intent_engine.repository import (
    _ensure_scoring_schema, count_pending_sessions, fetch_pending_sessions_by_start, updated_at_index_present,
)
from intent_engine.session_scorer import score_sessions_isolated, score_window_for_brand
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


@contextlib.contextmanager
def _brand_lock(cursor, brand_index: int):
    """A MySQL named lock per brand database, taken without waiting, so the windowed pass, the startup catch-up and
    the recurring drain (and a second container, should one ever run) never score the same brand at the same time.
    Yields whether the lock was obtained. The server also releases it if the connection dies."""
    name = f"intent-scoring:{brand_index}"
    got = False
    try:
        cursor.execute("SELECT GET_LOCK(%s, 0) AS got", (name,))
        row = cursor.fetchall()[0]
        got = (row["got"] if isinstance(row, dict) else row[0]) == 1
    except Exception as exc:    # no lock service: carry on rather than stop scoring
        logger.warning("[intent scoring] category=scoring_lock_unavailable brand_index=%s %s: %s",
                       brand_index, type(exc).__name__, exc)
        got = True
        name = None
    try:
        yield got
    finally:
        if got and name:
            try:
                cursor.execute("SELECT RELEASE_LOCK(%s) AS released", (name,))
                cursor.fetchall()
            except Exception:
                pass


def _run_scoring_for_brand(brand_index: int, brand_label: str) -> None:
    with get_db_cursor(brand_index) as (cursor, connection):
        _ensure_scoring_schema(brand_index, cursor, connection)
        with _brand_lock(cursor, brand_index) as acquired:
            if not acquired:
                logger.info("[intent scoring] brand=%s skipped: another scoring run holds the lock "
                            "(the watermark is untouched, the next run covers this window)", brand_label)
                return
            _run_scoring_window(brand_index, brand_label, cursor, connection)


def _run_scoring_window(brand_index: int, brand_label: str, cursor, connection) -> None:
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


def _env_number(name: str, default: float, minimum: float = 0) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        return max(minimum, float(raw)) if raw else default
    except ValueError:
        return default


def _drain_budget_s() -> float:
    """INTENT_SCORING_CATCHUP_BUDGET_S: seconds one drain run may work per brand (0 = no limit)."""
    return _env_number("INTENT_SCORING_CATCHUP_BUDGET_S", SCORING_DRAIN_BUDGET_S)


def _catch_up_brand(brand_index: int, brand_label: str, lookback_days: float = None) -> int:
    """Scores the sessions still `pending` that started in the last `lookback_days` days (default
    INTENT_SCORING_PENDING_LOOKBACK_DAYS, 45), NEWEST first, in chunks of INTENT_SCORING_CATCHUP_CHUNK sessions.

    - Newest first, so today's sessions are scored before the old backlog: the Score view is usable at once.
    - It looks at the status of the sessions, not at "everything after the last scored one", so a scoring pass
      that scored recent sessions while older ones were still pending can no longer make those older ones
      invisible to it.
    - Pages by (session_start, id) on the index that exists; it needs no updated_at index.
    - One session that cannot be scored is skipped (and logged) instead of stopping the chunk.
    - Stops after INTENT_SCORING_CATCHUP_BUDGET_S seconds so it never holds the scoring lock for hours; the
      recurring drain carries on.
    - Takes the per-brand scoring lock, so it never overlaps the windowed pass.
    Actors are recomputed after each chunk, so a restart part-way loses nothing. Returns the number of sessions
    it scored or excluded."""
    chunk_size = _catchup_chunk_size()
    budget_s = _drain_budget_s()
    days = lookback_days if lookback_days is not None else _env_number(
        "INTENT_SCORING_PENDING_LOOKBACK_DAYS", SCORING_PENDING_LOOKBACK_DAYS, minimum=1)
    with get_db_cursor(brand_index) as (cursor, connection):
        _ensure_scoring_schema(brand_index, cursor, connection)
        with _brand_lock(cursor, brand_index) as acquired:
            if not acquired:
                logger.info("[intent catchup] brand=%s skipped: another scoring run holds the lock", brand_label)
                return 0
            return _drain_pending(brand_index, brand_label, cursor, connection, chunk_size, budget_s, days)


def _drain_pending(brand_index, brand_label, cursor, connection, chunk_size, budget_s, lookback_days) -> int:
    started = time.monotonic()
    now = datetime.now(IST)
    # session_start is the store-local wall clock (IST), like `now`, so the lookback needs no database-clock shift.
    since = (now - timedelta(days=lookback_days)).replace(tzinfo=None)
    logger.info("[intent catchup] brand=%s draining pending sessions that started since %s (newest first, chunk=%s, "
                "budget=%ss)", brand_label, since, chunk_size, int(budget_s) if budget_s else "none")

    total = scored = excluded = failed = 0
    before = None
    stopped = "drained"
    with get_db_read_cursor(brand_index) as read_cursor:
        try:
            if not updated_at_index_present(read_cursor):
                logger.warning(
                    "[intent scoring] category=scoring_index_missing brand=%s intent_sessions has no index on updated_at; "
                    "the windowed scoring pass scans the whole table without it. Run: ALTER TABLE intent_sessions "
                    "ADD KEY idx_sessions_updated_at (updated_at)", brand_label)
        except Exception as exc:    # a read-only metadata check must never stop scoring
            logger.debug("[intent scoring] index check failed: %s", exc)

        while True:
            if budget_s and time.monotonic() - started >= budget_s:
                stopped = "budget"
                break
            sessions = fetch_pending_sessions_by_start(read_cursor, since, chunk_size, before)
            if not sessions:
                break
            batch_started = time.monotonic()
            stats = {}
            actors, failed_ids = score_sessions_isolated(
                cursor, connection, sessions, now, read_cursor=read_cursor, stats=stats,
                label=f"catchup brand={brand_label} chunk_start={sessions[0]['session_start']}",
            )
            if actors:
                recompute_actors(cursor, connection, actors, now)
            batch_s = time.monotonic() - batch_started
            total += len(sessions)
            scored += stats.get("scored", 0)
            excluded += stats.get("excluded", 0)
            failed += len(failed_ids)
            logger.info(
                "[intent scoring] category=scoring_batch brand=%s scoring_batch_size=%s scoring_duration_ms=%s "
                "sessions_scored=%s sessions_excluded=%s sessions_failed=%s sessions_per_second=%.1f",
                brand_label, len(sessions), int(batch_s * 1000), stats.get("scored", 0), stats.get("excluded", 0),
                len(failed_ids), (len(sessions) / batch_s) if batch_s > 0 else 0.0,
            )
            last = sessions[-1]
            before = (last["session_start"], last["id"])
            if len(sessions) < chunk_size:
                break

        # Remaining backlog: known to be 0 (bar the failed ones) when the queue ran dry; counted, up to a cap, only
        # when the budget cut the run short, where many rows match and the bounded count stops early.
        if stopped == "budget":
            try:
                pending = count_pending_sessions(read_cursor, since, SCORING_PENDING_COUNT_CAP)
            except Exception:
                pending = None
        else:
            pending = failed

    elapsed = time.monotonic() - started
    logger.info(
        "[intent catchup] category=scoring_drain_summary brand=%s stopped=%s sessions_processed=%s sessions_scored=%s "
        "sessions_excluded=%s sessions_failed=%s sessions_pending=%s%s elapsed_s=%.1f sessions_per_second=%.1f",
        brand_label, stopped, total, scored, excluded, failed,
        pending if pending is not None else "unknown",
        f"(capped at {SCORING_PENDING_COUNT_CAP})" if pending is not None and pending >= SCORING_PENDING_COUNT_CAP else "",
        elapsed, (total / elapsed) if elapsed > 0 else 0.0,
    )
    return total


def run_intent_scoring_catchup(lookback_days: float = None) -> None:
    """Drains the pending sessions of every brand. Runs shortly after the worker starts (wide lookback) and, via
    run_intent_scoring_drain, on a schedule (short lookback). Set INTENT_SCORING_STARTUP_CATCHUP=false to skip it."""
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
            executor.submit(_catch_up_brand, brand_index, brand_label, lookback_days): brand_label
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


def run_intent_scoring_drain() -> None:
    """The recurring drain: pending sessions of the last INTENT_SCORING_DRAIN_LOOKBACK_DAYS days (default 3), so a
    brand with nothing pending costs one short range read instead of a scan of the whole 45-day startup lookback."""
    run_intent_scoring_catchup(_env_number("INTENT_SCORING_DRAIN_LOOKBACK_DAYS", 3, minimum=1))


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
