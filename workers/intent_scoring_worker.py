"""
Standalone worker: runs the Datum V1 Intent Scoring Engine
(intent_engine/) in its own long-lived process, on its own schedule,
independent of the main ingestion+rollup container (aws_background.py).

Same shape as this repo's other standalone workers would be (and matches
the sibling orders-pipeline repo's workers/intent_events_worker.py
pattern): reuses aws_background.py for brand-config discovery rather than
reimplementing it, and runs its own APScheduler BlockingScheduler instead
of piggy-backing on aws_background.py's own scheduler/Flask app - those
only start when aws_background.py is run directly (`python
aws_background.py`), which this worker never does; it only imports it as a
module.

Two jobs, intentionally on different cadences (see
intent_engine/INTENT_SCORING.md for why they're split):
    - intent scoring:     every 2 hours
    - daily calibration:  once a day, 02:00 IST, targets the prior day
"""

import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# When run directly (`python workers/intent_scoring_worker.py`, which is how
# the Dockerfile/CMD invokes it), Python sets sys.path[0] to this script's
# own directory (workers/), not the repo root - so aws_background.py (at the
# repo root) isn't importable without this. Same pattern every other worker
# in the sibling repo uses to reach aws_background.py.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Imported (not run) - only its module-level brand-config machinery is
# reused (initialize_brand_configs, decrypt_value, etc. via
# intent_engine.runner). Its own __main__ block, scheduler, and Flask app
# never execute here.
import aws_background  # noqa: F401  (see module docstring)

from intent_engine.runner import (
    run_intent_scoring_catchup, run_intent_scoring_pipeline, run_intent_calibration_pipeline,
)


def _run_migrations() -> None:
    """Brings every mapped brand database up to date (migrations/*.sql) before anything reads or writes it.
    Once per container start; safe to run from several containers at once and never raises."""
    try:
        from pipeline.migrations import mapped_brand_indices, run_on_start
        from pipeline.state import active_brand_indices

        active_brand_indices.clear()
        aws_background.initialize_brand_configs()
        run_on_start(mapped_brand_indices())
    except Exception as e:
        logger.error("Error running database migrations on startup: %s", e)


# Scoring runs on even hours at :27 IST (was :00) and calibration at 02:10 (was 02:00): off the ETL's
# :00/:10/... ticks, the rollup's :00/:15/:30/:45 ticks and the midnight job cluster. Scoring only reads
# sessions already written, so the minute does not change what it scores.
SCORING_CRON_MINUTE = 27
CALIBRATION_CRON_MINUTE = 10
STARTUP_DELAY_S = max(0, int(os.environ.get("INTENT_SCORING_STARTUP_DELAY_S", "210") or 0))
_IST = ZoneInfo("Asia/Kolkata")
# One heavy pass at a time inside this process (startup runs, scheduled scoring, calibration).
_heavy_lock = threading.Lock()


def _locked(fn, name, wait=False):
    # Scheduled runs skip when another heavy pass is still running (the next tick catches up); the
    # startup passes wait their turn instead, so the catch-up is never silently dropped.
    if not _heavy_lock.acquire(blocking=wait):
        logger.info("%s skipped: another intent job is still running", name)
        return
    try:
        fn()
    except Exception as e:
        logger.error("Error in %s: %s", name, e)
    finally:
        _heavy_lock.release()


def _scoring_job() -> None:
    _locked(run_intent_scoring_pipeline, "intent scoring")


def _calibration_job() -> None:
    _locked(run_intent_calibration_pipeline, "intent calibration")


def _startup_runs() -> None:
    """Catch-up of pending sessions, then scoring, then calibration - each independently guarded."""
    for fn, name in (
        (run_intent_scoring_catchup, "startup catch-up of pending sessions"),
        (run_intent_scoring_pipeline, "startup run of intent scoring"),
        (run_intent_calibration_pipeline, "startup run of intent calibration"),
    ):
        logger.info("Running %s", name)
        _locked(fn, name, wait=True)


def _run_scheduled() -> None:
    """
    Runs the catch-up, the scoring pipeline and the calibration pipeline once
    shortly after startup (so a redeploy/restart doesn't wait up to 2
    hours - or a full day, for calibration - for fresh results). The startup
    runs wait INTENT_SCORING_STARTUP_DELAY_S (default 210 s) so a host restart
    doesn't start every pipeline's work at the same moment; the schedule below
    is registered first so it is never missed. Then
    schedules both jobs on their own cron triggers from that point on.
    Each immediate run is independently try/excepted so one failing
    doesn't block the other. Both are safe to run redundantly on every
    restart: scoring only ever recomputes sessions in its watermark
    window, and calibration only ever recomputes "yesterday" and always
    overwrites with the same numbers if nothing has changed since the
    last run - see intent_engine/INTENT_SCORING.md.

    Set INTENT_SCORING_WORKER_SELF_RUN=false to instead run the scoring
    pipeline once and exit (e.g. for a one-off/manual invocation).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    _run_migrations()

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    # Catch-up, scoring and calibration, one after the other, once, after the startup delay. A single
    # job (not three) so the three never run at the same time, and it shares the scoring job's lock
    # so a scheduled scoring run cannot overlap it.
    scheduler.add_job(
        _startup_runs,
        "date",
        run_date=datetime.now(_IST) + timedelta(seconds=STARTUP_DELAY_S),
        id="intent_scoring_startup",
        max_instances=1,
    )
    scheduler.add_job(
        _scoring_job,
        "cron",
        hour="*/2",
        minute=SCORING_CRON_MINUTE,
        second=0,
        id="intent_scoring_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        _calibration_job,
        "cron",
        hour=2,
        minute=CALIBRATION_CRON_MINUTE,
        second=0,
        id="intent_calibration_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info(
        "Intent scoring worker scheduler started "
        "(scoring every 2 hours at :%02d, calibration daily at 02:%02d IST, startup runs in %ss)",
        SCORING_CRON_MINUTE, CALIBRATION_CRON_MINUTE, STARTUP_DELAY_S
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Intent scoring worker scheduler stopped")


if __name__ == "__main__":
    self_run = (
        os.environ.get("INTENT_SCORING_WORKER_SELF_RUN", "true").strip().lower() == "true"
    )
    if self_run:
        _run_scheduled()
    else:
        _run_migrations()
        run_intent_scoring_pipeline()
