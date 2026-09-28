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
import time

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

from intent_engine.runner import run_intent_scoring_pipeline, run_intent_calibration_pipeline


def _run_scheduled() -> None:
    """
    Runs the scoring pipeline once immediately on startup (so a
    redeploy/restart doesn't wait up to 2 hours for fresh scores), then
    schedules both jobs on their own cron triggers from that point on.
    Calibration is deliberately NOT run immediately on startup - it's a
    once-daily, date-targeted job; running it redundantly on every restart
    is harmless (idempotent) but unnecessary, so it's left to its cron only.

    Set INTENT_SCORING_WORKER_SELF_RUN=false to instead run the scoring
    pipeline once and exit (e.g. for a one-off/manual invocation).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    logger.info("Running intent scoring pipeline once immediately on startup")
    try:
        run_intent_scoring_pipeline()
    except Exception as e:
        logger.error("Error in immediate startup run of intent scoring pipeline: %s", e)

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_intent_scoring_pipeline,
        "cron",
        hour="*/2",
        minute=0,
        second=0,
        id="intent_scoring_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        run_intent_calibration_pipeline,
        "cron",
        hour=2,
        minute=0,
        second=0,
        id="intent_calibration_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info(
        "Intent scoring worker scheduler started "
        "(scoring every 2 hours, calibration daily at 02:00 IST)"
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
        run_intent_scoring_pipeline()
