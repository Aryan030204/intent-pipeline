"""
Per-brand orchestration for the Intent Data Aggregation pipeline: resolves
which of this pipeline's active brands each Mongo brand_id in INTENT_DB_MAP
maps to, runs the three intent syncs (behavioral events, click events,
session history) for that brand, the top-level job runner invoked by
APScheduler and manual triggers (run_data_pipeline), and the Flask routes
(/trigger, /health) - same shape as the reference orders-pipeline
architecture (pipeline/orchestration.py + aws_background.py facade).
"""

import os
import queue
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

from flask import jsonify, request

from pipeline.state import (
    IST,
    logger,
    app,
    scheduler,
    active_brand_indices,
    MANUAL_TRIGGER_QUEUE,
    _db_context,
    _last_logs_handler,
)
from pipeline.db import get_db_cursor, log_db_connection_metrics
from pipeline.intent_events import (
    _parse_intent_db_map,
    sync_intent_events_for_brand,
    sync_click_events_for_brand,
    sync_session_history_for_brand,
)
from pipeline.rollups import run_rollups_for_brand

# aws_background still owns the brand_config_init category (PIPELINE_AUTH_HEADER/
# PIPELINE_ID/SELF_RUN, manager webhook endpoints, get_brand_timezone, now_ist)
# and initialize_brand_configs itself. Imported at module level here is safe:
# attribute access only happens when these functions are actually CALLED,
# during real pipeline execution, long after aws_background.py has fully
# finished loading (same pattern the reference orders pipeline uses).
import aws_background


def _resolve_brand_index_by_db_database(db_database_value: str) -> Optional[int]:
    """
    Case-insensitive match against DB_DATABASE_<i> for every active brand -
    the actual per-brand MySQL database name (set from the brand-config
    source's db_database field during initialize_brand_configs()).
    INTENT_DB_MAP maps a Mongo brand_id straight to that database name, not
    the short brand_tag - so this doesn't reuse brand_tag_to_index_map,
    which is keyed differently.
    """
    target = (db_database_value or "").strip().lower()
    if not target:
        return None
    for brand_index in active_brand_indices:
        candidate = os.environ.get(f"DB_DATABASE_{brand_index}", "")
        if candidate.strip().lower() == target:
            return brand_index
    return None


def _process_mapped_brand(
    mongo_brand_id: str, db_database_value: str, job_id: Optional[str] = None
) -> None:
    _db_context.job_id = job_id
    brand_index = _resolve_brand_index_by_db_database(db_database_value)
    if brand_index is None:
        logger.warning(
            "Skipping intent events for mongo_brand_id=%s: db_database=%s does not "
            "match any active brand's DB_DATABASE_<i>",
            mongo_brand_id,
            db_database_value,
        )
        return

    logger.info(f"\n{'=' * 50}\nSTARTING INTENT SYNC FOR: {db_database_value}\n{'=' * 50}")
    log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_START brand={db_database_value}")

    logger.info(f"Starting behavioral events ingestion for brand={db_database_value}")
    try:
        sync_intent_events_for_brand(
            brand_index=brand_index,
            mongo_brand_id=mongo_brand_id,
            brand_label=db_database_value,
        )
    except Exception as e:
        logger.error(
            "Error running intent events sync for mongo_brand_id=%s db_database=%s: %s",
            mongo_brand_id,
            db_database_value,
            e,
        )

    # Independent try/except: a failure ingesting click events must not
    # block behavioral-event ingestion for this brand, or vice versa.
    logger.info(f"Starting click events ingestion for brand={db_database_value}")
    try:
        sync_click_events_for_brand(
            brand_index=brand_index,
            mongo_brand_id=mongo_brand_id,
            brand_label=db_database_value,
        )
    except Exception as e:
        logger.error(
            "Error running click events sync for mongo_brand_id=%s db_database=%s: %s",
            mongo_brand_id,
            db_database_value,
            e,
        )

    # Independent try/except: a failure ingesting session history must not
    # block either of the other two syncs, or vice versa.
    logger.info(f"Starting session history ingestion for brand={db_database_value}")
    try:
        sync_session_history_for_brand(
            brand_index=brand_index,
            mongo_brand_id=mongo_brand_id,
            brand_label=db_database_value,
        )
    except Exception as e:
        logger.error(
            "Error running session history sync for mongo_brand_id=%s db_database=%s: %s",
            mongo_brand_id,
            db_database_value,
            e,
        )

    # Independent try/except: rollup failures must not affect the ingestion
    # syncs' own reported success/failure above, and ingestion failures
    # above must not prevent rollups from running against whatever raw
    # data is already committed.
    logger.info(f"Starting analytical rollups for brand={db_database_value}")
    try:
        run_rollups_for_brand(
            brand_index=brand_index,
            brand_label=db_database_value,
        )
    except Exception as e:
        logger.error(
            "Error running analytical rollups for mongo_brand_id=%s db_database=%s: %s",
            mongo_brand_id,
            db_database_value,
            e,
        )

    logger.info(f"✅ COMPLETED INTENT SYNC FOR {db_database_value}")
    log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_COMPLETE brand={db_database_value}")
    _db_context.job_id = None


# ---------------------------
# Job runner
# ---------------------------
def run_data_pipeline():
    manual_run_id = None
    succeeded = False
    failure_exception: Optional[Exception] = None
    try:
        manual_run_id = MANUAL_TRIGGER_QUEUE.get_nowait()
    except queue.Empty:
        pass

    job_start_time = aws_background.now_ist()
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    logger.info(
        f"\n{'=' * 60}\nJOB TRIGGERED AT: {job_start_time.strftime('%Y-%m-%d %I:%M:%S %p')}\nJOB ID: {job_id}\nMANUAL TRIGGER: {bool(manual_run_id)}\n{'=' * 60}"
    )

    if manual_run_id and aws_background.MANAGER_STARTED_ENDPOINT:
        try:
            started_payload = {
                "runId": manual_run_id,
                "pipelineId": aws_background.PIPELINE_ID,
                "status": "running",
            }
            logger.info(f"📤 Sending manual trigger STARTED webhook to {aws_background.MANAGER_STARTED_ENDPOINT}")
            aws_background.http_session.post(
                aws_background.MANAGER_STARTED_ENDPOINT,
                json=started_payload,
                headers={"x-pipeline-key": aws_background.PIPELINE_AUTH_HEADER},
                timeout=10,
            )
        except Exception as e:
            logger.error(f"❌ Failed to send STARTED webhook: {e}")

    try:
        # Brand configs (and the active_brand_indices list they populate) are
        # re-discovered on every run, not just at process startup - a brand
        # newly mapped in INTENT_DB_MAP or newly marked active in the
        # brand-config source should be picked up without a restart. Clear
        # first: initialize_brand_configs() only appends, so without this a
        # long-lived process would grow the list unbounded across runs.
        active_brand_indices.clear()
        aws_background.initialize_brand_configs()

        intent_db_map = _parse_intent_db_map()
        if not intent_db_map:
            logger.warning("INTENT_DB_MAP is empty. Nothing to do.")
            succeeded = True
            return

        logger.info(
            f"===== Intent data aggregation run started: {len(intent_db_map)} mapped brand(s) "
            f"{sorted(intent_db_map)} ====="
        )

        max_concurrent_brands = int(os.environ.get("MAX_CONCURRENT_BRANDS", "3"))
        max_workers = max(1, min(len(intent_db_map), max_concurrent_brands))
        logger.info(
            f"Processing {len(intent_db_map)} mapped brand(s) with {max_workers} parallel workers"
            f" (MAX_CONCURRENT_BRANDS={max_concurrent_brands})"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            logger.info(f"🚀 Starting {len(intent_db_map)} brand worker threads...")
            futures = {
                executor.submit(_process_mapped_brand, mongo_brand_id, db_database_value, job_id): (
                    mongo_brand_id,
                    db_database_value,
                )
                for mongo_brand_id, db_database_value in intent_db_map.items()
            }
            for fut in as_completed(futures):
                mongo_brand_id, db_database_value = futures[fut]
                try:
                    fut.result()
                    logger.info(f"✅ Successfully completed brand: {db_database_value}")
                except Exception as e:
                    logger.error(f"❌ Failed processing brand {db_database_value}: {e}")
                    traceback.print_exc()
                finally:
                    log_db_connection_metrics(
                        f"DB_CONNECTION_METRICS_BRAND_FUTURE_DONE brand={db_database_value}"
                    )

        logger.info("🏁 All brand worker threads have returned.")

        end = aws_background.now_ist()
        dur = (end - job_start_time).total_seconds()
        logger.info(
            f"\n{'=' * 60}\nJOB COMPLETED AT: {end.strftime('%Y-%m-%d %I:%M:%S %p')}"
        )
        logger.info(f"Total Duration: {dur:.2f}s ({dur / 60:.2f} minutes)\n{'=' * 60}")

        succeeded = True

    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED with error: {e}")
        traceback.print_exc()
        failure_exception = e
    finally:
        if manual_run_id and succeeded and aws_background.MANAGER_COMPLETED_ENDPOINT:
            try:
                completed_payload = {
                    "runId": manual_run_id,
                    "pipelineId": aws_background.PIPELINE_ID,
                    "status": "completed",
                }
                logger.info(f"📤 Sending manual trigger COMPLETED webhook to {aws_background.MANAGER_COMPLETED_ENDPOINT}")
                aws_background.http_session.post(
                    aws_background.MANAGER_COMPLETED_ENDPOINT,
                    json=completed_payload,
                    headers={"x-pipeline-key": aws_background.PIPELINE_AUTH_HEADER},
                    timeout=10,
                )
            except Exception as e:
                logger.error(f"❌ Failed to send COMPLETED webhook: {e}")

        if manual_run_id and (not succeeded) and aws_background.MANAGER_FAILED_ENDPOINT:
            try:
                recent_logs = _last_logs_handler.get_recent()
                error_body = "\n".join(recent_logs) if recent_logs else "No logs captured."
                error_code = type(failure_exception).__name__ if failure_exception else "UNKNOWN_ERROR"
                failed_payload = {
                    "runId": manual_run_id,
                    "pipelineId": aws_background.PIPELINE_ID,
                    "errorCode": error_code,
                    "errorBody": error_body,
                }
                logger.info(f"📤 Sending manual trigger FAILED webhook to {aws_background.MANAGER_FAILED_ENDPOINT}")
                aws_background.http_session.post(
                    aws_background.MANAGER_FAILED_ENDPOINT,
                    json=failed_payload,
                    headers={"x-pipeline-key": aws_background.PIPELINE_AUTH_HEADER},
                    timeout=10,
                )
            except Exception as e:
                logger.error(f"❌ Failed to send FAILED webhook: {e}")


# ---------------------------
# Flask API Endpoints
# ---------------------------
@app.route("/trigger", methods=["POST"])
def manual_trigger():
    """
    Manual trigger endpoint to run the pipeline immediately.
    Expects 'x-pipeline-key' header for authentication and a JSON body with
    'runId' provided by the pipeline manager.
    """
    auth_key = request.headers.get("x-pipeline-key")
    if not auth_key or auth_key != aws_background.PIPELINE_AUTH_HEADER:
        logger.warning(f"🚫 Unauthorized trigger attempt from {request.remote_addr}")
        return jsonify({"error": "Unauthorized"}), 401

    logger.info("🚀 Manual trigger received via /trigger endpoint")

    request_body = request.get_json(silent=True) or {}
    run_id = request_body.get("runId")
    if not isinstance(run_id, str) or not run_id.strip():
        logger.warning("Manual trigger rejected: missing or invalid runId")
        return jsonify({"status": "error", "message": "runId is required"}), 400

    run_id = run_id.strip()
    logger.info(f"Manual trigger runId received: {run_id}")

    # We use the scheduler to trigger the job immediately.
    # This ensures that APScheduler's concurrency control (max_instances=1) is respected.
    try:
        job = scheduler.get_job("intent_pipeline_job")
        if not job:
            logger.info("Pipeline job not found in scheduler (likely finished previous manual run). Re-adding job.")
            if aws_background.SELF_RUN:
                job = scheduler.add_job(
                    run_data_pipeline,
                    "cron",
                    minute="0,15,30,45",
                    second=0,
                    coalesce=True,
                    max_instances=1,
                    misfire_grace_time=120,
                    replace_existing=True,
                    id="intent_pipeline_job",
                )
            else:
                job = scheduler.add_job(
                    run_data_pipeline,
                    trigger=None,
                    next_run_time=None,
                    coalesce=True,
                    max_instances=1,
                    misfire_grace_time=120,
                    replace_existing=True,
                    id="intent_pipeline_job",
                )

        MANUAL_TRIGGER_QUEUE.put(run_id)
        # Trigger the job to run now
        job.modify(next_run_time=datetime.now(IST))
        return (
            jsonify(
                {"status": "success", "message": "Pipeline triggered immediately", "runId": run_id}
            ),
            200,
        )
    except Exception as e:
        logger.error(f"❌ Error in manual trigger: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now(IST).isoformat()}), 200
