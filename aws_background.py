# -*- coding: utf-8 -*-
"""
Intent Data Aggregation Pipeline
(Standalone pipeline extracted from the intent-events sub-worker of the
orders ETL pipeline - same facade architecture as that pipeline's
aws_background.py: brand-config discovery/decryption, SQLAlchemy engine
setup per brand, APScheduler-driven scheduling, and a small Flask app for
manual trigger / health check. The actual sync logic (Mongo -> MySQL for
behavioral events, click events, and session history) lives in
pipeline/intent_events.py and pipeline/orchestration.py, unchanged from
the reference.)
"""

import sys

# When this file is run directly (`python aws_background.py`), Python registers
# it as sys.modules['__main__'], NOT sys.modules['aws_background']. Several
# pipeline/*.py modules do `import aws_background` to reach names that stay in
# this facade (TEST_MODE, get_brand_timezone, etc.), relying on Python finding
# an already-registered 'aws_background' module rather than re-executing this
# file from scratch. Without this alias, direct execution triggers a second,
# independent import of this same file partway through the first one's pipeline
# imports, which fails with a circular-import ImportError. Must run before any
# `from pipeline.X import (...)` that could reach a submodule's `import aws_background`.
sys.modules.setdefault("aws_background", sys.modules[__name__])

from dotenv import load_dotenv

load_dotenv()

import base64
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL
from sqlalchemy.pool import NullPool
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from pipeline.state import (
    IST,
    logger,
    brand_timezones,
    brand_tag_to_index_map,
    brand_id_from_config,
    brand_db_configs,
    sqlalchemy_engines,
    active_brand_indices,
    scheduler,
    app,
    http_session,
)
from pipeline.db import (
    _metric_inc,
    _metric_dec,
    log_db_connection_metrics,
    close_all_database_resources,
)
from pipeline.orchestration import (
    run_data_pipeline,
    manual_trigger,
    health_check,
)


# ---- Globals ----
def parse_store_timezone(tz_str: Optional[str]) -> ZoneInfo:
    """
    Parses timezone strings of the form "(GMT+05:30) Asia/Kolkata" or "Asia/Kolkata".
    Falls back to IST (Asia/Kolkata) if missing or invalid.
    """
    if not tz_str:
        return ZoneInfo("Asia/Kolkata")
    tz_str = tz_str.strip()
    if ") " in tz_str:
        tz_name = tz_str.split(") ", 1)[1].strip()
    else:
        tz_name = tz_str
    try:
        return ZoneInfo(tz_name)
    except Exception as e:
        logger.warning(
            f"⚠️ Could not load timezone '{tz_name}' from raw value '{tz_str}' (error: {e}). "
            "Falling back to Asia/Kolkata."
        )
        return ZoneInfo("Asia/Kolkata")


def get_brand_timezone(brand_index: int) -> ZoneInfo:
    return brand_timezones.get(brand_index, ZoneInfo("Asia/Kolkata"))


def now_ist() -> datetime:
    return datetime.now(IST)


# --- TEST_MODE ---
TEST_MODE = os.environ.get("TEST_MODE", "false").strip().lower() == "true"
if TEST_MODE:
    load_dotenv(".env.local", override=True)
    logger.info(
        "⚠️ TEST_MODE enabled: DB SSL will be DISABLED, loading config from local env (.env.local)."
    )

PIPELINE_AUTH_HEADER = os.environ.get("PIPELINE_AUTH_HEADER", "pipeline@nosecret123")
SELF_RUN = os.environ.get("SELF_RUN", "true").strip().lower() == "true"
MANAGER_STARTED_ENDPOINT = os.environ.get("MANAGER_STARTED_ENDPOINT")
MANAGER_COMPLETED_ENDPOINT = os.environ.get("MANAGER_COMPLETED_ENDPOINT")
MANAGER_FAILED_ENDPOINT = os.environ.get("MANAGER_FAILED_ENDPOINT")
PIPELINE_ID = os.environ.get("PIPELINE_ID", "intent-data-aggregation")


class RDSProxyValidationError(RuntimeError):
    pass


DB_TEST_ON_STARTUP = os.environ.get("DB_TEST_ON_STARTUP", "false").strip().lower() == "true"
REQUIRE_RDS_PROXY = os.environ.get("REQUIRE_RDS_PROXY", "true").strip().lower() == "true"
EXPECTED_RDS_PROXY_HOST = os.environ.get("EXPECTED_RDS_PROXY_HOST", "").strip()


# ---------------------------
# API-Driven Config & Decryption
# ---------------------------
GET_BRANDS_API = os.environ.get("GET_BRANDS_API")
PASSWORD_AES_KEY = os.environ.get("PASSWORD_AES_KEY")
ACTIVE_BRAND_IDS = [
    b.strip() for b in os.environ.get("ACTIVE_BRAND_IDS", "").split(",") if b.strip()
]


def decrypt_value(encrypted_val: str) -> str:
    """
    Decrypts a value that was encrypted using AES-256-CBC.
    Expected format: 'iv_b64:ciphertext_b64'
    """
    if not encrypted_val or ":" not in encrypted_val:
        return encrypted_val

    if not PASSWORD_AES_KEY:
        logger.warning(
            "PASSWORD_AES_KEY not found in environment. Returning raw value."
        )
        return encrypted_val

    try:
        iv_b64, ciphertext_b64 = encrypted_val.split(":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        # AES-256 requires 32-byte key
        key_bytes = PASSWORD_AES_KEY.encode("utf-8")
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b"\0")
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]

        cipher = Cipher(
            algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend()
        )
        decryptor = cipher.decryptor()
        padded_content = decryptor.update(ciphertext) + decryptor.finalize()

        # Unpadding (assuming standard PKCS7 style where the last byte is the padding length)
        padding_len = padded_content[-1]
        if padding_len < 1 or padding_len > 16:
            # Fallback if padding looks invalid
            return padded_content.decode("utf-8", errors="ignore")

        content = padded_content[:-padding_len]
        return content.decode("utf-8")
    except Exception as e:
        logger.error(f"❌ Decryption failed: {e}")
        return encrypted_val


def fetch_active_brands() -> Dict[str, str]:
    """
    Fetch active brand ID-to-name mapping from the brands API via GET.
    Returns dict like {"1": "PTS", "2": "BBB", ...} where keys are brand_ids.
    """
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error(
            "❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set in environment."
        )
        return {}

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(GET_BRANDS_API, headers=headers, timeout=30)
        if resp.status_code == 200:
            active_brands = resp.json()
            if isinstance(active_brands, dict):
                return active_brands
            logger.error(
                f"❌ Active brands API returned unexpected type: {type(active_brands)}"
            )
            return {}
        logger.error(f"❌ Failed to fetch active brands: {resp.status_code}")
        return {}
    except Exception as e:
        logger.error(f"❌ Brand discovery API error: {e}")
        return {}


def fetch_brand_config(brand_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch brand configuration from the tenants API via GET /{brand_id}.
    """
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error(
            "❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set in environment."
        )
        return None

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id.lower()}"

    logger.info(f"📡 Fetching config from: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"❌ Failed to fetch config for {brand_id}: {resp.status_code}")
        logger.error(f"   Response: {resp.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"❌ Config fetch error for {brand_id}: {e}")
        return None


def is_brand_config_active(config: Dict[str, Any]) -> bool:
    return config.get("is_active") is True


def _build_db_config(
    db_host: str,
    db_user: str,
    db_password: str,
    db_port: int,
    db_name: str,
    ca_path: Optional[str],
    verify_cert: bool,
    verify_identity: bool,
) -> Dict[str, Any]:
    effective_verify_identity = verify_identity
    if verify_identity and (
        "elb.amazonaws.com" in db_host
        or ("amazonaws.com" in db_host and "rds.amazonaws.com" not in db_host)
    ):
        effective_verify_identity = False

    config: Dict[str, Any] = {
        "host": db_host,
        "port": db_port,
        "user": db_user,
        "password": db_password,
        "database": db_name,
        "connection_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_S", "10")),
        "read_timeout": int(os.environ.get("DB_READ_TIMEOUT_S", "120")),
        "write_timeout": int(os.environ.get("DB_WRITE_TIMEOUT_S", "120")),
    }

    effective_ca_path = ca_path
    if db_host.lower() in ["localhost", "127.0.0.1"]:
        effective_ca_path = None

    if effective_ca_path:
        config.update(
            {
                "ssl_ca": effective_ca_path,
                "ssl_verify_cert": verify_cert,
                "ssl_verify_identity": effective_verify_identity,
            }
        )

    return config


def _validate_rds_proxy_host(db_host: str, brand_name: str) -> None:
    if TEST_MODE:
        return
    if not REQUIRE_RDS_PROXY:
        return

    host = (db_host or "").strip().lower()
    expected = EXPECTED_RDS_PROXY_HOST.lower()
    if expected:
        if host != expected:
            raise RDSProxyValidationError(
                f"DB host rejected for {brand_name}: expected configured RDS Proxy host"
            )
        return

    if ".proxy-" not in host and "proxy-" not in host:
        raise RDSProxyValidationError(
            f"DB host rejected for {brand_name}: host does not look like an RDS Proxy endpoint"
        )


def _create_sqlalchemy_engine_for_brand(
    brand_idx: int,
    brand_name: str,
    db_config: Dict[str, Any],
) -> Any:
    connect_args = {
        "connection_timeout": db_config.get("connection_timeout", 10),
        "read_timeout": db_config.get("read_timeout", 120),
        "write_timeout": db_config.get("write_timeout", 120),
    }
    for key in ("ssl_ca", "ssl_verify_cert", "ssl_verify_identity"):
        if key in db_config:
            connect_args[key] = db_config[key]

    engine = create_engine(
        URL.create(
            "mysql+mysqlconnector",
            username=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=int(db_config.get("port", 3306)),
            database=db_config["database"],
            query={"charset": "utf8mb4"},
        ),
        connect_args=connect_args,
        poolclass=NullPool,
        pool_pre_ping=True,
        echo=False,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _on_sqlalchemy_connect(dbapi_connection, connection_record):
        _metric_inc("active_sqlalchemy_connections")
        logger.debug(
            "SQLALCHEMY_CONNECTION_OPENED brand=%s brand_index=%s",
            brand_name,
            brand_idx,
        )

    @event.listens_for(engine, "close")
    def _on_sqlalchemy_close(dbapi_connection, connection_record):
        _metric_dec("active_sqlalchemy_connections")
        logger.debug(
            "SQLALCHEMY_CONNECTION_CLOSED brand=%s brand_index=%s",
            brand_name,
            brand_idx,
        )

    return engine


def _replace_sqlalchemy_engine_for_brand(
    brand_idx: int, brand_name: str, db_config: Dict[str, Any]
) -> None:
    old_engine = sqlalchemy_engines.get(brand_idx)
    if old_engine is not None:
        try:
            old_engine.dispose()
        except Exception:
            pass
    sqlalchemy_engines[brand_idx] = _create_sqlalchemy_engine_for_brand(
        brand_idx, brand_name, db_config
    )


def _test_brand_database_connection(brand_idx: int, brand_name: str) -> None:
    from pipeline.db import get_db_connection

    with get_db_connection(brand_idx) as cnx:
        cursor = cnx.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        finally:
            cursor.close()
    engine = sqlalchemy_engines.get(brand_idx)
    if engine is not None:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))


def initialize_brand_configs():
    import tempfile
    import certifi

    def _trunc(val: Any, length: int = 4) -> str:
        s = str(val) if val is not None else ""
        if len(s) <= length:
            return s
        return f"{s[:length]}..."

    logger.info("============================================================")
    logger.info("🔍 INITIALIZING INTENT PIPELINE ENVIRONMENT")
    logger.info(f"   TEST_MODE: {TEST_MODE}")
    logger.info("============================================================")

    # --------------------------
    # Infrastructure Setup Helper
    # --------------------------
    def _setup_brand_infra(
        brand_idx,
        brand_name,
        brand_tag,
        db_host,
        db_user,
        db_password,
        db_port,
        db_name,
        ca_path,
        verify_cert,
        verify_identity,
        store_timezone,
    ):
        logger.info(
            f"🔑 Config for {brand_name} ({'LOCAL' if TEST_MODE else 'API'}): "
            f"host={_trunc(db_host, 12)}, user={_trunc(db_user)}, db={db_name}"
        )

        brand_timezones[brand_idx] = parse_store_timezone(store_timezone)

        _validate_rds_proxy_host(db_host, brand_name)

        db_config = _build_db_config(
            db_host=db_host,
            db_user=db_user,
            db_password=db_password,
            db_port=db_port,
            db_name=db_name,
            ca_path=ca_path,
            verify_cert=verify_cert,
            verify_identity=verify_identity,
        )
        brand_db_configs[brand_idx] = db_config

        try:
            sqlalchemy_engines[brand_idx] = _create_sqlalchemy_engine_for_brand(
                brand_idx, brand_name, db_config
            )
        except Exception as e:
            brand_db_configs.pop(brand_idx, None)
            logger.error(f"❌ Engine setup error for {brand_name}: {e}")
            return

        if DB_TEST_ON_STARTUP:
            try:
                _test_brand_database_connection(brand_idx, brand_name)
            except Exception as e:
                sqlalchemy_engines.pop(brand_idx, None)
                brand_db_configs.pop(brand_idx, None)
                logger.error(f"❌ Startup DB test failed for {brand_name}: {e}")
                return

        os.environ[f"BRAND_NAME_{brand_idx}"] = brand_name
        os.environ[f"BRAND_TAG_{brand_idx}"] = brand_tag
        os.environ[f"DB_DATABASE_{brand_idx}"] = db_name

        brand_id_from_config[brand_idx] = brand_idx
        active_brand_indices.append(brand_idx)

    # --------------------------
    # TLS / CA resolution helpers
    # --------------------------
    def _write_b64_to_tempfile(b64_str: str, prefix: str) -> str:
        data = base64.b64decode(b64_str.encode("utf-8"))
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        return path

    def _resolve_ca_bundle_path() -> str | None:
        if TEST_MODE:
            return None  # No SSL in TEST_MODE
        mode = os.environ.get("DB_TLS_CA_MODE", "certifi").strip().lower()
        if mode == "certifi":
            return certifi.where()
        if mode == "rds":
            p = os.environ.get("RDS_CA_PATH")
            if p and os.path.exists(p):
                return p
            b64v = os.environ.get("RDS_CA_BUNDLE_B64")
            if b64v:
                return _write_b64_to_tempfile(b64v, prefix="rds-ca-")
            return None
        if mode == "custom":
            p = os.environ.get("CUSTOM_CA_PATH")
            if p and os.path.exists(p):
                return p
            b64v = os.environ.get("CUSTOM_CA_BUNDLE_B64")
            if b64v:
                return _write_b64_to_tempfile(b64v, prefix="custom-ca-")
            return None
        if mode == "none":
            return None
        return certifi.where()

    ca_path = _resolve_ca_bundle_path()
    verify_cert = os.environ.get("DB_SSL_VERIFY_CERT", "true").strip().lower() == "true"
    verify_identity = (
        os.environ.get("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"
    )

    if TEST_MODE:
        logger.info("🧪 [TEST_MODE] Loading local config from .env...")

        for idx_str in ACTIVE_BRAND_IDS:
            try:
                i = int(idx_str)
                brand_name = os.environ.get(f"BRAND_NAME_{i}")
                if not brand_name:
                    continue

                brand_idx = i
                brand_tag = os.environ.get(f"BRAND_TAG_{i}", brand_name.lower())
                brand_tag_to_index_map[brand_tag] = brand_idx

                db_host = os.environ.get(f"DB_HOST_{i}")
                db_user = os.environ.get(f"DB_USER_{i}")
                db_password = os.environ.get(f"DB_PASSWORD_{i}")
                db_port = int(os.environ.get(f"DB_PORT_{i}", 3306))
                db_name = os.environ.get(f"DB_DATABASE_{i}")

                if not all([db_host, db_user, db_password, db_name]):
                    logger.error(f"❌ Missing local env vars for brand index {i}")
                    continue

                _setup_brand_infra(
                    brand_idx,
                    brand_name,
                    brand_tag,
                    db_host,
                    db_user,
                    db_password,
                    db_port,
                    db_name,
                    ca_path,
                    verify_cert,
                    verify_identity,
                    os.environ.get(f"STORE_TIMEZONE_{i}"),
                )
            except Exception as e:
                logger.error(f"❌ Local config error for {idx_str}: {e}")
                continue
    else:
        logger.info("📡 [PROD_MODE] Discovering active brands via API...")
        active_brands = fetch_active_brands()
        logger.info(f"🔍 Active brands: {active_brands}")

        for brand_id_key, brand_tag_value in active_brands.items():
            logger.info(
                f"🔍 Fetching API config for brand: {brand_tag_value} (brand_id={brand_id_key})"
            )
            config = fetch_brand_config(brand_id_key)
            if not config:
                continue
            if not is_brand_config_active(config):
                logger.info(
                    "⏭️ Skipping brand %s (brand_id=%s): tenant config is not active",
                    config.get("brand_name", brand_tag_value),
                    brand_id_key,
                )
                continue

            try:
                # Use brand_id from the API response key (authoritative source)
                brand_idx = int(brand_id_key)
                brand_name = config.get("brand_name", brand_tag_value)
                brand_tag = config.get("brand_tag", brand_tag_value.lower())
                brand_tag_to_index_map[brand_tag] = brand_idx

                db_host = config["db_host"]
                db_user = config["db_user"]
                db_password = decrypt_value(config["db_password"])
                db_port = int(config.get("port", 3306))
                db_name = config.get("db_database", brand_tag_value)

                _setup_brand_infra(
                    brand_idx,
                    brand_name,
                    brand_tag,
                    db_host,
                    db_user,
                    db_password,
                    db_port,
                    db_name,
                    ca_path,
                    verify_cert,
                    verify_identity,
                    config.get("store_timezone"),
                )
            except RDSProxyValidationError:
                raise
            except Exception as e:
                logger.error(f"❌ API config error for {brand_tag_value}: {e}")
                continue

    logger.info(f"✅ Active brands initialized: {active_brand_indices}")
    logger.info("Database connection mode: lazy")
    logger.info("SQLAlchemy pool mode: NullPool")
    logger.info(f"Configured brands: {len(active_brand_indices)}")
    logger.info(f"RDS Proxy enforcement: {'enabled' if REQUIRE_RDS_PROXY and not TEST_MODE else 'disabled'}")
    logger.info(f"Startup connection test: {'enabled' if DB_TEST_ON_STARTUP else 'disabled'}")
    log_db_connection_metrics("DB_CONNECTION_METRICS_STARTUP")


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    # Conditionally schedule the pipeline. initialize_brand_configs() itself
    # runs inside run_data_pipeline() on every invocation (not just here at
    # startup) so a long-lived process picks up new/changed brands without a
    # restart - see pipeline/orchestration.py.
    if SELF_RUN:
        # Run once immediately on startup so a redeploy doesn't wait up to 15
        # minutes for fresh data, then schedule at fixed 15-minute wall-clock
        # slots (:00, :15, :30, :45) from then on.
        try:
            run_data_pipeline()
        except Exception as e:
            logger.error(f"Error in immediate startup run: {e}")

        scheduler.add_job(
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
        # Add the job without a trigger so it ONLY runs via manual trigger
        scheduler.add_job(
            run_data_pipeline,
            trigger=None,
            next_run_time=None,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=120,
            replace_existing=True,
            id="intent_pipeline_job",
        )

    scheduler.start()

    logger.info("✅ Intent Data Aggregation pipeline started (WITH MANUAL TRIGGER API)")

    if SELF_RUN:
        job = scheduler.get_job("intent_pipeline_job")
        next_run = job.next_run_time if job else None
        if next_run:
            logger.info(f"   - Schedule: Every 15 mins (Next: {next_run.strftime('%Y-%m-%d %I:%M:%S %p %Z')})")
        else:
            logger.info("   - Schedule: Every 15 mins")
    else:
        logger.info("   - Schedule: MANUAL ONLY (SELF_RUN=false)")

    logger.info("   - Database connections: Lazy short-lived mysql.connector + SQLAlchemy NullPool")
    logger.info("   - Manual Trigger API: POST /trigger (port 5000)")

    try:
        # Run the Flask app (replaces the sleep loop)
        # Using 0.0.0.0 to listen on all interfaces
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        try:
            scheduler.shutdown()
        except Exception:
            pass
        close_all_database_resources()
        logger.info("✅ Shutdown complete")
