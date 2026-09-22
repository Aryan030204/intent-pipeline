"""
Database connection management: connection/cursor context managers with
retry + SSL-fallback, connection metrics, TLS/CA bundle resolution,
graceful shutdown (atexit/signal), bulk-executemany chunking, timing
context manager, and the generic pipeline_metadata key/value table CRUD
reused by the intent-events watermark tracking.
"""

import atexit
import os
import random
import signal
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
import requests

from pipeline.state import (
    IST,
    logger,
    brand_db_configs,
    sqlalchemy_engines,
    http_session,
    _db_metrics_lock,
    _db_shutdown_lock,
    _db_context,
    _db_connection_metrics,
)

DB_RETRY_ATTEMPTS = int(os.environ.get("DB_RETRY_ATTEMPTS", "3"))
DB_RETRY_BASE_DELAY_S = float(os.environ.get("DB_RETRY_BASE_DELAY_S", "0.5"))

_db_shutdown_done = False


def _metric_inc(name: str, amount: int = 1) -> None:
    with _db_metrics_lock:
        _db_connection_metrics[name] = _db_connection_metrics.get(name, 0) + amount
        if name == "active_connector_connections":
            _db_connection_metrics["peak_connector_connections"] = max(
                _db_connection_metrics["peak_connector_connections"],
                _db_connection_metrics["active_connector_connections"],
            )
        elif name == "active_sqlalchemy_connections":
            _db_connection_metrics["peak_sqlalchemy_connections"] = max(
                _db_connection_metrics["peak_sqlalchemy_connections"],
                _db_connection_metrics["active_sqlalchemy_connections"],
            )


def _metric_dec(name: str, amount: int = 1) -> None:
    with _db_metrics_lock:
        _db_connection_metrics[name] = max(
            0, _db_connection_metrics.get(name, 0) - amount
        )


def get_db_connection_metrics_snapshot() -> Dict[str, int]:
    with _db_metrics_lock:
        return dict(_db_connection_metrics)


def log_db_connection_metrics(prefix: str = "DB_CONNECTION_METRICS") -> None:
    metrics = get_db_connection_metrics_snapshot()
    logger.info(
        "%s active_connector=%s peak_connector=%s opened_total=%s failures_total=%s "
        "active_sqlalchemy=%s peak_sqlalchemy=%s",
        prefix,
        metrics["active_connector_connections"],
        metrics["peak_connector_connections"],
        metrics["connections_opened_total"],
        metrics["connection_failures_total"],
        metrics["active_sqlalchemy_connections"],
        metrics["peak_sqlalchemy_connections"],
    )

def close_all_database_resources() -> None:
    global _db_shutdown_done
    with _db_shutdown_lock:
        if _db_shutdown_done:
            return
        _db_shutdown_done = True

    log_db_connection_metrics("DB_CONNECTION_METRICS_SHUTDOWN_BEGIN")
    disposed = 0
    for brand_idx, engine in list(sqlalchemy_engines.items()):
        try:
            engine.dispose()
            disposed += 1
        except Exception as e:
            logger.warning(
                "Error disposing SQLAlchemy engine for brand_index=%s: %s",
                brand_idx,
                e,
            )

    sqlalchemy_engines.clear()
    brand_db_configs.clear()
    try:
        http_session.close()
    except Exception:
        pass

    logger.info(
        "Database resource shutdown complete: disposed_sqlalchemy_engines=%s",
        disposed,
    )
    log_db_connection_metrics("DB_CONNECTION_METRICS_SHUTDOWN_COMPLETE")


def _handle_shutdown_signal(signum, frame) -> None:
    logger.info("Received shutdown signal %s", signum)
    close_all_database_resources()
    raise SystemExit(0)


atexit.register(close_all_database_resources)
if threading.current_thread() is threading.main_thread():
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)


def _ensure_ca_file_from_env() -> Optional[str]:
    """
    Returns a filesystem path to an RDS CA bundle.

    Priority:
      1) RDS_CA_PATH if it exists
      2) Download from RDS_CA_URL (or default global bundle) into /tmp and reuse
    """
    ca_path = os.environ.get("RDS_CA_PATH")
    if ca_path and Path(ca_path).exists():
        return ca_path

    ca_url = os.environ.get(
        "RDS_CA_URL",
        "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem",
    )
    write_path = os.environ.get("RDS_CA_WRITE_PATH", "/tmp/rds-ca.pem")
    p = Path(write_path)

    try:
        if p.exists() and p.stat().st_size > 0:
            return str(p)

        p.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"⬇️ Downloading RDS CA bundle from {ca_url} → {write_path}")
        resp = requests.get(ca_url, timeout=30)
        resp.raise_for_status()

        # Basic sanity check
        if b"BEGIN CERTIFICATE" not in resp.content:
            raise RuntimeError(
                "Downloaded CA bundle does not look like a PEM certificate file."
            )

        p.write_bytes(resp.content)
        return str(p)

    except Exception as e:
        logger.error(f"❌ Failed to obtain RDS CA bundle: {e}")
        return None


def _sqlalchemy_connect_args_for_tls(
    mysql_connect_str: str, ca_path: Optional[str]
) -> dict:
    """
    Produce SQLAlchemy connect_args for common MySQL drivers.
    """
    if not ca_path:
        return {}

    s = (mysql_connect_str or "").lower()

    if "mysql+mysqlconnector" in s:
        return {
            "ssl_ca": ca_path,
            "ssl_verify_cert": True,
            "ssl_verify_identity": False,
        }

    return {"ssl": {"ca": ca_path}}


def _is_ssl_connection_error(exc: BaseException) -> bool:
    err_msg = str(exc).lower()
    return "ssl" in err_msg or "certificate verify failed" in err_msg


@contextmanager
def timed(label: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        logger.info(f"⏱️ {label} took {dt:.2f}s")


# Chunk size for bulk INSERT ... ON DUPLICATE KEY UPDATE and plain bulk inserts.
EXECUTEMANY_CHUNK_SIZE = int(os.environ.get("EXECUTEMANY_CHUNK_SIZE", "1000"))


def executemany_chunked(
    cursor,
    connection,
    sql: str,
    rows: List[Tuple],
    chunk_size: int = EXECUTEMANY_CHUNK_SIZE,
    commit_between_chunks: bool = True,
) -> int:
    """Run executemany in bounded chunks. Commits between chunks by default so
    a single slow chunk does not hold locks across the whole batch.

    Returns total row count passed in (mysql-connector's rowcount sums affected
    rows across chunks in a way that isn't portable, so we return len(rows)).
    """
    if not rows:
        return 0
    total = len(rows)
    for i in range(0, total, chunk_size):
        cursor.executemany(sql, rows[i : i + chunk_size])
        if commit_between_chunks:
            connection.commit()
    if not commit_between_chunks:
        connection.commit()
    return total


def _connector_config_without_ssl(config: Dict[str, Any]) -> Dict[str, Any]:
    clean = dict(config)
    clean.pop("ssl_ca", None)
    clean.pop("ssl_verify_cert", None)
    clean.pop("ssl_verify_identity", None)
    return clean


def _open_mysql_connection_once(
    brand_index: int,
    brand_name: str,
    config: Dict[str, Any],
):
    cnx = None
    try:
        cnx = mysql.connector.connect(**config)
        cnx.ping(reconnect=False, attempts=1, delay=0)
    except Exception:
        if cnx is not None:
            try:
                cnx.close()
            except Exception:
                pass
        raise

    _metric_inc("active_connector_connections")
    _metric_inc("connections_opened_total")

    connection_id = None
    try:
        cursor = cnx.cursor()
        try:
            cursor.execute("SELECT CONNECTION_ID()")
            row = cursor.fetchone()
            connection_id = row[0] if row else None
        finally:
            cursor.close()
    except Exception as e:
        logger.debug(
            "Could not fetch MySQL connection id for brand=%s brand_index=%s: %s",
            brand_name,
            brand_index,
            e,
        )

    logger.info(
        "DB_CONNECTION_OPENED connection_id=%s brand=%s brand_index=%s pipeline=intent_data_aggregation "
        "job_id=%s thread=%s",
        connection_id,
        brand_name,
        brand_index,
        getattr(_db_context, "job_id", None),
        threading.current_thread().name,
    )
    return cnx


@contextmanager
def get_db_connection(
    brand_index: int,
    attempts: int = DB_RETRY_ATTEMPTS,
    sleep_s: float = DB_RETRY_BASE_DELAY_S,
):
    config = brand_db_configs.get(brand_index)
    if not config:
        raise ValueError(f"No database config for brand {brand_index}")

    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    last_err: Optional[BaseException] = None
    cnx = None

    for attempt in range(1, max(1, attempts) + 1):
        try:
            cnx = _open_mysql_connection_once(brand_index, brand_name, config)
            break
        except mysql.connector.Error as e:
            last_err = e
            _metric_inc("connection_failures_total")
            if "ssl_ca" in config and _is_ssl_connection_error(e):
                logger.warning(
                    "DB_CONNECT_SSL_FAILED brand=%s brand_index=%s attempt=%s/%s; retrying without SSL",
                    brand_name,
                    brand_index,
                    attempt,
                    attempts,
                )
                fallback_config = _connector_config_without_ssl(config)
                try:
                    cnx = _open_mysql_connection_once(
                        brand_index, brand_name, fallback_config
                    )
                    brand_db_configs[brand_index] = fallback_config
                    # Lazy import: _replace_sqlalchemy_engine_for_brand stays in
                    # aws_background.py (brand_config_init category), which itself
                    # imports this module, so a top-level import here would be
                    # circular. By call time (actual pipeline execution) both
                    # modules are fully loaded.
                    import aws_background

                    aws_background._replace_sqlalchemy_engine_for_brand(
                        brand_index, brand_name, fallback_config
                    )
                    break
                except mysql.connector.Error as fallback_err:
                    last_err = fallback_err
                    _metric_inc("connection_failures_total")

            if attempt >= attempts:
                break

            delay = sleep_s * (2 ** (attempt - 1)) + random.uniform(0, sleep_s)
            logger.warning(
                "DB_CONNECT_RETRY brand=%s brand_index=%s attempt=%s/%s delay=%.2fs error=%s",
                brand_name,
                brand_index,
                attempt,
                attempts,
                delay,
                e,
            )
            log_db_connection_metrics("DB_CONNECTION_METRICS_RETRY")
            time.sleep(delay)

    if cnx is None:
        raise last_err or mysql.connector.Error(
            f"Failed opening database connection for brand {brand_index}"
        )

    try:
        yield cnx
    except Exception:
        try:
            if cnx.in_transaction:
                cnx.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            try:
                if cnx.in_transaction:
                    cnx.rollback()
            except Exception:
                pass
            cnx.close()
        finally:
            _metric_dec("active_connector_connections")
            logger.info(
                "DB_CONNECTION_CLOSED brand=%s brand_index=%s pipeline=intent_data_aggregation job_id=%s thread=%s",
                brand_name,
                brand_index,
                getattr(_db_context, "job_id", None),
                threading.current_thread().name,
            )


@contextmanager
def get_db_cursor(brand_index: int, dictionary=True):
    with get_db_connection(brand_index) as connection:
        cursor = connection.cursor(dictionary=dictionary)
        try:
            yield cursor, connection
        except Exception:
            try:
                if connection.in_transaction:
                    connection.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass


# ---------------------------
# Pipeline metadata (watermark) helpers
# ---------------------------
def ensure_pipeline_metadata_table(cursor, connection):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            key_name VARCHAR(50) PRIMARY KEY,
            key_value DATETIME
        );
    """
    )
    if connection is not None:
        connection.commit()


def get_pipeline_metadata_timestamp(
    cursor,
    key_name: str,
    default: Optional[datetime] = None,
) -> Optional[datetime]:
    cursor.execute(
        "SELECT key_value FROM pipeline_metadata WHERE key_name = %s",
        (key_name,),
    )
    result = cursor.fetchone()
    if not result:
        return default

    val = result[0] if not isinstance(result, dict) else result.get("key_value")
    if not val:
        return default

    if isinstance(val, datetime):
        return val.replace(tzinfo=IST) if val.tzinfo is None else val.astimezone(IST)

    if isinstance(val, str):
        try:
            parsed = datetime.fromisoformat(val)
            return (
                parsed.replace(tzinfo=IST)
                if parsed.tzinfo is None
                else parsed.astimezone(IST)
            )
        except ValueError:
            return default

    return default


def update_pipeline_metadata_timestamp(cursor, connection, key_name: str, new_timestamp: datetime):
    ensure_pipeline_metadata_table(cursor, connection)
    cursor.execute(
        """
        INSERT INTO pipeline_metadata (key_name, key_value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE key_value = VALUES(key_value);
    """,
        (key_name, new_timestamp),
    )
    connection.commit()
