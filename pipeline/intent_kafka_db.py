"""
MySQL connections for the Kafka intent consumer.

Reuses pipeline/db.py's get_db_connection (brand config, TLS settings and the SSL fallback,
connection metrics, rollback-and-close on exit) instead of opening connections itself.
The difference from the old per-sync pattern is lifetime: the consumer commits a batch
every second or two, so a connection per brand is kept open across batches and pinged
before reuse, instead of paying a TLS handshake per batch.

One BrandConnections belongs to one consumer thread and is never shared: a connection
carries an open transaction, and the cursors are buffered so a SELECT can never be left
unread on it.
"""

import contextlib
import time
from typing import Callable, Dict, Iterable, Optional, Tuple

from pipeline.db import get_db_connection, get_db_cursor
from pipeline.intent_kafka_store import (
    MySqlIntentStore,
    assert_rowcount_semantics,
    utm_columns_present,
    verify_schema,
)
from pipeline.state import logger

UTM_RECHECK_S = 300   # how often a database without the utm_* columns is asked again whether it has them now


class BrandConnections:
    def __init__(self, connection_factory: Callable = get_db_connection, clock: Callable[[], float] = time.monotonic) -> None:
        self._factory = connection_factory
        self._clock = clock
        self._open: Dict[int, Tuple[object, object]] = {}  # brand_index -> (context manager, connection)
        self._utm: Dict[int, Tuple[float, bool]] = {}      # brand_index -> (checked at, has the utm_* columns)

    def _has_utm(self, brand_index: int, cursor) -> bool:
        """Whether this brand's event tables have the utm_* columns. A database that has them is not asked
        again; one that does not is re-checked every UTM_RECHECK_S, so applying the migration by hand takes
        effect without a restart. If the check itself fails the events are written without UTM values (they
        can be filled in later with scripts/backfill_utm.py) rather than risking an insert on a missing column."""
        cached = self._utm.get(brand_index)
        if cached is not None and (cached[1] or self._clock() - cached[0] < UTM_RECHECK_S):
            return cached[1]
        try:
            present = utm_columns_present(cursor)
        except Exception as exc:
            logger.warning(f"[intent-kafka] category=utm_check_failed brand_index={brand_index} {type(exc).__name__}: {exc}")
            present = False
        if present and (cached is None or not cached[1]):
            logger.info(f"[intent-kafka] category=utm_columns_present brand_index={brand_index} storing UTM values")
        self._utm[brand_index] = (self._clock(), present)
        return present

    def _connection(self, brand_index: int):
        entry = self._open.get(brand_index)
        if entry is not None:
            try:
                entry[1].ping(reconnect=False, attempts=1, delay=0)
            except Exception as exc:
                logger.warning(
                    f"[intent-kafka] category=db_reconnect brand_index={brand_index} "
                    f"stale connection dropped: {type(exc).__name__}"
                )
                self._discard(brand_index)
                entry = None
        if entry is None:
            manager = self._factory(brand_index)
            connection = manager.__enter__()
            try:
                assert_rowcount_semantics(connection)
            except Exception:
                manager.__exit__(None, None, None)
                raise
            entry = (manager, connection)
            self._open[brand_index] = entry
        return entry[1]

    def _discard(self, brand_index: int) -> None:
        entry = self._open.pop(brand_index, None)
        if entry is not None:
            try:
                entry[0].__exit__(None, None, None)
            except Exception:
                pass

    @contextlib.contextmanager
    def transaction(self, brand_index: int):
        """Yields a store inside one transaction. Commits on a normal exit; on any error
        rolls back (dropping the connection if even that fails) and re-raises."""
        connection = self._connection(brand_index)
        cursor = connection.cursor(dictionary=True, buffered=True)
        try:
            yield MySqlIntentStore(cursor, utm=self._has_utm(brand_index, cursor))
            connection.commit()
        except BaseException:
            try:
                connection.rollback()
            except Exception:
                self._discard(brand_index)
            raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def close(self) -> None:
        for brand_index in list(self._open):
            self._discard(brand_index)


class RunLock:
    """Keeps two workers from running an ingest cycle at once: a MySQL named lock (GET_LOCK) held on a
    dedicated connection for the length of the cycle. It is taken without waiting, so a second worker
    skips its cycle (and says so) instead of queueing behind the first. The server releases the lock by
    itself if the holder's connection dies."""

    def __init__(self, connection_factory: Callable, brand_index: int, name: str) -> None:
        self._factory, self._brand_index, self._name = connection_factory, brand_index, name[:64]
        self._manager = None
        self._connection = None

    def acquire(self) -> bool:
        manager = self._factory(self._brand_index)
        connection = manager.__enter__()
        try:
            cursor = connection.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT GET_LOCK(%s, 0) AS got", (self._name,))
            got = cursor.fetchall()[0]["got"]
            cursor.close()
        except Exception:
            manager.__exit__(None, None, None)
            raise
        if got != 1:
            manager.__exit__(None, None, None)
            return False
        self._manager, self._connection = manager, connection
        return True

    def release(self) -> None:
        if self._manager is None:
            return
        try:
            cursor = self._connection.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT RELEASE_LOCK(%s) AS released", (self._name,))
            cursor.fetchall()
            cursor.close()
        except Exception:
            pass                      # closing the connection releases it anyway
        finally:
            try:
                self._manager.__exit__(None, None, None)
            finally:
                self._manager = self._connection = None


def verify_brand_schemas(brand_indices: Iterable[int]) -> None:
    """Startup check, read-only. Raises SchemaMissing naming the brand database that is not
    ready. All mapped brands must pass: records of different brands share Kafka partitions,
    so one unmigrated brand would stall the others."""
    for brand_index in brand_indices:
        with get_db_cursor(brand_index) as (cursor, _connection):
            try:
                verify_schema(cursor)
            except Exception as exc:
                raise type(exc)(f"brand_index={brand_index}: {exc}") from exc
