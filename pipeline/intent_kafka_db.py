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
from typing import Callable, Dict, Iterable, Optional, Tuple

from pipeline.db import get_db_connection, get_db_cursor
from pipeline.intent_kafka_store import MySqlIntentStore, assert_rowcount_semantics, verify_schema
from pipeline.state import logger


class BrandConnections:
    def __init__(self, connection_factory: Callable = get_db_connection) -> None:
        self._factory = connection_factory
        self._open: Dict[int, Tuple[object, object]] = {}  # brand_index -> (context manager, connection)

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
            yield MySqlIntentStore(cursor)
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
