"""
Runs the SQL files in migrations/ against every mapped brand database, once per container start.

Why it exists: the migrations used to be applied by hand and the worker only verified the schema. Now each
container (intent-kafka-worker, intent-data-aggregation, intent-scoring-worker) calls run_on_start() before it
does anything else, so a rebuild or restart brings every brand database up to date by itself.

How it stays safe to call from three containers that start together, on every restart, forever:
- Applied versions are recorded per brand database in schema_migrations; a recorded file is never run again.
- A MySQL named lock (GET_LOCK) held on the same connection serialises concurrent runners; the second one
  finds everything applied.
- "Already exists" errors (duplicate column, duplicate key name, table exists) count as already applied, so
  the files that were applied by hand before this runner existed (001, 002) are adopted and recorded.
- A migration that declares `-- requires-mysql: X.Y.Z` is run only on a server at least that new, read from
  SELECT VERSION(); otherwise it is deferred with a log line, never run blind. INTENT_MIGRATE_ALLOW_REBUILD=true
  rewrites ALGORITHM=INSTANT to an online rebuild (heavy I/O), so it is opt-in.
- It never raises out of run_on_start(): a failure is logged (category=migration_failed) and the container
  carries on. The Kafka worker keeps ingesting without the new columns until the migration is applied.

Settings: INTENT_MIGRATE_ON_START (default on), INTENT_MIGRATE_ALLOW_REBUILD (default off),
INTENT_MIGRATE_LOCK_TIMEOUT_S (default 120).
"""

import hashlib
import os
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.state import logger

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "migrations")
TRACKING_TABLE = "schema_migrations"
LOCK_NAME = "intent-migrations"

# ER_DUP_FIELDNAME, ER_DUP_KEYNAME, ER_TABLE_EXISTS_ERROR: the change is already in place.
ADOPTED_ERRNOS = frozenset({1060, 1061, 1050})

_FILE_PATTERN = re.compile(r"^(\d+)_.*\.sql$")
_REQUIRES = re.compile(r"^\s*--\s*requires-mysql:\s*(\d+(?:\.\d+){0,2})\s*$", re.IGNORECASE | re.MULTILINE)
_INSTANT = re.compile(r"ALGORITHM\s*=\s*INSTANT", re.IGNORECASE)

Version = Tuple[int, int, int]

CREATE_TRACKING_SQL = (
    f"CREATE TABLE IF NOT EXISTS {TRACKING_TABLE} ("
    "version VARCHAR(100) NOT NULL, checksum CHAR(64) NOT NULL, "
    "applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), note VARCHAR(255) NULL, "
    "PRIMARY KEY (version)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)


@dataclass(frozen=True)
class Migration:
    version: str                       # the file name without .sql, e.g. 003_event_utm_columns
    checksum: str
    statements: Tuple[str, ...]
    requires_mysql: Optional[Version] = None


@dataclass
class BrandOutcome:
    applied: List[str] = field(default_factory=list)
    adopted: List[str] = field(default_factory=list)     # applied, but part or all of it already existed
    deferred: List[str] = field(default_factory=list)    # needs a newer server than this one
    failed: Optional[str] = None                         # "version: reason", the run for this brand stopped there
    skipped_reason: Optional[str] = None                 # lock not obtained, tracking table unreadable, ...


def parse_version(text) -> Optional[Version]:
    """'8.0.35' -> (8, 0, 35); '5.7.44-log' -> (5, 7, 44); '8.0' -> (8, 0, 0); anything else -> None."""
    match = re.match(r"^\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?", str(text or ""))
    if not match:
        return None
    return tuple(int(part or 0) for part in match.groups())  # type: ignore[return-value]


def split_statements(sql: str) -> Tuple[str, ...]:
    body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return tuple(part.strip() for part in body.split(";") if part.strip())


def discover(directory: str = MIGRATIONS_DIR) -> List[Migration]:
    """Every NNN_*.sql in the directory, in numeric order."""
    found = []
    for name in os.listdir(directory):
        match = _FILE_PATTERN.match(name)
        if not match:
            continue
        with open(os.path.join(directory, name), "rb") as handle:
            raw = handle.read()
        text = raw.decode("utf-8")
        requires = _REQUIRES.search(text)
        found.append((int(match.group(1)), Migration(
            version=name[:-4],
            checksum=hashlib.sha256(raw).hexdigest(),
            statements=split_statements(text),
            requires_mysql=parse_version(requires.group(1)) if requires else None,
        )))
    return [migration for _n, migration in sorted(found, key=lambda item: (item[0], item[1].version))]


def rewrite_for_rebuild(statement: str) -> str:
    """The online-rebuild form of an INSTANT-only statement, for servers without INSTANT."""
    return _INSTANT.sub("ALGORITHM=INPLACE, LOCK=NONE", statement)


def _first(row, name: str):
    if row is None:
        return None
    return row.get(name) if isinstance(row, dict) else row[0]


def server_version(cursor) -> Optional[Version]:
    cursor.execute("SELECT VERSION() AS v")
    rows = cursor.fetchall()
    return parse_version(_first(rows[0], "v")) if rows else None


def applied_versions(cursor) -> Dict[str, str]:
    """{version: checksum} recorded in this database; empty if the tracking table does not exist yet."""
    try:
        cursor.execute(f"SELECT version, checksum FROM {TRACKING_TABLE}")
    except Exception as exc:
        if getattr(exc, "errno", None) == 1146:      # ER_NO_SUCH_TABLE
            return {}
        raise
    return {(r["version"] if isinstance(r, dict) else r[0]): (r["checksum"] if isinstance(r, dict) else r[1])
            for r in cursor.fetchall()}


def pending(cursor, migrations: Optional[Sequence[Migration]] = None) -> List[str]:
    """Versions not recorded yet. Read-only (used by --preflight)."""
    done = applied_versions(cursor)
    return [m.version for m in (migrations if migrations is not None else discover()) if m.version not in done]


def _apply(cursor, connection, migration: Migration, rebuild_ok: bool, outcome: BrandOutcome) -> bool:
    """Runs one migration. Returns False when the run for this brand must stop (deferred or failed)."""
    adopted_any = False
    for statement in migration.statements:
        sql = rewrite_for_rebuild(statement) if rebuild_ok and migration.requires_mysql else statement
        try:
            cursor.execute(sql)
        except Exception as exc:
            errno = getattr(exc, "errno", None)
            if errno in ADOPTED_ERRNOS:
                adopted_any = True
                continue
            outcome.failed = f"{migration.version}: errno={errno} {type(exc).__name__}: {str(exc)[:200]}"
            return False
    cursor.execute(
        f"INSERT INTO {TRACKING_TABLE} (version, checksum, note) VALUES (%s, %s, %s) "
        f"ON DUPLICATE KEY UPDATE version = version",
        (migration.version, migration.checksum, "adopted: already in place" if adopted_any else None),
    )
    connection.commit()
    (outcome.adopted if adopted_any else outcome.applied).append(migration.version)
    return True


def migrate_connection(cursor, connection, migrations: Sequence[Migration], allow_rebuild: bool = False,
                       lock_timeout_s: int = 120) -> BrandOutcome:
    """Brings the database behind `cursor` up to date. Holds the named lock for the duration."""
    outcome = BrandOutcome()
    cursor.execute("SELECT GET_LOCK(%s, %s) AS got", (LOCK_NAME, int(lock_timeout_s)))
    if _first(cursor.fetchall()[0], "got") != 1:
        outcome.skipped_reason = f"could not get the {LOCK_NAME} lock within {lock_timeout_s}s"
        return outcome
    try:
        cursor.execute(CREATE_TRACKING_SQL)
        connection.commit()
        done = applied_versions(cursor)
        version = None
        for migration in migrations:
            recorded = done.get(migration.version)
            if recorded is not None:
                if recorded != migration.checksum:
                    logger.warning(f"[migrations] category=migration_changed version={migration.version} "
                                   f"the file differs from what was applied; it is not run again")
                continue
            if migration.requires_mysql is not None:
                version = version or server_version(cursor)
                supported = version is not None and version >= migration.requires_mysql
                if not supported and not allow_rebuild:
                    outcome.deferred.append(migration.version)
                    break   # later files may depend on this one
            if not _apply(cursor, connection, migration, allow_rebuild and migration.requires_mysql is not None
                          and not (version is not None and version >= migration.requires_mysql), outcome):
                break
    finally:
        try:
            cursor.execute("SELECT RELEASE_LOCK(%s) AS released", (LOCK_NAME,))
            cursor.fetchall()
        except Exception:
            pass   # closing the connection releases the lock anyway
    return outcome


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def run_for_brand(brand_index: int, migrations: Sequence[Migration], connection_factory: Optional[Callable] = None,
                  allow_rebuild: bool = False, lock_timeout_s: int = 120) -> BrandOutcome:
    from pipeline.db import get_db_connection

    factory = connection_factory or get_db_connection
    with factory(brand_index) as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        try:
            return migrate_connection(cursor, connection, migrations, allow_rebuild, lock_timeout_s)
        finally:
            try:
                cursor.close()
            except Exception:
                pass


def run_on_start(brand_indices: Iterable[int], connection_factory: Optional[Callable] = None,
                 migrations: Optional[Sequence[Migration]] = None) -> Dict[int, BrandOutcome]:
    """Applies pending migrations to each brand database. Never raises."""
    results: Dict[int, BrandOutcome] = {}
    if not _env_flag("INTENT_MIGRATE_ON_START", True):
        logger.info("[migrations] category=migrations_disabled INTENT_MIGRATE_ON_START is off")
        return results
    try:
        files = list(migrations) if migrations is not None else discover()
    except Exception as exc:
        logger.error(f"[migrations] category=migration_failed could not read {MIGRATIONS_DIR}: {type(exc).__name__}: {exc}")
        return results
    allow_rebuild = _env_flag("INTENT_MIGRATE_ALLOW_REBUILD", False)
    try:
        lock_timeout_s = int(os.environ.get("INTENT_MIGRATE_LOCK_TIMEOUT_S", "120") or 120)
    except ValueError:
        lock_timeout_s = 120

    for brand_index in sorted(set(brand_indices)):
        try:
            outcome = run_for_brand(brand_index, files, connection_factory, allow_rebuild, lock_timeout_s)
        except Exception as exc:
            logger.error(f"[migrations] category=migration_failed brand_index={brand_index} {type(exc).__name__}: {exc}")
            continue
        results[brand_index] = outcome
        for version in outcome.applied:
            logger.info(f"[migrations] category=migration_applied brand_index={brand_index} version={version}")
        for version in outcome.adopted:
            logger.info(f"[migrations] category=migration_adopted brand_index={brand_index} version={version} "
                        f"(already in place; recorded)")
        for version in outcome.deferred:
            logger.warning(
                f"[migrations] category=migration_deferred brand_index={brand_index} version={version} needs MySQL "
                f"8.0.12 or newer for an INSTANT change. Apply it by hand off-peak (ALGORITHM=INPLACE, LOCK=NONE) or "
                f"set INTENT_MIGRATE_ALLOW_REBUILD=true. Until then events are stored without the new columns.")
        if outcome.failed:
            logger.error(f"[migrations] category=migration_failed brand_index={brand_index} {outcome.failed}")
        if outcome.skipped_reason:
            logger.warning(f"[migrations] category=migration_skipped brand_index={brand_index} {outcome.skipped_reason}")
        if not (outcome.applied or outcome.adopted or outcome.deferred or outcome.failed or outcome.skipped_reason):
            logger.info(f"[migrations] category=migrations_current brand_index={brand_index}")
    return results


def mapped_brand_indices() -> List[int]:
    """Database indices of the INTENT_DB_MAP brands, once brand configs are initialised (DB_DATABASE_<i> set).
    The same matching the aggregation and scoring jobs use, without importing them."""
    from pipeline.intent_events import _parse_intent_db_map
    from pipeline.state import active_brand_indices

    wanted = {str(v).strip().lower() for v in _parse_intent_db_map().values()}
    indices = []
    for index in active_brand_indices:
        if os.environ.get(f"DB_DATABASE_{index}", "").strip().lower() in wanted:
            indices.append(index)
    return sorted(set(indices))
