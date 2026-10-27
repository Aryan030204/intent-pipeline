"""The migration runner: applies migrations/*.sql once per database, safely, from any number of containers."""

import contextlib
import os

import pytest

from pipeline import migrations as mig
from pipeline.migrations import Migration


class DbError(Exception):
    def __init__(self, errno, message="boom"):
        super().__init__(message)
        self.errno = errno


class Cursor:
    """Answers the runner's own statements; every other statement is just recorded."""

    def __init__(self, version="8.0.35", applied=None, fail=None, lock=1, no_tracking_table=False):
        self.executed, self.version, self.lock = [], version, lock
        self.applied = dict(applied or {})
        self.fail = dict(fail or {})
        self.no_tracking_table = no_tracking_table
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append(sql.strip())
        self._rows = []
        for needle, error in self.fail.items():
            if needle in sql:
                raise error
        text = sql.strip()
        if text.startswith("SELECT GET_LOCK"):
            self._rows = [{"got": self.lock}]
        elif text.startswith("SELECT RELEASE_LOCK"):
            self._rows = [{"released": 1}]
        elif text.startswith("SELECT VERSION"):
            self._rows = [{"v": self.version}]
        elif text.startswith("SELECT version, checksum"):
            if self.no_tracking_table:
                raise DbError(1146)
            self._rows = [{"version": v, "checksum": c} for v, c in self.applied.items()]
        elif text.startswith("INSERT INTO schema_migrations"):
            self.applied[params[0]] = params[1]

    def fetchall(self):
        return self._rows

    def close(self):
        pass

    def work(self):
        """The migration statements only, without the runner's bookkeeping."""
        skip = ("SELECT", "CREATE TABLE IF NOT EXISTS schema_migrations", "INSERT INTO schema_migrations")
        return [s for s in self.executed if not s.startswith(skip)]


class Conn:
    def __init__(self, cursor):
        self._cursor, self.commits = cursor, 0

    def cursor(self, **kwargs):
        return self._cursor

    def commit(self):
        self.commits += 1


def migration(version, *statements, requires=None):
    return Migration(version=version, checksum=f"sum-{version}", statements=tuple(statements), requires_mysql=requires)


def run(cursor, files, **kwargs):
    return mig.migrate_connection(cursor, Conn(cursor), files, **kwargs)


def test_pending_files_run_in_order_and_are_recorded():
    cursor = Cursor()
    outcome = run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT"),
                           migration("002_b", "ALTER TABLE b ADD y INT", "ALTER TABLE c ADD z INT")])
    assert cursor.work() == ["ALTER TABLE a ADD x INT", "ALTER TABLE b ADD y INT", "ALTER TABLE c ADD z INT"]
    assert outcome.applied == ["001_a", "002_b"] and not outcome.failed
    assert cursor.applied == {"001_a": "sum-001_a", "002_b": "sum-002_b"}


def test_a_recorded_file_is_never_run_again():
    cursor = Cursor(applied={"001_a": "sum-001_a"})
    files = [migration("001_a", "ALTER TABLE a ADD x INT"), migration("002_b", "ALTER TABLE b ADD y INT")]
    outcome = run(cursor, files)
    assert cursor.work() == ["ALTER TABLE b ADD y INT"]
    assert outcome.applied == ["002_b"]
    assert run(cursor, files).applied == []     # a second run does nothing


def test_a_changed_file_that_was_already_applied_is_not_rerun():
    cursor = Cursor(applied={"001_a": "an-older-checksum"})
    outcome = run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert cursor.work() == [] and outcome.applied == []


def test_already_exists_errors_count_as_applied_and_the_rest_still_runs():
    cursor = Cursor(fail={"ADD x": DbError(1060, "Duplicate column name 'x'")})
    outcome = run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT", "ALTER TABLE a ADD y INT")])
    assert cursor.work() == ["ALTER TABLE a ADD x INT", "ALTER TABLE a ADD y INT"]
    assert outcome.adopted == ["001_a"] and outcome.applied == [] and not outcome.failed
    assert "001_a" in cursor.applied


@pytest.mark.parametrize("errno", [1060, 1061, 1050])
def test_every_adopted_error_number(errno):
    cursor = Cursor(fail={"ALTER": DbError(errno)})
    assert run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT")]).adopted == ["001_a"]


def test_a_real_failure_stops_the_brand_without_recording_it_or_running_later_files():
    cursor = Cursor(fail={"ADD y": DbError(1142, "ALTER command denied")})
    outcome = run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT"), migration("002_b", "ALTER TABLE b ADD y INT"),
                           migration("003_c", "ALTER TABLE c ADD z INT")])
    assert outcome.applied == ["001_a"]
    assert outcome.failed.startswith("002_b: errno=1142")
    assert "002_b" not in cursor.applied and "003_c" not in cursor.applied
    assert "ALTER TABLE c ADD z INT" not in cursor.work()


def test_the_lock_is_taken_first_released_last_and_released_after_a_failure():
    cursor = Cursor(fail={"ALTER": DbError(1142)})
    run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert cursor.executed[0].startswith("SELECT GET_LOCK")
    assert cursor.executed[-1].startswith("SELECT RELEASE_LOCK")


def test_not_getting_the_lock_changes_nothing():
    cursor = Cursor(lock=0)
    outcome = run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert cursor.work() == [] and "lock" in outcome.skipped_reason
    assert not any(s.startswith("SELECT RELEASE_LOCK") for s in cursor.executed)


def test_the_tracking_table_is_created_if_missing():
    cursor = Cursor(no_tracking_table=True)
    run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS schema_migrations") for s in cursor.executed)


# ---- the MySQL version gate ----

INSTANT_ALTER = "ALTER TABLE e ADD COLUMN u VARCHAR(255) NULL, ALGORITHM=INSTANT"


@pytest.mark.parametrize("version", ["5.7.44-log", "8.0.11", None, ""])
def test_a_migration_that_needs_a_newer_server_is_deferred_not_run(version):
    cursor = Cursor(version=version)
    outcome = run(cursor, [migration("003_utm", INSTANT_ALTER, requires=(8, 0, 12)),
                           migration("004_next", "ALTER TABLE z ADD q INT")])
    assert cursor.work() == []                       # nothing ran, and later files wait for it
    assert outcome.deferred == ["003_utm"] and "003_utm" not in cursor.applied


@pytest.mark.parametrize("version", ["8.0.12", "8.0.35", "8.4.0", "9.1.0"])
def test_it_runs_unchanged_on_a_server_that_supports_it(version):
    cursor = Cursor(version=version)
    outcome = run(cursor, [migration("003_utm", INSTANT_ALTER, requires=(8, 0, 12))])
    assert cursor.work() == [INSTANT_ALTER] and outcome.applied == ["003_utm"]


def test_allow_rebuild_rewrites_instant_to_an_online_rebuild_on_an_old_server():
    cursor = Cursor(version="5.7.44-log")
    outcome = run(cursor, [migration("003_utm", INSTANT_ALTER, requires=(8, 0, 12))], allow_rebuild=True)
    assert cursor.work() == ["ALTER TABLE e ADD COLUMN u VARCHAR(255) NULL, ALGORITHM=INPLACE, LOCK=NONE"]
    assert outcome.applied == ["003_utm"]


def test_allow_rebuild_leaves_a_supported_server_on_instant():
    cursor = Cursor(version="8.0.35")
    run(cursor, [migration("003_utm", INSTANT_ALTER, requires=(8, 0, 12))], allow_rebuild=True)
    assert cursor.work() == [INSTANT_ALTER]


def test_files_without_a_requirement_never_ask_for_the_version():
    cursor = Cursor(version=None)
    run(cursor, [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert not any(s.startswith("SELECT VERSION") for s in cursor.executed)


def test_version_parsing():
    assert mig.parse_version("8.0.35") == (8, 0, 35)
    assert mig.parse_version("5.7.44-log") == (5, 7, 44)
    assert mig.parse_version("8.0") == (8, 0, 0)
    assert mig.parse_version("10.11.2-MariaDB") == (10, 11, 2)
    assert mig.parse_version("") is None and mig.parse_version(None) is None and mig.parse_version("x") is None


# ---- reading the real files ----

def test_the_repo_migrations_are_found_in_order_with_comments_stripped():
    files = mig.discover()
    assert [f.version for f in files][:3] == ["001_intent_kafka_state", "002_intent_cursor_idle_index", "003_event_utm_columns"]
    utm = files[2]
    assert utm.requires_mysql == (8, 0, 12)
    assert len(utm.statements) == 2 and all(s.startswith("ALTER TABLE") and "ALGORITHM=INSTANT" in s for s in utm.statements)
    for table in ("behavioral_events", "click_events"):
        assert any(f"ALTER TABLE {table}" in s for s in utm.statements)
    for column in ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"):
        assert all(f"ADD COLUMN {column} VARCHAR(255) NULL" in s for s in utm.statements)
    assert all("--" not in s for f in files for s in f.statements)
    assert files[0].checksum != files[1].checksum and files[0].requires_mysql is None


def test_discover_ignores_files_that_are_not_numbered_sql(tmp_path):
    (tmp_path / "010_b.sql").write_text("ALTER TABLE b ADD y INT;")
    (tmp_path / "002_a.sql").write_text("-- requires-mysql: 8.0.12\nALTER TABLE a ADD x INT;")
    (tmp_path / "notes.txt").write_text("x")
    (tmp_path / "README.sql").write_text("x")
    files = mig.discover(str(tmp_path))
    assert [f.version for f in files] == ["002_a", "010_b"] and files[0].requires_mysql == (8, 0, 12)


def test_pending_lists_what_is_not_recorded_and_reads_a_missing_tracking_table_as_empty():
    files = [migration("001_a", "x"), migration("002_b", "y")]
    assert mig.pending(Cursor(applied={"001_a": "s"}), files) == ["002_b"]
    assert mig.pending(Cursor(no_tracking_table=True), files) == ["001_a", "002_b"]


# ---- run_on_start: the container entry point ----

def factory_for(cursors, opened=None):
    @contextlib.contextmanager
    def make(index):
        if opened is not None:
            opened.append(index)
        cursor = cursors[index]
        if isinstance(cursor, Exception):
            raise cursor
        yield Conn(cursor)

    return make


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("INTENT_MIGRATE_ON_START", "INTENT_MIGRATE_ALLOW_REBUILD", "INTENT_MIGRATE_LOCK_TIMEOUT_S"):
        monkeypatch.delenv(name, raising=False)


def test_run_on_start_migrates_every_brand_once_in_index_order():
    cursors, opened = {1: Cursor(), 2: Cursor()}, []
    results = mig.run_on_start([2, 1, 2], factory_for(cursors, opened), [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert opened == [1, 2] and set(results) == {1, 2}
    assert all(c.work() == ["ALTER TABLE a ADD x INT"] for c in cursors.values())


def test_one_brand_failing_to_connect_does_not_stop_the_others_or_raise():
    cursors = {1: ConnectionError("down"), 2: Cursor()}
    results = mig.run_on_start([1, 2], factory_for(cursors), [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert list(results) == [2] and results[2].applied == ["001_a"]


def test_a_failed_migration_is_reported_not_raised():
    cursors = {1: Cursor(fail={"ALTER": DbError(1142)})}
    results = mig.run_on_start([1], factory_for(cursors), [migration("001_a", "ALTER TABLE a ADD x INT")])
    assert results[1].failed


def test_it_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("INTENT_MIGRATE_ON_START", "off")
    opened = []
    assert mig.run_on_start([1], factory_for({1: Cursor()}, opened), [migration("001_a", "x")]) == {} and opened == []


def test_the_rebuild_opt_in_reaches_the_runner(monkeypatch):
    monkeypatch.setenv("INTENT_MIGRATE_ALLOW_REBUILD", "true")
    cursors = {1: Cursor(version="5.7.44-log")}
    mig.run_on_start([1], factory_for(cursors), [migration("003_utm", INSTANT_ALTER, requires=(8, 0, 12))])
    assert "ALGORITHM=INPLACE, LOCK=NONE" in cursors[1].work()[0]


def test_an_unreadable_migrations_folder_is_reported_not_raised(monkeypatch):
    def boom(directory=None):
        raise OSError("no such directory")

    monkeypatch.setattr(mig, "discover", boom)
    assert mig.run_on_start([1], factory_for({1: Cursor()})) == {}


def test_every_container_entry_point_runs_the_migrations_on_start():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for path in ("aws_background.py", os.path.join("workers", "intent_scoring_worker.py"),
                 os.path.join("workers", "intent_kafka_worker.py")):
        with open(os.path.join(root, path), encoding="utf-8") as handle:
            source = handle.read()
        assert "run_on_start" in source or "run_migrations_on_start" in source, path
