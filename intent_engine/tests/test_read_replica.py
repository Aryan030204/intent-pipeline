"""
Read replica routing. Pure reads may use DB_READ_REPLICA_HOST; writes, and anything that
reads rows it has just written, stay on the primary. An unset or unreachable replica must
fall back to the primary so a replica outage never stops scoring.
"""

from contextlib import contextmanager
from datetime import date, datetime

import pytest

import aws_background  # noqa: F401  (must load before the modules that import it lazily)
from pipeline import db
from intent_engine import runner, session_scorer, threshold_calibration

REPLICA = "read-replica-5.cd6gimoi6871.ap-south-1.rds.amazonaws.com"
PRIMARY_CONFIG = {
    "host": "primary.example.internal", "port": 3306, "user": "u", "password": "p",
    "database": "BBB", "ssl_ca": "/tmp/ca.pem", "connection_timeout": 10,
}


@pytest.fixture(autouse=True)
def _config(monkeypatch):
    monkeypatch.setitem(db.brand_db_configs, 2, dict(PRIMARY_CONFIG))
    monkeypatch.setenv("DB_READ_REPLICA_HOST", REPLICA)
    db._replica_checked.clear()


class _Cursor:
    def __init__(self, row):
        self.row = row

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchone(self):
        return self.row

    def close(self):
        pass


class _Cnx:
    in_transaction = False

    def __init__(self, row=("BBB", 1)):
        self.row = row
        self.closed = False

    def cursor(self, dictionary=False):
        return _Cursor(self.row)

    def close(self):
        self.closed = True


# --- connection routing ---------------------------------------------------------------------

def test_replica_config_changes_only_the_host(monkeypatch):
    config = db._read_replica_config(2)
    assert config["host"] == REPLICA
    assert {k: v for k, v in config.items() if k != "host"} == {k: v for k, v in PRIMARY_CONFIG.items() if k != "host"}
    assert db.brand_db_configs[2]["host"] == "primary.example.internal"   # the primary config is untouched


def test_no_replica_when_unset_or_when_it_is_the_primary(monkeypatch):
    monkeypatch.delenv("DB_READ_REPLICA_HOST")
    assert db._read_replica_config(2) is None
    monkeypatch.setenv("DB_READ_REPLICA_HOST", "PRIMARY.example.internal")
    assert db._read_replica_config(2) is None


def test_read_connection_opens_the_replica_and_closes_it(monkeypatch):
    opened = []
    cnx = _Cnx()

    def fake_open(brand_index, name, config):
        opened.append(config["host"])
        return cnx

    monkeypatch.setattr(db, "_open_mysql_connection_once", fake_open)
    monkeypatch.setattr(db, "get_db_connection", lambda *a, **k: pytest.fail("primary must not be used"))
    with db.get_db_read_connection(2) as got:
        assert got is cnx
    assert opened == [REPLICA]
    assert cnx.closed


def test_unreachable_replica_falls_back_to_the_primary(monkeypatch):
    primary = _Cnx()

    def failing_open(*a, **k):
        raise RuntimeError("connection refused")

    @contextmanager
    def fake_primary(brand_index):
        yield primary

    monkeypatch.setattr(db, "_open_mysql_connection_once", failing_open)
    monkeypatch.setattr(db, "get_db_connection", fake_primary)
    with db.get_db_read_connection(2) as got:
        assert got is primary


def test_a_replica_serving_the_wrong_database_is_refused(monkeypatch):
    wrong = _Cnx(row=("OTHER_BRAND", 1))
    primary = _Cnx()

    @contextmanager
    def fake_primary(brand_index):
        yield primary

    monkeypatch.setattr(db, "_open_mysql_connection_once", lambda *a, **k: wrong)
    monkeypatch.setattr(db, "get_db_connection", fake_primary)
    with db.get_db_read_connection(2) as got:
        assert got is primary
    assert wrong.closed


def test_without_a_replica_the_read_connection_is_the_primary(monkeypatch):
    monkeypatch.delenv("DB_READ_REPLICA_HOST")
    primary = _Cnx()

    @contextmanager
    def fake_primary(brand_index):
        yield primary

    monkeypatch.setattr(db, "get_db_connection", fake_primary)
    monkeypatch.setattr(db, "_open_mysql_connection_once", lambda *a, **k: pytest.fail("no replica configured"))
    with db.get_db_read_connection(2) as got:
        assert got is primary


def test_an_error_in_the_body_still_closes_the_replica_connection(monkeypatch):
    cnx = _Cnx()
    monkeypatch.setattr(db, "_open_mysql_connection_once", lambda *a, **k: cnx)
    with pytest.raises(ValueError):
        with db.get_db_read_connection(2):
            raise ValueError("boom")
    assert cnx.closed


# --- which queries use which cursor ---------------------------------------------------------

PRIMARY, READER = "primary-cursor", "read-cursor"


def test_scoring_reads_use_the_read_cursor_and_writes_the_primary(monkeypatch):
    calls = []
    session = {"session_id": "s1", "actor_id": "a1", "session_start": datetime(2026, 10, 14, 10, 0), "user_agent": None}

    class Enr:
        user_agent = "Googlebot/2.1"   # excluded as a bot: exercises the write path with minimal data

    monkeypatch.setattr(session_scorer, "fetch_eligible_sessions", lambda c, *a: (calls.append(("sessions", c)), [session])[1])
    monkeypatch.setattr(session_scorer, "_build_enrichment", lambda c, ids: (calls.append(("enrichment", c)), {"s1": Enr()})[1])
    monkeypatch.setattr(session_scorer, "get_effective_thresholds", lambda c, d: (calls.append(("thresholds", c)), {})[1])
    monkeypatch.setattr(session_scorer, "bulk_update_scored_sessions", lambda c, conn, rows: (calls.append(("write_scored", c)), 0)[1])
    monkeypatch.setattr(session_scorer, "bulk_update_excluded_sessions", lambda c, conn, rows: (calls.append(("write_excluded", c)), 1)[1])

    now = datetime(2026, 10, 15, 4, 0)
    session_scorer.score_window_for_brand(PRIMARY, object(), now, now, now, read_cursor=READER)

    used = dict(calls)
    assert used["sessions"] == READER
    assert used["enrichment"] == READER
    assert used["thresholds"] == READER
    assert used["write_scored"] == PRIMARY
    assert used["write_excluded"] == PRIMARY


def test_scoring_without_a_read_cursor_uses_the_primary_for_everything(monkeypatch):
    calls = []
    monkeypatch.setattr(session_scorer, "fetch_eligible_sessions", lambda c, *a: (calls.append(c), [])[1])
    now = datetime(2026, 10, 15, 4, 0)
    session_scorer.score_window_for_brand(PRIMARY, object(), now, now, now)
    assert calls == [PRIMARY]


def test_calibration_bulk_reads_use_the_replica_and_the_writes_and_lookup_the_primary(monkeypatch):
    calls = []
    monkeypatch.setattr(threshold_calibration, "compute_dead_click_p90", lambda c, d: (calls.append(("p90", c)), 0.5)[1])
    monkeypatch.setattr(threshold_calibration, "fetch_scored_predictive_scores_for_date", lambda c, d, m: (calls.append(("scores", c)), [10.0, 20.0])[1])
    monkeypatch.setattr(threshold_calibration, "upsert_daily_thresholds", lambda c, *a: calls.append(("write_thresholds", c)))
    monkeypatch.setattr(threshold_calibration, "bulk_reassign_buckets_for_date", lambda c, *a: (calls.append(("write_reassign", c)), 0)[1])
    monkeypatch.setattr(threshold_calibration, "fetch_actor_ids_scored_on_date", lambda c, d: (calls.append(("actors_after_writes", c)), [])[1])

    threshold_calibration.calibrate_date_for_brand(PRIMARY, object(), date(2026, 10, 14), datetime(2026, 10, 15, 2, 0), read_cursor=READER)

    used = dict(calls)
    assert used["p90"] == READER
    assert used["scores"] == READER
    assert used["write_thresholds"] == PRIMARY
    assert used["write_reassign"] == PRIMARY
    assert used["actors_after_writes"] == PRIMARY


def test_runner_hands_the_read_cursor_to_the_scorer(monkeypatch):
    seen = {}

    @contextmanager
    def fake_primary(_i):
        yield _Cursor({"db_now": datetime(2026, 10, 14, 22, 30)}), object()

    @contextmanager
    def fake_reader(_i):
        yield READER

    monkeypatch.setattr(runner, "get_db_cursor", fake_primary)
    monkeypatch.setattr(runner, "get_db_read_cursor", fake_reader)
    monkeypatch.setattr(runner, "_ensure_scoring_schema", lambda *a, **k: None)
    monkeypatch.setattr(runner, "get_pipeline_metadata_timestamp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "update_pipeline_metadata_timestamp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "recompute_actors", lambda *a, **k: None)
    monkeypatch.setattr(runner, "score_window_for_brand", lambda *a, **k: (seen.update(read=k.get("read_cursor")), set())[1])

    runner._run_scoring_for_brand(2, "BBB")
    assert seen["read"] == READER
