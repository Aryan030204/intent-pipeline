"""The optional covering index must never be picked up by the migration runner."""
import os

from pipeline import migrations

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_NAME = "idx_intent_scan_cover"


def test_the_runner_does_not_see_the_optional_index_file():
    for migration in migrations.discover():
        assert INDEX_NAME not in " ".join(migration.statements), migration.version
    assert not any("covering" in m.version for m in migrations.discover())


def test_the_file_exists_outside_the_migrations_folder_and_builds_the_index_online():
    sql_path = os.path.join(ROOT, "ops", "intent_sessions_covering_index.sql")
    assert os.path.isfile(sql_path)
    assert not os.path.exists(os.path.join(ROOT, "migrations", "intent_sessions_covering_index.sql"))
    text = open(sql_path, encoding="utf-8").read()
    assert f"ADD INDEX {INDEX_NAME}" in text
    assert "ALGORITHM=INPLACE, LOCK=NONE" in text


def test_the_index_holds_every_column_the_dashboard_scan_reads():
    # the columns of intent_sessions that analytics buildCombinedScanQuery selects
    scan_columns = [
        "actor_id", "session_id", "session_start", "intent_status", "intent_bucket", "predictive_score",
        "session_time_spent_ms", "intent_exclusion_reason", "click_count", "scroll_count",
        "product_view_count", "page_view_count", "add_to_cart_count", "checkout_started_count",
    ]
    text = open(os.path.join(ROOT, "ops", "intent_sessions_covering_index.sql"), encoding="utf-8").read()
    body = text.split("ADD INDEX", 1)[1]
    for column in scan_columns:
        assert column in body, column
    assert body.split("(", 1)[1].split(",", 1)[0].strip() == "session_start"   # the range column leads
