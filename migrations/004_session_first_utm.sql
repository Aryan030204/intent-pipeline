-- requires-mysql: 8.0.12
--
-- The first utm_source of a session, on its intent_sessions row. The pipeline writes it when the session closes (the
-- utm_source of the session's first event that has one, from the page URL), so the dashboard's User Overlap card
-- reads one table instead of joining intent_sessions to behavioral_events.
--
-- ALGORITHM=INSTANT (MySQL 8.0.12 and newer) only changes the table definition: no rebuild, no copy, no I/O burst.
-- The runner (pipeline/migrations.py) applies this file automatically at container start, and only on a server that
-- supports INSTANT; on an older server it is deferred and logged. Sessions closed before this column existed stay NULL
-- until scripts/backfill_first_utm.py fills them. Not idempotent by itself (MySQL has no ADD COLUMN IF NOT EXISTS);
-- the runner treats "duplicate column" as already applied. No index on purpose: nothing filters on it.

ALTER TABLE intent_sessions
    ADD COLUMN first_utm_source VARCHAR(255) NULL,
    ALGORITHM=INSTANT;
