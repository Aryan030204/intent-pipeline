-- requires-mysql: 8.0.12
--
-- UTM parameters of the page URL as their own columns on the two event tables (pipeline/utm.py fills them).
-- Existing rows stay NULL; scripts/backfill_utm.py fills them from the stored url for a date range.
--
-- ALGORITHM=INSTANT (MySQL 8.0.12 and newer) only changes the table definition: no rebuild, no copy, no I/O
-- burst. The runner (pipeline/migrations.py) applies this file automatically at container start, and only on a
-- server that supports INSTANT. On an older server it is deferred and logged; to apply it there by hand,
-- off-peak, replace ALGORITHM=INSTANT with ALGORITHM=INPLACE, LOCK=NONE (an online rebuild of each table, heavy
-- I/O) or set INTENT_MIGRATE_ALLOW_REBUILD=true so the runner does that rewrite itself.
--
-- Not idempotent by itself (MySQL has no ADD COLUMN IF NOT EXISTS); the runner treats "duplicate column" as
-- already applied. No indexes on purpose: they would add write cost to tables that are already I/O heavy.

ALTER TABLE behavioral_events
    ADD COLUMN utm_source VARCHAR(255) NULL,
    ADD COLUMN utm_medium VARCHAR(255) NULL,
    ADD COLUMN utm_campaign VARCHAR(255) NULL,
    ADD COLUMN utm_term VARCHAR(255) NULL,
    ADD COLUMN utm_content VARCHAR(255) NULL,
    ALGORITHM=INSTANT;

ALTER TABLE click_events
    ADD COLUMN utm_source VARCHAR(255) NULL,
    ADD COLUMN utm_medium VARCHAR(255) NULL,
    ADD COLUMN utm_campaign VARCHAR(255) NULL,
    ADD COLUMN utm_term VARCHAR(255) NULL,
    ADD COLUMN utm_content VARCHAR(255) NULL,
    ALGORITHM=INSTANT;
