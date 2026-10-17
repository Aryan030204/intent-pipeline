-- Index for the idle-session sweep (pipeline/intent_session_sweep.py).
--
-- The sweep reads MAX(last_event_at) and "open sessions older than X" from intent_actor_cursors once per
-- brand per ingest cycle. Without this index each of those is a scan of the whole table (it still works,
-- it is only slower as the table grows). With it, both read the index.
--
-- OPTIONAL and applied by hand, like every migration; the worker never runs DDL. Not idempotent (MySQL has
-- no CREATE INDEX IF NOT EXISTS): run it once per brand database. If it fails with "Duplicate key name",
-- the index is already there.
--   mysql -h <host> -u <user> -p <brand_db> < migrations/002_intent_cursor_idle_index.sql

ALTER TABLE intent_actor_cursors ADD KEY idx_cursor_last_event_at (last_event_at);
