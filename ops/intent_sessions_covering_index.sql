-- OPTIONAL, applied BY HAND, once per brand database, off-peak. This file is deliberately NOT in migrations/: the
-- migration runner applies every NNN_*.sql there at container start, and building an index on a large intent_sessions
-- must never happen by itself. See docs/INTENT_SESSIONS_COVERING_INDEX.md for when it is worth it and how to check it.
--
-- A covering index for the dashboard's Intent page scan (analytics/services/intentFunnelService.js,
-- buildCombinedScanQuery). That query reads every closed session of a date range and only these columns, so with this
-- index MySQL reads the index in order (session_start first) and never fetches the wide table rows (which carry the
-- JSON event_sequence). Without it the scan goes through idx_session_start and then fetches each row from the table.
--
--   mysql -h <host> -u <user> -p <brand_db> < ops/intent_sessions_covering_index.sql
--
-- ALGORITHM=INPLACE, LOCK=NONE builds it online (reads and the pipeline's writes continue), at the cost of I/O while it
-- runs and extra disk for the index. If MySQL cannot do it online it refuses; it never silently locks the table.
-- Undo: ALTER TABLE intent_sessions DROP INDEX idx_intent_scan_cover;

ALTER TABLE intent_sessions
    ADD INDEX idx_intent_scan_cover (
        session_start,
        actor_id,
        session_id,
        intent_status,
        intent_bucket,
        predictive_score,
        session_time_spent_ms,
        intent_exclusion_reason,
        click_count,
        scroll_count,
        product_view_count,
        page_view_count,
        add_to_cart_count,
        checkout_started_count
    ),
    ALGORITHM=INPLACE, LOCK=NONE;
