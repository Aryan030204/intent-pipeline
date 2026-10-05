-- Intent SQS consumer state tables and the session version column.
--
-- NOT APPLIED. Run once per brand database, by hand, in a planned window:
--   mysql -h <host> -u <user> -p <brand_db> < migrations/003_intent_sqs_state.sql
--
-- The worker never runs this DDL. At runtime it only verifies (read-only) that
-- these tables and the column exist, and refuses to write to a database where
-- they are missing.

CREATE TABLE IF NOT EXISTS intent_actor_cursors (
    actor_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    session_start DATETIME(6) NOT NULL,
    last_event_at DATETIME(6) NOT NULL,
    last_event_id VARCHAR(100) NULL,
    events_seq JSON NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (actor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS intent_atc_dedupe (
    session_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (session_id, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Version guard for session snapshots (pipeline/intent_sqs_writer.py).
-- Fails with "Duplicate column name" if already applied; that is the expected no-op.
ALTER TABLE intent_sessions
    ADD COLUMN source_updated_at DATETIME(6) NULL;
