-- State tables for the Kafka intent consumer (workers/intent_kafka_worker.py).
--
-- These replace the Mongo actor_cursors collection and the Mongo partial unique index
-- that /track used to keep. They are the only schema the Kafka consumer needs beyond
-- the tables pipeline/intent_events.py already creates (behavioral_events, click_events,
-- intent_sessions).
--
-- Idempotent: CREATE TABLE IF NOT EXISTS, no ALTER, safe to re-run. BBB already has
-- both tables with exactly this definition. Run once per brand database, by hand:
--   mysql -h <host> -u <user> -p <brand_db> < migrations/001_intent_kafka_state.sql
--
-- The worker never runs this DDL. At startup it only verifies, read-only, that the
-- tables exist, and it refuses to consume while any mapped brand database lacks them.

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
