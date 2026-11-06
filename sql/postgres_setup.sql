-- PostgreSQL setup for the Real-Time E-Commerce Data Ingestion pipeline.
--
-- Usage (two steps, because CREATE DATABASE cannot run inside a transaction
-- and PostgreSQL has no CREATE DATABASE IF NOT EXISTS):
--
--   1. Create the database (once), connected to the default "postgres" db:
--        psql -U postgres -c "CREATE DATABASE ecommerce_streaming;"
--
--   2. Create the table, connected to the new database:
--        psql -U postgres -d ecommerce_streaming -f sql/postgres_setup.sql
--
-- Step 1 is repeated here for reference. Run it manually, then skip it on re-runs:
--   CREATE DATABASE ecommerce_streaming;
--   \c ecommerce_streaming

CREATE TABLE IF NOT EXISTS user_events (
    event_id       BIGINT PRIMARY KEY,
    user_id        BIGINT NOT NULL,
    product_id     BIGINT NOT NULL,
    event_type     VARCHAR(20) NOT NULL
                   CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    price          NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    event_time     TIMESTAMP NOT NULL,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_events_event_time
    ON user_events (event_time);

CREATE INDEX IF NOT EXISTS idx_user_events_event_type
    ON user_events (event_type);
