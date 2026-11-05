
CREATE TABLE IF NOT EXISTS user_events (
    event_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
