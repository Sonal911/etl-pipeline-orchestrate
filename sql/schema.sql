-- ============================================================
-- Star Schema for Mobile App User Action Logs
-- ============================================================

-- Dimension: Users
CREATE TABLE IF NOT EXISTS dim_users (
    user_key    SERIAL PRIMARY KEY,
    user_id     VARCHAR(50) NOT NULL UNIQUE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Actions
CREATE TABLE IF NOT EXISTS dim_actions (
    action_key  SERIAL PRIMARY KEY,
    action_type VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: User Actions
CREATE TABLE IF NOT EXISTS fact_user_actions (
    action_id       SERIAL PRIMARY KEY,
    user_key        INTEGER NOT NULL REFERENCES dim_users(user_key),
    action_key      INTEGER NOT NULL REFERENCES dim_actions(action_key),
    event_timestamp TIMESTAMP NOT NULL,
    device          VARCHAR(50),
    location        VARCHAR(100),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_user_key ON fact_user_actions(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_action_key ON fact_user_actions(action_key);
CREATE INDEX IF NOT EXISTS idx_fact_event_ts ON fact_user_actions(event_timestamp);
