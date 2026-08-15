CREATE TABLE IF NOT EXISTS raw.deposits (
    id TEXT,
    player_id TEXT,
    deposit_date TEXT,
    provider_id TEXT,
    amount TEXT,
    currency TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS deposits_date_idx
    ON raw.deposits (deposit_date);
