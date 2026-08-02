CREATE TABLE IF NOT EXISTS raw.withdrawals (
    id TEXT,
    player_id TEXT,
    withdrawal_date TEXT,
    provider_id TEXT,
    amount TEXT,
    currency TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
