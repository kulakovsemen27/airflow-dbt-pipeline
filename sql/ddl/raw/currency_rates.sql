CREATE TABLE IF NOT EXISTS raw.currency_rates (
    date TEXT,
    currency TEXT,
    rate_to_usd TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
