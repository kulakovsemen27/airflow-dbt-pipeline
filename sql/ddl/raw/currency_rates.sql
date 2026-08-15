CREATE TABLE IF NOT EXISTS raw.currency_rates (
    date TEXT,
    currency TEXT,
    rate_to_usd TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS currency_rates_key_idx
    ON raw.currency_rates (date, currency);
