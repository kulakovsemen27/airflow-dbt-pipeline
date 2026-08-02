CREATE TABLE IF NOT EXISTS raw.players (
    id TEXT,
    registration_date TEXT,
    registration_type TEXT,
    country TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
