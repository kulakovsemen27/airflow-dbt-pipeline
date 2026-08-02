CREATE TABLE IF NOT EXISTS raw.providers_map (
    id TEXT,
    provider_name TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
