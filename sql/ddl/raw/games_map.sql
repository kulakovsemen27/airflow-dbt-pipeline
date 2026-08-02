CREATE TABLE IF NOT EXISTS raw.games_map (
    id TEXT,
    game_name TEXT,
    provider_id TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
