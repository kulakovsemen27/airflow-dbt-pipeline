CREATE TABLE IF NOT EXISTS raw.games (
    id TEXT,
    player_id TEXT,
    game_date TEXT,
    amount TEXT,
    currency TEXT,
    provider_id TEXT,
    game_id TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS games_date_idx
    ON raw.games (game_date);
