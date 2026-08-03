{{
    config(
        schema='staging',
        materialized='table',
        indexes=[
            {'columns': ['game_id'], 'unique': True}
        ]
    )
}}

WITH ranked_games AS (
    SELECT
        id::BIGINT          AS game_id,
        trim(game_name)     AS game_name,
        provider_id::BIGINT AS provider_id,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_number
    FROM {{ source('raw', 'games_map') }}
)

SELECT
    game_id,
    game_name,
    provider_id,
    now() AS loaded_at
FROM ranked_games
WHERE row_number = 1
