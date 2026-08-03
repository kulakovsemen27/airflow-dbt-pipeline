{{
    config(
        schema='staging',
        materialized='table',
        indexes=[
            {'columns': ['bet_id'], 'unique': True}
        ]
    )
}}

WITH ranked_bets AS (
    SELECT
        id::BIGINT                AS bet_id,
        player_id::BIGINT         AS player_id,
        game_date::DATE           AS game_date,
        amount::NUMERIC           AS amount,
        upper(trim(currency))     AS currency,
        provider_id::BIGINT       AS provider_id,
        game_id::BIGINT           AS game_id,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_number
    FROM {{ source('raw', 'games') }}
)

SELECT
    bet_id,
    player_id,
    game_date,
    amount,
    currency,
    provider_id,
    game_id,
    now() AS loaded_at
FROM ranked_bets
WHERE row_number = 1
