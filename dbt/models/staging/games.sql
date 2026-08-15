{{
    config(
        schema='staging',
        materialized='incremental',
        unique_key='bet_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['game_date']}
        ]
    )
}}

WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM {{ source('raw', 'games') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    id::BIGINT                AS bet_id,
    player_id::BIGINT         AS player_id,
    game_date::DATE           AS game_date,
    amount::NUMERIC           AS amount,
    upper(trim(currency))     AS currency,
    provider_id::BIGINT       AS provider_id,
    game_id::BIGINT           AS game_id,
    loaded_at
FROM ranked
WHERE row_num = 1
