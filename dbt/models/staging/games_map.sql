{{
    config(
        schema='staging',
        materialized='incremental',
        unique_key='game_id',
        incremental_strategy='delete+insert'
    )
}}

WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM {{ source('raw', 'games_map') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    id::BIGINT          AS game_id,
    trim(game_name)     AS game_name,
    provider_id::BIGINT AS provider_id,
    loaded_at
FROM ranked
WHERE row_num = 1
