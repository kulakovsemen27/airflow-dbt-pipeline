{{
    config(
        schema='staging',
        materialized='incremental',
        unique_key='player_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['player_id'], 'unique': True}
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
    FROM {{ source('raw', 'players') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    id::BIGINT              AS player_id,
    registration_date::DATE AS registration_date,
    trim(registration_type) AS registration_type,
    upper(trim(country))    AS country,
    loaded_at
FROM ranked
WHERE row_num = 1
