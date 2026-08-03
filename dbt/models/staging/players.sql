{{
    config(
        schema='staging',
        materialized='table',
        indexes=[
            {'columns': ['player_id'], 'unique': True}
        ]
    )
}}

WITH ranked_players AS (
    SELECT
        id::BIGINT              AS player_id,
        registration_date::DATE AS registration_date,
        trim(registration_type) AS registration_type,
        upper(trim(country))    AS country,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_number
    FROM {{ source('raw', 'players') }}
)

SELECT
    player_id,
    registration_date,
    registration_type,
    country,
    now() AS loaded_at
FROM ranked_players
WHERE row_number = 1
