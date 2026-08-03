{{
    config(
        schema='data_mart',
        materialized='table',
        indexes=[
            {'columns': ['player_id'], 'unique': True}
        ]
    )
}}

SELECT
    player_id,
    registration_date,
    registration_type,
    country,
    now() AS loaded_at
FROM {{ ref('players') }}
