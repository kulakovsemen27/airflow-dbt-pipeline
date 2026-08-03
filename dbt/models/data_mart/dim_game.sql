{{
    config(
        schema='data_mart',
        materialized='table',
        indexes=[
            {'columns': ['game_id'], 'unique': True}
        ]
    )
}}

SELECT
    game_id,
    game_name,
    now() AS loaded_at
FROM {{ ref('games_map') }}
