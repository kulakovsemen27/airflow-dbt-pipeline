{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='game_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    game_id,
    game_name,
    loaded_at
FROM {{ ref('games_map') }}
{% if is_incremental() %}
WHERE loaded_at > (
    SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
    FROM {{ this }}
)
{% endif %}
