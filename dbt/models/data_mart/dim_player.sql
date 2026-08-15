{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='player_id',
        incremental_strategy='delete+insert',
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
    loaded_at
FROM {{ ref('players') }}
{% if is_incremental() %}
WHERE loaded_at > (
    SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
    FROM {{ this }}
)
{% endif %}
