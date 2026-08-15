{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='provider_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    provider_id,
    provider_name,
    loaded_at
FROM {{ ref('providers_map') }}
{% if is_incremental() %}
WHERE loaded_at > (
    SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
    FROM {{ this }}
)
{% endif %}
