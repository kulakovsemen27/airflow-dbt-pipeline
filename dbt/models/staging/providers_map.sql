{{
    config(
        schema='staging',
        materialized='incremental',
        unique_key='provider_id',
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
    FROM {{ source('raw', 'providers_map') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    id::BIGINT          AS provider_id,
    trim(provider_name) AS provider_name,
    loaded_at
FROM ranked
WHERE row_num = 1
