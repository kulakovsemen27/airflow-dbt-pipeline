{{
    config(
        schema='data_mart',
        materialized='table'
    )
}}

SELECT
    provider_id,
    provider_name,
    now() AS loaded_at
FROM {{ ref('providers_map') }}
