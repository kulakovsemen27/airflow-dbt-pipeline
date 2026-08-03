{{
    config(
        schema='staging',
        materialized='table',
        indexes=[
            {'columns': ['provider_id'], 'unique': True}
        ]
    )
}}

WITH ranked_providers AS (
    SELECT
        id::BIGINT          AS provider_id,
        trim(provider_name) AS provider_name,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_number
    FROM {{ source('raw', 'providers_map') }}
)

SELECT
    provider_id,
    provider_name,
    now() AS loaded_at
FROM ranked_providers
WHERE row_number = 1
