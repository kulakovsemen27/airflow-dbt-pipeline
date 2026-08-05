{{
    config(
        schema='staging',
        materialized='table',
        indexes=[
            {'columns': ['rate_date']}
        ]
    )
}}

WITH ranked_currency_rates AS (
    SELECT
        date::DATE                   AS rate_date,
        upper(trim(currency))        AS currency,
        rate_to_usd::NUMERIC         AS currency_units_per_usd,
        row_number() OVER (
            PARTITION BY date, currency
            ORDER BY loaded_at DESC
        )                            AS row_number
    FROM {{ source('raw', 'currency_rates') }}
)

SELECT
    rate_date,
    currency,
    currency_units_per_usd,
    now() AS loaded_at
FROM ranked_currency_rates
WHERE row_number = 1
