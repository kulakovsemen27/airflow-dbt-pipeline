{{
    config(
        schema='staging',
        materialized='incremental',
        unique_key=['rate_date', 'currency'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['rate_date']}
        ]
    )
}}

WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY date, currency
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM {{ source('raw', 'currency_rates') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    date::DATE                   AS rate_date,
    upper(trim(currency))        AS currency,
    rate_to_usd::NUMERIC         AS currency_units_per_usd,
    loaded_at
FROM ranked
WHERE row_num = 1
