{{
    config(
        schema='data_mart',
        materialized='table',
        indexes=[
            {'columns': ['deposit_id'], 'unique': True},
            {'columns': ['deposit_date', 'player_id']}
        ]
    )
}}

SELECT
    d.deposit_id,
    d.player_id,
    d.provider_id,
    d.deposit_date,
    d.currency,
    d.amount,
    d.amount / nullif(r.currency_units_per_usd, 0) AS amount_usd,
    now() AS loaded_at
FROM {{ ref('deposits') }} AS d
LEFT JOIN {{ ref('currency_rates') }} AS r
    ON d.deposit_date = r.rate_date
    AND d.currency = r.currency
