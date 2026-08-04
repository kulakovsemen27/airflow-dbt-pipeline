{{
    config(
        schema='data_mart',
        materialized='table',
        indexes=[
            {'columns': ['bet_date']}
        ]
    )
}}

SELECT
    b.bet_id,
    b.player_id,
    b.game_id,
    b.provider_id,
    b.game_date AS bet_date,
    b.currency,
    b.amount,
    b.amount / nullif(r.currency_units_per_usd, 0) AS amount_usd,
    now() AS loaded_at
FROM {{ ref('games') }} AS b
LEFT JOIN {{ ref('currency_rates') }} AS r
    ON b.game_date = r.rate_date
    AND b.currency = r.currency
