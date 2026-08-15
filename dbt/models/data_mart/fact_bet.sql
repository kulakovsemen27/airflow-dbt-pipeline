{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='bet_id',
        incremental_strategy='delete+insert',
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
    greatest(b.loaded_at, r.loaded_at)             AS loaded_at
FROM {{ ref('games') }} AS b
LEFT JOIN {{ ref('currency_rates') }} AS r
    ON b.game_date = r.rate_date
    AND b.currency = r.currency
{% if is_incremental() %}
WHERE greatest(b.loaded_at, r.loaded_at) > (
    SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
    FROM {{ this }}
)
{% endif %}
