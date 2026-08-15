{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='withdrawal_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['withdrawal_date']}
        ]
    )
}}

SELECT
    w.withdrawal_id,
    w.player_id,
    w.provider_id,
    w.withdrawal_date,
    w.currency,
    w.amount,
    w.amount / nullif(r.currency_units_per_usd, 0) AS amount_usd,
    greatest(w.loaded_at, r.loaded_at)             AS loaded_at
FROM {{ ref('withdrawals') }} AS w
LEFT JOIN {{ ref('currency_rates') }} AS r
    ON w.withdrawal_date = r.rate_date
    AND w.currency = r.currency
{% if is_incremental() %}
WHERE greatest(w.loaded_at, r.loaded_at) > (
    SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
    FROM {{ this }}
)
{% endif %}
