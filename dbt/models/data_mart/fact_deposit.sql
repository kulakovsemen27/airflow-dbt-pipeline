{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='deposit_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['deposit_date']}
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
    greatest(d.loaded_at, r.loaded_at)             AS loaded_at
FROM {{ ref('deposits') }} AS d
LEFT JOIN {{ ref('currency_rates') }} AS r
    ON d.deposit_date = r.rate_date
    AND d.currency = r.currency
{% if is_incremental() %}
WHERE greatest(d.loaded_at, r.loaded_at) > (
    SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
    FROM {{ this }}
)
{% endif %}
