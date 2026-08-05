{{
    config(
        schema='data_mart_report',
        materialized='view'
    )
}}

SELECT
    date_trunc('month', activity_date)::DATE AS activity_month,
    country,
    sum(deposits_amount_usd)                AS deposits_amount_usd,
    sum(withdrawals_amount_usd)             AS withdrawals_amount_usd,
    sum(bets_amount_usd)                    AS bets_amount_usd,
    max(loaded_at)                          AS loaded_at
FROM {{ ref('metrics_overview') }}
GROUP BY
    date_trunc('month', activity_date)::DATE,
    country
