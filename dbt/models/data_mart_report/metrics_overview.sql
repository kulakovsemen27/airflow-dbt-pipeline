{{
    config(
        schema='data_mart_report',
        materialized='table',
        indexes=[
            {'columns': ['activity_date']}
        ]
    )
}}

SELECT
    pm.activity_date,
    p.country,
    p.registration_type,
    sum(pm.deposits_amount_usd)    AS deposits_amount_usd,
    sum(pm.withdrawals_amount_usd) AS withdrawals_amount_usd,
    sum(pm.bets_amount_usd)        AS bets_amount_usd,
    now()                          AS loaded_at
FROM {{ ref('player_metrics') }} AS pm
LEFT JOIN {{ ref('dim_player') }} AS p
    ON pm.player_id = p.player_id
GROUP BY
    pm.activity_date,
    p.country,
    p.registration_type
    
