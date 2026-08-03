{{
    config(
        schema='data_mart_report',
        materialized='table',
        indexes=[
            {
                'columns': ['country', 'registration_type', 'activity_date'],
                'unique': True
            },
            {'columns': ['activity_date']}
        ]
    )
}}

SELECT
    p.country,
    p.registration_type,
    pm.activity_date,
    sum(pm.deposits_amount_usd)    AS deposits_amount_usd,
    sum(pm.withdrawals_amount_usd) AS withdrawals_amount_usd,
    sum(pm.bets_amount_usd)        AS bets_amount_usd,
    now()                          AS loaded_at
FROM {{ ref('player_metrics') }} AS pm
LEFT JOIN {{ ref('dim_player') }} AS p
    ON pm.player_id = p.player_id
GROUP BY
    p.country,
    p.registration_type,
    pm.activity_date
