{{
    config(
        schema='data_mart',
        materialized='table',
        indexes=[
            {'columns': ['player_id', 'activity_date'], 'unique': True}
        ]
    )
}}

WITH player_operations AS (
    SELECT
        player_id,
        deposit_date         AS activity_date,
        amount_usd           AS deposits_amount_usd,
        0::NUMERIC           AS withdrawals_amount_usd,
        0::NUMERIC           AS bets_amount_usd
    FROM {{ ref('fact_deposit') }}

    UNION ALL

    SELECT
        player_id,
        withdrawal_date      AS activity_date,
        0::NUMERIC           AS deposits_amount_usd,
        amount_usd           AS withdrawals_amount_usd,
        0::NUMERIC           AS bets_amount_usd
    FROM {{ ref('fact_withdrawal') }}

    UNION ALL

    SELECT
        player_id,
        bet_date              AS activity_date,
        0::NUMERIC           AS deposits_amount_usd,
        0::NUMERIC           AS withdrawals_amount_usd,
        amount_usd           AS bets_amount_usd
    FROM {{ ref('fact_bet') }}
)

SELECT
    player_id,
    activity_date,
    sum(deposits_amount_usd)    AS deposits_amount_usd,
    sum(withdrawals_amount_usd) AS withdrawals_amount_usd,
    sum(bets_amount_usd)        AS bets_amount_usd,
    now()                       AS loaded_at
FROM player_operations
GROUP BY
    player_id,
    activity_date
