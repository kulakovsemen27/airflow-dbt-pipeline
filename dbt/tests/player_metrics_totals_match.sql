WITH operations AS (
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
        bet_date             AS activity_date,
        0::NUMERIC           AS deposits_amount_usd,
        0::NUMERIC           AS withdrawals_amount_usd,
        amount_usd           AS bets_amount_usd
    FROM {{ ref('fact_bet') }}
),

expected AS (
    SELECT
        player_id,
        activity_date,
        sum(deposits_amount_usd)    AS deposits_amount_usd,
        sum(withdrawals_amount_usd) AS withdrawals_amount_usd,
        sum(bets_amount_usd)        AS bets_amount_usd
    FROM operations
    GROUP BY
        player_id,
        activity_date
),

actual AS (
    SELECT
        player_id,
        activity_date,
        deposits_amount_usd,
        withdrawals_amount_usd,
        bets_amount_usd
    FROM {{ ref('player_metrics') }}
)

SELECT
    coalesce(e.player_id, a.player_id)         AS player_id,
    coalesce(e.activity_date, a.activity_date) AS activity_date,
    e.deposits_amount_usd                      AS expected_deposits_amount_usd,
    a.deposits_amount_usd                      AS actual_deposits_amount_usd,
    e.withdrawals_amount_usd                   AS expected_withdrawals_amount_usd,
    a.withdrawals_amount_usd                   AS actual_withdrawals_amount_usd,
    e.bets_amount_usd                          AS expected_bets_amount_usd,
    a.bets_amount_usd                          AS actual_bets_amount_usd
FROM expected AS e
FULL OUTER JOIN actual AS a
    ON e.player_id = a.player_id
    AND e.activity_date = a.activity_date
WHERE e.deposits_amount_usd IS DISTINCT FROM a.deposits_amount_usd
    OR e.withdrawals_amount_usd IS DISTINCT FROM a.withdrawals_amount_usd
    OR e.bets_amount_usd IS DISTINCT FROM a.bets_amount_usd
