WITH expected AS (
    SELECT
        activity_date,
        sum(deposits_amount_usd)    AS deposits_amount_usd,
        sum(withdrawals_amount_usd) AS withdrawals_amount_usd,
        sum(bets_amount_usd)        AS bets_amount_usd
    FROM {{ ref('player_metrics') }}
    GROUP BY activity_date
),

actual AS (
    SELECT
        activity_date,
        sum(deposits_amount_usd)    AS deposits_amount_usd,
        sum(withdrawals_amount_usd) AS withdrawals_amount_usd,
        sum(bets_amount_usd)        AS bets_amount_usd
    FROM {{ ref('metrics_overview') }}
    GROUP BY activity_date
)

SELECT
    coalesce(e.activity_date, a.activity_date) AS activity_date,
    e.deposits_amount_usd                      AS expected_deposits_amount_usd,
    a.deposits_amount_usd                      AS actual_deposits_amount_usd,
    e.withdrawals_amount_usd                   AS expected_withdrawals_amount_usd,
    a.withdrawals_amount_usd                   AS actual_withdrawals_amount_usd,
    e.bets_amount_usd                          AS expected_bets_amount_usd,
    a.bets_amount_usd                          AS actual_bets_amount_usd
FROM expected AS e
FULL OUTER JOIN actual AS a
    ON e.activity_date = a.activity_date
WHERE e.deposits_amount_usd IS DISTINCT FROM a.deposits_amount_usd
    OR e.withdrawals_amount_usd IS DISTINCT FROM a.withdrawals_amount_usd
    OR e.bets_amount_usd IS DISTINCT FROM a.bets_amount_usd
