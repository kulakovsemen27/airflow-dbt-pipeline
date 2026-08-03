WITH expected AS (
    SELECT
        d.deposit_date AS activity_date,
        d.currency,
        count(*) AS row_count,
        sum(d.amount) AS amount,
        sum(d.amount / nullif(r.currency_units_per_usd, 0)) AS amount_usd
    FROM {{ ref('deposits') }} AS d
    LEFT JOIN {{ ref('currency_rates') }} AS r
        ON d.deposit_date = r.rate_date
        AND d.currency = r.currency
    GROUP BY
        d.deposit_date,
        d.currency
),

actual AS (
    SELECT
        deposit_date AS activity_date,
        currency,
        count(*) AS row_count,
        sum(amount) AS amount,
        sum(amount_usd) AS amount_usd
    FROM {{ ref('fact_deposit') }}
    GROUP BY
        deposit_date,
        currency
)

SELECT
    coalesce(e.activity_date, a.activity_date) AS activity_date,
    coalesce(e.currency, a.currency) AS currency,
    e.row_count AS expected_row_count,
    a.row_count AS actual_row_count,
    e.amount AS expected_amount,
    a.amount AS actual_amount,
    e.amount_usd AS expected_amount_usd,
    a.amount_usd AS actual_amount_usd
FROM expected AS e
FULL OUTER JOIN actual AS a
    ON e.activity_date = a.activity_date
    AND e.currency = a.currency
WHERE e.row_count IS DISTINCT FROM a.row_count
    OR e.amount IS DISTINCT FROM a.amount
    OR e.amount_usd IS DISTINCT FROM a.amount_usd
