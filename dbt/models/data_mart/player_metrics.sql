{{
    config(
        schema='data_mart',
        materialized='incremental',
        unique_key='activity_date',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['activity_date']}
        ]
    )
}}

WITH affected_dates AS (
    SELECT deposit_date AS activity_date
    FROM {{ ref('fact_deposit') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}

    UNION

    SELECT withdrawal_date AS activity_date
    FROM {{ ref('fact_withdrawal') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}

    UNION

    SELECT bet_date AS activity_date
    FROM {{ ref('fact_bet') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
),

player_operations AS (
    SELECT
        d.player_id,
        d.deposit_date         AS activity_date,
        d.amount_usd           AS deposits_amount_usd,
        0::NUMERIC             AS withdrawals_amount_usd,
        0::NUMERIC             AS bets_amount_usd,
        d.loaded_at
    FROM {{ ref('fact_deposit') }} AS d
    INNER JOIN affected_dates AS a
        ON d.deposit_date = a.activity_date

    UNION ALL

    SELECT
        w.player_id,
        w.withdrawal_date      AS activity_date,
        0::NUMERIC             AS deposits_amount_usd,
        w.amount_usd           AS withdrawals_amount_usd,
        0::NUMERIC             AS bets_amount_usd,
        w.loaded_at
    FROM {{ ref('fact_withdrawal') }} AS w
    INNER JOIN affected_dates AS a
        ON w.withdrawal_date = a.activity_date

    UNION ALL

    SELECT
        b.player_id,
        b.bet_date             AS activity_date,
        0::NUMERIC             AS deposits_amount_usd,
        0::NUMERIC             AS withdrawals_amount_usd,
        b.amount_usd           AS bets_amount_usd,
        b.loaded_at
    FROM {{ ref('fact_bet') }} AS b
    INNER JOIN affected_dates AS a
        ON b.bet_date = a.activity_date
)

SELECT
    player_id,
    activity_date,
    sum(deposits_amount_usd)    AS deposits_amount_usd,
    sum(withdrawals_amount_usd) AS withdrawals_amount_usd,
    sum(bets_amount_usd)        AS bets_amount_usd,
    max(loaded_at)              AS loaded_at
FROM player_operations
GROUP BY
    player_id,
    activity_date
