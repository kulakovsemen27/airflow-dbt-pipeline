{{
    config(
        schema='data_mart_report',
        materialized='incremental',
        unique_key='activity_date',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['activity_date']}
        ]
    )
}}

WITH affected_dates AS (
    SELECT DISTINCT activity_date
    FROM {{ ref('player_metrics') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}

    {% if is_incremental() %}
    UNION

    SELECT pm.activity_date
    FROM {{ ref('player_metrics') }} AS pm
    INNER JOIN {{ ref('dim_player') }} AS p
        ON pm.player_id = p.player_id
    WHERE p.loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    pm.activity_date,
    p.country,
    p.registration_type,
    sum(pm.deposits_amount_usd)    AS deposits_amount_usd,
    sum(pm.withdrawals_amount_usd) AS withdrawals_amount_usd,
    sum(pm.bets_amount_usd)        AS bets_amount_usd,
    max(greatest(pm.loaded_at, p.loaded_at))  AS loaded_at
FROM {{ ref('player_metrics') }} AS pm
INNER JOIN affected_dates AS a
    ON pm.activity_date = a.activity_date
LEFT JOIN {{ ref('dim_player') }} AS p
    ON pm.player_id = p.player_id
GROUP BY
    pm.activity_date,
    p.country,
    p.registration_type
