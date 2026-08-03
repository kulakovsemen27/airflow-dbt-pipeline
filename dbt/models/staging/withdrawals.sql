{{
    config(
        schema='staging',
        materialized='table'
    )
}}

WITH ranked_withdrawals AS (
    SELECT
        id::BIGINT                AS withdrawal_id,
        player_id::BIGINT         AS player_id,
        withdrawal_date::DATE     AS withdrawal_date,
        provider_id::BIGINT       AS provider_id,
        amount::NUMERIC           AS amount,
        upper(trim(currency))     AS currency,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_number
    FROM {{ source('raw', 'withdrawals') }}
)

SELECT
    withdrawal_id,
    player_id,
    withdrawal_date,
    provider_id,
    amount,
    currency,
    now() AS loaded_at
FROM ranked_withdrawals
WHERE row_number = 1
