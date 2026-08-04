{{
    config(
        schema='staging',
        materialized='table',
        indexes=[
            {'columns': ['deposit_date']}
        ]
    )
}}

WITH ranked_deposits AS (
    SELECT
        id::BIGINT                AS deposit_id,
        player_id::BIGINT         AS player_id,
        deposit_date::DATE        AS deposit_date,
        provider_id::BIGINT       AS provider_id,
        amount::NUMERIC           AS amount,
        upper(trim(currency))     AS currency,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_number
    FROM {{ source('raw', 'deposits') }}
)

SELECT
    deposit_id,
    player_id,
    deposit_date,
    provider_id,
    amount,
    currency,
    now() AS loaded_at
FROM ranked_deposits
WHERE row_number = 1
