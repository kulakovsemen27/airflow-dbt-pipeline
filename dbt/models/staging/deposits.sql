{{
    config(
        schema='staging',
        materialized='incremental',
        unique_key='deposit_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['deposit_date']}
        ]
    )
}}

WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY id
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM {{ source('raw', 'deposits') }}
    {% if is_incremental() %}
    WHERE loaded_at > (
        SELECT coalesce(max(loaded_at), '1900-01-01'::TIMESTAMPTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    id::BIGINT                AS deposit_id,
    player_id::BIGINT         AS player_id,
    deposit_date::DATE        AS deposit_date,
    provider_id::BIGINT       AS provider_id,
    amount::NUMERIC           AS amount,
    upper(trim(currency))     AS currency,
    loaded_at
FROM ranked
WHERE row_num = 1
