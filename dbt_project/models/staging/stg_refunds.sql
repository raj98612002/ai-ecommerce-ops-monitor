WITH source AS (
    SELECT * FROM {{ source("raw", "RAW_REFUNDS") }}
),
cleaned AS (
    SELECT
        REFUND_ID                                           AS refund_id,
        ORDER_ID                                            AS order_id,
        REFUND_TS                                           AS refund_ts,
        DATE(REFUND_TS)                                     AS refund_date,
        HOUR(REFUND_TS)                                     AS refund_hour,
        COALESCE(REFUND_AMOUNT, 0)                          AS refund_amount,
        LOWER(TRIM(COALESCE(REFUND_REASON, 'unknown')))     AS refund_reason,
        CREATED_AT                                          AS created_at,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE REFUND_ID IS NOT NULL
)
SELECT * FROM cleaned