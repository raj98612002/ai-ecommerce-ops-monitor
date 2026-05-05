WITH source AS (
    SELECT * FROM {{ source("raw", "RAW_PAYMENTS") }}
),
cleaned AS (
    SELECT
        PAYMENT_ID                                          AS payment_id,
        ORDER_ID                                            AS order_id,
        PAYMENT_TS                                          AS payment_ts,
        DATE(PAYMENT_TS)                                    AS payment_date,
        HOUR(PAYMENT_TS)                                    AS payment_hour,
        LOWER(TRIM(COALESCE(PAYMENT_METHOD, 'unknown')))    AS payment_method,
        LOWER(TRIM(COALESCE(PAYMENT_STATUS, 'unknown')))    AS payment_status,
        COALESCE(TRY_TO_DECIMAL(AMOUNT, 10, 2), 0)          AS amount,
        CASE WHEN LOWER(PAYMENT_STATUS) = 'failed'
             THEN COALESCE(LOWER(TRIM(FAILURE_REASON)),
                  'unknown_failure')
             ELSE NULL END                                  AS failure_reason,
        CASE WHEN LOWER(PAYMENT_STATUS) = 'success'
             THEN TRUE ELSE FALSE END                       AS is_success,
        CASE WHEN LOWER(PAYMENT_STATUS) = 'failed'
             THEN TRUE ELSE FALSE END                       AS is_failed,
        CREATED_AT                                          AS created_at,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE PAYMENT_ID IS NOT NULL
)
SELECT * FROM cleaned
