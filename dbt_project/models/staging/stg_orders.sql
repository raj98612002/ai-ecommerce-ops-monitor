WITH source AS (
    SELECT * FROM {{ source("raw", "RAW_ORDERS") }}
),
cleaned AS (
    SELECT
        ORDER_ID                                            AS order_id,
        CUSTOMER_ID                                         AS customer_id,
        COALESCE(CITY, 'Unknown')                           AS city,
        ORDER_TS                                            AS order_ts,
        DATE(ORDER_TS)                                      AS order_date,
        HOUR(ORDER_TS)                                      AS order_hour,
        COALESCE(TRY_TO_DECIMAL(ORDER_AMOUNT, 10, 2), 0)    AS order_amount,
        LOWER(TRIM(COALESCE(ORDER_STATUS, 'unknown')))      AS order_status,
        CASE WHEN LOWER(ORDER_STATUS) = 'cancelled'
             THEN TRUE ELSE FALSE END                       AS is_cancelled,
        CASE WHEN LOWER(ORDER_STATUS)
             IN ('delivered','shipped','confirmed')
             THEN TRUE ELSE FALSE END                       AS is_active,
        CREATED_AT                                          AS created_at,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE ORDER_ID IS NOT NULL
)
SELECT * FROM cleaned
