WITH source AS (
    SELECT * FROM {{ source("raw", "RAW_DELIVERIES") }}
),
cleaned AS (
    SELECT
        DELIVERY_ID                                         AS delivery_id,
        ORDER_ID                                            AS order_id,
        COALESCE(CITY, 'Unknown')                           AS city,
        COALESCE(DELIVERY_PARTNER, 'Unknown')               AS delivery_partner,
        CREATED_AT                                          AS delivery_ts,
        DATE(CREATED_AT)                                    AS delivery_date,
        HOUR(CREATED_AT)                                    AS delivery_hour,
        COALESCE(TRY_TO_NUMBER(PROMISED_MINUTES), 0)        AS promised_minutes,
        COALESCE(TRY_TO_NUMBER(ACTUAL_MINUTES), 0)          AS actual_minutes,
        COALESCE(TRY_TO_NUMBER(ACTUAL_MINUTES), 0)
            - COALESCE(TRY_TO_NUMBER(PROMISED_MINUTES), 0)  AS delay_minutes,
        CASE WHEN COALESCE(TRY_TO_NUMBER(ACTUAL_MINUTES), 0)
               > COALESCE(TRY_TO_NUMBER(PROMISED_MINUTES), 0)
             THEN TRUE ELSE FALSE END                       AS is_delayed,
        LOWER(TRIM(COALESCE(DELIVERY_STATUS, 'unknown')))   AS delivery_status,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE DELIVERY_ID IS NOT NULL
)
SELECT * FROM cleaned
