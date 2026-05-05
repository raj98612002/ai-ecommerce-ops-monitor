WITH source AS (
    SELECT * FROM {{ source("raw", "RAW_COMPLAINTS") }}
),
cleaned AS (
    SELECT
        COMPLAINT_ID                                        AS complaint_id,
        ORDER_ID                                            AS order_id,
        COMPLAINT_TS                                        AS complaint_ts,
        DATE(COMPLAINT_TS)                                  AS complaint_date,
        HOUR(COMPLAINT_TS)                                  AS complaint_hour,
        COMPLAINT_TEXT                                      AS complaint_text,
        LOWER(TRIM(COALESCE(AI_CATEGORY,'uncategorized')))  AS ai_category,
        LOWER(TRIM(COALESCE(AI_SEVERITY,'unknown')))        AS ai_severity,
        LOWER(TRIM(COALESCE(CUSTOMER_EMOTION,'unknown')))   AS customer_emotion,
        LOWER(TRIM(COALESCE(BUSINESS_IMPACT,'unknown')))    AS business_impact,
        CASE WHEN LOWER(AI_SEVERITY) IN ('high','critical')
             THEN TRUE ELSE FALSE END                       AS is_severe,
        CASE WHEN LOWER(AI_SEVERITY) = 'critical'
             THEN TRUE ELSE FALSE END                       AS is_critical,
        CASE WHEN LOWER(AI_SEVERITY) = 'critical' THEN 4
             WHEN LOWER(AI_SEVERITY) = 'high'     THEN 3
             WHEN LOWER(AI_SEVERITY) = 'medium'   THEN 2
             WHEN LOWER(AI_SEVERITY) = 'low'      THEN 1
             ELSE 0 END                                     AS severity_score,
        CREATED_AT                                          AS created_at,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE COMPLAINT_ID IS NOT NULL
)
SELECT * FROM cleaned
