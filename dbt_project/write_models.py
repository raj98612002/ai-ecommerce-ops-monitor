import os

os.makedirs('models/staging', exist_ok=True)
os.makedirs('models/marts', exist_ok=True)

files = {

'models/staging/stg_orders.sql': """WITH source AS (
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
""",

'models/staging/stg_payments.sql': """WITH source AS (
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
""",

'models/staging/stg_deliveries.sql': """WITH source AS (
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
""",

'models/staging/stg_complaints.sql': """WITH source AS (
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
""",

'models/staging/stg_refunds.sql': """WITH source AS (
    SELECT * FROM {{ source("raw", "RAW_REFUNDS") }}
),
cleaned AS (
    SELECT
        REFUND_ID                                           AS refund_id,
        ORDER_ID                                            AS order_id,
        REFUND_TS                                           AS refund_ts,
        DATE(REFUND_TS)                                     AS refund_date,
        HOUR(REFUND_TS)                                     AS refund_hour,
        COALESCE(TRY_TO_DECIMAL(REFUND_AMOUNT, 10, 2), 0)   AS refund_amount,
        LOWER(TRIM(COALESCE(REFUND_REASON, 'unknown')))     AS refund_reason,
        CREATED_AT                                          AS created_at,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE REFUND_ID IS NOT NULL
)
SELECT * FROM cleaned
""",

'models/staging/stg_ai_diagnosis.sql': """WITH source AS (
    SELECT * FROM {{ source("raw", "AI_PIPELINE_DIAGNOSIS") }}
),
cleaned AS (
    SELECT
        "id"                                                AS diagnosis_id,
        RUN_TIMESTAMP                                       AS run_timestamp,
        DATE(RUN_TIMESTAMP)                                 AS run_date,
        HOUR(RUN_TIMESTAMP)                                 AS run_hour,
        LOWER(TRIM(COALESCE(STATUS, 'unknown')))            AS status,
        COALESCE(TRY_TO_DECIMAL(SEVERITY, 5, 3), 0)         AS severity,
        COALESCE(TRY_TO_DECIMAL(CONFIDENCE, 5, 3), 0)       AS confidence,
        ISSUE_SUMMARY                                       AS issue_summary,
        ROOT_CAUSE                                          AS root_cause,
        BUSINESS_IMPACT                                     AS business_impact,
        SUGGESTED_FIX                                       AS suggested_fix,
        LOWER(TRIM(COALESCE(SOURCE, 'unknown')))            AS diagnosis_source,
        COALESCE(LLM_PROVIDER, 'n/a')                       AS llm_provider,
        COALESCE(LLM_MODEL, 'n/a')                          AS llm_model,
        COALESCE(TRY_TO_NUMBER(LLM_LATENCY_MS), 0)          AS llm_latency_ms,
        ALERT_FINGERPRINT                                   AS alert_fingerprint,
        ALERTED_AT                                          AS alerted_at,
        CASE WHEN ALERTED_AT IS NOT NULL
             THEN TRUE ELSE FALSE END                       AS was_alerted,
        CASE WHEN LOWER(STATUS) = 'healthy'
             THEN TRUE ELSE FALSE END                       AS is_healthy,
        CASE WHEN LOWER(STATUS) = 'degraded'
             THEN TRUE ELSE FALSE END                       AS is_degraded,
        CASE WHEN LOWER(STATUS) = 'critical'
             THEN TRUE ELSE FALSE END                       AS is_critical,
        CASE WHEN LOWER(SOURCE) = 'llm'
             THEN TRUE ELSE FALSE END                       AS is_llm_diagnosis,
        CURRENT_TIMESTAMP()                                 AS dbt_loaded_at
    FROM source
    WHERE "id" IS NOT NULL
)
SELECT * FROM cleaned
""",

'models/marts/mart_payment_health.sql': """WITH payments AS (
    SELECT * FROM {{ ref("stg_payments") }}
),
daily_summary AS (
    SELECT
        payment_date,
        payment_method,
        COUNT(*)                                            AS total_payments,
        SUM(CASE WHEN is_success THEN 1 ELSE 0 END)        AS successful_payments,
        SUM(CASE WHEN is_failed  THEN 1 ELSE 0 END)        AS failed_payments,
        ROUND(SUM(CASE WHEN is_success THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS success_rate,
        ROUND(SUM(CASE WHEN is_failed THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS failure_rate,
        SUM(CASE WHEN is_success THEN amount ELSE 0 END)    AS successful_revenue,
        SUM(CASE WHEN is_failed  THEN amount ELSE 0 END)    AS failed_revenue,
        CASE
            WHEN SUM(CASE WHEN is_failed THEN 1 ELSE 0 END)::FLOAT
               / NULLIF(COUNT(*), 0) >= 0.30 THEN 'critical'
            WHEN SUM(CASE WHEN is_failed THEN 1 ELSE 0 END)::FLOAT
               / NULLIF(COUNT(*), 0) >= 0.20 THEN 'warning'
            ELSE 'healthy'
        END                                                 AS health_status,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM payments
    GROUP BY payment_date, payment_method
)
SELECT * FROM daily_summary
ORDER BY payment_date DESC, failure_rate DESC
""",

'models/marts/mart_delivery_performance.sql': """WITH deliveries AS (
    SELECT * FROM {{ ref("stg_deliveries") }}
),
city_daily AS (
    SELECT
        delivery_date,
        city,
        delivery_partner,
        COUNT(*)                                            AS total_deliveries,
        SUM(CASE WHEN is_delayed = FALSE THEN 1 ELSE 0 END) AS on_time_deliveries,
        SUM(CASE WHEN is_delayed = TRUE  THEN 1 ELSE 0 END) AS delayed_deliveries,
        ROUND(SUM(CASE WHEN is_delayed = FALSE THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS on_time_rate,
        ROUND(SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS delay_rate,
        ROUND(AVG(promised_minutes), 1)                     AS avg_promised_minutes,
        ROUND(AVG(actual_minutes), 1)                       AS avg_actual_minutes,
        ROUND(AVG(delay_minutes), 1)                        AS avg_delay_minutes,
        MAX(delay_minutes)                                  AS max_delay_minutes,
        CASE
            WHEN SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)::FLOAT
               / NULLIF(COUNT(*), 0) >= 0.40 THEN 'critical'
            WHEN SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)::FLOAT
               / NULLIF(COUNT(*), 0) >= 0.25 THEN 'warning'
            ELSE 'healthy'
        END                                                 AS health_status,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM deliveries
    GROUP BY delivery_date, city, delivery_partner
)
SELECT * FROM city_daily
ORDER BY delivery_date DESC, delay_rate DESC
""",

'models/marts/mart_order_summary.sql': """WITH orders AS (
    SELECT * FROM {{ ref("stg_orders") }}
),
daily_city AS (
    SELECT
        order_date,
        city,
        COUNT(*)                                            AS total_orders,
        COUNT(DISTINCT customer_id)                         AS unique_customers,
        SUM(CASE WHEN is_cancelled = FALSE THEN 1 ELSE 0 END) AS active_orders,
        SUM(CASE WHEN is_cancelled = TRUE  THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(order_amount)                                   AS total_revenue,
        ROUND(AVG(order_amount), 2)                         AS avg_order_value,
        ROUND(SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS cancellation_rate,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM orders
    GROUP BY order_date, city
)
SELECT * FROM daily_city
ORDER BY order_date DESC, total_revenue DESC
""",

'models/marts/mart_complaint_insights.sql': """WITH complaints AS (
    SELECT * FROM {{ ref("stg_complaints") }}
),
daily_category AS (
    SELECT
        complaint_date,
        ai_category,
        ai_severity,
        customer_emotion,
        COUNT(*)                                            AS total_complaints,
        SUM(CASE WHEN is_severe   THEN 1 ELSE 0 END)       AS severe_complaints,
        SUM(CASE WHEN is_critical THEN 1 ELSE 0 END)       AS critical_complaints,
        ROUND(SUM(CASE WHEN is_severe THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS severe_rate,
        ROUND(AVG(severity_score), 2)                       AS avg_severity_score,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM complaints
    GROUP BY complaint_date, ai_category, ai_severity, customer_emotion
)
SELECT * FROM daily_category
ORDER BY complaint_date DESC, total_complaints DESC
""",

'models/marts/mart_ai_diagnosis_summary.sql': """WITH diagnoses AS (
    SELECT * FROM {{ ref("stg_ai_diagnosis") }}
),
daily_summary AS (
    SELECT
        run_date,
        COUNT(*)                                            AS total_diagnoses,
        SUM(CASE WHEN is_healthy  THEN 1 ELSE 0 END)       AS healthy_count,
        SUM(CASE WHEN is_degraded THEN 1 ELSE 0 END)       AS degraded_count,
        SUM(CASE WHEN is_critical THEN 1 ELSE 0 END)       AS critical_count,
        SUM(CASE WHEN was_alerted THEN 1 ELSE 0 END)       AS alerts_sent,
        SUM(CASE WHEN is_llm_diagnosis     THEN 1 ELSE 0 END) AS llm_diagnoses,
        SUM(CASE WHEN NOT is_llm_diagnosis THEN 1 ELSE 0 END) AS fallback_diagnoses,
        ROUND(SUM(CASE WHEN is_healthy  THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS healthy_rate,
        ROUND(SUM(CASE WHEN is_critical THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS critical_rate,
        ROUND(SUM(CASE WHEN was_alerted THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS alert_rate,
        ROUND(SUM(CASE WHEN is_llm_diagnosis THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS llm_usage_rate,
        ROUND(AVG(severity), 3)                             AS avg_severity,
        ROUND(AVG(confidence), 3)                           AS avg_confidence,
        ROUND((
            SUM(CASE WHEN is_healthy  THEN 1.0 ELSE 0 END) +
            SUM(CASE WHEN is_degraded THEN 0.5 ELSE 0 END)
        ) / NULLIF(COUNT(*), 0) * 100, 1)                  AS pipeline_health_score,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM diagnoses
    GROUP BY run_date
)
SELECT * FROM daily_summary
ORDER BY run_date DESC
""",

'models/marts/mart_business_health_daily.sql': """WITH
orders AS (
    SELECT order_date AS dt,
           SUM(total_revenue) AS total_revenue,
           SUM(total_orders) AS total_orders,
           ROUND(AVG(avg_order_value), 2) AS avg_order_value,
           ROUND(AVG(cancellation_rate), 4) AS cancellation_rate
    FROM {{ ref("mart_order_summary") }}
    GROUP BY order_date
),
payments AS (
    SELECT payment_date AS dt,
           SUM(total_payments) AS total_payments,
           SUM(failed_payments) AS failed_payments,
           ROUND(AVG(failure_rate), 4) AS payment_failure_rate,
           ROUND(AVG(success_rate), 4) AS payment_success_rate
    FROM {{ ref("mart_payment_health") }}
    GROUP BY payment_date
),
deliveries AS (
    SELECT delivery_date AS dt,
           SUM(total_deliveries) AS total_deliveries,
           ROUND(AVG(delay_rate), 4) AS delivery_delay_rate,
           ROUND(AVG(on_time_rate), 4) AS delivery_on_time_rate,
           ROUND(AVG(avg_delay_minutes), 1) AS avg_delay_minutes
    FROM {{ ref("mart_delivery_performance") }}
    GROUP BY delivery_date
),
complaints AS (
    SELECT complaint_date AS dt,
           SUM(total_complaints) AS total_complaints,
           ROUND(AVG(severe_rate), 4) AS complaint_severe_rate
    FROM {{ ref("mart_complaint_insights") }}
    GROUP BY complaint_date
),
ai_health AS (
    SELECT run_date AS dt,
           pipeline_health_score, alerts_sent,
           llm_usage_rate, avg_severity, avg_confidence
    FROM {{ ref("mart_ai_diagnosis_summary") }}
),
combined AS (
    SELECT
        COALESCE(o.dt, p.dt, d.dt, c.dt)      AS business_date,
        COALESCE(o.total_orders, 0)            AS total_orders,
        COALESCE(o.total_revenue, 0)           AS total_revenue,
        COALESCE(o.avg_order_value, 0)         AS avg_order_value,
        COALESCE(o.cancellation_rate, 0)       AS cancellation_rate,
        COALESCE(p.total_payments, 0)          AS total_payments,
        COALESCE(p.failed_payments, 0)         AS failed_payments,
        COALESCE(p.payment_failure_rate, 0)    AS payment_failure_rate,
        COALESCE(p.payment_success_rate, 0)    AS payment_success_rate,
        COALESCE(d.total_deliveries, 0)        AS total_deliveries,
        COALESCE(d.delivery_delay_rate, 0)     AS delivery_delay_rate,
        COALESCE(d.delivery_on_time_rate, 0)   AS delivery_on_time_rate,
        COALESCE(d.avg_delay_minutes, 0)       AS avg_delay_minutes,
        COALESCE(c.total_complaints, 0)        AS total_complaints,
        COALESCE(c.complaint_severe_rate, 0)   AS complaint_severe_rate,
        COALESCE(a.pipeline_health_score, 100) AS pipeline_health_score,
        COALESCE(a.alerts_sent, 0)             AS ai_alerts_sent,
        COALESCE(a.llm_usage_rate, 0)          AS ai_llm_usage_rate,
        COALESCE(a.avg_severity, 0)            AS ai_avg_severity,
        COALESCE(a.avg_confidence, 0)          AS ai_avg_confidence,
        CASE
            WHEN COALESCE(p.payment_failure_rate, 0) > 0.30
            THEN 'critical'
            WHEN COALESCE(p.payment_failure_rate, 0) > 0.20
              OR COALESCE(d.delivery_delay_rate, 0) > 0.25
            THEN 'degraded'
            ELSE 'healthy'
        END AS overall_status,
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM orders o
    FULL OUTER JOIN payments   p ON o.dt = p.dt
    FULL OUTER JOIN deliveries d ON COALESCE(o.dt, p.dt) = d.dt
    FULL OUTER JOIN complaints c ON COALESCE(o.dt, p.dt, d.dt) = c.dt
    LEFT  JOIN ai_health       a ON COALESCE(o.dt, p.dt, d.dt, c.dt) = a.dt
)
SELECT * FROM combined
WHERE business_date IS NOT NULL
ORDER BY business_date DESC
""",

}

for path, content in files.items():
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f'OK {path}')

print('')
print('All 12 files written!')