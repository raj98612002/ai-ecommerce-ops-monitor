WITH
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
