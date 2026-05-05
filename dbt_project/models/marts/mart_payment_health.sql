WITH payments AS (
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
