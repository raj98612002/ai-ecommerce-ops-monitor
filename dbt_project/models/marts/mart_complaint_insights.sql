WITH complaints AS (
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
