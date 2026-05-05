WITH diagnoses AS (
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
