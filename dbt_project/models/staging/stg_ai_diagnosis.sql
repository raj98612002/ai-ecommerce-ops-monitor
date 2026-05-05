WITH source AS (
    SELECT * FROM {{ source("raw", "AI_PIPELINE_DIAGNOSIS") }}
),
cleaned AS (
    SELECT
        DIAGNOSIS_ID                                        AS diagnosis_id,
        RUN_TIMESTAMP                                       AS run_timestamp,
        DATE(RUN_TIMESTAMP)                                 AS run_date,
        HOUR(RUN_TIMESTAMP)                                 AS run_hour,
        LOWER(TRIM(COALESCE(STATUS, 'unknown')))            AS status,
        COALESCE(SEVERITY, 0)                               AS severity,
        COALESCE(CONFIDENCE, 0)                             AS confidence,
        ISSUE_SUMMARY                                       AS issue_summary,
        ROOT_CAUSE                                          AS root_cause,
        BUSINESS_IMPACT                                     AS business_impact,
        SUGGESTED_FIX                                       AS suggested_fix,
        LOWER(TRIM(COALESCE(SOURCE, 'unknown')))            AS diagnosis_source,
        COALESCE(LLM_PROVIDER, 'n/a')                       AS llm_provider,
        COALESCE(LLM_MODEL, 'n/a')                          AS llm_model,
        COALESCE(LLM_LATENCY_MS, 0)                         AS llm_latency_ms,
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
    WHERE DIAGNOSIS_ID IS NOT NULL
)
SELECT * FROM cleaned