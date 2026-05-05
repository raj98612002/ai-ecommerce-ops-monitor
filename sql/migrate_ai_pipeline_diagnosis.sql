-- ============================================================
-- MIGRATION: Upgrade ai_pipeline_diagnosis to full schema
-- Safe to run multiple times (uses IF NOT EXISTS)
-- Preserves any existing rows
-- ============================================================

-- Add missing columns one by one
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS run_timestamp     TIMESTAMP;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS severity          FLOAT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS confidence        FLOAT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS issue_summary     TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS root_cause        TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS business_impact   TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS suggested_fix     TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS investigation_sql TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS tags              TEXT[];
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS features_json     JSONB;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS anomalies_json    JSONB;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS source            TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS llm_provider      TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS llm_model         TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS llm_latency_ms    INT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS alert_fingerprint TEXT;
ALTER TABLE ai_pipeline_diagnosis ADD COLUMN IF NOT EXISTS alerted_at        TIMESTAMP;

-- Add helpful index for alert dedup queries
CREATE INDEX IF NOT EXISTS idx_aipd_fingerprint_time
    ON ai_pipeline_diagnosis (alert_fingerprint, alerted_at DESC);

CREATE INDEX IF NOT EXISTS idx_aipd_run_time
    ON ai_pipeline_diagnosis (run_timestamp DESC);

-- Verify the schema
\d ai_pipeline_diagnosis