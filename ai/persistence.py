"""
================================================================
  PERSISTENCE
================================================================
  Single source of truth for writing AI diagnoses to Postgres.
  Schema must match the migration SQL we ran.
================================================================
"""
import json
import logging
from datetime import datetime

from ai.feature_builder import pg_cursor

logger = logging.getLogger(__name__)


def save_diagnosis(
    run_ts:    datetime,
    features:  dict,
    anomalies: dict,
    diagnosis: dict,
    decision:  dict,
    source:    str,
) -> None:
    """Write one diagnosis row with full audit context."""
    meta       = diagnosis.get("_meta", {}) or {}
    alerted_at = datetime.utcnow() if decision.get("should_alert") else None

    with pg_cursor() as cur:
        cur.execute("""
            INSERT INTO ai_pipeline_diagnosis (
                run_timestamp,
                status,
                severity,
                confidence,
                issue_summary,
                root_cause,
                business_impact,
                suggested_fix,
                investigation_sql,
                tags,
                features_json,
                anomalies_json,
                source,
                llm_provider,
                llm_model,
                llm_latency_ms,
                alert_fingerprint,
                alerted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            run_ts,
            diagnosis.get("status"),
            diagnosis.get("severity"),
            diagnosis.get("confidence"),
            diagnosis.get("issue_summary"),
            diagnosis.get("root_cause"),
            diagnosis.get("business_impact"),
            diagnosis.get("suggested_fix"),
            diagnosis.get("investigation_sql"),
            diagnosis.get("tags", []),
            json.dumps(features,  default=str),
            json.dumps(anomalies, default=str),
            source,
            meta.get("provider"),
            meta.get("model"),
            meta.get("latency_ms"),
            decision.get("alert_fingerprint"),
            alerted_at,
        ))

    logger.info(f"💾 Diagnosis persisted (source={source})")