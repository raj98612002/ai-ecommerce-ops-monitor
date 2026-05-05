"""
Post-LLM business logic.
Decides whether to alert, suppress (dedup), or escalate.
This is intentionally NOT done by the LLM — alerting policy must be auditable.
"""
import logging
import hashlib
from datetime import datetime, timedelta

from ai.feature_builder import pg_cursor

logger = logging.getLogger(__name__)

ALERT_DEDUP_WINDOW_MINUTES = 30


def _fingerprint(diagnosis: dict) -> str:
    key = f"{diagnosis['status']}|{','.join(sorted(diagnosis.get('tags', [])))}"
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def _recently_alerted(fingerprint: str) -> bool:
    cutoff = datetime.utcnow() - timedelta(minutes=ALERT_DEDUP_WINDOW_MINUTES)
    with pg_cursor() as cur:
        cur.execute("""
            SELECT 1 FROM ai_pipeline_diagnosis
            WHERE alert_fingerprint = %s
              AND alerted_at >= %s
            LIMIT 1
        """, (fingerprint, cutoff))
        return cur.fetchone() is not None


def decide_action(diagnosis: dict, anomalies: dict) -> dict:
    fingerprint = _fingerprint(diagnosis)
    severity = diagnosis.get("severity", 0)
    confidence = diagnosis.get("confidence", 0)
    status = diagnosis.get("status", "healthy")

    should_alert = (
        status in {"degraded", "critical"}
        and severity >= 0.5
        and confidence >= 0.4
    )

    if should_alert and _recently_alerted(fingerprint):
        logger.info(f"Suppressing duplicate alert (fingerprint={fingerprint})")
        should_alert = False
        suppression_reason = "duplicate_within_window"
    else:
        suppression_reason = None

    routing = "telegram"
    if status == "critical" and severity >= 0.85:
        routing = "telegram+pager"  # placeholder for future PagerDuty

    return {
        "should_alert": should_alert,
        "alert_fingerprint": fingerprint,
        "routing": routing,
        "suppression_reason": suppression_reason,
    }