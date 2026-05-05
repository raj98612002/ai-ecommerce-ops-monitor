"""
Cost-optimized Pipeline Doctor

LLM is called ONLY when:
1. anomaly exists, OR
2. rule-based severity >= 0.7, OR
3. critical business issue is detected

Healthy runs use rule-based diagnosis only.
"""

import logging
from datetime import datetime, timezone

from ai.config import CONFIG
from ai.feature_builder import build_features
from ai.anomaly_detector import detect_anomalies
from ai.llm_engine import diagnose, LLMError
from ai.decision_engine import decide_action
from ai.persistence import save_diagnosis

logger = logging.getLogger(__name__)


def _rule_based_diagnosis(features: dict, anomalies: dict) -> dict:
    pay = features["payments"]["failure_rate"]
    delay = features["deliveries"]["delay_rate"]
    severe = features["complaints"]["severe_rate"]
    orders = features["volume"]["orders"]

    issues = []
    severity = 0.0
    tags = ["rule_based"]

    if pay > CONFIG.payment_failure_threshold:
        issues.append(f"payment failure rate {pay:.1%}")
        severity = max(severity, 0.7)
        tags.append("payment")

    if delay > CONFIG.delivery_delay_threshold:
        issues.append(f"delivery delay rate {delay:.1%}")
        severity = max(severity, 0.6)
        tags.append("delivery")

    if severe > CONFIG.complaint_severity_threshold:
        issues.append(f"severe complaint rate {severe:.1%}")
        severity = max(severity, 0.6)
        tags.append("complaints")

    if anomalies["anomaly_count"] > 0:
        issues.append(f"{anomalies['anomaly_count']} statistical anomalies")
        severity = max(severity, anomalies["max_severity"])
        tags.append("anomaly")

    if orders == 0:
        issues.append("no orders in current window")
        severity = max(severity, 0.5)
        tags.append("volume")

    if not issues:
        status = "healthy"
        summary = "All metrics within normal range."
    elif severity >= 0.7:
        status = "critical"
        summary = "; ".join(issues)
    else:
        status = "degraded"
        summary = "; ".join(issues)

    return {
        "status": status,
        "severity": round(severity, 2),
        "confidence": 0.5,
        "issue_summary": summary,
        "root_cause": "Rule-based diagnosis. LLM skipped to reduce cost.",
        "business_impact": "Low cost monitoring mode used. Escalate to LLM only for serious issues.",
        "suggested_fix": "Review flagged metrics and run deeper investigation if issue continues.",
        "investigation_sql": (
            "SELECT failure_reason, COUNT(*) "
            "FROM raw_payments "
            "WHERE payment_status='failed' "
            "AND payment_ts >= NOW() - INTERVAL '1 hour' "
            "GROUP BY failure_reason ORDER BY 2 DESC;"
        ),
        "tags": tags,
        "_meta": {"provider": "rule_based", "model": "n/a"},
    }


def _should_call_llm(rule_diag: dict, anomalies: dict) -> bool:
    """
    Cost-control gate.
    This prevents OpenAI from running on every DAG cycle.
    """
    if not CONFIG.has_llm_configured:
        return False

    if rule_diag["status"] == "healthy":
        return False

    if rule_diag["severity"] >= 0.7:
        return True

    if anomalies["anomaly_count"] >= 2:
        return True

    if anomalies["max_severity"] >= 0.8:
        return True

    return False


def run() -> dict:
    run_ts = datetime.now(timezone.utc)
    logger.info(f"=== Pipeline Doctor run @ {run_ts.isoformat()} ===")

    features = build_features()
    anomalies = detect_anomalies(features)

    rule_diag = _rule_based_diagnosis(features, anomalies)

    if _should_call_llm(rule_diag, anomalies):
        logger.info("LLM gate passed — calling OpenAI/LLM for deeper diagnosis")

        try:
            diagnosis = diagnose(features, anomalies)
            diagnosis_source = "llm"
        except LLMError as e:
            logger.error(f"LLM failed, using rule-based fallback: {e}")
            diagnosis = rule_diag
            diagnosis_source = "fallback"

    else:
        logger.info("LLM skipped — cost-saving rule-based diagnosis used")
        diagnosis = rule_diag
        diagnosis_source = "rule_based_cost_saved"

    decision = decide_action(diagnosis, anomalies)

    try:
        save_diagnosis(
            run_ts=run_ts,
            features=features,
            anomalies=anomalies,
            diagnosis=diagnosis,
            decision=decision,
            source=diagnosis_source,
        )
    except Exception as e:
        logger.error(f"Failed to persist diagnosis: {e}")

    if decision.get("should_alert"):
        try:
            from ai.telegram_alert import send_alert
            send_alert(diagnosis, decision)
        except Exception as e:
            logger.error(f"Telegram alert failed: {e}")

    logger.info(
        f"Run complete. status={diagnosis['status']}, "
        f"severity={diagnosis['severity']}, "
        f"source={diagnosis_source}, "
        f"alerted={decision.get('should_alert')}"
    )

    return {"diagnosis": diagnosis, "decision": decision}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import json
    print(json.dumps(run(), indent=2, default=str))