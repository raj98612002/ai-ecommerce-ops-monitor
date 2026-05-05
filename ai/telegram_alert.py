"""Pure delivery channel. No business logic — that lives in decision_engine."""
import logging
import requests
from ai.config import CONFIG

logger = logging.getLogger(__name__)


def _format(diagnosis: dict, decision: dict) -> str:
    sev_pct = int(diagnosis["severity"] * 100)
    conf_pct = int(diagnosis["confidence"] * 100)
    icon = {"healthy": "🟢", "degraded": "🟡", "critical": "🔴"}.get(diagnosis["status"], "⚪")
    return (
        f"{icon} *Pipeline {diagnosis['status'].upper()}*\n"
        f"*Severity:* {sev_pct}% | *Confidence:* {conf_pct}%\n\n"
        f"*Issue:* {diagnosis['issue_summary']}\n"
        f"*Root Cause:* {diagnosis['root_cause']}\n"
        f"*Business Impact:* {diagnosis['business_impact']}\n"
        f"*Suggested Fix:* {diagnosis['suggested_fix']}\n\n"
        f"*Investigation SQL:*\n```\n{diagnosis['investigation_sql']}\n```\n"
        f"_Source: {diagnosis['_meta'].get('provider', 'unknown')} "
        f"| fingerprint: {decision['alert_fingerprint']}_"
    )


def send_alert(diagnosis: dict, decision: dict) -> bool:
    if not CONFIG.telegram_bot_token or not CONFIG.telegram_chat_id:
        logger.warning("Telegram not configured — skipping alert.")
        return False

    url = f"https://api.telegram.org/bot{CONFIG.telegram_bot_token}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": CONFIG.telegram_chat_id,
            "text": _format(diagnosis, decision),
            "parse_mode": "Markdown",
        }, timeout=10)
        resp.raise_for_status()
        logger.info(f"Telegram alert sent (fingerprint={decision['alert_fingerprint']})")
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False