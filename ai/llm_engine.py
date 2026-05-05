"""
================================================================
  LLM ENGINE — provider-agnostic AI diagnosis
================================================================
  Responsibilities:
    1. Build the system + user prompts
    2. Call OpenAI or Anthropic (chosen via CONFIG.llm_provider)
    3. Validate the response against a strict JSON schema
    4. Retry on failure with exponential backoff
    5. Raise LLMError on unrecoverable failure (caller falls back)

  This module is provider-agnostic. Switching providers is one
  line in .env: LLM_PROVIDER=openai → LLM_PROVIDER=anthropic.
================================================================
"""

import json
import logging
import time

from ai.config import CONFIG

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------
#  STRICT OUTPUT SCHEMA
# ----------------------------------------------------------------
# Every LLM response MUST contain these keys. Any missing key =
# we treat the response as invalid and either retry or fall back.
# ----------------------------------------------------------------
DIAGNOSIS_SCHEMA_KEYS = {
    "status", "severity", "confidence",
    "issue_summary", "root_cause", "business_impact",
    "suggested_fix", "investigation_sql", "tags",
}

ALLOWED_STATUSES = {"healthy", "degraded", "critical"}


# ================================================================
#  CUSTOM EXCEPTION
# ================================================================
class LLMError(Exception):
    """Raised when the LLM call fails or returns invalid output."""
    pass


# ================================================================
#  PROMPT BUILDERS
# ================================================================
def _build_system_prompt() -> str:
    """The 'persona + rules' prompt. Sets behavior, never changes per-call."""
    return (
        "You are a Senior Data Engineer at a high-traffic e-commerce company. "
        "You receive a structured snapshot of pipeline metrics and detected "
        "anomalies. Your job is to diagnose the root cause and recommend action.\n\n"
        "STRICT RULES:\n"
        "1. Output ONLY valid JSON. No markdown, no preamble, no commentary.\n"
        "2. Base every claim on the provided metrics. Do NOT invent numbers, "
        "table names, or services.\n"
        "3. If data is insufficient for a confident diagnosis, set "
        "confidence < 0.5 and say so in issue_summary.\n"
        "4. investigation_sql must reference only these tables: "
        "raw_orders, raw_payments, raw_deliveries, raw_refunds, raw_complaints.\n"
        "5. severity must reflect business impact, not just metric size.\n\n"
        "OUTPUT JSON SCHEMA:\n"
        "{\n"
        '  "status": "healthy" | "degraded" | "critical",\n'
        '  "severity": float (0.0-1.0),\n'
        '  "confidence": float (0.0-1.0),\n'
        '  "issue_summary": "one sentence",\n'
        '  "root_cause": "specific technical cause grounded in the metrics",\n'
        '  "business_impact": "revenue / customer / SLA impact in one sentence",\n'
        '  "suggested_fix": "concrete action a senior engineer would take next",\n'
        '  "investigation_sql": "a SQL query to dig deeper",\n'
        '  "tags": ["payment", "delivery", "complaints", "volume", "anomaly"]\n'
        "}"
    )


def _build_user_prompt(features: dict, anomalies: dict) -> str:
    """The 'data' prompt. Changes every call — feeds the LLM the snapshot."""
    return (
        "Analyze the following pipeline snapshot.\n\n"
        f"=== CURRENT METRICS (last {features.get('window_minutes', '?')} min) ===\n"
        f"{json.dumps(features, indent=2, default=str)}\n\n"
        f"=== ANOMALIES vs {CONFIG.baseline_window_hours}h baseline ===\n"
        f"{json.dumps(anomalies, indent=2, default=str)}\n\n"
        "Return your diagnosis as JSON."
    )


# ================================================================
#  PROVIDER CALLS
# ================================================================
def _call_openai(system: str, user: str) -> str:
    """Call OpenAI with JSON mode forced on."""
    from openai import OpenAI

    client = OpenAI(
        api_key=CONFIG.llm_api_key,
        timeout=CONFIG.llm_timeout_seconds,
    )

    resp = client.chat.completions.create(
        model=CONFIG.llm_model,
        temperature=CONFIG.llm_temperature,
        max_tokens=CONFIG.llm_max_tokens,
        response_format={"type": "json_object"},   # forces valid JSON
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
    )

    content = resp.choices[0].message.content
    if not content:
        raise LLMError("OpenAI returned empty content (possibly content-filtered)")
    return content


def _call_anthropic(system: str, user: str) -> str:
    """Call Anthropic Claude. No native JSON mode → we instruct it explicitly."""
    import anthropic

    client = anthropic.Anthropic(
        api_key=CONFIG.llm_api_key,
        timeout=CONFIG.llm_timeout_seconds,
    )

    resp = client.messages.create(
        model=CONFIG.llm_model,
        max_tokens=CONFIG.llm_max_tokens,
        temperature=CONFIG.llm_temperature,
        system=system,
        messages=[
            {"role": "user",
             "content": user + "\n\nRespond with ONLY a valid JSON object."},
        ],
    )

    if not resp.content or not resp.content[0].text:
        raise LLMError("Anthropic returned empty content")
    return resp.content[0].text


# ================================================================
#  RESPONSE VALIDATION
# ================================================================
def _validate_diagnosis(raw: str) -> dict:
    """
    Parse + validate the LLM's response.
    Catches:
      - invalid JSON
      - missing keys
      - bad types or out-of-range values
      - invalid status enum
    Raises LLMError on any failure.
    """
    # 1. Parse JSON
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise LLMError(f"Invalid JSON from LLM: {e}")

    if not isinstance(parsed, dict):
        raise LLMError(f"LLM returned non-object JSON: {type(parsed).__name__}")

    # 2. Check required keys
    missing = DIAGNOSIS_SCHEMA_KEYS - set(parsed.keys())
    if missing:
        raise LLMError(f"LLM response missing required keys: {missing}")

    # 3. Validate severity & confidence (must be float 0-1, NOT bool)
    for key in ("severity", "confidence"):
        value = parsed.get(key)
        # In Python, bool is a subclass of int — reject it explicitly
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise LLMError(f"Invalid type for {key}: {type(value).__name__}")
        if not (0.0 <= float(value) <= 1.0):
            raise LLMError(f"{key} out of range [0,1]: {value}")

    # 4. Validate status enum
    if parsed["status"] not in ALLOWED_STATUSES:
        raise LLMError(f"Invalid status '{parsed['status']}'. "
                       f"Must be one of {ALLOWED_STATUSES}")

    # 5. Validate tags is a list
    if not isinstance(parsed.get("tags"), list):
        raise LLMError(f"Invalid tags: must be a list, got {type(parsed.get('tags'))}")

    return parsed


# ================================================================
#  PUBLIC API
# ================================================================
def diagnose(features: dict, anomalies: dict) -> dict:
    """
    Get a diagnosis from the LLM.

    Returns a validated diagnosis dict with an extra "_meta" field
    containing provider/model/latency info.

    Raises LLMError if all retries fail (caller should fall back).
    """
    # Pre-flight: do we even have an API key?
    if not CONFIG.has_llm_configured:
        raise LLMError(f"No API key configured for provider '{CONFIG.llm_provider}'")

    system = _build_system_prompt()
    user   = _build_user_prompt(features, anomalies)
    last_error = None

    # Retry loop with exponential backoff (1s, 2s, 4s, ...)
    for attempt in range(CONFIG.llm_max_retries + 1):
        try:
            t_start = time.time()

            if CONFIG.llm_provider == "anthropic":
                raw = _call_anthropic(system, user)
            else:
                raw = _call_openai(system, user)

            latency_ms = int((time.time() - t_start) * 1000)
            diagnosis = _validate_diagnosis(raw)

            # Attach metadata for debugging / cost tracking
            diagnosis["_meta"] = {
                "provider":   CONFIG.llm_provider,
                "model":      CONFIG.llm_model,
                "latency_ms": latency_ms,
                "attempt":    attempt + 1,
            }

            logger.info(f"✅ LLM diagnosis OK in {latency_ms}ms "
                        f"(attempt {attempt + 1})")
            return diagnosis

        except Exception as e:
            last_error = e
            logger.warning(f"⚠️  LLM attempt {attempt + 1} failed: {e}")

            # Don't sleep after the last attempt
            if attempt < CONFIG.llm_max_retries:
                backoff = 2 ** attempt
                logger.info(f"   Retrying in {backoff}s...")
                time.sleep(backoff)

    # All retries exhausted
    raise LLMError(f"All {CONFIG.llm_max_retries + 1} LLM attempts failed. "
                   f"Last error: {last_error}")