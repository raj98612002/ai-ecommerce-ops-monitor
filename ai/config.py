"""
================================================================
  CENTRAL CONFIGURATION FOR THE AI LAYER
================================================================
  All env vars are read here — once.
  Every other module imports CONFIG from this file.
  This way, if a setting changes, we change it in ONE place.
================================================================
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AIConfig:
    """Frozen config — immutable after creation. Safer for production."""

    # ------------------------------------------------------------
    # LLM PROVIDER & MODEL
    # ------------------------------------------------------------
    llm_provider: str = os.getenv("LLM_PROVIDER", "openai")          # openai | anthropic
    llm_model:    str = os.getenv("LLM_MODEL",    "gpt-4o-mini")

    # Separate API keys for each provider (we pick the right one via property)
    openai_api_key:    str = os.getenv("OPENAI_API_KEY",    "")
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")

    # LLM call settings
    llm_timeout_seconds: int = 30
    llm_max_retries:     int = 2
    llm_temperature:     float = 0.1
    llm_max_tokens:      int = 1024

    # ------------------------------------------------------------
    # POSTGRES (your e-commerce DB)
    # ------------------------------------------------------------
    pg_host:     str = os.getenv("PG_HOST",     "postgres")           # docker service name
    pg_port:     int = int(os.getenv("PG_PORT", "5432"))
    pg_db:       str = os.getenv("PG_DB",       "ecommerce_ops")
    pg_user:     str = os.getenv("PG_USER",     "postgres")
    pg_password: str = os.getenv("PG_PASSWORD", "postgres")

    # ------------------------------------------------------------
    # ALERTING THRESHOLDS
    # ------------------------------------------------------------
    payment_failure_threshold:    float = 0.20    # 20% failed payments → alert
    delivery_delay_threshold:     float = 0.25    # 25% late deliveries → alert
    complaint_severity_threshold: float = 0.30    # 30% severe complaints → alert
    anomaly_zscore_threshold:     float = 2.5     # 2.5σ deviation = anomaly

    # ------------------------------------------------------------
    # ANALYSIS WINDOWS
    # ------------------------------------------------------------
    current_window_minutes: int = 30              # "now" window
    baseline_window_hours:  int = 24              # "normal" baseline

    # ------------------------------------------------------------
    # TELEGRAM
    # ------------------------------------------------------------
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id:   str = os.getenv("TELEGRAM_CHAT_ID",   "")

    # ------------------------------------------------------------
    # AWS S3 (next phase)
    # ------------------------------------------------------------
    aws_region:        str = os.getenv("AWS_REGION",        "ap-south-1")
    s3_backup_bucket:  str = os.getenv("S3_BACKUP_BUCKET",  "")

    # ------------------------------------------------------------
    # PROPERTIES
    # ------------------------------------------------------------
    @property
    def llm_api_key(self) -> str:
        """Return the right API key based on the chosen provider."""
        if self.llm_provider == "anthropic":
            return self.anthropic_api_key
        return self.openai_api_key

    @property
    def has_llm_configured(self) -> bool:
        """True if we have a usable API key for the selected provider."""
        return bool(self.llm_api_key.strip())

    @property
    def has_telegram_configured(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)


# Singleton instance — import this everywhere
CONFIG = AIConfig()