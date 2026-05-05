"""
================================================================
  ANOMALY DETECTOR
================================================================
  Compares current metrics against a 24h rolling baseline.
  Uses z-score on 30-min buckets.
  Cheap, statistical — runs BEFORE the LLM to save tokens.
================================================================
"""
import logging
from ai.config import CONFIG
from ai.feature_builder import pg_cursor, _rows

logger = logging.getLogger(__name__)


def _baseline_stats(metric_sql: str) -> tuple[float, float]:
    """Returns (mean, stddev) for the metric across historical buckets."""
    sql = f"""
        WITH buckets AS ({metric_sql})
        SELECT
            COALESCE(AVG(value), 0)    AS mean,
            COALESCE(STDDEV(value), 0) AS std
        FROM buckets
    """
    with pg_cursor() as cur:
        result = _rows(cur, sql)[0]
    return float(result["mean"]), float(result["std"])


def detect_anomalies(features: dict) -> dict:
    """
    Returns:
      {
        "anomalies":    [{"metric": ..., "current": ..., "z_score": ...}, ...],
        "anomaly_count": int,
        "max_severity":  float (0-1)
      }
    """
    anomalies = []

    # ----------------------------------------------------------------
    # Each spec: (metric_name, current_value, historical_bucket_sql)
    # ----------------------------------------------------------------
    metric_specs = [

        (
            "payment_failure_rate",
            features["payments"]["failure_rate"],
            f"""
                SELECT
                    date_trunc('hour', payment_ts)
                      + INTERVAL '30 min' * (EXTRACT(MINUTE FROM payment_ts)::int / 30)
                      AS bucket,
                    COUNT(*) FILTER (WHERE payment_status='failed')::float
                      / NULLIF(COUNT(*), 0) AS value
                FROM raw_payments
                WHERE payment_ts >= NOW() - INTERVAL '{CONFIG.baseline_window_hours} hours'
                GROUP BY bucket
                HAVING COUNT(*) > 0
            """,
        ),

        (
            "delivery_delay_rate",
            features["deliveries"]["delay_rate"],
            f"""
                SELECT
                    date_trunc('hour', created_at)
                      + INTERVAL '30 min' * (EXTRACT(MINUTE FROM created_at)::int / 30)
                      AS bucket,
                    COUNT(*) FILTER (WHERE actual_minutes > promised_minutes)::float
                      / NULLIF(COUNT(*), 0) AS value
                FROM raw_deliveries
                WHERE created_at >= NOW() - INTERVAL '{CONFIG.baseline_window_hours} hours'
                  AND actual_minutes IS NOT NULL
                  AND promised_minutes IS NOT NULL
                GROUP BY bucket
                HAVING COUNT(*) > 0
            """,
        ),

        (
            "order_volume",
            features["volume"]["orders"],
            f"""
                SELECT
                    date_trunc('hour', order_ts)
                      + INTERVAL '30 min' * (EXTRACT(MINUTE FROM order_ts)::int / 30)
                      AS bucket,
                    COUNT(*)::float AS value
                FROM raw_orders
                WHERE order_ts >= NOW() - INTERVAL '{CONFIG.baseline_window_hours} hours'
                GROUP BY bucket
            """,
        ),

        (
            "complaint_severe_rate",
            features["complaints"]["severe_rate"],
            f"""
                SELECT
                    date_trunc('hour', complaint_ts)
                      + INTERVAL '30 min' * (EXTRACT(MINUTE FROM complaint_ts)::int / 30)
                      AS bucket,
                    COUNT(*) FILTER (WHERE ai_severity IN ('high','critical'))::float
                      / NULLIF(COUNT(*), 0) AS value
                FROM raw_complaints
                WHERE complaint_ts >= NOW() - INTERVAL '{CONFIG.baseline_window_hours} hours'
                GROUP BY bucket
                HAVING COUNT(*) > 0
            """,
        ),
    ]

    max_severity = 0.0

    for name, current, sql in metric_specs:
        try:
            mean, std = _baseline_stats(sql)
            if std == 0:
                # No variance → skip (nothing to compare against)
                continue

            z = (current - mean) / std
            if abs(z) >= CONFIG.anomaly_zscore_threshold:
                # Normalize severity: z=2.5 → 0.5, z=5+ → 1.0
                severity = min(abs(z) / 5.0, 1.0)
                max_severity = max(max_severity, severity)
                anomalies.append({
                    "metric":        name,
                    "current":       round(current, 4),
                    "baseline_mean": round(mean, 4),
                    "baseline_std":  round(std, 4),
                    "z_score":       round(z, 2),
                    "direction":     "spike" if z > 0 else "drop",
                    "severity":      round(severity, 2),
                })
        except Exception as e:
            logger.warning(f"Anomaly check failed for {name}: {e}")

    logger.info(f"Detected {len(anomalies)} anomalies, "
                f"max_severity={max_severity:.2f}")

    return {
        "anomalies":     anomalies,
        "anomaly_count": len(anomalies),
        "max_severity":  round(max_severity, 2),
    }