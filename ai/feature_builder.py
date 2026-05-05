"""
================================================================
  FEATURE BUILDER
================================================================
  Pulls a structured snapshot of pipeline metrics from Postgres.
  Deterministic — no AI here. The LLM consumes this dict.

  Uses your REAL column names:
    - raw_orders.order_ts, order_status, order_amount, city
    - raw_payments.payment_ts, payment_status, failure_reason
    - raw_deliveries.created_at, delivery_status,
                     promised_minutes, actual_minutes, city
    - raw_complaints.complaint_ts, ai_category, ai_severity
================================================================
"""
import logging
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from ai.config import CONFIG

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------
#  DB HELPER
# ----------------------------------------------------------------
@contextmanager
def pg_cursor():
    conn = psycopg2.connect(
        host=CONFIG.pg_host, port=CONFIG.pg_port,
        dbname=CONFIG.pg_db, user=CONFIG.pg_user,
        password=CONFIG.pg_password,
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _scalar(cur, sql: str, params: tuple = ()) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return None
    return list(row.values())[0]


def _rows(cur, sql: str, params: tuple = ()) -> list[dict]:
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


# ================================================================
#  PUBLIC API
# ================================================================
def build_features(window_minutes: int = None) -> dict:
    """
    Returns a structured snapshot of pipeline health for the
    given window (default = CONFIG.current_window_minutes).
    """
    window = window_minutes or CONFIG.current_window_minutes

    with pg_cursor() as cur:

        # --------------------------------------------------------
        #  ORDERS
        # --------------------------------------------------------
        orders_recent = _scalar(cur, f"""
            SELECT COUNT(*) FROM raw_orders
            WHERE order_ts >= NOW() - INTERVAL '{window} minutes'
        """) or 0

        orders_revenue = _scalar(cur, f"""
            SELECT COALESCE(SUM(order_amount), 0) FROM raw_orders
            WHERE order_ts >= NOW() - INTERVAL '{window} minutes'
              AND order_status NOT IN ('cancelled', 'failed')
        """) or 0

        top_order_cities = _rows(cur, f"""
            SELECT city, COUNT(*) AS cnt
            FROM raw_orders
            WHERE order_ts >= NOW() - INTERVAL '{window} minutes'
              AND city IS NOT NULL
            GROUP BY city ORDER BY cnt DESC LIMIT 5
        """)

        # --------------------------------------------------------
        #  PAYMENTS
        # --------------------------------------------------------
        payment_stats = _rows(cur, f"""
            SELECT
                COUNT(*)                                          AS total,
                COUNT(*) FILTER (WHERE payment_status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE payment_status = 'success') AS success
            FROM raw_payments
            WHERE payment_ts >= NOW() - INTERVAL '{window} minutes'
        """)[0]
        total_pay    = payment_stats["total"] or 0
        failure_rate = (payment_stats["failed"] / total_pay) if total_pay else 0.0

        top_failure_reasons = _rows(cur, f"""
            SELECT failure_reason, COUNT(*) AS cnt
            FROM raw_payments
            WHERE payment_status = 'failed'
              AND payment_ts >= NOW() - INTERVAL '{window} minutes'
              AND failure_reason IS NOT NULL
            GROUP BY failure_reason ORDER BY cnt DESC LIMIT 5
        """)

        # --------------------------------------------------------
        #  DELIVERIES — uses created_at (no event timestamp)
        #  "Delayed" = actual_minutes > promised_minutes
        # --------------------------------------------------------
        delivery_stats = _rows(cur, f"""
            SELECT
                COUNT(*)                                                 AS total,
                COUNT(*) FILTER (WHERE actual_minutes > promised_minutes) AS delayed,
                COALESCE(AVG(actual_minutes - promised_minutes), 0)::float AS avg_delay_min
            FROM raw_deliveries
            WHERE created_at >= NOW() - INTERVAL '{window} minutes'
              AND actual_minutes IS NOT NULL
              AND promised_minutes IS NOT NULL
        """)[0]
        total_del   = delivery_stats["total"] or 0
        delay_rate  = (delivery_stats["delayed"] / total_del) if total_del else 0.0

        top_delayed_cities = _rows(cur, f"""
            SELECT city, COUNT(*) AS cnt
            FROM raw_deliveries
            WHERE actual_minutes > promised_minutes
              AND created_at >= NOW() - INTERVAL '{window} minutes'
              AND city IS NOT NULL
            GROUP BY city ORDER BY cnt DESC LIMIT 5
        """)

        # --------------------------------------------------------
        #  COMPLAINTS
        # --------------------------------------------------------
        complaint_stats = _rows(cur, f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE ai_severity IN ('high', 'critical')
                ) AS severe
            FROM raw_complaints
            WHERE complaint_ts >= NOW() - INTERVAL '{window} minutes'
        """)[0]
        total_comp  = complaint_stats["total"] or 0
        severe_rate = (complaint_stats["severe"] / total_comp) if total_comp else 0.0

        top_complaint_categories = _rows(cur, f"""
            SELECT ai_category, COUNT(*) AS cnt
            FROM raw_complaints
            WHERE complaint_ts >= NOW() - INTERVAL '{window} minutes'
              AND ai_category IS NOT NULL
            GROUP BY ai_category ORDER BY cnt DESC LIMIT 5
        """)

    # --------------------------------------------------------
    #  ASSEMBLE FINAL FEATURE DICT
    # --------------------------------------------------------
    features = {
        "window_minutes": window,
        "volume": {
            "orders":           orders_recent,
            "revenue":          float(orders_revenue),
            "top_order_cities": top_order_cities,
        },
        "payments": {
            "total":               total_pay,
            "failed":              payment_stats["failed"],
            "success":             payment_stats["success"],
            "failure_rate":        round(failure_rate, 4),
            "top_failure_reasons": top_failure_reasons,
        },
        "deliveries": {
            "total":              total_del,
            "delayed":            delivery_stats["delayed"],
            "delay_rate":         round(delay_rate, 4),
            "avg_delay_minutes":  round(delivery_stats["avg_delay_min"], 1),
            "top_delayed_cities": top_delayed_cities,
        },
        "complaints": {
            "total":          total_comp,
            "severe":         complaint_stats["severe"],
            "severe_rate":    round(severe_rate, 4),
            "top_categories": top_complaint_categories,
        },
    }

    logger.info(f"Built features: orders={orders_recent}, "
                f"pay_fail={failure_rate:.2%}, "
                f"delay={delay_rate:.2%}, "
                f"severe_complaints={severe_rate:.2%}")
    return features