"""
================================================================
  E-COMMERCE OPS MONITORING DAG
================================================================

  WHAT THIS DAG DOES:
  -------------------
  Every 15 minutes, this DAG wakes up and checks the health of
  our real-time e-commerce pipeline. If something looks wrong,
  the AI Doctor diagnoses the issue and (optionally) sends a
  Telegram alert.

  TASKS (in order):
    1. check_postgres          → Is the database alive?
    2. check_recent_orders     → Are new orders flowing in?
    3. check_payment_failure   → How many payments are failing?
    4. run_ai_doctor           → AI analyzes everything + alerts
    5. s3_backup_placeholder   → Stub for next phase (AWS S3)

  TASK FLOW:
                ┌──→ check_recent_orders ────┐
   check_pg ───┤                              ├──→ ai_doctor ──→ s3_backup
                └──→ check_payment_failure ──┘

  Tasks 2 and 3 run in PARALLEL (faster).
  Task 4 waits for both, then runs the AI.
  Task 5 always runs (even if upstream failed) — useful for backups.

  Author : raj_data_engineer
  Schedule: every 15 minutes
================================================================
"""

# ----------------------------------------------------------------
#  IMPORTS
# ----------------------------------------------------------------
import os
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import psycopg2

# pyright: reportMissingImports=false

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# ================================================================
#  SECTION 1 — CONFIGURATION
# ================================================================
#  All settings come from .env via docker-compose.
#  Defaults are safe for local dev.
# ================================================================

POSTGRES_CONFIG = {
    "host":     os.getenv("PG_HOST",     "postgres"),     # docker service name
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "ecommerce_ops"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

# Business thresholds
RECENT_ORDERS_WINDOW_MINUTES = 15
PAYMENT_FAILURE_WINDOW_HOURS = 1
MAX_PAYMENT_FAILURE_RATE     = 0.30
MIN_EXPECTED_ORDERS          = 1

logger = logging.getLogger(__name__)


# ================================================================
#  SECTION 2 — DEFAULT ARGS
# ================================================================
default_args = {
    "owner":            "raj_data_engineer",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ================================================================
#  SECTION 3 — DB HELPER (context manager — no leaks)
# ================================================================
@contextmanager
def pg_cursor():
    """
    Open a Postgres cursor, commit on success, rollback on error,
    always close the connection. Use with `with`.
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ================================================================
#  SECTION 4 — TASK FUNCTIONS
# ================================================================


# ----------------------------------------------------------------
#  TASK 1: check_postgres
# ----------------------------------------------------------------
#  WHY: If the database is down, no other task can work.
# ----------------------------------------------------------------
def check_postgres(**context):
    logger.info("🔍 Checking Postgres connection...")

    try:
        with pg_cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()

        if result and result[0] == 1:
            logger.info("✅ Postgres is healthy")
            return "ok"

        raise AirflowFailException("Postgres returned unexpected result")

    except Exception as e:
        logger.error(f"❌ Postgres check failed: {e}")
        raise AirflowFailException(f"Postgres unreachable: {e}")


# ----------------------------------------------------------------
#  TASK 2: check_recent_orders
# ----------------------------------------------------------------
#  WHY: If orders stop flowing in, our Kafka consumer might be broken.
# ----------------------------------------------------------------
def check_recent_orders(**context):
    logger.info(f"🔍 Counting orders in the last "
                f"{RECENT_ORDERS_WINDOW_MINUTES} minutes...")

    sql = """
        SELECT COUNT(*)
        FROM raw_orders
        WHERE order_ts >= NOW() - (%s || ' minutes')::interval
    """

    with pg_cursor() as cur:
        cur.execute(sql, (str(RECENT_ORDERS_WINDOW_MINUTES),))
        count = cur.fetchone()[0]

    logger.info(f"📦 Recent orders count: {count}")

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="orders_count", value=count)

    if count < MIN_EXPECTED_ORDERS:
        logger.warning(f"⚠️  Low order volume detected: {count}")

    return count


# ----------------------------------------------------------------
#  TASK 3: check_payment_failure
# ----------------------------------------------------------------
#  WHY: A spike in failed payments usually means the payment
#       gateway is having issues. We measure the RATE so it
#       works at any traffic level.
# ----------------------------------------------------------------
def check_payment_failure(**context):
    logger.info(f"🔍 Calculating payment failure rate "
                f"(last {PAYMENT_FAILURE_WINDOW_HOURS} hour)...")

    sql = """
        SELECT
            COUNT(*) FILTER (WHERE payment_status = 'failed')::float
            / NULLIF(COUNT(*), 0) AS failure_rate,
            COUNT(*) AS total_payments
        FROM raw_payments
        WHERE payment_ts >= NOW() - (%s || ' hours')::interval
    """

    with pg_cursor() as cur:
        cur.execute(sql, (str(PAYMENT_FAILURE_WINDOW_HOURS),))
        row = cur.fetchone()

    failure_rate = row[0] or 0.0
    total        = row[1] or 0

    logger.info(f"💳 Total payments: {total} | "
                f"Failure rate: {failure_rate:.2%}")

    context["ti"].xcom_push(key="failure_rate",  value=failure_rate)
    context["ti"].xcom_push(key="payment_total", value=total)

    if failure_rate > MAX_PAYMENT_FAILURE_RATE:
        logger.error(f"🚨 Failure rate {failure_rate:.2%} "
                     f"exceeds threshold {MAX_PAYMENT_FAILURE_RATE:.2%}!")

    return failure_rate


# ----------------------------------------------------------------
#  TASK 4: run_ai_doctor
# ----------------------------------------------------------------
#  WHY: The AI looks at ALL the metrics together, finds the root
#       cause, and decides whether to alert. If it fails, we use
#       a clean fallback so the pipeline never goes silent.
# ----------------------------------------------------------------
def run_ai_doctor(**context):
    logger.info("🧠 Running AI Pipeline Doctor...")

    ti           = context["ti"]
    orders_count = ti.xcom_pull(task_ids="check_recent_orders",  key="orders_count")
    failure_rate = ti.xcom_pull(task_ids="check_payment_failure", key="failure_rate")

    logger.info(f"AI inputs → orders={orders_count}, failure_rate={failure_rate}")

    # ---- TRY: full AI pipeline (LLM-based) ----
    try:
        from ai.pipeline_doctor import run as run_doctor

        result    = run_doctor()
        diagnosis = result["diagnosis"]
        decision  = result["decision"]

        logger.info(f"✅ AI diagnosis: {diagnosis['status']} "
                    f"(severity={diagnosis['severity']}, "
                    f"confidence={diagnosis['confidence']})")

        ti.xcom_push(key="ai_status",   value=diagnosis["status"])
        ti.xcom_push(key="ai_severity", value=diagnosis["severity"])
        ti.xcom_push(key="alert_sent",  value=decision["should_alert"])

        return diagnosis["status"]

    # ---- EXCEPT: AI broke → store fallback diagnosis ----
    except Exception as e:
        logger.error(f"❌ AI Doctor failed: {e}")
        logger.warning("Using minimal fallback diagnosis")

        try:
            # Use the proper persistence module if available
            from ai.persistence import save_diagnosis

            fallback_diagnosis = {
                "status":            "degraded",
                "severity":          0.5,
                "confidence":        0.3,
                "issue_summary":     f"AI unavailable. orders={orders_count}, "
                                     f"failure_rate={failure_rate}",
                "root_cause":        "AI module failed — manual investigation needed",
                "business_impact":   "Unknown without AI diagnosis",
                "suggested_fix":     "Check AI service logs and Postgres",
                "investigation_sql": "SELECT * FROM raw_payments WHERE "
                                     "payment_status='failed' AND payment_ts >= "
                                     "NOW() - INTERVAL '1 hour';",
                "tags":              ["dag_fallback"],
                "_meta":             {"provider": "fallback", "model": "n/a"},
            }
            fallback_decision = {
                "should_alert":        False,
                "alert_fingerprint":   "dag_fallback",
                "routing":             None,
                "suppression_reason":  None,
            }

            save_diagnosis(
                run_ts=datetime.now(timezone.utc),
                features={}, anomalies={},
                diagnosis=fallback_diagnosis,
                decision=fallback_decision,
                source="dag_fallback",
            )

        except Exception as inner_e:
            # Last resort: write a minimal row directly
            logger.error(f"Persistence module also failed: {inner_e}")
            with pg_cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_pipeline_diagnosis (
                        run_timestamp, status, issue_summary, source
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    datetime.now(timezone.utc),
                    "warning",
                    f"Total fallback. orders={orders_count}, failure_rate={failure_rate}",
                    "dag_emergency_fallback",
                ))

        ti.xcom_push(key="ai_status", value="warning")
        return "warning"


# ----------------------------------------------------------------
#  TASK 5: s3_backup_placeholder
# ----------------------------------------------------------------
#  WHY: Reserves a slot for the upcoming S3 phase.
#       trigger_rule="all_done" → runs even if upstream failed.
# ----------------------------------------------------------------
def s3_backup_placeholder(**context):
    logger.info("☁️  S3 backup placeholder — coming in the next phase")
    logger.info("Future plan: dump raw_* tables to Parquet → "
                "upload to s3://ecommerce-ops-raw/year=YYYY/month=MM/day=DD/")
    return "skipped"


# ================================================================
#  SECTION 5 — DAG DEFINITION
# ================================================================
with DAG(
    dag_id          = "ecommerce_ops_pipeline",
    description     = "Real-time monitoring + AI diagnosis for e-commerce pipeline",
    default_args    = default_args,
    start_date      = datetime(2026, 1, 1),
    schedule        = "*/15 * * * *",          # cron: every 15 minutes
    catchup         = False,
    max_active_runs = 1,
    tags            = ["ecommerce", "monitoring", "ai", "production"],
) as dag:

    t1_check_postgres = PythonOperator(
        task_id         = "check_postgres",
        python_callable = check_postgres,
    )

    t2_check_orders = PythonOperator(
        task_id         = "check_recent_orders",
        python_callable = check_recent_orders,
    )

    t3_check_payments = PythonOperator(
        task_id         = "check_payment_failure",
        python_callable = check_payment_failure,
    )

    t4_ai_doctor = PythonOperator(
        task_id         = "run_ai_doctor",
        python_callable = run_ai_doctor,
    )

    t5_s3_backup = PythonOperator(
        task_id         = "s3_backup_placeholder",
        python_callable = s3_backup_placeholder,
        trigger_rule    = "all_done",          # run even if upstream failed
    )

    # ---- Task dependencies ----
    # check_postgres FIRST.
    # Then check_orders + check_payments in PARALLEL.
    # Then run_ai_doctor.
    # Then s3_backup at the end.
    t1_check_postgres >> [t2_check_orders, t3_check_payments] >> t4_ai_doctor >> t5_s3_backup