"""
S3 Backup — PostgreSQL → Parquet → S3
Run: python loaders/s3_backup.py
"""
import os
import logging
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from io import BytesIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Config
POSTGRES_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5433")),
    "dbname":   os.getenv("PG_DB",       "ecommerce_ops"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}
S3_BUCKET = os.getenv("S3_BACKUP_BUCKET", "ecommerce-ops-raw-raj")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

TABLES = [
    "raw_orders",
    "raw_payments",
    "raw_deliveries",
    "raw_complaints",
    "raw_refunds",
    "ai_pipeline_diagnosis",
]

def run():
    run_date = datetime.utcnow()
    logger.info(f"=== S3 Backup @ {run_date.isoformat()} ===")

    # Connect to S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )

    # Test connection
    s3.head_bucket(Bucket=S3_BUCKET)
    logger.info(f"✅ S3 connected: {S3_BUCKET}")

    # Connect to Postgres
    conn = psycopg2.connect(**POSTGRES_CONFIG)

    results = []
    for table in TABLES:
        try:
            logger.info(f"Backing up {table}...")

            # Read table
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            logger.info(f"  {len(df):,} rows read")

            if df.empty:
                logger.warning(f"  {table} empty - skipping")
                continue

            # Convert to Parquet
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            # S3 path with date partition
            s3_key = (
                f"{table}/"
                f"year={run_date.year}/"
                f"month={run_date.month:02d}/"
                f"day={run_date.day:02d}/"
                f"{table}_{run_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            )

            # Upload
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=buffer.getvalue(),
            )

            size_kb = len(buffer.getvalue()) / 1024
            logger.info(f"  ✅ s3://{S3_BUCKET}/{s3_key}")
            logger.info(f"     {len(df):,} rows | {size_kb:.1f} KB")

            results.append({
                "table":   table,
                "rows":    len(df),
                "size_kb": round(size_kb, 1),
                "s3_key":  s3_key,
                "status":  "success"
            })

        except Exception as e:
            logger.error(f"  ❌ {table} failed: {e}")
            results.append({"table": table, "status": "failed"})

    conn.close()

    # Summary
    logger.info("\n" + "="*50)
    logger.info("BACKUP SUMMARY")
    logger.info("="*50)
    total = 0
    for r in results:
        icon = "✅" if r["status"] == "success" else "❌"
        rows = r.get("rows", 0)
        total += rows
        logger.info(f"  {icon} {r['table']}: {rows:,} rows")
    logger.info(f"\n  Total: {total:,} rows backed up to S3")
    logger.info(f"  Bucket: s3://{S3_BUCKET}/")
    logger.info("="*50)
    return results

if __name__ == "__main__":
    run()