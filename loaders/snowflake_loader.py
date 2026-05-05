"""
================================================================
  SNOWFLAKE LOADER
================================================================
  Loads data directly from PostgreSQL into Snowflake RAW schema.
  No CSV files needed — reads from Postgres, writes to Snowflake.

  Flow:
    PostgreSQL raw tables → pandas DataFrame → Snowflake RAW

  Tables loaded:
    raw_orders          → ECOMMERCE_OPS_DB.RAW.RAW_ORDERS
    raw_payments        → ECOMMERCE_OPS_DB.RAW.RAW_PAYMENTS
    raw_deliveries      → ECOMMERCE_OPS_DB.RAW.RAW_DELIVERIES
    raw_complaints      → ECOMMERCE_OPS_DB.RAW.RAW_COMPLAINTS
    raw_refunds         → ECOMMERCE_OPS_DB.RAW.RAW_REFUNDS
    ai_pipeline_diagnosis → ECOMMERCE_OPS_DB.RAW.AI_PIPELINE_DIAGNOSIS

  Run:
    python loaders/snowflake_loader.py
================================================================
"""

import os
import logging
import pandas as pd
import psycopg2
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

from dotenv import load_dotenv
load_dotenv()          # reads .env from the current working directory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ================================================================
#  CONFIG — reads from your .env
# ================================================================

POSTGRES_CONFIG = {
    "host": os.getenv("LOCAL_POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("LOCAL_POSTGRES_PORT", "5433")),
    "dbname": os.getenv("LOCAL_POSTGRES_DB", "ecommerce_ops"),
    "user": os.getenv("LOCAL_POSTGRES_USER", "postgres"),
    "password": os.getenv("LOCAL_POSTGRES_PASSWORD", "postgres"),
}

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "ECOMMERCE_OPS_DB"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",     "RAW"),
    "role":      os.getenv("SNOWFLAKE_ROLE",       "ACCOUNTADMIN"),
}

# Tables to load: (postgres_table, snowflake_table, batch_size)
TABLES = [
    ("raw_orders",            "RAW_ORDERS",            5000),
    ("raw_payments",          "RAW_PAYMENTS",           5000),
    ("raw_deliveries",        "RAW_DELIVERIES",         5000),
    ("raw_complaints",        "RAW_COMPLAINTS",         5000),
    ("raw_refunds",           "RAW_REFUNDS",            5000),
    ("ai_pipeline_diagnosis", "AI_PIPELINE_DIAGNOSIS",  1000),
]


# ================================================================
#  CONNECTIONS
# ================================================================

def get_postgres_connection():
    """Connect to local Postgres."""
    logger.info(f"Connecting to Postgres at {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}...")
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    logger.info("✅ Postgres connected")
    return conn


def get_snowflake_connection():
    """Connect to Snowflake."""
    logger.info(f"Connecting to Snowflake account: {SNOWFLAKE_CONFIG['account']}...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    logger.info("✅ Snowflake connected")
    return conn


# ================================================================
#  HELPERS
# ================================================================

def read_from_postgres(pg_conn, table: str) -> pd.DataFrame:
    """Read entire table from Postgres into a DataFrame."""
    logger.info(f"  Reading {table} from Postgres...")
    df = pd.read_sql(f"SELECT * FROM {table}", pg_conn)
    logger.info(f"  ✅ {len(df):,} rows read from {table}")
    return df


def clean_dataframe(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    Clean the DataFrame before loading to Snowflake:
    - Uppercase all column names (Snowflake convention)
    - Convert timestamps to strings (avoids timezone issues)
    - Drop any fully-null rows
    """
    # Uppercase column names
    df.columns = [c.upper() for c in df.columns]

    # Convert timestamps to string to avoid tz issues
    for col in df.columns:
        if df[col].dtype == "datetime64[ns]" or "timestamp" in col.lower():
            df[col] = df[col].astype(str).replace("NaT", None)

    # Convert any lists/dicts to strings (e.g., tags[] array)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: str(x) if isinstance(x, (list, dict)) else x
            )

    # Drop fully empty rows
    df = df.dropna(how="all")

    logger.info(f"  ✅ DataFrame cleaned: {len(df):,} rows, {len(df.columns)} columns")
    return df


def load_to_snowflake(sf_conn, df: pd.DataFrame, sf_table: str) -> bool:
    """
    Load DataFrame to Snowflake using write_pandas (fastest method).
    Uses TRUNCATE + INSERT so runs are idempotent.
    """
    if df.empty:
        logger.warning(f"  ⚠️  {sf_table}: DataFrame is empty — skipping")
        return False

    logger.info(f"  Loading {len(df):,} rows → {sf_table}...")

    # Truncate first (idempotent loads)
    cursor = sf_conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_CONFIG['schema']}.{sf_table}")
        logger.info(f"  ✅ {sf_table} truncated")
    except Exception as e:
        logger.warning(f"  Truncate skipped (table may not exist yet): {e}")
    finally:
        cursor.close()

    # Load with write_pandas (uses Snowflake COPY INTO internally — fast)
    success, num_chunks, num_rows, _ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name=sf_table,
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        quote_identifiers=False,
        auto_create_table=True,     # creates table if not exists
        overwrite=False,
    )

    if success:
        logger.info(f"  ✅ {sf_table}: {num_rows:,} rows loaded ({num_chunks} chunk(s))")
    else:
        logger.error(f"  ❌ {sf_table}: write_pandas returned failure")

    return success


# ================================================================
#  VERIFICATION
# ================================================================

def verify_counts(sf_conn) -> None:
    """Print row counts in Snowflake to confirm load."""
    logger.info("\n" + "="*50)
    logger.info("SNOWFLAKE VERIFICATION — Row Counts:")
    logger.info("="*50)

    cursor = sf_conn.cursor()
    for _, sf_table, _ in TABLES:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM RAW.{sf_table}")
            count = cursor.fetchone()[0]
            status = "✅" if count > 0 else "❌"
            logger.info(f"  {status} RAW.{sf_table}: {count:,} rows")
        except Exception as e:
            logger.error(f"  ❌ RAW.{sf_table}: {e}")
    cursor.close()
    logger.info("="*50)


# ================================================================
#  MAIN
# ================================================================

def run():
    pg_conn = None
    sf_conn = None
    results = {"success": [], "failed": []}

    try:
        # Connect to both
        pg_conn = get_postgres_connection()
        sf_conn = get_snowflake_connection()

        logger.info(f"\nLoading {len(TABLES)} tables from Postgres → Snowflake...\n")

        for pg_table, sf_table, _ in TABLES:
            logger.info(f"--- {pg_table} → {sf_table} ---")
            try:
                # 1. Read from Postgres
                df = read_from_postgres(pg_conn, pg_table)

                # 2. Clean
                df = clean_dataframe(df, pg_table)

                # 3. Load to Snowflake
                success = load_to_snowflake(sf_conn, df, sf_table)

                if success:
                    results["success"].append(sf_table)
                else:
                    results["failed"].append(sf_table)

            except Exception as e:
                logger.error(f"  ❌ Failed to load {pg_table}: {e}")
                results["failed"].append(sf_table)

        # Verify
        verify_counts(sf_conn)

    finally:
        if pg_conn:
            pg_conn.close()
            logger.info("Postgres connection closed")
        if sf_conn:
            sf_conn.close()
            logger.info("Snowflake connection closed")

    # Summary
    logger.info(f"\n✅ Success: {results['success']}")
    if results["failed"]:
        logger.error(f"❌ Failed:  {results['failed']}")
    else:
        logger.info("🎉 All tables loaded successfully!")

    return results


if __name__ == "__main__":
    run()