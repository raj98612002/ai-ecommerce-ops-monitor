import os
from pathlib import Path
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()
OUT = Path("data/raw")
OUT.mkdir(parents=True, exist_ok=True)
TABLES = ["raw_orders", "raw_payments", "raw_deliveries", "raw_refunds", "raw_complaints", "pipeline_logs", "ai_pipeline_diagnosis"]


def get_conn():
    return psycopg2.connect(
        host=os.getenv("LOCAL_POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("LOCAL_POSTGRES_PORT", "5433"),
        dbname=os.getenv("LOCAL_POSTGRES_DB", "ecommerce_ops"),
        user=os.getenv("LOCAL_POSTGRES_USER", "postgres"),
        password=os.getenv("LOCAL_POSTGRES_PASSWORD", "postgres"),
    )

def export_tables():
    conn = get_conn()
    for table in TABLES:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        path = OUT / f"{table}.csv"
        df.to_csv(path, index=False)
        print(f"Exported {table}: {len(df)} rows -> {path}")
    conn.close()


if __name__ == "__main__":
    export_tables()
