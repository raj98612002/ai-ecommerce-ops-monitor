# AI E-Commerce Operations Monitor

A real-time data engineering pipeline that continuously monitors e-commerce operations — tracking orders, payments, deliveries, complaints, and refunds — and deploys an AI-powered diagnostic layer to detect anomalies, identify root causes, and surface actionable insights across a fully integrated analytics stack.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      AI E-COMMERCE OPERATIONS MONITOR                        │
│                                                                               │
│   ┌─────────────┐     ┌──────────────┐     ┌────────────────────────────┐   │
│   │   Kafka     │────▶│  PostgreSQL  │────▶│     Apache Airflow         │   │
│   │  (5 topics) │     │ (Operational)│     │  DAG: every 15 minutes     │   │
│   └─────────────┘     └──────────────┘     └────────────┬───────────────┘   │
│                                                          │                    │
│                              ┌───────────────────────────┘                   │
│                              ▼                                                │
│                   ┌─────────────────────┐                                    │
│                   │   AI Pipeline       │  GPT-4o-mini anomaly diagnosis     │
│                   │   Doctor            │  Z-score feature detection          │
│                   │                     │  Rule-based fallback engine         │
│                   └──────────┬──────────┘  Persisted audit trail             │
│                              │                                                │
│               ┌──────────────┴──────────────┐                               │
│               ▼                             ▼                                │
│       ┌──────────────┐            ┌──────────────────┐                      │
│       │   AWS S3     │            │    Snowflake      │                      │
│       │  (Parquet)   │            │  Data Warehouse   │                      │
│       │ date-part.   │            │  RAW → STAGING    │                      │
│       └──────────────┘            │      → MARTS      │                      │
│                                   └────────┬─────────┘                      │
│                                            │                                 │
│                                   ┌────────▼─────────┐                      │
│                                   │       dbt         │                      │
│                                   │  12 models        │                      │
│                                   │  69 data tests    │                      │
│                                   └────────┬─────────┘                      │
│                                            │                                 │
│                                   ┌────────▼─────────┐                      │
│                                   │    Power BI       │                      │
│                                   │  3-page dashboard │                      │
│                                   │  Live Snowflake   │                      │
│                                   └──────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Airflow DAG Task Flow:**
```
check_postgres
      │
      ├──▶ check_recent_orders ──┐
      │                          ├──▶ run_ai_doctor ──▶ s3_backup
      └──▶ check_payment_failure ┘
```

---

## What This System Does

This platform ingests high-volume e-commerce event streams across five operational domains — orders, payments, deliveries, complaints, and refunds — and runs a full analytics and AI monitoring stack on top of them.

Every 15 minutes, the Airflow DAG wakes up, checks pipeline health metrics, feeds them into an AI diagnostic engine backed by GPT-4o-mini, and persists structured diagnoses with severity scores, root cause analysis, and suggested remediation SQL. All raw data is simultaneously loaded into Snowflake, transformed through a medallion architecture via dbt, and backed up to AWS S3 as date-partitioned Parquet files — ready for downstream analytics in Power BI.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Apache Kafka + Zookeeper | Real-time event streaming (5 topics) |
| **Operational DB** | PostgreSQL 15 | Raw event storage, AI diagnosis audit trail |
| **Orchestration** | Apache Airflow 2.9 | Pipeline scheduling, retry logic, XCom passing |
| **AI / LLM** | OpenAI GPT-4o-mini | Anomaly diagnosis, root cause analysis |
| **Data Warehouse** | Snowflake | Cloud-scale analytical storage |
| **Transformation** | dbt 1.11 + dbt-snowflake | Medallion architecture, data quality testing |
| **Cloud Backup** | AWS S3 + PyArrow | Date-partitioned Parquet archival |
| **Visualization** | Power BI Desktop | Live-connected operational dashboards |
| **Containerization** | Docker + Docker Compose | 8-service local stack, one-command startup |
| **Language** | Python 3.11 | End-to-end pipeline, AI layer, loaders |

---

## Key Features

**Real-time streaming pipeline**
Kafka producer emits events across five topics simultaneously. A dedicated consumer reads and persists them to PostgreSQL with deduplication and error handling.

**Airflow orchestration with parallel execution**
The DAG runs `check_recent_orders` and `check_payment_failure` in parallel, then fans into the AI doctor task. XCom passes metrics between tasks. Retry logic with 2-minute backoff protects against transient failures.

**AI Pipeline Doctor**
A four-stage AI system extracts 15+ operational metrics, runs z-score anomaly detection, calls GPT-4o-mini for structured diagnosis, and persists results with a full audit trail including confidence scores, LLM provider, latency, and alert fingerprint for deduplication.

**Rule-based fallback engine**
When the LLM is unavailable, a deterministic fallback generates diagnoses from raw metric thresholds — ensuring the pipeline never goes silent regardless of external API availability.

**Medallion architecture with dbt**
Twelve dbt models transform raw Snowflake tables through staging views into business-ready mart tables. Sixty-nine automated tests validate uniqueness, nullability, and accepted values across all layers.

**AWS S3 Parquet archival**
All six raw tables are serialized to Parquet and uploaded to S3 with year/month/day partitioning, making them queryable directly via Athena and compatible with any Spark or Iceberg-based downstream system.

**Power BI live dashboards**
Three dashboard pages connect directly to Snowflake mart tables — no scheduled exports, no stale data. Covers executive KPIs, operational health, and AI diagnosis trends.

---

## Project Structure

```
ai_ecommerce_ops_monitor/
│
├── ai/
│   ├── feature_builder.py          # Extracts 15+ metrics from PostgreSQL
│   ├── anomaly_detector.py         # Z-score detection on operational metrics
│   ├── llm_engine.py               # GPT-4o-mini wrapper with JSON validation
│   ├── pipeline_doctor.py          # Main orchestrator: features → anomalies → LLM → persist
│   ├── decision_engine.py          # Alert deduplication via fingerprinting
│   ├── persistence.py              # Writes structured diagnoses to PostgreSQL
│   └── config.py                   # AIConfig dataclass reading env vars
│
├── airflow/
│   └── dags/
│       └── ecommerce_ops_pipeline.py   # Main DAG: 5 tasks, parallel execution
│
├── producers/
│   └── ecommerce_event_producer.py     # Kafka producer across 5 topics
│
├── consumers/
│   └── kafka_to_postgres_consumer.py   # Kafka → PostgreSQL consumer
│
├── loaders/
│   ├── snowflake_loader.py             # PostgreSQL → Snowflake batch loader
│   └── s3_backup.py                    # PostgreSQL → Parquet → S3 (date-partitioned)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/                    # 6 views: light cleaning, type casting
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_orders.sql
│       │   ├── stg_payments.sql
│       │   ├── stg_deliveries.sql
│       │   ├── stg_complaints.sql
│       │   ├── stg_refunds.sql
│       │   └── stg_ai_diagnosis.sql
│       └── marts/                      # 6 tables: business logic, aggregations
│           ├── schema.yml
│           ├── mart_order_summary.sql
│           ├── mart_payment_health.sql
│           ├── mart_delivery_performance.sql
│           ├── mart_complaint_insights.sql
│           ├── mart_ai_diagnosis_summary.sql
│           └── mart_business_health_daily.sql
│
├── config/
│   └── topics.py                   # Kafka topic definitions
│
├── sql/
│   ├── init_postgres.sql           # Schema initialization
│   └── migrate_ai_pipeline_diagnosis.sql
│
├── docker-compose.yml              # 8 services: Kafka, Zookeeper, PostgreSQL,
│                                   # Airflow webserver/scheduler/init, Kafka UI
├── requirements.txt
├── .env.example
└── README.md
```

---

## Data Models

### PostgreSQL (Operational Layer)

| Table | Description |
|---|---|
| `raw_orders` | Order events with status, amount, city, timestamp |
| `raw_payments` | Payment events with method, status, failure reason |
| `raw_deliveries` | Delivery records with promised vs actual minutes |
| `raw_complaints` | Customer complaints with AI category and severity |
| `raw_refunds` | Refund events with reason and amount |
| `ai_pipeline_diagnosis` | Full AI audit trail with LLM metadata |

### Snowflake (Analytical Layer)

**Staging schema — `RAW_STAGING`** (views)

| Model | Rows |
|---|---|
| `stg_orders` | 5,572 |
| `stg_payments` | 5,572 |
| `stg_deliveries` | 5,572 |
| `stg_complaints` | 659 |
| `stg_refunds` | ~800 |
| `stg_ai_diagnosis` | 25 |

**Marts schema — `RAW_MARTS`** (tables)

| Model | Description | Rows |
|---|---|---|
| `mart_order_summary` | Daily orders and revenue by city | 24 |
| `mart_payment_health` | Failure rates by method with health status | 15 |
| `mart_delivery_performance` | Delay analysis by city and partner | 120 |
| `mart_complaint_insights` | Complaint breakdown by AI category and severity | 25 |
| `mart_ai_diagnosis_summary` | Daily AI Doctor performance metrics | 3 |
| `mart_business_health_daily` | Master KPI table joining all domains | 3 |

---

## AI Pipeline Doctor

```
PostgreSQL metrics
      │
      ▼
feature_builder.py          ← 15+ operational metrics extracted via SQL
      │
      ▼
anomaly_detector.py         ← Z-score detection on key time-series metrics
      │
      ▼
llm_engine.py               ← GPT-4o-mini call with structured JSON response
      │                         Validates schema before accepting output
      ▼
decision_engine.py          ← Deduplicates alerts via fingerprinting
      │
      ▼
persistence.py              ← Writes to ai_pipeline_diagnosis with full metadata
```

**Each diagnosis record contains:**

```python
{
  "status":            "healthy | degraded | critical",
  "severity":          0.0 - 1.0,
  "confidence":        0.0 - 1.0,
  "issue_summary":     "...",
  "root_cause":        "...",
  "business_impact":   "...",
  "suggested_fix":     "...",
  "investigation_sql": "SELECT ...",
  "llm_provider":      "openai",
  "llm_model":         "gpt-4o-mini",
  "llm_latency_ms":    843,
  "alert_fingerprint": "sha256_hash",
  "source":            "llm | rule_based | dag_fallback"
}
```

**Fallback chain:**
`GPT-4o-mini` → `Rule-based engine` → `Emergency DAG fallback`

The system never drops a diagnosis cycle regardless of LLM availability.

---

## dbt Layer

```bash
# Run all 12 models
dbt run

# Validate 69 data quality tests
dbt test
# Expected: PASS=69 WARN=0 ERROR=0

# Generate documentation with lineage graph
dbt docs generate && dbt docs serve
```

**Test coverage includes:**
- `unique` — primary key integrity across all models
- `not_null` — required field validation
- `accepted_values` — status fields constrained to known enumerations
- `source` tests — raw table validation before transformation

---

## AWS S3 Backup

Raw tables are serialized to Parquet and uploaded with date-based partitioning:

```
s3://ecommerce-ops-raw-raj/
├── raw_orders/
│   └── year=2026/month=05/day=05/
│       └── raw_orders_20260505_101400.parquet
├── raw_payments/
├── raw_deliveries/
├── raw_complaints/
├── raw_refunds/
└── ai_pipeline_diagnosis/
```

Each file includes row count and backup timestamp in S3 object metadata. The structure is Athena-compatible and Iceberg-ready for future migration.

---

## Setup

### Prerequisites

- Docker Desktop
- Python 3.11
- Snowflake account
- OpenAI API key
- AWS account with S3 access
- Power BI Desktop

### 1. Clone and configure

```bash
git clone https://github.com/raj98612002/ai-ecommerce-ops-monitor.git
cd ai-ecommerce-ops-monitor
cp .env.example .env
# Fill in .env with your credentials
```

### 2. Python environments

```bash
# Main environment
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# dbt environment (isolated to avoid dependency conflicts)
python -m venv dbt_venv
dbt_venv\Scripts\activate
pip install dbt-snowflake
```

### 3. Snowflake setup

```sql
CREATE DATABASE ECOMMERCE_OPS_DB;
CREATE SCHEMA ECOMMERCE_OPS_DB.RAW;
CREATE SCHEMA ECOMMERCE_OPS_DB.RAW_STAGING;
CREATE SCHEMA ECOMMERCE_OPS_DB.RAW_MARTS;
```

### 4. dbt profile

```bash
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
# Fill in your Snowflake credentials
```

---

## Running the Pipeline

```bash
# Start all Docker services (Kafka, PostgreSQL, Airflow, Kafka UI)
docker compose up -d

# Terminal 1 — Event producer
python -m producers.ecommerce_event_producer

# Terminal 2 — Kafka consumer
python -m consumers.kafka_to_postgres_consumer

# Load to Snowflake
python loaders/snowflake_loader.py

# Transform with dbt
cd dbt_project
dbt run && dbt test

# Backup to S3
python loaders/s3_backup.py

# Trigger Airflow DAG manually
docker exec airflow-scheduler airflow dags trigger ecommerce_ops_pipeline
```

**Airflow UI:** http://localhost:8080 `admin / admin`
**Kafka UI:** http://localhost:8085

---

## Power BI Dashboard

Connect Power BI directly to Snowflake:

```
Server:    <account>.snowflakecomputing.com
Warehouse: ECOMMERCE_WH
Database:  ECOMMERCE_OPS_DB
```

**Page 1 — Executive Overview**
Total orders, revenue trend, cancellation rate, overall pipeline status

**Page 2 — Operations Health**
Payment failure rate by method, delivery delay heatmap by city, complaint severity breakdown

**Page 3 — AI Diagnosis Insights**
Pipeline health score over time, LLM vs fallback usage rate, recent diagnosis table with status and confidence

---

## Environment Variables

See `.env.example` for the full reference. Key variables:

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=

# PostgreSQL
PG_HOST=localhost
PG_PORT=5433

# OpenAI
OPENAI_API_KEY=

# AWS
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BACKUP_BUCKET=
```

---

## Resume Points

```
• Designed and implemented an end-to-end real-time data engineering pipeline
  processing 5,500+ events across five operational domains using Apache Kafka,
  PostgreSQL, and Apache Airflow with 15-minute scheduling and parallel task execution

• Built an AI diagnostic engine (AI Pipeline Doctor) using GPT-4o-mini with
  z-score anomaly detection, structured JSON output validation, alert fingerprint
  deduplication, and a three-tier fallback chain ensuring 100% diagnosis coverage

• Implemented a medallion architecture in Snowflake using dbt with 12 models
  across staging and marts layers, validated by 69 automated data quality tests
  covering uniqueness, nullability, and value constraints

• Engineered an AWS S3 archival system serializing PostgreSQL tables to
  date-partitioned Parquet (year/month/day), producing Athena-compatible
  and Iceberg-ready output with per-file row count metadata

• Built a three-page Power BI dashboard with live Snowflake DirectQuery
  connection covering executive KPIs, operational health metrics, and
  AI diagnosis trend analysis

• Containerized an 8-service stack with Docker Compose including Kafka,
  Zookeeper, PostgreSQL, Airflow webserver/scheduler/init, and Kafka UI —
  enabling single-command local deployment
```

---



## Author

**Biswajit Panda**
Data Engineer

[GitHub](https://github.com/raj98612002) · [LinkedIn](https://linkedin.com/in/your-profile)