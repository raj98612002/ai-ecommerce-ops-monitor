# AI E-commerce Operations Monitor

A live data engineering portfolio project using Kafka, PostgreSQL, Airflow, Snowflake, dbt, Power BI, and AI.

## One-line resume description
Built an AI-powered e-commerce operations monitor that streams live orders, payments, deliveries, refunds, and complaints through Kafka, stores raw data in PostgreSQL, loads curated data into Snowflake, transforms it with dbt, and uses an LLM-based Pipeline Doctor to detect anomalies, explain root causes, and suggest fixes.

## Architecture

```text
Fake E-commerce Events
        ↓
Kafka Producer
        ↓
Kafka Topics
orders_topic, payments_topic, delivery_topic, refunds_topic, complaints_topic
        ↓
Kafka Consumer
        ↓
PostgreSQL RAW tables
        ↓
Airflow DAG every 15 min
        ↓
CSV/S3-style raw export → Snowflake RAW
        ↓
dbt STAGING + MARTS
        ↓
Power BI Dashboard
        ↓
AI Pipeline Doctor + Telegram Alert
```

## Tools used

- Python
- Kafka
- PostgreSQL
- Airflow
- Snowflake
- dbt
- Power BI
- OpenAI / LLM
- Telegram alerts

## Folder structure

```text
ai_ecommerce_ops_monitor/
├── producers/                 # Kafka event generator
├── consumers/                 # Kafka to Postgres consumer
├── ai/                        # AI complaint classifier + pipeline doctor
├── loaders/                   # Postgres export + Snowflake loader
├── airflow/dags/              # Airflow DAG
├── dbt_project/               # dbt staging and mart models
├── sql/                       # PostgreSQL init tables
├── config/                    # Kafka topics
├── scripts/                   # Local run scripts
├── data/raw/                  # Local raw exports
└── dashboard/                 # Power BI screenshots or pbix later
```

## What makes this project unique

Most beginner projects are:

```text
Data → Dashboard
```

This project is:

```text
Live Events → Data Pipeline → dbt Tests → AI Diagnosis → Fix Suggestion → Dashboard Alert
```

The AI layer does not just generate text. It behaves like a junior data reliability engineer.

## Main AI features

### 1. AI Complaint Classifier
Classifies customer complaints into:

- payment_issue
- late_delivery
- wrong_product
- refund_delay
- app_issue
- other

### 2. AI Pipeline Doctor
Checks:

- Missing rows
- Null columns
- Duplicate records
- High payment failure rate
- High delivery delay rate
- Complaint severity spike

Returns:

```json
{
  "status": "critical",
  "issue_summary": "Payment failure rate is unusually high",
  "root_cause": "Possible payment gateway issue or Kafka payment consumer bug",
  "suggested_fix": "Check payments_consumer.py mapping and payment gateway logs",
  "severity": "high"
}
```

### 3. Telegram Alert
Sends the AI diagnosis when status is warning or critical.

## How to run locally

### Step 1: Create environment file

```bash
cp .env.example .env
```

Edit `.env` with your OpenAI and Snowflake details.

### Step 2: Start Kafka and PostgreSQL

```bash
docker compose up -d
```

Kafka UI:

```text
http://localhost:8085
```

### Step 3: Install Python libraries

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### Step 4: Start producing live e-commerce events

```bash
python producers/ecommerce_event_producer.py
```

Keep this terminal running.

### Step 5: Start consuming Kafka events into PostgreSQL

Open another terminal:

```bash
python consumers/kafka_to_postgres_consumer.py
```

Keep this terminal running.

### Step 6: Run AI doctor locally

Open another terminal:

```bash
python ai/pipeline_doctor.py
```

### Step 7: Export Postgres data to CSV

```bash
python loaders/postgres_to_csv.py
```

### Step 8: Load data to Snowflake

```bash
python loaders/snowflake_loader.py
```

### Step 9: Run dbt

Copy `dbt_project/profiles.yml.example` to your local `~/.dbt/profiles.yml`, then edit credentials.

```bash
cd dbt_project
dbt debug
dbt run
dbt test
```

## Airflow DAG

The DAG file is:

```text
airflow/dags/ecommerce_ops_pipeline.py
```

It runs every 15 minutes:

```text
export_postgres_to_csv
→ ai_pipeline_doctor
→ reload_diagnosis_csv
→ load_to_snowflake
→ run_dbt_models
→ run_dbt_tests
```

For a full Airflow Docker setup, mount this whole project to `/opt/airflow/project` and mount `airflow/dags` to Airflow's DAG folder.

## Snowflake schemas

This starter uses:

```text
ECOMMERCE_OPS_DB.RAW
ECOMMERCE_OPS_DB.STAGING
ECOMMERCE_OPS_DB.MARTS
```

## Power BI dashboard pages

### Page 1: Executive Monitor
- Total orders
- Revenue
- Failed payments
- Refund amount
- Complaint count

### Page 2: Payment Health
- Success vs failed payments
- Failure by payment method
- Hourly failure trend

### Page 3: Delivery Health
- Delayed deliveries
- Delay by city
- Average delivery time

### Page 4: AI Pipeline Doctor
- Status
- Severity
- Issue summary
- Root cause
- Suggested fix

## Interview explanation

Say this:

> I built an AI-powered e-commerce operations monitor. Kafka streams live orders, payments, deliveries, refunds, and complaints. PostgreSQL stores raw live data, Airflow orchestrates the batch movement into Snowflake, dbt transforms and tests the data, and an AI Pipeline Doctor analyzes data quality, business anomalies, and pipeline logs to suggest root causes and fixes. The final dashboard shows both business health and pipeline health.

## Next improvements

- Add AWS S3 instead of local CSV export
- Add Kafka lag monitoring
- Add Great Expectations data quality checks
- Add Streamlit live dashboard
- Add automatic GitHub issue creation when AI detects critical failure
