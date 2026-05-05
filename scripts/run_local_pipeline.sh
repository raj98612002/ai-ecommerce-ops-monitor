#!/usr/bin/env bash
set -e
python loaders/postgres_to_csv.py
python ai/pipeline_doctor.py
python loaders/postgres_to_csv.py
python loaders/snowflake_loader.py
cd dbt_project && dbt run && dbt test
