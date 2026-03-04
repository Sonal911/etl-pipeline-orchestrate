"""
Airflow DAG: myoncare ETL pipeline
-----------------------------------
Ingests raw JSON logs → cleans / transforms → loads into PostgreSQL star schema
→ runs data quality checks.
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure the etl package is importable
sys.path.insert(0, "/opt/airflow")

from etl.extract import extract_from_json
from etl.load import (
    get_engine,
    init_schema,
    load_dim_actions,
    load_dim_users,
    load_fact_user_actions,
    truncate_tables,
)
from etl.quality_checks import run_all_checks
from etl.transform import (
    build_dim_actions,
    build_dim_users,
    build_fact_table,
    clean_and_transform,
)

logger = logging.getLogger(__name__)

RAW_DATA_PATH = "/opt/airflow/data/raw_logs.json"
SCHEMA_PATH = "/opt/airflow/sql/schema.sql"

# ── DAG default args ─────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── Task callables ───────────────────────────────────────────────────────────

def task_extract(**context):
    """Extract raw logs from JSON file and push to XCom."""
    raw_data = extract_from_json(RAW_DATA_PATH)
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    logger.info("Extracted %d records", len(raw_data))


def task_transform(**context):
    """Clean/transform data and build star-schema DataFrames."""
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")

    df_clean = clean_and_transform(raw_data)
    dim_users = build_dim_users(df_clean)
    dim_actions = build_dim_actions(df_clean)
    fact = build_fact_table(df_clean, dim_users, dim_actions)

    # Convert Timestamps to ISO strings so XCom JSON serialisation works
    fact["event_timestamp"] = fact["event_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Push as dicts for XCom serialisation
    context["ti"].xcom_push(key="dim_users", value=dim_users.to_dict(orient="records"))
    context["ti"].xcom_push(key="dim_actions", value=dim_actions.to_dict(orient="records"))
    context["ti"].xcom_push(key="fact", value=fact.to_dict(orient="records"))
    logger.info(
        "Transform done – dim_users=%d, dim_actions=%d, facts=%d",
        len(dim_users), len(dim_actions), len(fact),
    )


def task_load(**context):
    """Initialise schema and load all tables (full-reload / idempotent)."""
    import pandas as pd

    dim_users = pd.DataFrame(context["ti"].xcom_pull(key="dim_users", task_ids="transform"))
    dim_actions = pd.DataFrame(context["ti"].xcom_pull(key="dim_actions", task_ids="transform"))
    fact = pd.DataFrame(context["ti"].xcom_pull(key="fact", task_ids="transform"))

    engine = get_engine()
    init_schema(engine, SCHEMA_PATH)
    truncate_tables(engine)
    load_dim_users(engine, dim_users)
    load_dim_actions(engine, dim_actions)
    load_fact_user_actions(engine, fact)
    logger.info("Load complete")


def task_quality_checks(**context):
    """Run post-load data quality checks."""
    engine = get_engine()
    run_all_checks(engine)


# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="myoncare_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: JSON logs → star schema in PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["etl", "myoncare"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=task_quality_checks,
    )

    extract >> transform >> load >> quality_checks
