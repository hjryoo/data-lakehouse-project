"""Airflow DAG for the e-commerce ELT pipeline."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Ensure project root is on sys.path so `scripts` package is importable
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DBT_DIR = Path(PROJECT_ROOT) / "dbt_project"
VENV_BIN = Path(PROJECT_ROOT) / "venv" / "bin"
DBT_BIN = VENV_BIN / "dbt"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_elt",
    default_args=default_args,
    description="ELT pipeline: DummyJSON API -> MinIO -> Spark -> DuckDB -> dbt",
    schedule="@daily",
    start_date=datetime(2026, 2, 14),
    catchup=False,
    tags=["ecommerce", "elt"],
) as dag:

    def _extract(**context):
        from scripts.extract_api import run_extraction
        run_extraction(context["execution_date"])

    def _spark_transform(**context):
        from scripts.spark_transform import run_spark_transform
        run_spark_transform(context["execution_date"])

    def _load_duckdb(**context):
        from scripts.duckdb_loader import load_all_to_duckdb
        load_all_to_duckdb(context["execution_date"])

    extract_and_load_to_lake = PythonOperator(
        task_id="extract_and_load_to_lake",
        python_callable=_extract,
    )

    spark_cleanse_transform = PythonOperator(
        task_id="spark_cleanse_transform",
        python_callable=_spark_transform,
    )

    load_to_duckdb = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=_load_duckdb,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --profiles-dir . --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --profiles-dir . --target dev",
    )

    extract_and_load_to_lake >> spark_cleanse_transform >> load_to_duckdb >> dbt_run >> dbt_test
