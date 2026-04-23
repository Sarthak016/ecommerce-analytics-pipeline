from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

logger = logging.getLogger(__name__)

# ── Default arguments applied to all tasks ──────────────────────────────────
default_args = {
    "owner": "sarthak",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_analytics_pipeline",
    default_args=default_args,
    description="End to end ecommerce analytics pipeline using PySpark, dbt and PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "pyspark", "dbt", "analytics"],
) as dag:

    # ── Start marker ─────────────────────────────────────────────────────────
    start = DummyOperator(
        task_id="start_pipeline"
    )

    # ── PySpark ETL tasks ─────────────────────────────────────────────────────
    run_orders_etl = BashOperator(
        task_id="run_orders_etl",
        bash_command="""
            cd /opt/airflow && python spark_jobs/etl_orders.py
        """,
    )

    run_customers_etl = BashOperator(
        task_id="run_customers_etl",
        bash_command="""
            cd /opt/airflow && python spark_jobs/etl_customers.py
        """,
    )

    run_products_etl = BashOperator(
        task_id="run_products_etl",
        bash_command="""
            cd /opt/airflow && python spark_jobs/etl_products.py
        """,
    )

    # ── Wait for all ETL jobs to finish ───────────────────────────────────────
    etl_complete = DummyOperator(
        task_id="etl_complete"
    )

    # ── dbt staging models ────────────────────────────────────────────────────
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt run --select staging --profiles-dir .
        """,
    )

    # ── dbt tests on staging ──────────────────────────────────────────────────
    test_dbt_staging = BashOperator(
        task_id="test_dbt_staging",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt test --select staging --profiles-dir .
        """,
    )

    # ── dbt mart models ───────────────────────────────────────────────────────
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt run --select marts --profiles-dir .
        """,
    )

    # ── dbt tests on marts ────────────────────────────────────────────────────
    test_dbt_marts = BashOperator(
        task_id="test_dbt_marts",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt test --select marts --profiles-dir .
        """,
    )

    # ── Generate dbt docs ─────────────────────────────────────────────────────
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt docs generate --profiles-dir .
        """,
    )

    # ── End marker ────────────────────────────────────────────────────────────
    end = DummyOperator(
        task_id="pipeline_complete"
    )

    # ── Task dependencies (the actual DAG flow) ───────────────────────────────
    start >> [run_orders_etl, run_customers_etl, run_products_etl]
    [run_orders_etl, run_customers_etl, run_products_etl] >> etl_complete
    etl_complete >> run_dbt_staging
    run_dbt_staging >> test_dbt_staging
    test_dbt_staging >> run_dbt_marts
    run_dbt_marts >> test_dbt_marts
    test_dbt_marts >> generate_dbt_docs
    generate_dbt_docs >> end