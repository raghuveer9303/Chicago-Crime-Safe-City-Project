from __future__ import annotations

"""Airflow DAG: Chicago Crime ETL → MinIO (handles 10‑day source lag)

Each daily DAG run processes the crime data that is **10 days older** than its
logical execution date because the public data source publishes with a 10‑day
lag.  For example, the DAG run with `execution_date = 2025‑06‑15` will fetch
crime incidents for **2025‑06‑05**.

Key points
-----------
* No `end_date` — the DAG can run indefinitely.
* `start_date` shifted by `DATA_LAG_DAYS` so the very first run aligns with the
  first available data slice.
* A guard (`AirflowSkipException`) prevents accidental fetches of future data
  if the lag ever changes.
* A guard (`AirflowSkipException`) is also raised when the API returns zero
  records, so no empty Parquet files land in MinIO and the downstream sensor
  never fires on an empty partition.

Update `DATA_LAG_DAYS` if the API lag changes.
"""

import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from chicago_crime_scripts.bronze.bronze_ingest_chicago_crime import run_etl
from great_expectations.expectations.bronze import validate_bronze_parquet

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MINIO_BUCKET = "chicago-crime-data"
DATA_LAG_DAYS = 10  # source lags real time by 10 days

# Default DAG‑level arguments
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

# ---------------------------------------------------------------------------
# Task callable
# ---------------------------------------------------------------------------

def etl_callable_to_minio(ds: str, **context):
    """Fetch Chicago‑crime data (execution_date ‑ DATA_LAG_DAYS) and write to MinIO."""

    # Read credentials inside the callable (not at parse time) so that
    # Airflow Variable lookups happen only when the task actually runs, not on
    # every scheduler heartbeat that re-parses the DAG file.
    dataset_id = Variable.get("DATASET_ID", default_var=os.getenv("DATASET_ID", "ijzp-q8t2"))
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var=os.getenv("MINIO_ACCESS_KEY", ""))
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var=os.getenv("MINIO_SECRET_KEY", ""))
    socrata_api_key_id = Variable.get("SOCRATA_API_KEY_ID", default_var=os.getenv("SOCRATA_API_KEY_ID", ""))
    socrata_api_key_secret = Variable.get("SOCRATA_API_KEY_SECRET", default_var=os.getenv("SOCRATA_API_KEY_SECRET", ""))
    socrata_domain = Variable.get("SOCRATA_DOMAIN", default_var=os.getenv("SOCRATA_DOMAIN", "data.cityofchicago.org"))

    # Propagate to the ETL script via environment (it reads os.getenv at call time)
    os.environ["SOCRATA_API_KEY_ID"] = socrata_api_key_id
    os.environ["SOCRATA_API_KEY_SECRET"] = socrata_api_key_secret
    os.environ["DATASET_ID"] = dataset_id
    os.environ["SOCRATA_DOMAIN"] = socrata_domain
    os.environ["MINIO_ENDPOINT"] = minio_endpoint
    os.environ["MINIO_ACCESS_KEY"] = minio_access_key
    os.environ["MINIO_SECRET_KEY"] = minio_secret_key

    exec_date = pendulum.parse(ds)
    target_date = exec_date.subtract(days=DATA_LAG_DAYS)

    # Safety guard: skip if target_date is still in the future
    if target_date > pendulum.today("UTC"):
        raise AirflowSkipException(
            f"Skipping {ds}: target date {target_date.date()} is within the "
            f"{DATA_LAG_DAYS}-day lag window."
        )

    date_str = target_date.format("YYYY-MM-DD")
    output_path = (
        f"s3a://{MINIO_BUCKET}/raw/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        f"crime_data_{date_str.replace('-', '')}.parquet"
    )

    record_count = run_etl(date_str=date_str, output_path=output_path)

    if record_count == 0:
        raise AirflowSkipException(
            f"API returned zero records for {date_str}. "
            "No file written to MinIO; downstream sensor will not fire."
        )

# ---------------------------------------------------------------------------
# Quality Check Callable
# ---------------------------------------------------------------------------

def bronze_quality_check_callable(**context):
    """Run Great Expectations data quality checks on bronze layer Parquet file."""
    # Ensure MinIO credentials are set in the environment for GX to use
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var=os.getenv("MINIO_ACCESS_KEY", ""))
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var=os.getenv("MINIO_SECRET_KEY", ""))
    
    os.environ["MINIO_ENDPOINT"] = minio_endpoint
    os.environ["MINIO_ACCESS_KEY"] = minio_access_key
    os.environ["MINIO_SECRET_KEY"] = minio_secret_key

    # Airflow provides a timezone-aware datetime for logical_date/execution_date.
    exec_date = context.get("logical_date") or context["execution_date"]
    target_date = exec_date.subtract(days=DATA_LAG_DAYS)
    process_date = target_date.format("YYYY-MM-DD")
    
    # Construct path to Parquet file that was just written
    input_path = (
        f"s3a://{MINIO_BUCKET}/raw/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        f"crime_data_{target_date.format('YYYYMMDD')}.parquet"
    )
    
    # Run validation
    result = validate_bronze_parquet(input_path=input_path, process_date=process_date, **context)
    
    return result

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="chicago_crime_raw_to_minio_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 2, 17, tz="UTC"),
    schedule_interval="30 0 * * *",
    catchup=True,
    max_active_runs=5,
    tags=["chicago", "crime", "minio"],
    doc_md="""
    ### Chicago Crime RAW → MinIO (Bronze) DAG

    * Fetches daily Chicago crime data from the public API.
    * Source dataset lags by 10 days, so each DAG run processes `execution_date − 10 days`.
    * Writes a partitioned Parquet file (RAW/Bronze layer) to MinIO at the path:
      `s3a://chicago-crime-data/raw/year=YYYY/month=MM/day=DD/crime_data_YYYYMMDD.parquet`.
    * Skips gracefully (AirflowSkipException) when the API returns zero records.
    """,
):

    run_etl_task = PythonOperator(
        task_id="fetch_and_load_to_minio",
        python_callable=etl_callable_to_minio,
    )

    bronze_quality_check_task = PythonOperator(
        task_id="bronze_quality_check",
        python_callable=bronze_quality_check_callable,
        trigger_rule="all_done",  # Run even if ETL skipped
    )

    # Task dependencies
    run_etl_task >> bronze_quality_check_task
