from __future__ import annotations
import os
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

MINIO_BUCKET = os.getenv("CHICAGO_BUCKET", "chicago-crime-data")
PROCESSING_DAG_ID = "chicago_crime_raw_to_silver_dag"
MINIO_CONN_ID = os.getenv("CHICAGO_MINIO_CONN_ID", "chi_minio")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}

with DAG(
    dag_id="chicago_crime_raw_to_silver_sensor_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 2, 17, tz="UTC"),
    schedule="@daily",
    catchup=True,
# ──────────  concurrency caps  ──────────
    max_active_runs=5,        # at most 5 RUNNING DagRuns at once
    max_active_tasks=10,      # equivalent to max_queued_dagruns in newer versions
    # ────────────────────────────────────────
    tags=["chicago", "crime", "minio", "sensor", "silver"],
    doc_md="""
    ### Chicago Crime RAW → Silver Sensor DAG

    This DAG waits for a daily RAW/Bronze crime data file to appear in the
    MinIO bucket and then triggers the RAW → Silver processing DAG
    (`chicago_crime_raw_to_silver_dag`).
    """,
) as dag:

    s3_key_path = (
        "raw/"
        "year={{ (execution_date.subtract(days=10)).year }}/"
        "month={{ (execution_date.subtract(days=10)).strftime('%m') }}/"
        "day={{ (execution_date.subtract(days=10)).strftime('%d') }}/"
        "crime_data_{{ (execution_date.subtract(days=10)).strftime('%Y%m%d') }}.parquet"
    )

    wait_for_file_in_minio = S3KeySensor(
        task_id="wait_for_file_in_minio",
        bucket_name=MINIO_BUCKET,
        bucket_key=s3_key_path,
        aws_conn_id=MINIO_CONN_ID,
        poke_interval=60,
        timeout=18 * 60 * 60,
    )

    trigger_processing_dag = TriggerDagRunOperator(
        task_id="trigger_processing_dag",
        trigger_dag_id=PROCESSING_DAG_ID,
        conf={
            "s3_key": s3_key_path,
            "target_date": "{{ (execution_date.subtract(days=10)).strftime('%Y-%m-%d') }}",
        },
    )

    wait_for_file_in_minio >> trigger_processing_dag