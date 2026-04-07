"""
Chicago Crime Silver → Gold ML DAG.

Builds the enriched, feature-engineered gold Iceberg table for on-prem Spark MLlib
(RF / XGBoost). Triggered in parallel with the BQ gold DAG when silver completes.
See docs/chicago_crime_enterprise_system_design.md (steps 4–5, 10).
"""
from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_client_python_conf import SPARK_CLIENT_PYTHON_CONF

MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var=os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var=os.getenv("MINIO_SECRET_KEY", ""))
CHICAGO_BUCKET = Variable.get("CHICAGO_BUCKET", default_var=os.getenv("CHICAGO_BUCKET", "chicago-crime-data"))
ICEBERG_WAREHOUSE = Variable.get(
    "CHICAGO_ICEBERG_WAREHOUSE",
    default_var=os.getenv("CHICAGO_ICEBERG_WAREHOUSE", f"s3a://{CHICAGO_BUCKET}/silver"),
)
ICEBERG_GOLD_WAREHOUSE = Variable.get(
    "CHICAGO_ICEBERG_GOLD_WAREHOUSE",
    default_var=os.getenv("CHICAGO_ICEBERG_GOLD_WAREHOUSE", f"s3a://{CHICAGO_BUCKET}/gold"),
)

SPARK_CONF = {
    **SPARK_CLIENT_PYTHON_CONF,
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.list.version": "1",
    "spark.hadoop.fs.s3a.directory.marker.policy": "keep",
    "spark.sql.catalog.chicago_crime": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.chicago_crime.type": "hadoop",
    "spark.sql.catalog.chicago_crime.warehouse": ICEBERG_WAREHOUSE,
    "spark.sql.catalog.chicago_crime_gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.chicago_crime_gold.type": "hadoop",
    "spark.sql.catalog.chicago_crime_gold.warehouse": ICEBERG_GOLD_WAREHOUSE,
    "spark.sql.adaptive.enabled": "true",
    "spark.local.dir": "/tmp",
    "spark.sql.shuffle.partitions": "200",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",
    "spark.cores.max": "2",
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.cores": os.getenv("SPARK_DRIVER_CORES", "1"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "spark.dynamicAllocation.enabled": "false",
}

JARS_DIR = "/opt/airflow/jars"
HADOOP_AWS_JAR = f"{JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_DIR}/aws-java-sdk-bundle-1.12.367.jar"
ICEBERG_JAR = f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
ALL_JARS = f"{HADOOP_AWS_JAR},{AWS_SDK_JAR},{ICEBERG_JAR}"

with DAG(
    dag_id="chicago_crime_silver_to_gold_ml_dag",
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
    },
    start_date=pendulum.datetime(2025, 2, 17, tz="UTC"),
    schedule=None,
    catchup=True,
    tags=["chicago", "crime", "gold", "ml", "iceberg"],
    description="Gold Iceberg ML feature table for Spark MLlib (RF/XGBoost). Triggered with BQ gold DAG after silver.",
    max_active_runs=3,
    max_active_tasks=4,
    doc_md="""
    ### Chicago Crime Silver → Gold ML (Iceberg)

    Builds the enriched, feature-engineered gold Iceberg table `chicago_crime_gold.crime_ml_features`
    for on-prem Spark MLlib (Random Forest, GradientBoostedTrees/XGBoost). Features are aligned for
    tree models and future area-level risk prediction (see enterprise design doc steps 4–5).

    Triggered by `chicago_crime_raw_to_silver_dag` in parallel with `chicago_crime_silver_to_gold_dag`.
    """,
) as dag:

    # When triggered by upstream DAGs, dag_run.conf['process_date'] is present.
    # When run manually, we can pass in 'process_date' as a config parameter.
    _process_date_arg = "{{ params.process_date }}"

    build_gold_iceberg_ml = SparkSubmitOperator(
        task_id="build_gold_iceberg_ml_features",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_gold_iceberg_ml_features.py",
        jars=ALL_JARS,
        application_args=[
            "--iceberg_silver_table", "chicago_crime.chicago_crime_silver",
            "--iceberg_gold_table", "chicago_crime_gold.crime_ml_features",
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=45),
    )

    gold_ml_quality_check_task = SparkSubmitOperator(
        task_id="gold_ml_quality_check",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/great_expectations/run_validation_job.py",
        jars=ALL_JARS,
        application_args=[
            "--validation",
            "gold_ml_features",
            "--table_name",
            "chicago_crime_gold.crime_ml_features",
            "--process_date",
            _process_date_arg,
            "--persist",
        ],
        conf=SPARK_CONF,
        trigger_rule="all_done",
        execution_timeout=pendulum.duration(minutes=20),
    )

    trigger_ml_daily_pipeline = TriggerDagRunOperator(
        task_id="trigger_ml_daily_pipeline",
        trigger_dag_id="chicago_crime_ml_daily_dag",
        execution_date="{{ data_interval_start }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # Task dependencies
    build_gold_iceberg_ml >> gold_ml_quality_check_task >> trigger_ml_daily_pipeline
