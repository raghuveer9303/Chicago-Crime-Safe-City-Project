from __future__ import annotations
import os
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

from spark_client_python_conf import SPARK_CLIENT_PYTHON_CONF
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2, 
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var=os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var=os.getenv("MINIO_SECRET_KEY", ""))
CHICAGO_BUCKET = Variable.get("CHICAGO_BUCKET", default_var=os.getenv("CHICAGO_BUCKET", "chicago-crime-data"))
ICEBERG_WAREHOUSE = Variable.get("CHICAGO_ICEBERG_WAREHOUSE", default_var=os.getenv("CHICAGO_ICEBERG_WAREHOUSE", f"s3a://{CHICAGO_BUCKET}/silver"))
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_BQ_DATASET = os.getenv("GCP_BQ_DATASET", "chicago_crime_gold")
GCP_BQ_DATASET_STAGING = os.getenv("GCP_BQ_DATASET_STAGING", "chicago_crime_gold_temp")

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
    "spark.sql.adaptive.enabled": "true",
    "spark.local.dir": "/tmp",
    "spark.sql.shuffle.partitions": "200",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",
    "spark.cores.max": "2",
    # Cap each dimensional-model job at 2 total cores by default
    # (1 executor × 2 cores). This keeps individual jobs bounded while
    # allowing multiple apps to share the 6-core cluster safely.
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.cores": os.getenv("SPARK_DRIVER_CORES", "1"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "spark.dynamicAllocation.enabled": "false",
}

POSTGRES_CONFIG = {
    "jdbc_url": os.getenv(
        "CHICAGO_ANALYTICS_JDBC_URL",
        "jdbc:postgresql://postgres_db:5432/chicago_crime_analytics",
    ),
    "user": os.getenv("CHICAGO_ANALYTICS_DB_USER", "admin"),
    "password": os.getenv("CHICAGO_ANALYTICS_DB_PASSWORD", ""),
}

JARS_DIR = "/opt/airflow/jars"
POSTGRES_DRIVER_PATH = f"{JARS_DIR}/postgresql-42.6.0.jar"
HADOOP_AWS_JAR = f"{JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_DIR}/aws-java-sdk-bundle-1.12.367.jar"
ICEBERG_JAR = f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
BIGQUERY_JAR = f"{JARS_DIR}/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

ALL_JARS = f"{HADOOP_AWS_JAR},{AWS_SDK_JAR},{ICEBERG_JAR},{BIGQUERY_JAR}"

with DAG(
    dag_id="chicago_crime_silver_to_gold_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 2, 17, tz="UTC"),
    schedule=None,  # Triggered by chicago_crime_process_to_silver after silver completes
    catchup=True,
    tags=["chicago", "crime", "dimensional", "postgres", "analytics"],
    description="Dimensional modeling pipeline for Chicago Crime analytics - creates star schema in PostgreSQL",
    max_active_runs=3,
    max_active_tasks=10,
) as dag:

    # When triggered by upstream DAGs, dag_run.conf['process_date'] is present.
    # When run manually, we can pass in 'process_date' as a config parameter.
    _process_date_arg = "{{ params.process_date }}"

    # Build Dimensions into staging dataset
    build_dim_date = SparkSubmitOperator(
        task_id="build_dim_date",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_crime_dim_date.py",
        jars=ALL_JARS,
        application_args=[
            "--gcp_project_id", GCP_PROJECT_ID,
            "--bq_dataset", GCP_BQ_DATASET_STAGING,
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=30),
    )

    build_dim_location = SparkSubmitOperator(
        task_id="build_dim_location",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_crime_dim_location.py",
        jars=ALL_JARS,
        application_args=[
            "--gcp_project_id", GCP_PROJECT_ID,
            "--bq_dataset", GCP_BQ_DATASET_STAGING,
            "--iceberg_table", "chicago_crime.chicago_crime_silver",
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=30),
    )

    build_dim_crime_type = SparkSubmitOperator(
        task_id="build_dim_crime_type",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_crime_dim_crime_type.py",
        jars=ALL_JARS,
        application_args=[
            "--gcp_project_id", GCP_PROJECT_ID,
            "--bq_dataset", GCP_BQ_DATASET_STAGING,
            "--iceberg_table", "chicago_crime.chicago_crime_silver",
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=30),
    )

    build_dim_time_of_day = SparkSubmitOperator(
        task_id="build_dim_time_of_day",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_crime_dim_time_of_day.py",
        jars=ALL_JARS,
        application_args=[
            "--gcp_project_id", GCP_PROJECT_ID,
            "--bq_dataset", GCP_BQ_DATASET_STAGING,
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=30),
    )

    # Build Facts into staging dataset
    build_fact_crime_incidents = SparkSubmitOperator(
        task_id="build_fact_crime_incidents",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_fact_crime_incidents.py",
        jars=ALL_JARS,
        application_args=[
            "--gcp_project_id", GCP_PROJECT_ID,
            "--bq_dataset", GCP_BQ_DATASET_STAGING,
            "--iceberg_table", "chicago_crime.chicago_crime_silver",
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=45),
    )

    build_fact_crime_summary = SparkSubmitOperator(
        task_id="build_fact_crime_summary",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/build_fact_crime_summary.py",
        jars=ALL_JARS,
        application_args=[
            "--gcp_project_id", GCP_PROJECT_ID,
            "--bq_dataset", GCP_BQ_DATASET_STAGING,
            "--iceberg_table", "chicago_crime.chicago_crime_silver",
            "--process_date", _process_date_arg,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=30),
    )

    # Allow BigQuery dimension writes to be visible before fact tasks read (avoid read-after-write race).
    wait_for_bq_visibility = BashOperator(
        task_id="wait_for_bq_visibility",
        bash_command="sleep 60",
    )

    # Final merge from staging to gold (idempotent per process_date)
    merge_gold = BashOperator(
        task_id="merge_gold_temp_to_final",
        bash_command=(
            "python {{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold/merge_gold_from_staging.py "
            "--process_date {{ dag_run.conf['process_date'] }}"
        ),
    )

    gold_bi_quality_check_task = SparkSubmitOperator(
        task_id="gold_bi_quality_check",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/great_expectations/run_validation_job.py",
        jars=ALL_JARS,
        application_args=[
            "--validation",
            "gold_bi",
            "--fact_table_name",
            "chicago_crime_gold.fact_crime_incidents",
            "--dim_tables_json",
            (
                '{"dim_date":"chicago_crime_gold.dim_date",'
                '"dim_location":"chicago_crime_gold.dim_location",'
                '"dim_crime_type":"chicago_crime_gold.dim_crime_type",'
                '"dim_time_of_day":"chicago_crime_gold.dim_time_of_day"}'
            ),
            "--process_date",
            _process_date_arg,
            "--persist",
        ],
        conf=SPARK_CONF,
        trigger_rule="all_done",
        execution_timeout=pendulum.duration(minutes=20),
    )

    # Dimensions run sequentially to prevent simultaneous Spark apps from
    # overwhelming the Airflow worker heartbeat (zombie detection loop).
    build_dim_date >> build_dim_location >> build_dim_crime_type >> build_dim_time_of_day
    # 60s delay so BQ dimension tables are visible before fact tasks read.
    build_dim_time_of_day >> wait_for_bq_visibility >> build_fact_crime_incidents >> build_fact_crime_summary >> merge_gold >> gold_bi_quality_check_task
