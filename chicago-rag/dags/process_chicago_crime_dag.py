from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_client_python_conf import SPARK_CLIENT_PYTHON_CONF

MINIO_ENDPOINT = Variable.get(
    "MINIO_ENDPOINT",
    default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
)
MINIO_ACCESS_KEY = Variable.get(
    "MINIO_ACCESS_KEY",
    default_var=os.getenv("MINIO_ACCESS_KEY", ""),
)
MINIO_SECRET_KEY = Variable.get(
    "MINIO_SECRET_KEY",
    default_var=os.getenv("MINIO_SECRET_KEY", ""),
)
CHICAGO_BUCKET = Variable.get("CHICAGO_BUCKET", default_var="chicago-crime-data")
ICEBERG_WAREHOUSE = Variable.get(
    "CHICAGO_ICEBERG_WAREHOUSE", default_var=f"s3a://{CHICAGO_BUCKET}/silver"
)

GOLD_DAG_ID = "chicago_crime_silver_to_gold_dag"
GOLD_ML_DAG_ID = "chicago_crime_silver_to_gold_ml_dag"
AREA_RISK_FEATURE_DAG_ID = "chicago_crime_ml_daily_dag"
NEO4J_LOAD_DAG_ID = "chicago_crime_neo4j_maintenance_and_load"

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
    # Spatial SQL (Sedona) for high-performance boundary joins
    "spark.sql.extensions": "org.apache.sedona.sql.SedonaSqlExtensions",
    # NOTE: Do NOT use Kryo as the global serializer with Iceberg writes.
    # It causes KryoException/ClassCastException when serializing Iceberg commit metadata.
    # Use JavaSerializer here; Sedona can still use Kryo internally for its own types.
    "spark.serializer": "org.apache.spark.serializer.JavaSerializer",
    "spark.local.dir": "/tmp/spark-local",
    "spark.sql.shuffle.partitions": "200",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",
    "spark.cores.max": "2",
    # Cap each job at 2 total cores by default:
    #   1 executor × 2 cores. This ensures that even when many jobs are
    # queued, no single Spark application can consume more than 2 CPU
    # cores on the 6-core cluster unless overridden via env vars.
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.cores": os.getenv("SPARK_DRIVER_CORES", "1"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "spark.dynamicAllocation.enabled": "false",
}

# Pre-downloaded JAR paths
JARS_DIR = "/opt/airflow/jars"
POSTGRES_DRIVER_PATH = f"{JARS_DIR}/postgresql-42.6.0.jar"
HADOOP_AWS_JAR = f"{JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_DIR}/aws-java-sdk-bundle-1.12.367.jar"
ICEBERG_JAR = f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
SEDONA_JAR = f"{JARS_DIR}/sedona-spark-shaded-3.5_2.12-1.8.1.jar"
GEOTOOLS_JAR = f"{JARS_DIR}/geotools-wrapper-1.8.1-33.1.jar"

ALL_JARS = ",".join(
    [
        POSTGRES_DRIVER_PATH,
        HADOOP_AWS_JAR,
        AWS_SDK_JAR,
        ICEBERG_JAR,
        SEDONA_JAR,
        GEOTOOLS_JAR,
    ]
)

with DAG(
    dag_id="chicago_crime_raw_to_silver_dag",
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    start_date=pendulum.datetime(2025, 2, 17, tz="UTC"),
    # Triggered only by the MinIO sensor DAG once the RAW file lands.
    schedule=None,
    max_active_runs=5,
    max_active_tasks=16,
    catchup=True,
    tags=["chicago", "crime", "silver", "iceberg"],
    doc_md="""
    ### Chicago Crime RAW → Silver Iceberg DAG

    Cleans, validates, and deduplicates crime data from the bronze layer (MinIO)
    and writes it to the Silver Iceberg table using dynamic partition overwrite
    (idempotent — safe to re-run for the same date).

    It is triggered by `chicago_crime_raw_to_silver_sensor_dag` once the
    corresponding RAW/Bronze parquet file lands in MinIO, and then triggers
    BI gold (BigQuery), gold ML (Iceberg), area-risk features, and Neo4j load
    in parallel after the silver quality check.
    """,
) as dag:

    process_and_load_task = SparkSubmitOperator(
        task_id="clean_validate_dedup_insert_to_iceberg",
        conn_id="spark_default",
        application="/opt/airflow/dags/chicago_crime_scripts/silver/chicago_silver_layer_etl.py",
        jars=ALL_JARS,
        application_args=[
            "--input",
            f"s3a://{CHICAGO_BUCKET}/{{{{ dag_run.conf['s3_key'] }}}}",
            "--table",
            "chicago_crime.chicago_crime_silver",
            "--process_date",
            "{{ dag_run.conf['target_date'] }}",
            "--wards_path",
            f"s3a://{CHICAGO_BUCKET}/reference/chicago_wards_2023.parquet",
            "--community_areas_path",
            f"s3a://{CHICAGO_BUCKET}/reference/chicago_community_areas.parquet",
            "--police_districts_path",
            f"s3a://{CHICAGO_BUCKET}/reference/chicago_police_districts_2012.parquet",
            "--police_beats_path",
            f"s3a://{CHICAGO_BUCKET}/reference/chicago_police_beats_2012.parquet",
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=60),
    )

    silver_quality_check_task = SparkSubmitOperator(
        task_id="silver_quality_check",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/great_expectations/run_validation_job.py",
        jars=ALL_JARS,
        application_args=[
            "--validation",
            "silver_iceberg",
            "--table_name",
            "chicago_crime.chicago_crime_silver",
            "--process_date",
            "{{ dag_run.conf['target_date'] }}",
            "--persist",
        ],
        conf=SPARK_CONF,
        trigger_rule="all_done",
        execution_timeout=pendulum.duration(minutes=20),
    )

    trigger_gold_dag = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id=GOLD_DAG_ID,
        conf={
            "process_date": "{{ dag_run.conf['target_date'] }}",
            "triggered_by": "chicago_crime_raw_to_silver_dag",
        },
        wait_for_completion=False,
    )

    trigger_gold_ml_dag = TriggerDagRunOperator(
        task_id="trigger_gold_ml_dag",
        trigger_dag_id=GOLD_ML_DAG_ID,
        conf={
            "process_date": "{{ dag_run.conf['target_date'] }}",
            "triggered_by": "chicago_crime_raw_to_silver_dag",
        },
        wait_for_completion=False,
    )

    trigger_area_risk_dag = TriggerDagRunOperator(
        task_id="trigger_area_risk_features_dag",
        trigger_dag_id=AREA_RISK_FEATURE_DAG_ID,
        conf={
            "process_date": "{{ dag_run.conf['target_date'] }}",
            "triggered_by": "chicago_crime_raw_to_silver_dag",
        },
        wait_for_completion=False,
    )

    trigger_neo4j_load = TriggerDagRunOperator(
        task_id="trigger_neo4j_load",
        trigger_dag_id=NEO4J_LOAD_DAG_ID,
        conf={
            "process_date": "{{ dag_run.conf['target_date'] }}",
            "triggered_by": "chicago_crime_raw_to_silver_dag",
        },
        wait_for_completion=False,
    )

    # Task dependencies: quality check after load, then trigger downstream DAGs in parallel
    process_and_load_task >> silver_quality_check_task >> [
        trigger_gold_dag,
        trigger_gold_ml_dag,
        trigger_area_risk_dag,
        trigger_neo4j_load,
    ]
