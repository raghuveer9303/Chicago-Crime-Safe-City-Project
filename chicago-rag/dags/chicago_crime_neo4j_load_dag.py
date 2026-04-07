from __future__ import annotations

import os
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_client_python_conf import SPARK_CLIENT_PYTHON_CONF

# --- Environment/Airflow Variables ---
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var=os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var=os.getenv("MINIO_SECRET_KEY", ""))
CHICAGO_BUCKET = Variable.get("CHICAGO_BUCKET", default_var=os.getenv("CHICAGO_BUCKET", "chicago-crime-data"))
ICEBERG_WAREHOUSE = Variable.get(
    "CHICAGO_ICEBERG_WAREHOUSE",
    default_var=os.getenv("CHICAGO_ICEBERG_WAREHOUSE", f"s3a://{CHICAGO_BUCKET}/silver"),
)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USERNAME = Variable.get("NEO4J_USERNAME", default_var=os.getenv("NEO4J_USERNAME", "neo4j"))
NEO4J_PASSWORD = Variable.get("NEO4J_PASSWORD", default_var=os.getenv("NEO4J_PASSWORD", ""))
AIRFLOW_HOME = Variable.get("airflow_home", default_var=os.getenv("AIRFLOW_HOME", "/opt/airflow"))

# --- Spark Configuration ---
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
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.cores": os.getenv("SPARK_DRIVER_CORES", "1"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "spark.dynamicAllocation.enabled": "false",
}

# --- JARs ---
JARS_DIR = f"{AIRFLOW_HOME}/jars"
HADOOP_AWS_JAR = f"{JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_DIR}/aws-java-sdk-bundle-1.12.367.jar"
ICEBERG_JAR = f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
BIGQUERY_JAR = f"{JARS_DIR}/spark-bigquery-with-dependencies_2.12-0.36.1.jar"
NEO4J_SPARK_JAR = f"{JARS_DIR}/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar"
ALL_JARS = f"{HADOOP_AWS_JAR},{AWS_SDK_JAR},{ICEBERG_JAR},{BIGQUERY_JAR},{NEO4J_SPARK_JAR}"

# --- Python Scripts ---
SCRIPTS_PATH = f"{AIRFLOW_HOME}/dags/chicago_crime_scripts/gold"
ENSURE_INDEXES_SCRIPT = f"{SCRIPTS_PATH}/ensure_neo4j_indexes.py"
LOAD_NEO4J_SCRIPT = f"{SCRIPTS_PATH}/build_gold_neo4j_nodes_edges.py"
PRUNE_DATA_SCRIPT = f"{SCRIPTS_PATH}/prune_old_neo4j_data.py"

# --- DAG Definition ---
with DAG(
    dag_id="chicago_crime_neo4j_maintenance_and_load",
    start_date=pendulum.datetime(2025, 2, 17, tz="UTC"),
    catchup=True,
    schedule=None,  # Trigger-only
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["chicago-crime", "neo4j", "production"],
    max_active_runs=1,
    max_active_tasks=3,
) as dag:
    # When triggered by upstream DAGs, dag_run.conf['process_date'] is present.
    # For scheduler/manual runs, fall back to `ds`.
    _process_date_arg = "{{ (dag_run.conf or {}).get('process_date', ds) }}"
    _neo4j_cli_args = f"--neo4j_uri {NEO4J_URI} --neo4j_user {NEO4J_USERNAME} --neo4j_password {NEO4J_PASSWORD}"

    ensure_neo4j_indexes = BashOperator(
        task_id="ensure_neo4j_indexes",
        bash_command=f"python {ENSURE_INDEXES_SCRIPT} {_neo4j_cli_args}",
        doc_md="Ensures Neo4j unique constraints and indexes exist for performance.",
    )

    load_neo4j_nodes_and_edges = SparkSubmitOperator(
        task_id="load_neo4j_nodes_and_edges",
        conn_id="spark_default",
        application=LOAD_NEO4J_SCRIPT,
        jars=ALL_JARS,
        application_args=[
            "--iceberg_table", "chicago_crime.chicago_crime_silver",
            "--process_date", _process_date_arg,
            "--neo4j_uri", NEO4J_URI,
            "--neo4j_user", NEO4J_USERNAME,
            "--neo4j_password", NEO4J_PASSWORD,
        ],
        conf=SPARK_CONF,
        execution_timeout=pendulum.duration(minutes=30),
        doc_md="Loads daily crime data from Iceberg into Neo4j.",
    )

    prune_old_neo4j_data = BashOperator(
        task_id="prune_old_neo4j_data",
        bash_command=f"python {PRUNE_DATA_SCRIPT} {_neo4j_cli_args} --retention_days 365",
        doc_md="Prunes data older than one year to maintain a rolling window.",
    )

    # --- Task Dependencies ---
    ensure_neo4j_indexes >> load_neo4j_nodes_and_edges >> prune_old_neo4j_data

