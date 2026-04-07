"""
Chicago Crime ML — Daily Pipeline
==================================
Daily run (03:00 UTC): builds area-risk features → guards for Production model
→ scores ds..ds+10 with horizon-aware model → loads safety scores + crime
range into Postgres for the FastAPI map.

Task chain:
    build_area_risk_features
        >> production_model_guard          (ShortCircuit — skips score+load if no Prod model)
        >> score_area_risk                 (SparkSubmit — forward-fill + horizon + CI)
        >> load_studio_postgres            (SparkSubmit — writes to studio_area_snapshot)

Weekly training / eval / drift / promotion lives in chicago_crime_ml_weekly_dag.
"""
from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_client_python_conf import SPARK_CLIENT_PYTHON_CONF

logger = logging.getLogger(__name__)


def _db_creds_from_airflow_sqlalchemy_conn() -> tuple[str, str]:
    conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    if not conn:
        return "", ""
    parsed = urlparse(conn)
    return parsed.username or "", parsed.password or ""

# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var=os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var=os.getenv("MINIO_SECRET_KEY", ""))
CHICAGO_BUCKET = Variable.get("CHICAGO_BUCKET", default_var=os.getenv("CHICAGO_BUCKET", "chicago-crime-data"))
ICEBERG_WAREHOUSE = Variable.get("CHICAGO_ICEBERG_WAREHOUSE", default_var=f"s3a://{CHICAGO_BUCKET}/silver")
ICEBERG_GOLD_WAREHOUSE = Variable.get("CHICAGO_ICEBERG_GOLD_WAREHOUSE", default_var=f"s3a://{CHICAGO_BUCKET}/gold")
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI", default_var=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
MLFLOW_SPARK_DFS_TMPDIR = f"s3a://{CHICAGO_BUCKET}/_mlflow_spark_tmp"
_AIRFLOW_DB_USER, _AIRFLOW_DB_PASSWORD = _db_creds_from_airflow_sqlalchemy_conn()
CHICAGO_ANALYTICS_DB_USER = Variable.get(
    "CHICAGO_ANALYTICS_DB_USER", default_var=os.getenv("CHICAGO_ANALYTICS_DB_USER", _AIRFLOW_DB_USER or "admin")
)
CHICAGO_ANALYTICS_DB_PASSWORD = Variable.get(
    "CHICAGO_ANALYTICS_DB_PASSWORD",
    default_var=os.getenv("CHICAGO_ANALYTICS_DB_PASSWORD", _AIRFLOW_DB_PASSWORD),
)

JARS_DIR = "/opt/airflow/jars"
HADOOP_AWS_JAR = f"{JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_DIR}/aws-java-sdk-bundle-1.12.367.jar"
ICEBERG_JAR = f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
POSTGRES_JAR = f"{JARS_DIR}/postgresql-42.6.0.jar"
FEATURE_JARS = f"{HADOOP_AWS_JAR},{AWS_SDK_JAR},{ICEBERG_JAR}"
ALL_JARS = f"{FEATURE_JARS},{POSTGRES_JAR}"


def _analytics_jdbc_url() -> str:
    explicit = os.getenv("CHICAGO_ANALYTICS_JDBC_URL")
    if explicit:
        return explicit
    host = os.getenv("CHICAGO_ANALYTICS_DB_HOST", "postgres_db")
    port = os.getenv("CHICAGO_ANALYTICS_DB_PORT", "5432")
    db = os.getenv("CHICAGO_ANALYTICS_DB_NAME", "carvana_db")
    return f"jdbc:postgresql://{host}:{port}/{db}"


POSTGRES_CONFIG = {
    "jdbc_url": _analytics_jdbc_url(),
}

# ---------------------------------------------------------------------------
# Spark configs
# ---------------------------------------------------------------------------
_S3A_BASE = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.list.version": "1",
    "spark.hadoop.fs.s3a.directory.marker.policy": "keep",
}

SPARK_CONF_FEATURES = {
    **SPARK_CLIENT_PYTHON_CONF,
    **_S3A_BASE,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.catalog.chicago_crime": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.chicago_crime.type": "hadoop",
    "spark.sql.catalog.chicago_crime.warehouse": ICEBERG_WAREHOUSE,
    "spark.sql.catalog.chicago_crime_gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.chicago_crime_gold.type": "hadoop",
    "spark.sql.catalog.chicago_crime_gold.warehouse": ICEBERG_GOLD_WAREHOUSE,
    "spark.sql.adaptive.enabled": "true",
    "spark.local.dir": "/tmp",
    "spark.sql.shuffle.partitions": "200",
    "spark.cores.max": "2",
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
    "spark.dynamicAllocation.enabled": "false",
}

SPARK_CONF_ML = {
    **SPARK_CLIENT_PYTHON_CONF,
    **_S3A_BASE,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3.path.style.access": "true",
    "spark.sql.catalog.chicago_crime_gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.chicago_crime_gold.type": "hadoop",
    "spark.sql.catalog.chicago_crime_gold.warehouse": ICEBERG_GOLD_WAREHOUSE,
    "spark.sql.adaptive.enabled": "true",
    "spark.local.dir": "/tmp",
    "spark.sql.shuffle.partitions": "200",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",
    "spark.cores.max": "4",
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
    "spark.dynamicAllocation.enabled": "false",
    "spark.driverEnv.MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
    "spark.executorEnv.MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
    "spark.driverEnv.AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "spark.driverEnv.AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "spark.executorEnv.AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "spark.executorEnv.AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "spark.driverEnv.MLFLOW_S3_ENDPOINT_URL": MINIO_ENDPOINT,
    "spark.executorEnv.MLFLOW_S3_ENDPOINT_URL": MINIO_ENDPOINT,
}

# ---------------------------------------------------------------------------
# Template variables
# ---------------------------------------------------------------------------
PROCESS_DATE = "{{ dag_run.conf.get('process_date', ds) }}"
PREDICTION_END_DATE = "{{ macros.ds_add(dag_run.conf.get('process_date', ds), 10) }}"


# ---------------------------------------------------------------------------
# Guard callable
# ---------------------------------------------------------------------------
def _production_model_exists(model_name: str, tracking_uri: str) -> bool:
    """Return True iff a Production version exists in MLflow."""
    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(tracking_uri)
        versions = MlflowClient().get_latest_versions(model_name, stages=["Production"])
        if versions:
            logger.info(
                "Production model found for '%s' (v%s) — proceeding with score+load.",
                model_name,
                versions[0].version,
            )
            return True
        logger.info("No Production model for '%s' — short-circuiting score+load tasks.", model_name)
        return False
    except Exception as exc:
        logger.warning("Could not check MLflow registry for '%s': %s — skipping.", model_name, exc)
        return False


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="chicago_crime_ml_daily_dag",
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
    },
    start_date=pendulum.datetime(2026, 2, 20, tz="UTC"),
    schedule=None,  # Triggered by chicago_crime_silver_to_gold_ml_dag
    catchup=True,
    tags=["chicago", "crime", "ml", "daily", "features", "scoring", "postgres"],
    max_active_runs=1,
    max_active_tasks=4,
    doc_md="""
    ### Chicago Crime ML — Daily Pipeline

    Runs **every day at 03:00 UTC** after silver ETL. Task chain:

    1. **build_area_risk_features** — writes one `process_date` partition to
       `chicago_crime_gold.feature_crime_risk_area_daily`.
    2. **production_model_guard** — ShortCircuit: skips score+load if no Production
       model exists in MLflow (safe no-op until first weekly training completes).
    3. **score_area_risk** — loads the Production XGBoost model; forward-fills
       the latest known features (process_date snapshot) across `ds .. ds+10`;
       passes an explicit `horizon` feature (0–10) so the model accounts for the
       9-day data ingestion lag. Writes predictions + safety_score +
       predicted_count_low/high to `chicago_crime_gold.fact_area_risk_predictions`.
    4. **load_studio_postgres** — writes area snapshots (predictions, safety score,
       top-3 crime categories, 90 % Poisson CI range) to `studio_area_snapshot`
       for the FastAPI map endpoint.

    Weekly training, evaluation, drift monitoring, and model promotion are
    handled by **`chicago_crime_ml_weekly_dag`**.
    """,
) as dag:

    # ------------------------------------------------------------------
    # 1. Feature engineering
    # ------------------------------------------------------------------
    build_area_risk_features = SparkSubmitOperator(
        task_id="build_area_risk_features",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold_ml/build_area_risk_features.py",
        jars=FEATURE_JARS,
        application_args=[
            "--iceberg_silver_table", "chicago_crime.chicago_crime_silver",
            "--iceberg_gold_table", "chicago_crime_gold.feature_crime_risk_area_daily",
            "--process_date", PROCESS_DATE,
            "--start_date", PROCESS_DATE,
            "--area_type", "community_area",
        ],
        conf=SPARK_CONF_FEATURES,
        execution_timeout=pendulum.duration(minutes=120),
    )

    # ------------------------------------------------------------------
    # 2. Production model guard (ShortCircuit)
    # ------------------------------------------------------------------
    production_model_guard = ShortCircuitOperator(
        task_id="production_model_guard",
        python_callable=_production_model_exists,
        op_kwargs={
            "model_name": "chicago_crime_area_risk",
            "tracking_uri": MLFLOW_TRACKING_URI,
        },
    )

    # ------------------------------------------------------------------
    # 3. Horizon-aware scoring (ds .. ds+10)
    # ------------------------------------------------------------------
    score_area_risk = SparkSubmitOperator(
        task_id="score_area_risk",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold_ml/ml/score_crime_risk_area.py",
        jars=FEATURE_JARS,
        application_args=[
            "--feature_table", "chicago_crime_gold.feature_crime_risk_area_daily",
            "--predictions_table", "chicago_crime_gold.fact_area_risk_predictions",
            "--model_uri", "models:/chicago_crime_area_risk/Production",
            "--prediction_start_date", PROCESS_DATE,
            "--prediction_end_date", PREDICTION_END_DATE,
            "--process_date", PROCESS_DATE,
            "--prediction_target", "total_count",
            "--tracking_uri", MLFLOW_TRACKING_URI,
            "--experiment", "crime_risk_area",
            "--dfs_tmpdir", MLFLOW_SPARK_DFS_TMPDIR,
        ],
        conf=SPARK_CONF_ML,
        execution_timeout=pendulum.duration(minutes=45),
    )

    # ------------------------------------------------------------------
    # 4. Postgres load (safety score + top-3 crimes + CI range)
    # ------------------------------------------------------------------
    load_studio_postgres = SparkSubmitOperator(
        task_id="load_studio_postgres",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold_ml/studio/load_studio_postgres.py",
        jars=ALL_JARS,
        application_args=[
            "--process_date", PROCESS_DATE,
            "--jdbc_url", POSTGRES_CONFIG["jdbc_url"],
            "--jdbc_user", CHICAGO_ANALYTICS_DB_USER,
            "--jdbc_password", CHICAGO_ANALYTICS_DB_PASSWORD,
            "--community_areas_path",
            f"s3a://{CHICAGO_BUCKET}/reference/chicago_community_areas.parquet",
        ],
        conf=SPARK_CONF_ML,
        execution_timeout=pendulum.duration(minutes=45),
    )

    # ------------------------------------------------------------------
    # Task chain
    # ------------------------------------------------------------------
    build_area_risk_features >> production_model_guard >> score_area_risk >> load_studio_postgres
