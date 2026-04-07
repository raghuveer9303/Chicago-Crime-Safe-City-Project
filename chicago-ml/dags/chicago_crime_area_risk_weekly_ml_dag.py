from __future__ import annotations

import logging
import math
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_client_python_conf import SPARK_CLIENT_PYTHON_CONF

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared config (MinIO / Iceberg / MLflow)
# ---------------------------------------------------------------------------
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
CHICAGO_BUCKET = Variable.get(
    "CHICAGO_BUCKET",
    default_var=os.getenv("CHICAGO_BUCKET", "chicago-crime-data"),
)
ICEBERG_GOLD_WAREHOUSE = Variable.get(
    "CHICAGO_ICEBERG_GOLD_WAREHOUSE",
    default_var=f"s3a://{CHICAGO_BUCKET}/gold",
)
MLFLOW_TRACKING_URI = Variable.get(
    "MLFLOW_TRACKING_URI",
    default_var=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
)

# Shared DFS tmpdir for mlflow.spark.log_model in cluster mode.
MLFLOW_SPARK_DFS_TMPDIR = "s3a://chicago-crime-data/_mlflow_spark_tmp"

# ---------------------------------------------------------------------------
# Spark configs
# ---------------------------------------------------------------------------
SPARK_CONF_ML = {
    **SPARK_CLIENT_PYTHON_CONF,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.list.version": "1",
    "spark.hadoop.fs.s3a.directory.marker.policy": "keep",
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
    "spark.cores.max": "6",
    "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.cores": os.getenv("SPARK_DRIVER_CORES", "1"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
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

JARS_DIR = "/opt/airflow/jars"
HADOOP_AWS_JAR = f"{JARS_DIR}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_DIR}/aws-java-sdk-bundle-1.12.367.jar"
ICEBERG_JAR = f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"
ALL_JARS = f"{HADOOP_AWS_JAR},{AWS_SDK_JAR},{ICEBERG_JAR}"


# ---------------------------------------------------------------------------
# Promotion logic
# ---------------------------------------------------------------------------
RUN_PARAM_SPARK_MODEL_S3A_PATH = "spark_model_s3a_path"


def _metric_or_inf(client, run_id: str, key: str) -> float:
    try:
        run = client.get_run(run_id)
        value = run.data.metrics.get(key)
        return float(value) if value is not None else float("inf")
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read metric %s from run %s: %s", key, run_id, exc)
        return float("inf")


def _sparkml_artifacts_complete(client, run_id: str, artifact_path: str) -> bool:
    """
    Return True iff the sparkml sentinel file (metadata/part-00000) is listed
    under <artifact_path>/sparkml/metadata/ for the given run.
    """
    try:
        sentinel_path = f"{artifact_path}/sparkml/metadata"
        artifacts = client.list_artifacts(run_id, sentinel_path)
        names = {a.path.split("/")[-1] for a in artifacts}
        return "part-00000" in names
    except Exception as exc:
        logger.warning(
            "Could not list sparkml artifacts for run %s (path=%s/sparkml/metadata): %s",
            run_id,
            artifact_path,
            exc,
        )
        return False


def _resolve_run_spark_model_path(client, run_id: str) -> str | None:
    """Read direct Spark model path logged by training run metadata."""
    try:
        run = client.get_run(run_id)
        return (
            run.data.params.get(RUN_PARAM_SPARK_MODEL_S3A_PATH)
            or run.data.tags.get(RUN_PARAM_SPARK_MODEL_S3A_PATH)
            or None
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not read run metadata for %s: %s", run_id, exc)
        return None


def _verify_s3a_sparkml_path_nonempty(
    s3a_path: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> bool:
    """Return True if s3a://.../metadata/part-00000 exists."""
    import boto3
    import botocore.exceptions

    rest = s3a_path.removeprefix("s3a://")
    bucket, _, prefix = rest.partition("/")
    sentinel_key = f"{prefix.rstrip('/')}/metadata/part-00000"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    try:
        s3.head_object(Bucket=bucket, Key=sentinel_key)
        return True
    except botocore.exceptions.ClientError:
        return False


def _promote_if_better(
    tracking_uri: str,
    model_name: str,
    metric_key: str = "test_rmse",
    artifact_path: str = "spark_xgboost_pipeline",
) -> None:
    """
    Promote the newest registered model version if it improves on Production.

    Uses MLflow Model Registry for controlled replacement and refuses to
    promote incomplete sparkml artifacts.
    """
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    def _safe_set_model_version_tag(version: str, key: str, value: str) -> None:
        try:
            client.set_model_version_tag(name=model_name, version=version, key=key, value=value)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to set model version tag %s=%s for %s v%s: %s", key, value, model_name, version, exc)

    def _safe_set_run_tag(run_id: str, key: str, value: str) -> None:
        try:
            client.set_tag(run_id=run_id, key=key, value=value)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to set run tag %s=%s on run %s: %s", key, value, run_id, exc)

    def _safe_log_run_metric(run_id: str, key: str, value: float) -> None:
        if not math.isfinite(float(value)):
            return
        try:
            client.log_metric(run_id=run_id, key=key, value=float(value))
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to log metric %s=%.6f on run %s: %s", key, value, run_id, exc)

    candidates = client.search_model_versions(
        f"name='{model_name}'",
        order_by=["creation_timestamp DESC"],
        max_results=1,
    )
    if not candidates:
        logger.info("No registered versions found for %s; skipping promotion.", model_name)
        return

    new_ver = candidates[0]

    # Guard: verify candidate model is loadable via the direct Spark model path.
    spark_model_s3a_path = _resolve_run_spark_model_path(client, run_id=new_ver.run_id)
    if spark_model_s3a_path:
        if not _verify_s3a_sparkml_path_nonempty(
            spark_model_s3a_path,
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        ):
            raise RuntimeError(
                f"Refusing to promote {model_name} v{new_ver.version}: direct spark model path is incomplete ({spark_model_s3a_path})."
            )
    elif not _sparkml_artifacts_complete(client, run_id=new_ver.run_id, artifact_path=artifact_path):
        raise RuntimeError(
            f"Refusing to promote {model_name} v{new_ver.version}: no direct model path found in run metadata and MLflow spark artifacts are incomplete under {artifact_path}/sparkml/metadata/."
        )

    new_score = _metric_or_inf(client, run_id=new_ver.run_id, key=metric_key)

    prod = client.get_latest_versions(model_name, stages=["Production"])
    if prod:
        prod_ver = prod[0]
        prod_score = _metric_or_inf(client, run_id=prod_ver.run_id, key=metric_key)
    else:
        prod_ver = None
        prod_score = float("inf")

    logger.info(
        "Promotion check for %s: candidate v%s %s=%.6f vs Production %s",
        model_name,
        new_ver.version,
        metric_key,
        new_score,
        f"v{prod_ver.version} {metric_key}={prod_score:.6f}" if prod_ver else "NONE",
    )

    # Ensure candidate is visible as challenger before decision.
    _safe_set_model_version_tag(new_ver.version, "model_role", "challenger")
    _safe_set_model_version_tag(new_ver.version, "challenger", "true")
    _safe_set_model_version_tag(new_ver.version, "champion", "false")

    # Persist decision context on candidate run for traceability.
    _safe_log_run_metric(new_ver.run_id, f"promotion_candidate_{metric_key}", new_score)
    _safe_log_run_metric(new_ver.run_id, f"promotion_production_{metric_key}", prod_score)
    _safe_set_run_tag(new_ver.run_id, "promotion_baseline_version", str(prod_ver.version) if prod_ver else "none")

    if new_score < prod_score:
        client.transition_model_version_stage(
            name=model_name,
            version=new_ver.version,
            stage="Production",
            archive_existing_versions=True,
        )
        _safe_set_model_version_tag(new_ver.version, "model_role", "champion")
        _safe_set_model_version_tag(new_ver.version, "champion", "true")
        _safe_set_model_version_tag(new_ver.version, "challenger", "false")
        _safe_set_run_tag(new_ver.run_id, "promotion_status", "promoted")
        _safe_set_run_tag(new_ver.run_id, "promotion_decision", "promoted")
        if prod_ver:
            _safe_set_model_version_tag(prod_ver.version, "model_role", "archived")
            _safe_set_model_version_tag(prod_ver.version, "champion", "false")
            _safe_set_model_version_tag(prod_ver.version, "challenger", "false")
        logger.info("Promoted %s v%s to Production.", model_name, new_ver.version)
    else:
        if prod_ver:
            _safe_set_model_version_tag(prod_ver.version, "model_role", "champion")
            _safe_set_model_version_tag(prod_ver.version, "champion", "true")
            _safe_set_model_version_tag(prod_ver.version, "challenger", "false")
        _safe_set_run_tag(new_ver.run_id, "promotion_status", "not_promoted")
        _safe_set_run_tag(new_ver.run_id, "promotion_decision", "rejected_no_improvement")
        logger.info("Not promoting %s v%s (no improvement).", model_name, new_ver.version)


with DAG(
    dag_id="chicago_crime_ml_weekly_dag",
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
    },
    start_date=pendulum.datetime(2026, 2, 20, tz="UTC"),
    schedule=timedelta(days=7),
    catchup=True,
    tags=["chicago", "crime", "ml", "weekly", "train", "eval", "drift"],
    max_active_runs=1,
    max_active_tasks=8,
    doc_md="""
    ### Chicago Crime ML — Weekly Train / Eval / Promote

    Runs **every 7 days**. Pipeline:
    `evaluate` → `drift` → `train (horizon-aware, h=1..10)` → `promote_if_better`.

    After promotion, the **`chicago_crime_ml_daily_dag`** automatically picks up
    the new Production model on its next scheduled run — no manual trigger needed.

    Eval/drift windows are backward-looking relative to `ds`.
    """,
) as dag:

    evaluate_area_risk = SparkSubmitOperator(
        task_id="evaluate_area_risk",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold_ml/ml/evaluate_crime_risk_predictions.py",
        jars=ALL_JARS,
        application_args=[
            "--predictions_table",
            "chicago_crime_gold.fact_area_risk_predictions",
            "--feature_table",
            "chicago_crime_gold.feature_crime_risk_area_daily",
            "--evaluation_table",
            "chicago_crime_gold.model_evaluation_runs",
            "--eval_window_start",
            "{{ macros.ds_add(ds, -7) }}",
            "--eval_window_end",
            "{{ macros.ds_add(ds, -1) }}",
            "--process_date",
            "{{ ds }}",
            "--prediction_target",
            "total_count",
        ],
        conf=SPARK_CONF_ML,
        execution_timeout=pendulum.duration(minutes=30),
    )

    compute_area_risk_drift = SparkSubmitOperator(
        task_id="compute_area_risk_drift",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold_ml/ml/compute_crime_risk_drift.py",
        jars=ALL_JARS,
        application_args=[
            "--feature_table",
            "chicago_crime_gold.feature_crime_risk_area_daily",
            "--predictions_table",
            "chicago_crime_gold.fact_area_risk_predictions",
            "--drift_table",
            "chicago_crime_gold.model_drift_metrics",
            "--prediction_date",
            "{{ macros.ds_add(ds, -1) }}",
            "--reference_days",
            "30",
            "--process_date",
            "{{ ds }}",
            "--prediction_target",
            "total_count",
        ],
        conf=SPARK_CONF_ML,
        execution_timeout=pendulum.duration(minutes=30),
    )

    train_area_risk_model = SparkSubmitOperator(
        task_id="train_area_risk_model",
        conn_id="spark_default",
        application="{{ var.value.airflow_home }}/dags/chicago_crime_scripts/gold_ml/ml/train_crime_risk_area_model.py",
        jars=ALL_JARS,
        env_vars={
            "MLFLOW_SPARK_DFS_TMPDIR": MLFLOW_SPARK_DFS_TMPDIR,
            "MLFLOW_DFS_TMPDIR": MLFLOW_SPARK_DFS_TMPDIR,
        },
        application_args=[
            "--feature_table",
            "chicago_crime_gold.feature_crime_risk_area_daily",
            "--train_start_date",
            "{{ macros.ds_add(ds, -392) }}",
            "--train_end_date",
            "{{ macros.ds_add(ds, -92) }}",
            "--val_start_date",
            "{{ macros.ds_add(ds, -91) }}",
            "--val_end_date",
            "{{ macros.ds_add(ds, -61) }}",
            "--test_start_date",
            "{{ macros.ds_add(ds, -60) }}",
            "--test_end_date",
            "{{ macros.ds_add(ds, -30) }}",
            "--experiment",
            "crime_risk_area",
            "--tracking_uri",
            MLFLOW_TRACKING_URI,
            "--dfs_tmpdir",
            MLFLOW_SPARK_DFS_TMPDIR,
            "--label_col",
            "label_next_day_count",
        ],
        conf=SPARK_CONF_ML,
        execution_timeout=pendulum.duration(minutes=90),
    )

    promote_if_better = PythonOperator(
        task_id="promote_if_better",
        python_callable=_promote_if_better,
        op_kwargs={
            "tracking_uri": MLFLOW_TRACKING_URI,
            "model_name": "chicago_crime_area_risk",
            "metric_key": "test_rmse",
        },
    )

    evaluate_area_risk >> compute_area_risk_drift >> train_area_risk_model >> promote_if_better
