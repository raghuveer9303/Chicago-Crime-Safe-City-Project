#!/usr/bin/env python3
"""
Train an area-level daily crime risk model on top of the
`chicago_crime_gold.feature_crime_risk_area_daily` Iceberg table.

This script:
  * Reads engineered area-day features and label_next_day_count.
  * Applies minimal preprocessing suitable for tree models.
  * Trains an XGBoost regressor as the primary model.
  * Evaluates on time-based validation and test windows.
  * Logs params, tags, metrics, signature, and the fitted PipelineModel to
    MLflow and registers it in the Model Registry.

It is intended to be submitted via SparkSubmit from Airflow.
"""

import argparse
import logging
import math
import os
import socket
from datetime import date

import botocore.exceptions
import mlflow
import mlflow.spark
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, lit, to_date

from xgboost.spark import SparkXGBRegressor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model registry / artifact constants
# ---------------------------------------------------------------------------
MODEL_ARTIFACT_PATH = "spark_xgboost_pipeline"
REGISTERED_MODEL_NAME = "chicago_crime_area_risk"
DEFAULT_MLFLOW_SPARK_DFS_TMPDIR = "s3a://chicago-crime-data/_mlflow_spark_tmp"
RUN_PARAM_SPARK_MODEL_S3A_PATH = "spark_model_s3a_path"

# ---------------------------------------------------------------------------
# XGBoost hyperparameters — single source of truth for both model construction
# and MLflow param logging.
# ---------------------------------------------------------------------------
XGBOOST_HYPERPARAMS: dict = {
    "objective": "reg:squarederror",
    "num_round": 1000,
    "max_depth": 5,
    "eta": 0.1,
    "subsample": 1.0,
    "colsample_bytree": 1.0,
    "gamma": 0.05,
    "min_child_weight": 1.0,
    "reg_lambda": 1.0,
    "reg_alpha": 0.5,
    "seed": 42,
}

# ---------------------------------------------------------------------------
# Feature / label schema — kept aligned with the feature builder script.
# ---------------------------------------------------------------------------
FEATURE_COLS: list[str] = [
    "count_1d",
    "count_3d",
    "count_7d",
    "count_14d",
    "violent_count_7d",
    "arrest_rate_7d",
    "domestic_rate_7d",
    "day_of_week",
    "is_weekend",
    "month",
    "season",
    "horizon",  # forecast horizon: 1 = next day, MAX_HORIZON = furthest-out prediction
]
MAX_HORIZON: int = 10  # must match the scoring window (process_date .. process_date+10)
DEFAULT_LABEL_COL = "label_next_day_count"
IMPUTER_STRATEGY = "median"
MODEL_VERSION_CHAMPION_TAG = "champion"
MODEL_VERSION_CHALLENGER_TAG = "challenger"
MODEL_VERSION_ROLE_TAG = "model_role"


def _filter_finite_metrics(metrics: dict[str, float]) -> tuple[dict[str, float], list[str]]:
    """Keep only finite metric values for MLflow metric logging."""
    kept: dict[str, float] = {}
    dropped: list[str] = []
    for key, value in metrics.items():
        if value is None or not math.isfinite(float(value)):
            dropped.append(key)
        else:
            kept[key] = float(value)
    return kept, dropped


def _set_model_version_tags_for_run(run_id: str, tags: dict[str, str]) -> None:
    """
    Best-effort: locate the newest model version produced by this run and tag it.
    """
    client = MlflowClient()
    try:
        versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not search model versions for tagging: %s", exc)
        return

    run_versions = [v for v in versions if getattr(v, "run_id", None) == run_id]
    if not run_versions:
        logger.warning("No registered model version found for run_id=%s", run_id)
        return

    run_versions.sort(key=lambda v: int(getattr(v, "creation_timestamp", 0)), reverse=True)
    target_version = run_versions[0]
    for key, value in tags.items():
        try:
            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=target_version.version,
                key=key,
                value=str(value),
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Failed to set model version tag %s=%s for %s v%s: %s",
                key,
                value,
                REGISTERED_MODEL_NAME,
                target_version.version,
                exc,
            )


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def build_pipeline(
    feature_cols: list[str],
    label_col: str,
    hyperparams: dict,
) -> Pipeline:
    """
    Build a Spark ML Pipeline for area risk regression.

    Stages:
      1. Imputer (median) — handles missing numeric values.
      2. VectorAssembler  — assembles imputed columns into a feature vector.
      3. SparkXGBRegressor — gradient-boosted tree regressor.
    """
    imputer = Imputer(
        inputCols=feature_cols,
        outputCols=[f"{c}_imputed" for c in feature_cols],
        strategy=IMPUTER_STRATEGY,
    )
    imputed_cols = [f"{c}_imputed" for c in feature_cols]

    assembler = VectorAssembler(
        inputCols=imputed_cols,
        outputCol="features_raw",
    )

    estimator = SparkXGBRegressor(
        features_col="features_raw",
        label_col=label_col,
        prediction_col="prediction",
        **hyperparams,
    )

    return Pipeline(stages=[imputer, assembler, estimator])


def evaluate_regression(pred_df, label_col: str) -> dict[str, float]:
    """Return RMSE, MAE, and R² for a prediction DataFrame."""
    metrics: dict[str, float] = {}
    for metric_name in ["rmse", "mae", "r2"]:
        evaluator = RegressionEvaluator(
            labelCol=label_col,
            predictionCol="prediction",
            metricName=metric_name,
        )
        try:
            value = evaluator.evaluate(pred_df)
            metrics[metric_name] = float(value)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Failed to compute metric %s: %s", metric_name, exc)
            metrics[metric_name] = float("nan")
    return metrics


def _verify_s3a_sparkml_path_nonempty(
    s3a_path: str,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    """
    Verify that a SparkML directory contains metadata/part-00000.
    """
    import boto3

    rest = s3a_path.removeprefix("s3a://")
    bucket, _, prefix = rest.partition("/")
    sentinel_key = f"{prefix.rstrip('/')}/metadata/part-00000"

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    try:
        s3.head_object(Bucket=bucket, Key=sentinel_key)
        logger.info(
            "Sparkml artifact verification passed: s3://%s/%s", bucket, sentinel_key
        )
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            raise RuntimeError(
                f"Sparkml model artifacts are incomplete.\n"
                f"Expected sentinel not found: s3://{bucket}/{sentinel_key}\n"
                f"This indicates Spark wrote only success markers without data files."
            ) from exc
        raise RuntimeError(
            f"Cannot verify sparkml artifacts at s3://{bucket}/{sentinel_key}: {exc}"
        ) from exc


def main(
    feature_table: str,
    train_start_date: date,
    train_end_date: date,
    val_start_date: date,
    val_end_date: date,
    test_start_date: date,
    test_end_date: date,
    experiment: str = "crime_risk_area",
    tracking_uri: str | None = None,
    dfs_tmpdir: str = DEFAULT_MLFLOW_SPARK_DFS_TMPDIR,
    label_col: str = DEFAULT_LABEL_COL,
) -> None:
    spark = None

    try:
        # Keep Hadoop's default LocalFileSystem. Overriding fs.file.impl to
        # RawLocalFileSystem breaks S3A temp-file allocation in cluster mode
        # (ClassCastException: RawLocalFileSystem cannot be cast to LocalFileSystem).
        spark = SparkSession.builder.appName("TrainCrimeRiskAreaModel").getOrCreate()
        logger.info("Spark session initialized.")

        # spark.driverEnv.* vars are injected into the JVM environment, not the
        # Python process, so boto3 (used by MLflow for S3 artifact uploads) cannot
        # see them.  Read the credentials that are already present in the Spark S3A
        # conf and backfill os.environ so that MLflow artifact uploads to MinIO work.
        _s3_key = spark.conf.get("spark.hadoop.fs.s3a.access.key", "")
        _s3_secret = spark.conf.get("spark.hadoop.fs.s3a.secret.key", "")
        _s3_endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "")
        if _s3_key and not os.environ.get("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_ACCESS_KEY_ID"] = _s3_key
        if _s3_secret and not os.environ.get("AWS_SECRET_ACCESS_KEY"):
            os.environ["AWS_SECRET_ACCESS_KEY"] = _s3_secret
        if _s3_endpoint and not os.environ.get("MLFLOW_S3_ENDPOINT_URL"):
            os.environ["MLFLOW_S3_ENDPOINT_URL"] = _s3_endpoint

        logger.info("Using MLflow Spark dfs_tmpdir: %s", dfs_tmpdir)

        logger.info(
            "Reading features from %s (train: %s→%s, val: %s→%s, test: %s→%s)",
            feature_table,
            train_start_date,
            train_end_date,
            val_start_date,
            val_end_date,
            test_start_date,
            test_end_date,
        )

        # ---------------------------------------------------------------
        # Horizon-aware dataset construction
        # Each historical area-day becomes MAX_HORIZON training rows:
        #   row h has features at t, label = count_1d at t+h.
        # This teaches the model that rolling-window signals decay with
        # forecast horizon so it scores correctly for the 9-day lag.
        # ---------------------------------------------------------------
        from datetime import timedelta as _td

        label_window_end = test_end_date + _td(days=MAX_HORIZON)

        # Base features — all columns except the horizon (derived below)
        feat_df = (
            spark.read.format("iceberg")
            .load(feature_table)
            .select(
                to_date(col("date")).alias("event_date"),
                col("area_type"),
                col("area_id"),
                *[col(c) for c in FEATURE_COLS if c != "horizon"],
            )
            .filter(
                (to_date(col("date")) >= lit(train_start_date))
                & (to_date(col("date")) <= lit(test_end_date))
            )
        )

        # Labels reference — count_1d for every date up to test_end + MAX_HORIZON
        labels_ref = (
            spark.read.format("iceberg")
            .load(feature_table)
            .select(
                col("area_type").alias("lbl_area_type"),
                col("area_id").alias("lbl_area_id"),
                to_date(col("date")).alias("label_date"),
                col("count_1d").cast("int").alias("horizon_label"),
            )
            .filter(
                (to_date(col("date")) > lit(train_start_date))
                & (to_date(col("date")) <= lit(label_window_end))
            )
        )

        # Expand each area-day to MAX_HORIZON rows, one per horizon value
        horizons_df = spark.createDataFrame(
            [(h,) for h in range(1, MAX_HORIZON + 1)], ["horizon"]
        )

        df_expanded = (
            feat_df.crossJoin(horizons_df)
            .withColumn("target_label_date", date_add(col("event_date"), col("horizon").cast("int")))
            .alias("feat")
            .join(
                labels_ref.alias("lref"),
                (
                    (col("feat.area_type") == col("lref.lbl_area_type"))
                    & (col("feat.area_id") == col("lref.lbl_area_id"))
                    & (col("feat.target_label_date") == col("lref.label_date"))
                ),
                "inner",  # drops edge rows where the future label doesn't exist yet
            )
            .drop("lbl_area_type", "lbl_area_id", "label_date", "target_label_date")
            .withColumnRenamed("horizon_label", label_col)
        )

        train_df = df_expanded.filter(
            (col("event_date") >= lit(train_start_date))
            & (col("event_date") <= lit(train_end_date))
        )
        val_df = df_expanded.filter(
            (col("event_date") >= lit(val_start_date))
            & (col("event_date") <= lit(val_end_date))
        )
        test_df = df_expanded.filter(
            (col("event_date") >= lit(test_start_date))
            & (col("event_date") <= lit(test_end_date))
        )

        train_count = train_df.count()
        val_count = val_df.count()
        test_count = test_df.count()
        logger.info(
            "Dataset sizes — train: %s, val: %s, test: %s",
            train_count,
            val_count,
            test_count,
        )

        if train_count == 0 or val_count == 0 or test_count == 0:
            logger.warning(
                "Insufficient data (train=%s, val=%s, test=%s). Aborting.",
                train_count,
                val_count,
                test_count,
            )
            return

        pipeline = build_pipeline(
            feature_cols=FEATURE_COLS,
            label_col=label_col,
            hyperparams=XGBOOST_HYPERPARAMS,
        )

        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
            logger.info("MLflow tracking URI: %s", tracking_uri)

        # Ensure experiment exists (handles first-run bootstrap).
        try:
            if not mlflow.get_experiment_by_name(experiment):
                logger.info("Creating MLflow experiment: %s", experiment)
                mlflow.create_experiment(experiment)
        except Exception as exc:
            logger.warning("Could not ensure experiment existence: %s. Proceeding...", exc)

        mlflow.set_experiment(experiment)

        run_name = f"xgboost_area_risk_{train_end_date}"

        with mlflow.start_run(run_name=run_name):
            # ---------------------------------------------------------------
            # Parameters — data provenance + preprocessing + hyperparameters
            # ---------------------------------------------------------------
            mlflow.log_params(
                {
                    # Data provenance
                    "feature_table": feature_table,
                    "label_col": label_col,
                    "train_start_date": train_start_date.isoformat(),
                    "train_end_date": train_end_date.isoformat(),
                    "val_start_date": val_start_date.isoformat(),
                    "val_end_date": val_end_date.isoformat(),
                    "test_start_date": test_start_date.isoformat(),
                    "test_end_date": test_end_date.isoformat(),
                    # Horizon expansion
                    "max_horizon": MAX_HORIZON,
                    "horizon_aware": True,
                    # Preprocessing
                    "num_features": len(FEATURE_COLS),
                    "feature_cols": ",".join(FEATURE_COLS),
                    "imputer_strategy": IMPUTER_STRATEGY,
                    # XGBoost hyperparameters
                    **{f"xgb_{k}": v for k, v in XGBOOST_HYPERPARAMS.items()},
                }
            )

            # ---------------------------------------------------------------
            # Tags — searchable metadata in the MLflow UI / registry
            # ---------------------------------------------------------------
            mlflow.set_tags(
                {
                    "model.framework": "xgboost",
                    "model.flavor": "spark",
                    "model.pipeline": "Imputer → VectorAssembler → SparkXGBRegressor",
                    "data.domain": "chicago_crime",
                    "data.granularity": "area_day",
                    "task": "regression",
                    "owner": "data-eng",
                    "host": socket.gethostname(),
                    "model_role": "challenger",
                    "promotion_status": "pending",
                }
            )

            # ---------------------------------------------------------------
            # Training
            # ---------------------------------------------------------------
            logger.info("Fitting XGBoost pipeline on %s training rows.", train_count)
            model = pipeline.fit(train_df)
            run_id = mlflow.active_run().info.run_id

            # ---------------------------------------------------------------
            # Evaluation
            # ---------------------------------------------------------------
            val_pred = model.transform(val_df)
            test_pred = model.transform(test_df)

            val_metrics = evaluate_regression(val_pred, label_col=label_col)
            test_metrics = evaluate_regression(test_pred, label_col=label_col)

            raw_metrics = {
                "train_row_count": float(train_count),
                "val_row_count": float(val_count),
                "test_row_count": float(test_count),
                **{f"val_{k}": float(v) for k, v in val_metrics.items()},
                **{f"test_{k}": float(v) for k, v in test_metrics.items()},
            }
            finite_metrics, dropped_metric_keys = _filter_finite_metrics(raw_metrics)
            if finite_metrics:
                mlflow.log_metrics(finite_metrics)
            if dropped_metric_keys:
                mlflow.set_tag("metrics_dropped_non_finite", ",".join(dropped_metric_keys))
                logger.warning("Dropped non-finite metrics from MLflow logging: %s", dropped_metric_keys)

            for split, metrics in (("val", val_metrics), ("test", test_metrics)):
                for name, value in metrics.items():
                    logger.info("%s %s: %.4f", split.capitalize(), name, value)

            # ---------------------------------------------------------------
            # Model logging — with signature and input example
            # ---------------------------------------------------------------
            input_example = (
                train_df.select(*FEATURE_COLS).limit(5).toPandas()
            )
            predictions_sample = (
                model.transform(train_df.limit(5))
                .select("prediction")
                .toPandas()
            )
            signature = infer_signature(input_example, predictions_sample)

            # Primary persisted model path (independent of MLflow Spark flavor quirks).
            warehouse = spark.conf.get(
                "spark.sql.catalog.chicago_crime_gold.warehouse",
                "s3a://chicago-crime-data/gold",
            )
            warehouse_bucket = warehouse.removeprefix("s3a://").split("/", 1)[0]
            spark_model_s3a_path = (
                f"s3a://{warehouse_bucket}/models/{REGISTERED_MODEL_NAME}/{run_id}/sparkml"
            )
            logger.info("Saving Spark model directly to %s", spark_model_s3a_path)
            model.write().overwrite().save(spark_model_s3a_path)
            _verify_s3a_sparkml_path_nonempty(
                s3a_path=spark_model_s3a_path,
                s3_endpoint=_s3_endpoint or "http://minio:9000",
                access_key=_s3_key,
                secret_key=_s3_secret,
            )
            mlflow.log_param(RUN_PARAM_SPARK_MODEL_S3A_PATH, spark_model_s3a_path)
            mlflow.set_tag(RUN_PARAM_SPARK_MODEL_S3A_PATH, spark_model_s3a_path)

            # Secondary registry artifact (kept for versioning/governance UI).
            # If this path becomes sparse (_SUCCESS-only), scoring will still use
            # spark_model_s3a_path logged above.
            mlflow.spark.log_model(
                model,
                artifact_path=MODEL_ARTIFACT_PATH,
                registered_model_name=REGISTERED_MODEL_NAME,
                signature=signature,
                input_example=input_example,
                dfs_tmpdir=dfs_tmpdir,
            )
            _set_model_version_tags_for_run(
                run_id=run_id,
                tags={
                    MODEL_VERSION_ROLE_TAG: "challenger",
                    MODEL_VERSION_CHAMPION_TAG: "false",
                    MODEL_VERSION_CHALLENGER_TAG: "true",
                },
            )
            logger.info(
                "Model logged as '%s' and registered under '%s'.",
                MODEL_ARTIFACT_PATH,
                REGISTERED_MODEL_NAME,
            )

            logger.info(
                "Logged run param %s=%s",
                RUN_PARAM_SPARK_MODEL_S3A_PATH,
                spark_model_s3a_path,
            )

    except Exception as exc:
        logger.error("Training job failed: %s", exc)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Train area-level daily crime risk model using Spark ML and MLflow.",
    )
    parser.add_argument(
        "--feature_table",
        default="chicago_crime_gold.feature_crime_risk_area_daily",
        help="Fully qualified Iceberg feature table.",
    )
    parser.add_argument(
        "--train_start_date",
        type=parse_date,
        required=True,
        help="Training window start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--train_end_date",
        type=parse_date,
        required=True,
        help="Training window end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--val_start_date",
        type=parse_date,
        required=True,
        help="Validation window start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--val_end_date",
        type=parse_date,
        required=True,
        help="Validation window end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--test_start_date",
        type=parse_date,
        required=True,
        help="Test window start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--test_end_date",
        type=parse_date,
        required=True,
        help="Test window end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--experiment",
        default="crime_risk_area",
        help="MLflow experiment name.",
    )
    parser.add_argument(
        "--label_col",
        default=DEFAULT_LABEL_COL,
        help="Feature-table label column to train against.",
    )
    parser.add_argument(
        "--tracking_uri",
        default=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
        help="MLflow tracking URI.",
    )
    parser.add_argument(
        "--dfs_tmpdir",
        default=os.getenv(
            "MLFLOW_SPARK_DFS_TMPDIR",
            os.getenv("MLFLOW_DFS_TMPDIR", DEFAULT_MLFLOW_SPARK_DFS_TMPDIR),
        ),
        help="Shared DFS tmpdir for mlflow.spark.log_model staging (e.g. s3a://bucket/prefix).",
    )

    args = parser.parse_args()

    logger.info(
        "Starting area risk model training — feature_table=%s, "
        "train=%s→%s, val=%s→%s, test=%s→%s, experiment=%s",
        args.feature_table,
        args.train_start_date,
        args.train_end_date,
        args.val_start_date,
        args.val_end_date,
        args.test_start_date,
        args.test_end_date,
        args.experiment,
    )

    main(
        feature_table=args.feature_table,
        train_start_date=args.train_start_date,
        train_end_date=args.train_end_date,
        val_start_date=args.val_start_date,
        val_end_date=args.val_end_date,
        test_start_date=args.test_start_date,
        test_end_date=args.test_end_date,
        experiment=args.experiment,
        tracking_uri=args.tracking_uri,
        dfs_tmpdir=args.dfs_tmpdir,
        label_col=args.label_col,
    )
