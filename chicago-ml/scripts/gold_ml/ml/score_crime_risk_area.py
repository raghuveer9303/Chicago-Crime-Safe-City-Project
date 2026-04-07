#!/usr/bin/env python3
from __future__ import annotations

"""
Batch scoring for the Chicago Crime area-level risk model.

Reads a date range of area-day features from Iceberg, loads the Production
model from MLflow Model Registry (Spark flavor) ONCE, generates predictions
for every day in [prediction_start_date, prediction_end_date], and writes all
results back to the Iceberg predictions table in a single job.

Typical invocation: triggered on a 10-day cadence by the MLOps DAG after scoring.
The DAG passes **ds .. ds+10** (11 inclusive prediction days) so forward coverage
accounts for operational lag (~10 days) while each batch uses one Production model.

For backward-compatible single-day runs, pass --prediction_date (sets both
start and end to the same date).

Output table (Iceberg): chicago_crime_gold.fact_area_risk_predictions
Grain: one row per (run_id, area_type, area_id, prediction_date).
Includes safety_score (0–100, higher = safer) from batch p99 normalization of predicted_count.
Partitioning: days(process_date). Each run **replaces only that batch’s** `process_date`
partition (delete + append) so older batches stay in the table for backfills and Postgres.
"""

import argparse
import logging
import os
import uuid
from datetime import date, timedelta

import botocore.exceptions
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    date_add,
    datediff,
    greatest,
    least,
    lit,
    round as spark_round,
    sqrt,
    to_date,
    explode,
    sequence,
    dayofweek,
    month,
    when,
)

logger = logging.getLogger(__name__)
RUN_PARAM_SPARK_MODEL_S3A_PATH = "spark_model_s3a_path"


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _verify_s3a_path_nonempty(s3a_path: str, boto3_endpoint: str, access_key: str, secret_key: str) -> None:
    """
    Verify that an s3a:// prefix contains at least one object in MinIO.

    Raises a descriptive RuntimeError if the path is empty or unreachable,
    so callers get a clear message instead of the opaque 'ValueError: RDD is empty'
    that PipelineModel.load raises when Spark finds zero input files.
    """
    import boto3  # imported here to keep top-level imports minimal in Spark driver

    bucket, _, prefix = s3a_path.removeprefix("s3a://").partition("/")
    s3 = boto3.client(
        "s3",
        endpoint_url=boto3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    # The sentinel that PipelineModel.load reads first is metadata/part-00000.
    # Checking for this specific key is more reliable than listing the prefix:
    # - 0-byte MinIO directory markers (directory.marker.policy=keep) would
    #   fool a KeyCount > 0 check.
    # - Hadoop _SUCCESS / ._SUCCESS.crc marker files (8B) are written even when
    #   the actual data files (part-00000, parquet, XGBoost model) were never
    #   uploaded, e.g. when a training job was interrupted mid-upload.
    sentinel_key = f"{prefix.rstrip('/')}/metadata/part-00000"
    try:
        s3.head_object(Bucket=bucket, Key=sentinel_key)
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            raise RuntimeError(
                f"Model sparkml artifacts are corrupt or missing in MinIO.\n"
                f"Expected sentinel file not found: s3a://{bucket}/{sentinel_key}\n"
                f"The sparkml/ directory may contain only Hadoop _SUCCESS markers with no "
                f"actual data files — this happens when a training job is interrupted "
                f"mid-upload or when the S3 write fails silently.\n"
                f"Re-run the training DAG to produce a new model version with complete "
                f"artifacts, then promote it to Production."
            ) from exc
        raise RuntimeError(
            f"Cannot verify MinIO path s3a://{bucket}/{sentinel_key} — S3 error: {exc}"
        ) from exc


def _resolve_sparkml_s3a_path(model_uri: str) -> str:
    """
    Resolve a 'models:/<name>/<stage_or_version>' URI to the s3a:// sparkml
    artifact path so PipelineModel.load() can read directly from MinIO via
    Spark's S3A driver — bypassing mlflow.spark.load_model's DFS staging step.

    MLflow's built-in DFS staging (dfs_tmpdir) uses Hadoop FileUtil to copy
    the model from a local temp dir to S3A. This copy silently produces zero
    files when the stored artifact URI uses the legacy s3:// scheme (pre-s3a
    migration), because the JVM-level write fails without raising a Python
    exception, leaving the staging path empty → ValueError: RDD is empty.

    Loading directly from the registry's source URI avoids the intermediate
    copy entirely. Spark's S3A filesystem (configured via spark.hadoop.fs.s3a.*)
    reads from MinIO the same way it reads feature/gold data.
    """
    client = MlflowClient()
    rest = model_uri.removeprefix("models:/").strip("/")
    name, stage_or_version = rest.split("/", 1)

    if stage_or_version.isdigit():
        version_info = client.get_model_version(name, stage_or_version)
    else:
        versions = client.get_latest_versions(name, stages=[stage_or_version])
        if not versions:
            raise ValueError(f"No Production version found for model '{name}'")
        version_info = versions[0]

    # Preferred: explicit direct Spark model path logged by training as a run param/tag.
    run = client.get_run(version_info.run_id)
    sparkml_path = (
        run.data.params.get(RUN_PARAM_SPARK_MODEL_S3A_PATH)
        or run.data.tags.get(RUN_PARAM_SPARK_MODEL_S3A_PATH)
        or ""
    )

    # Backward compatibility: fall back to MLflow spark flavor artifact layout.
    if not sparkml_path:
        # source is the base artifact dir, e.g. s3://mlflow-artifacts/<run>/artifacts/model
        source = version_info.source.rstrip("/")
        sparkml_path = f"{source}/sparkml"

    # Normalise s3:// → s3a:// so Spark's S3A driver handles it.
    # Old runs were logged with MLFLOW_DEFAULT_ARTIFACT_ROOT=s3://mlflow-artifacts/;
    # new runs use s3a://. Both point to the same MinIO bucket; only the scheme differs.
    sparkml_path = sparkml_path.replace("s3://", "s3a://", 1)

    logger.info(
        "Resolved sparkml S3A path: %s (model %s v%s, run %s)",
        sparkml_path,
        name,
        version_info.version,
        version_info.run_id,
    )
    return sparkml_path


def _resolve_model_version(model_uri: str, tracking_uri: str | None) -> tuple[str | None, str | None]:
    """
    Best-effort resolution of (model_name, model_version) for MLflow registry URIs.
    Works for: models:/<name>/<stage_or_version>
    """
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    if not model_uri.startswith("models:/"):
        return None, None

    rest = model_uri.removeprefix("models:/").strip("/")
    parts = rest.split("/")
    if len(parts) < 2:
        return None, None

    model_name, stage_or_version = parts[0], parts[1]
    client = MlflowClient()
    try:
        if stage_or_version.isdigit():
            return model_name, stage_or_version
        latest = client.get_latest_versions(model_name, stages=[stage_or_version])
        if latest:
            return model_name, str(latest[0].version)
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not resolve model version for %s: %s", model_uri, exc)
    return model_name, None


def main(
    feature_table: str,
    predictions_table: str,
    model_uri: str,
    prediction_start_date: date,
    prediction_end_date: date,
    process_date: date,
    prediction_target: str = "total_count",
    tracking_uri: str | None = None,
    experiment: str = "crime_risk_area",
    dfs_tmpdir: str | None = None,
) -> None:
    """
    Score all area-days in [prediction_start_date, prediction_end_date] in a
    single Spark job using one loaded model instance.

    10-day cadence batch pattern:
      - prediction_start_date = ds
      - prediction_end_date   = ds + 10 (11 inclusive prediction days)
      - process_date          = ds

    Dates that have no feature rows are silently skipped; the write is a
    no-op for those days.
    """
    if dfs_tmpdir:
        logger.info(
            "dfs_tmpdir=%s is ignored — model is loaded directly from S3A (see _resolve_sparkml_s3a_path).",
            dfs_tmpdir,
        )

    spark = None
    try:
        # Keep Hadoop default LocalFileSystem. Forcing RawLocalFileSystem causes
        # S3A temp-file allocation failures in cluster mode.
        spark = SparkSession.builder.appName("ScoreCrimeRiskArea").getOrCreate()
        logger.info("Spark session initialized.")

        # MLflow artifact uploads / registry calls need MinIO creds in the
        # Python process env. The Spark conf values are only visible to the JVM;
        # backfill os.environ here so boto3 (used by MLflow) can see them.
        _s3_key = spark.conf.get("spark.hadoop.fs.s3a.access.key", "")
        _s3_secret = spark.conf.get("spark.hadoop.fs.s3a.secret.key", "")
        _s3_endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "")
        if _s3_key and not os.environ.get("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_ACCESS_KEY_ID"] = _s3_key
        if _s3_secret and not os.environ.get("AWS_SECRET_ACCESS_KEY"):
            os.environ["AWS_SECRET_ACCESS_KEY"] = _s3_secret
        if _s3_endpoint and not os.environ.get("MLFLOW_S3_ENDPOINT_URL"):
            os.environ["MLFLOW_S3_ENDPOINT_URL"] = _s3_endpoint

        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)

        mlflow.set_experiment(experiment)

        window_days = (prediction_end_date - prediction_start_date).days + 1
        logger.info(
            "Area-risk scoring — feature_table=%s, model_uri=%s, "
            "prediction_window=%s → %s (%d days), process_date=%s",
            feature_table,
            model_uri,
            prediction_start_date,
            prediction_end_date,
            window_days,
            process_date,
        )

        # Forward fill the features from `process_date` across the sequence of horizon prediction_dates
        # since features for ds+1 to ds+10 do not exist structurally. We regenerate expected standard
        # calendar attributes representing those dates so tree structures score on correct weekday metrics.
        # Forward-fill the latest known features (process_date snapshot) across
        # the 10-day scoring horizon. Drop cols that are re-derived per future date.
        base_feat = (
            spark.read.format("iceberg")
            .load(feature_table)
            .filter(col("date") == lit(process_date))
            .drop("event_date", "day_of_week", "is_weekend", "month", "season", "horizon")
        )

        days_df = spark.sql(
            f"SELECT explode(sequence("
            f"DATE '{prediction_start_date.isoformat()}', "
            f"DATE '{prediction_end_date.isoformat()}')) AS future_date"
        )

        feat = base_feat.crossJoin(days_df)
        feat = feat.withColumn("event_date", col("future_date"))
        feat = feat.withColumn("day_of_week", dayofweek(col("future_date")))
        feat = feat.withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
        feat = feat.withColumn("month", month(col("future_date")))
        feat = feat.withColumn(
            "season",
            when(col("month").isin(12, 1, 2), 1)
            .when(col("month").isin(3, 4, 5), 2)
            .when(col("month").isin(6, 7, 8), 3)
            .when(col("month").isin(9, 10, 11), 4)
            .otherwise(1),
        )
        # horizon = how many days ahead from process_date (0 = today, 10 = 10 days out)
        # Must match the horizon feature the model was trained on.
        feat = feat.withColumn("horizon", datediff(col("future_date"), lit(process_date)))

        n = feat.count()
        logger.info(
            "Feature rows for window %s → %s: %d",
            prediction_start_date,
            prediction_end_date,
            n,
        )
        if n == 0:
            logger.info("No features found for window; skipping scoring write.")
            return

        # Load the Spark model directly from its S3A artifact path rather than
        # going through mlflow.spark.load_model's DFS staging step. MLflow's
        # staging copies the model from a local temp dir to dfs_tmpdir via
        # Hadoop FileUtil, which silently produces zero files when the registry
        # stores a legacy s3:// artifact URI — see _resolve_sparkml_s3a_path.
        # Direct loading via PipelineModel.load works because Spark's S3A driver
        # is already configured to reach MinIO (same config used for Iceberg).
        sparkml_s3a_path = _resolve_sparkml_s3a_path(model_uri)
        logger.info("Loading model from S3A: %s", sparkml_s3a_path)

        # Pre-flight: verify the sparkml artifacts exist in MinIO before handing
        # the path to Spark. PipelineModel.load raises the opaque "ValueError: RDD
        # is empty" when the S3A prefix has zero objects — this check surfaces the
        # real cause (missing artifacts) with an actionable error message.
        _verify_s3a_path_nonempty(
            sparkml_s3a_path,
            boto3_endpoint=_s3_endpoint or "http://minio:9000",
            access_key=_s3_key,
            secret_key=_s3_secret,
        )

        model = PipelineModel.load(sparkml_s3a_path)

        # Transform all area-days in the window in a single model pass.
        pred = model.transform(feat)

        run_id = str(uuid.uuid4())
        model_name, model_version = _resolve_model_version(model_uri, tracking_uri=tracking_uri)

        # target_date = prediction_date + 1 day (the day whose crime is being predicted).
        out = pred.select(
            lit(run_id).alias("run_id"),
            lit(model_name).alias("model_name"),
            lit(model_version).alias("model_version"),
            col("event_date").alias("prediction_date"),
            date_add(col("event_date"), 1).alias("target_date"),
            lit(prediction_target).alias("prediction_target"),
            col("area_type"),
            col("area_id").cast("int").alias("area_id"),
            col("prediction").cast("double").alias("predicted_count"),
            current_timestamp().alias("created_at"),
            lit(process_date).alias("process_date"),
        )

        # Poisson 90% prediction interval: ±1.65 × sqrt(predicted_count)
        # Gives the API a range to display (e.g. "Expected 4–9 incidents").
        out = (
            out
            .withColumn(
                "predicted_count_low",
                spark_round(
                    greatest(lit(0.0), col("predicted_count") - lit(1.65) * sqrt(col("predicted_count"))),
                    1,
                ),
            )
            .withColumn(
                "predicted_count_high",
                spark_round(col("predicted_count") + lit(1.65) * sqrt(col("predicted_count")), 1),
            )
        )

        # safety_score: 0–100, higher = safer. Normalise predicted_count by batch p99
        # (fallback p99=1.0 when degenerate) so the map is comparable across areas.
        qs = out.select(coalesce(col("predicted_count"), lit(0.0)).alias("pc")).approxQuantile(
            "pc", [0.99], 0.01
        )
        p99 = float(qs[0]) if qs and qs[0] is not None and qs[0] > 0 else 1.0
        logger.info("Batch p99(predicted_count)=%s for safety_score normalization", p99)
        out = out.withColumn(
            "safety_score",
            spark_round(
                lit(100.0)
                * (lit(1.0) - least(coalesce(col("predicted_count"), lit(0.0)) / lit(p99), lit(1.0))),
                2,
            ),
        )

        n_scored = out.count()
        logger.info(
            "AREA_RISK_ML_SCORING process_date=%s prediction_window=%s..%s window_days=%d rows=%d p99_predicted_count=%.6f model_name=%s model_version=%s",
            process_date,
            prediction_start_date,
            prediction_end_date,
            window_days,
            n_scored,
            p99,
            model_name or "",
            model_version or "",
        )

        # Bootstrap-safe table creation (includes prediction interval columns).
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {predictions_table} (
              run_id STRING,
              model_name STRING,
              model_version STRING,
              prediction_date DATE,
              target_date DATE,
              prediction_target STRING,
              area_type STRING,
              area_id INT,
              predicted_count DOUBLE,
              predicted_count_low DOUBLE,
              predicted_count_high DOUBLE,
              safety_score DOUBLE,
              created_at TIMESTAMP,
              process_date DATE
            )
            USING iceberg
            PARTITIONED BY (days(process_date))
            """
        )

        # Backfill schema evolution when the table already exists with the old column set.
        try:
            existing_cols = set(spark.table(predictions_table).columns)
        except Exception:  # pragma: no cover - defensive for early bootstrap
            existing_cols = set()

        for _col_def, _col_name in [
            ("prediction_target STRING", "prediction_target"),
            ("safety_score DOUBLE", "safety_score"),
            ("predicted_count_low DOUBLE", "predicted_count_low"),
            ("predicted_count_high DOUBLE", "predicted_count_high"),
        ]:
            if _col_name not in existing_cols:
                spark.sql(f"ALTER TABLE {predictions_table} ADD COLUMNS ({_col_def})")

        # Replace only this process_date partition — keep other batches for studio/history.
        # (Full .mode("overwrite") dropped the entire table and broke loads for older ds.)
        spark.sql(
            f"DELETE FROM {predictions_table} WHERE process_date = DATE '{process_date.isoformat()}'"
        )
        out.write.format("iceberg").mode("append").save(predictions_table)
        logger.info(
            "Wrote %d scored rows to %s for process_date=%s (window %s → %s, model v%s)",
            n_scored,
            predictions_table,
            process_date,
            prediction_start_date,
            prediction_end_date,
            model_version,
        )

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "Score area-level crime risk model for a date window and write predictions. "
            "Designed for batch invocation (e.g. 10-day cadence) after model promotion."
        )
    )
    parser.add_argument("--feature_table", default="chicago_crime_gold.feature_crime_risk_area_daily")
    parser.add_argument("--predictions_table", default="chicago_crime_gold.fact_area_risk_predictions")
    parser.add_argument("--model_uri", default="models:/chicago_crime_area_risk/Production")

    # Date window — pass either the pair (weekly batch) or just --prediction_date (single day).
    parser.add_argument(
        "--prediction_start_date",
        type=parse_date,
        default=None,
        help="First date to score (YYYY-MM-DD). Required if --prediction_date is not set.",
    )
    parser.add_argument(
        "--prediction_end_date",
        type=parse_date,
        default=None,
        help="Last date to score inclusive (YYYY-MM-DD). Required if --prediction_date is not set.",
    )
    parser.add_argument(
        "--prediction_date",
        type=parse_date,
        default=None,
        help="Convenience alias for single-day scoring: sets start=end=this date.",
    )

    parser.add_argument("--process_date", type=parse_date, required=True)
    parser.add_argument(
        "--prediction_target",
        default="total_count",
        help=(
            "Which label/target the model predicts. This is written to the "
            "output table as `prediction_target` for later evaluation/drift."
        ),
    )
    parser.add_argument("--tracking_uri", default=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    parser.add_argument("--experiment", default="crime_risk_area")
    parser.add_argument(
        "--dfs_tmpdir",
        default=os.getenv("MLFLOW_SPARK_DFS_TMPDIR", os.getenv("MLFLOW_DFS_TMPDIR", "")),
        help="DFS staging dir for MLflow Spark model loading (e.g. s3a://bucket/prefix).",
    )
    args = parser.parse_args()

    # Resolve date window.
    if args.prediction_date is not None:
        start = args.prediction_date
        end = args.prediction_date
    elif args.prediction_start_date is not None and args.prediction_end_date is not None:
        start = args.prediction_start_date
        end = args.prediction_end_date
    else:
        parser.error("Provide either --prediction_date OR both --prediction_start_date and --prediction_end_date.")

    if start > end:
        parser.error(f"prediction_start_date ({start}) must be <= prediction_end_date ({end}).")

    main(
        feature_table=args.feature_table,
        predictions_table=args.predictions_table,
        model_uri=args.model_uri,
        prediction_start_date=start,
        prediction_end_date=end,
        process_date=args.process_date,
        prediction_target=args.prediction_target,
        tracking_uri=args.tracking_uri,
        experiment=args.experiment,
        dfs_tmpdir=(args.dfs_tmpdir or None),
    )
