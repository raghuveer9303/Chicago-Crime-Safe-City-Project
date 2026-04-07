#!/usr/bin/env python3
from __future__ import annotations

"""
Compute simple drift signals for area-risk features and predictions.

This is a lightweight, Spark-native drift job intended for early-stage monitoring:
it compares the current scoring day's feature/prediction distribution to a recent
reference window (previous N days) and writes per-feature summary drift metrics.

Output table (Iceberg): chicago_crime_gold.model_drift_metrics
"""

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date
from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import stddev_pop as spark_std

logger = logging.getLogger(__name__)


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def main(
    feature_table: str,
    predictions_table: str,
    drift_table: str,
    prediction_date: date,
    reference_days: int,
    process_date: date,
    feature_cols: list[str],
    prediction_target: str = "total_count",
) -> None:
    spark = None
    try:
        spark = SparkSession.builder.appName("ComputeCrimeRiskDrift").getOrCreate()
        logger.info("Spark session initialized.")

        ref_start = prediction_date - timedelta(days=reference_days)
        ref_end = prediction_date - timedelta(days=1)

        feats = (
            spark.read.format("iceberg")
            .load(feature_table)
            .withColumn("event_date", to_date(col("date")))
            .select("event_date", *[col(c).cast("double").alias(c) for c in feature_cols])
        )

        ref = feats.filter((col("event_date") >= lit(ref_start)) & (col("event_date") <= lit(ref_end)))
        cur = feats.filter(col("event_date") == lit(prediction_date))

        # Drift signal: |mean_cur - mean_ref| / (std_ref + eps)
        eps = 1e-9
        rows = []
        for c in feature_cols:
            ref_stats = ref.agg(spark_avg(col(c)).alias("mean"), spark_std(col(c)).alias("std")).collect()[0]
            cur_stats = cur.agg(spark_avg(col(c)).alias("mean")).collect()[0]
            ref_mean = float(ref_stats["mean"]) if ref_stats["mean"] is not None else float("nan")
            ref_std = float(ref_stats["std"]) if ref_stats["std"] is not None else float("nan")
            cur_mean = float(cur_stats["mean"]) if cur_stats["mean"] is not None else float("nan")
            drift_score = abs(cur_mean - ref_mean) / (ref_std + eps) if ref_std == ref_std else float("nan")
            rows.append((
                "feature_mean_z", 
                c, 
                ref_start, 
                ref_end, 
                prediction_date, 
                ref_mean, 
                ref_std, 
                cur_mean, 
                drift_score
            ))

        # Prediction drift on predicted_count, if available.
        try:
            preds_df = (
                spark.read.format("iceberg")
                .load(predictions_table)
                .withColumn("prediction_date", to_date(col("prediction_date")))
            )
            if "prediction_target" in preds_df.columns and prediction_target and prediction_target != "total_count":
                preds_df = preds_df.filter(col("prediction_target") == lit(prediction_target))

            preds = preds_df.select(
                "prediction_date",
                col("predicted_count").cast("double").alias("predicted_count"),
            )
            ref_p = preds.filter((col("prediction_date") >= lit(ref_start)) & (col("prediction_date") <= lit(ref_end)))
            cur_p = preds.filter(col("prediction_date") == lit(prediction_date))
            if ref_p.count() > 0 and cur_p.count() > 0:
                ref_stats = ref_p.agg(spark_avg(col("predicted_count")).alias("mean"), spark_std(col("predicted_count")).alias("std")).collect()[0]
                cur_stats = cur_p.agg(spark_avg(col("predicted_count")).alias("mean")).collect()[0]
                ref_mean = float(ref_stats["mean"]) if ref_stats["mean"] is not None else float("nan")
                ref_std = float(ref_stats["std"]) if ref_stats["std"] is not None else float("nan")
                cur_mean = float(cur_stats["mean"]) if cur_stats["mean"] is not None else float("nan")
                drift_score = abs(cur_mean - ref_mean) / (ref_std + eps) if ref_std == ref_std else float("nan")
                rows.append((
                    "prediction_mean_z", 
                    "predicted_count", 
                    ref_start, 
                    ref_end, 
                    prediction_date, 
                    ref_mean, 
                    ref_std, 
                    cur_mean, 
                    drift_score
                ))
        except Exception as exc:
            logger.warning("Could not read predictions table %s for drift: %s. Skipping prediction drift.", predictions_table, exc)

        if not rows:
            logger.info("No drift rows computed; skipping write.")
            return

        for r in rows:
            metric_name, field_name, ref_start, ref_end, eval_dt, ref_mean, ref_std, cur_mean, drift_score = r
            logger.info(
                "AREA_RISK_ML_DRIFT_METRICS process_date=%s eval_date=%s metric=%s field=%s drift_score=%s ref_mean=%s ref_std=%s cur_mean=%s ref_window=%s..%s",
                process_date,
                eval_dt,
                metric_name,
                field_name,
                drift_score,
                ref_mean,
                ref_std,
                cur_mean,
                ref_start,
                ref_end,
            )

        df = spark.createDataFrame(
            rows,
            schema="""
              metric_name string,
              field_name string,
              ref_start date,
              ref_end date,
              eval_date date,
              ref_mean double,
              ref_std double,
              cur_mean double,
              drift_score double
            """,
        ).withColumn("process_date", lit(process_date)).withColumn(
            "created_at", current_timestamp()
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {drift_table} (
              metric_name STRING,
              field_name STRING,
              ref_start DATE,
              ref_end DATE,
              eval_date DATE,
              ref_mean DOUBLE,
              ref_std DOUBLE,
              cur_mean DOUBLE,
              drift_score DOUBLE,
              process_date DATE,
              created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(process_date))
            """
        )

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.format("iceberg").mode("overwrite").save(drift_table)
        logger.info("Wrote drift rows: %s", df.count())

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Compute simple drift metrics for features and predictions.")
    parser.add_argument("--feature_table", default="chicago_crime_gold.feature_crime_risk_area_daily")
    parser.add_argument("--predictions_table", default="chicago_crime_gold.fact_area_risk_predictions")
    parser.add_argument("--drift_table", default="chicago_crime_gold.model_drift_metrics")
    parser.add_argument("--prediction_date", type=parse_date, required=True)
    parser.add_argument("--reference_days", type=int, default=30)
    parser.add_argument("--process_date", type=parse_date, required=True)
    parser.add_argument(
        "--feature_cols",
        default="count_1d,count_3d,count_7d,count_14d,violent_count_7d,arrest_rate_7d,domestic_rate_7d,day_of_week,is_weekend,month,season",
    )
    parser.add_argument(
        "--prediction_target",
        default="total_count",
        help="Target corresponding to scoring output (used to filter predictions drift when available).",
    )
    args = parser.parse_args()
    main(
        feature_table=args.feature_table,
        predictions_table=args.predictions_table,
        drift_table=args.drift_table,
        prediction_date=args.prediction_date,
        reference_days=args.reference_days,
        process_date=args.process_date,
        feature_cols=[c.strip() for c in args.feature_cols.split(",") if c.strip()],
        prediction_target=args.prediction_target,
    )

