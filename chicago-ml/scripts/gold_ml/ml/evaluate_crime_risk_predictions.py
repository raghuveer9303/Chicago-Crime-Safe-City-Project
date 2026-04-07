#!/usr/bin/env python3
from __future__ import annotations

"""
Evaluate area-level crime risk predictions against realized actuals.

Joins predictions (t -> target_date=t+1) with actual area-day incident counts at
target_date, computes regression metrics, and persists a compact evaluation row
to an Iceberg table for monitoring and dashboards.

Predictions table (Iceberg): chicago_crime_gold.fact_area_risk_predictions
Actuals source (Iceberg): chicago_crime_gold.feature_crime_risk_area_daily (count_1d at target_date)
Output table (Iceberg): chicago_crime_gold.model_evaluation_runs
"""

import argparse
import logging
from datetime import date

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date

logger = logging.getLogger(__name__)


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _reg_metrics(df, label_col: str, pred_col: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for name in ["rmse", "mae", "r2"]:
        out[name] = float(
            RegressionEvaluator(labelCol=label_col, predictionCol=pred_col, metricName=name).evaluate(df)
        )
    return out


def main(
    predictions_table: str,
    feature_table: str,
    evaluation_table: str,
    eval_window_start: date,
    eval_window_end: date,
    process_date: date,
    prediction_target: str = "total_count",
) -> None:
    spark = None
    try:
        spark = SparkSession.builder.appName("EvaluateCrimeRiskPredictions").getOrCreate()
        logger.info("Spark session initialized.")

        try:
            preds = (
                spark.read.format("iceberg")
                .load(predictions_table)
                .withColumn("target_date", to_date(col("target_date")))
                .filter((col("target_date") >= lit(eval_window_start)) & (col("target_date") <= lit(eval_window_end)))
            )
        except Exception as exc:
            logger.warning("Could not read predictions table %s: %s. It may not exist yet.", predictions_table, exc)
            return

        # Target filtering (new schema) is optional for backward compatibility.
        if "prediction_target" in preds.columns:
            preds = preds.filter(col("prediction_target") == lit(prediction_target))
        else:
            # Older scoring outputs did not include `prediction_target`.
            # Only default-total evaluation is supported in that case.
            if prediction_target not in {"total", "total_count", "count_1d"}:
                logger.warning(
                    "Predictions table %s has no `prediction_target` column; defaulting to total_count evaluation.",
                    predictions_table,
                )
                prediction_target = "total_count"

        n_preds = preds.count()
        logger.info("Predictions in eval window %s→%s: %s", eval_window_start, eval_window_end, n_preds)
        if n_preds == 0:
            logger.info("No predictions to evaluate; skipping.")
            return

        target_to_actual_col = {
            "total": "count_1d",
            "total_count": "count_1d",
            "count_1d": "count_1d",
            "violent_count": "violent_count_1d",
            "violent": "violent_count_1d",
            "violent_count_1d": "violent_count_1d",
            "property_count": "property_count_1d",
            "property": "property_count_1d",
            "property_count_1d": "property_count_1d",
            "drug_count": "drug_count_1d",
            "drug": "drug_count_1d",
            "drug_count_1d": "drug_count_1d",
            "weapon_count": "weapon_count_1d",
            "weapon": "weapon_count_1d",
            "weapon_count_1d": "weapon_count_1d",
        }
        actual_count_col = target_to_actual_col.get(prediction_target)
        if not actual_count_col:
            raise ValueError(f"Unsupported prediction_target={prediction_target}")

        actuals = (
            spark.read.format("iceberg")
            .load(feature_table)
            .withColumn("event_date", to_date(col("date")))
            .select(
                col("area_type"),
                col("area_id").cast("int").alias("area_id"),
                col("event_date").alias("target_date"),
                col(actual_count_col).cast("double").alias("actual_count"),
            )
            .filter((col("target_date") >= lit(eval_window_start)) & (col("target_date") <= lit(eval_window_end)))
        )

        joined = (
            preds.select(
                "run_id",
                "model_name",
                "model_version",
                col("target_date"),
                "area_type",
                col("area_id").cast("int").alias("area_id"),
                col("predicted_count").cast("double").alias("predicted_count"),
            )
            .join(actuals, on=["area_type", "area_id", "target_date"], how="inner")
            .filter(col("actual_count").isNotNull())
        )

        n_joined = joined.count()
        logger.info("Joined prediction/actual rows: %s", n_joined)
        if n_joined == 0:
            logger.info("No joined rows; skipping evaluation write.")
            return

        # Metrics computed globally for the window (across all runs/models found).
        metrics = _reg_metrics(joined, label_col="actual_count", pred_col="predicted_count")
        logger.info(
            "AREA_RISK_ML_EVAL_METRICS process_date=%s eval_window=%s..%s rmse=%.6f mae=%.6f r2=%.6f joined_rows=%d",
            process_date,
            eval_window_start,
            eval_window_end,
            metrics["rmse"],
            metrics["mae"],
            metrics["r2"],
            n_joined,
        )

        # Produce one evaluation row per (model_name, model_version) for the window.
        by_model = joined.select("model_name", "model_version").distinct().collect()
        rows = []
        for r in by_model:
            rows.append(
                (
                    r["model_name"],
                    r["model_version"],
                    eval_window_start,
                    eval_window_end,
                    metrics["rmse"],
                    metrics["mae"],
                    metrics["r2"],
                    int(n_joined),
                    process_date,
                )
            )

        result_df = spark.createDataFrame(
            rows,
            schema="""
              model_name string,
              model_version string,
              eval_window_start date,
              eval_window_end date,
              rmse double,
              mae double,
              r2 double,
              row_count long,
              process_date date
            """,
        ).withColumn("created_at", current_timestamp())

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {evaluation_table} (
              model_name STRING,
              model_version STRING,
              eval_window_start DATE,
              eval_window_end DATE,
              rmse DOUBLE,
              mae DOUBLE,
              r2 DOUBLE,
              row_count BIGINT,
              process_date DATE,
              created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(process_date))
            """
        )

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        result_df.write.format("iceberg").mode("overwrite").save(evaluation_table)
        logger.info("Wrote evaluation rows: %s", result_df.count())

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Evaluate crime risk predictions vs actuals.")
    parser.add_argument("--predictions_table", default="chicago_crime_gold.fact_area_risk_predictions")
    parser.add_argument("--feature_table", default="chicago_crime_gold.feature_crime_risk_area_daily")
    parser.add_argument("--evaluation_table", default="chicago_crime_gold.model_evaluation_runs")
    parser.add_argument("--eval_window_start", type=parse_date, required=True)
    parser.add_argument("--eval_window_end", type=parse_date, required=True)
    parser.add_argument("--process_date", type=parse_date, required=True)
    parser.add_argument(
        "--prediction_target",
        default="total_count",
        help="Target corresponding to scoring output (e.g. total_count, violent_count, property_count, drug_count, weapon_count).",
    )
    args = parser.parse_args()
    main(
        predictions_table=args.predictions_table,
        feature_table=args.feature_table,
        evaluation_table=args.evaluation_table,
        eval_window_start=args.eval_window_start,
        eval_window_end=args.eval_window_end,
        process_date=args.process_date,
        prediction_target=args.prediction_target,
    )

