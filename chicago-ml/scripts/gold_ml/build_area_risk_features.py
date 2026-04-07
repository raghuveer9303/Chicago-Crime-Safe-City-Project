#!/usr/bin/env python3
from __future__ import annotations

"""
Build area-level daily crime risk feature table for ML (XGBoost / tree models).

Reads from the silver Iceberg incident table and produces one row per
(area_type, area_id, date_key) with rolling windows and a next-day label,
as described in docs/chicago_crime_ml_implementation_outline.md.

Table: chicago_crime_gold.feature_crime_risk_area_daily
Grain: one row per (area_type, area_id, date_key).
Partitioning: days(process_date), dynamic overwrite (idempotent per process_date).
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    date_add,
    date_format,
    dayofweek,
    explode,
    lit,
    sequence,
    to_date,
    when,
    coalesce,
    sum as spark_sum,
    count as spark_count,
    month as spark_month,
)

from chicago_crime_scripts.crime_taxonomy import ensure_crime_taxonomy_columns

logger = logging.getLogger(__name__)


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def build_area_daily_counts(
    df,
    area_type: str,
) :
    """
    Aggregate incident-level silver data to daily counts for a single area_type.

    Currently supports area_type="community_area" (default starting point).
    """
    if area_type != "community_area":
        raise ValueError(f"Unsupported area_type={area_type}; only 'community_area' is implemented.")

    area_id_col = col("community_area").cast("int")

    df = df.withColumn("event_date", to_date(col("date"))).withColumn(
        "date_key", date_format(col("date"), "yyyyMMdd").cast("int")
    )

    # Daily counts per (area_id, date_key)
    daily = (
        df.groupBy(
            lit(area_type).alias("area_type"),
            area_id_col.alias("area_id"),
            col("event_date"),
            col("date_key"),
        )
        .agg(
            spark_count("*").alias("count_1d"),
            spark_sum(when(col("violent_crime_flag") == 1, 1).otherwise(0)).alias("violent_count_1d"),
            spark_sum(when(col("property_crime_flag") == 1, 1).otherwise(0)).alias("property_count_1d"),
            spark_sum(when(col("drug_crime_flag") == 1, 1).otherwise(0)).alias("drug_count_1d"),
            spark_sum(when(col("weapon_crime_flag") == 1, 1).otherwise(0)).alias("weapon_count_1d"),
            spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("arrest_count_1d"),
            spark_sum(when(col("domestic") == True, 1).otherwise(0)).alias("domestic_count_1d"),
        )
    )

    return daily


def _ensure_table(spark, iceberg_gold_table: str) -> None:
    """Create the gold table if absent and apply any missing column additions."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {iceberg_gold_table} (
            area_type STRING,
            area_id INT,
            date DATE,
            date_key INT,
            count_1d INT,
            violent_count_1d INT,
            property_count_1d INT,
            drug_count_1d INT,
            weapon_count_1d INT,
            count_3d INT,
            count_7d INT,
            count_14d INT,
            violent_count_7d INT,
            property_count_7d INT,
            drug_count_7d INT,
            weapon_count_7d INT,
            arrest_count_1d INT,
            domestic_count_1d INT,
            arrest_rate_7d DOUBLE,
            domestic_rate_7d DOUBLE,
            day_of_week INT,
            is_weekend INT,
            month INT,
            season INT,
            label_next_day_count INT,
            label_next_day_violent_count INT,
            label_next_day_property_count INT,
            label_next_day_drug_count INT,
            label_next_day_weapon_count INT,
            feature_as_of_date DATE,
            process_date DATE
        )
        USING ICEBERG
        PARTITIONED BY (days(process_date))
        """
    )

    try:
        existing_cols = set(spark.table(iceberg_gold_table).columns)
    except Exception:  # pragma: no cover - defensive for early bootstrap
        existing_cols = set()

    add_cols: dict[str, str] = {
        "violent_count_1d": "INT",
        "property_count_1d": "INT",
        "drug_count_1d": "INT",
        "weapon_count_1d": "INT",
        "property_count_7d": "INT",
        "drug_count_7d": "INT",
        "weapon_count_7d": "INT",
        "arrest_count_1d": "INT",
        "domestic_count_1d": "INT",
        "label_next_day_violent_count": "INT",
        "label_next_day_property_count": "INT",
        "label_next_day_drug_count": "INT",
        "label_next_day_weapon_count": "INT",
    }
    missing = [f"{c} {t}" for c, t in add_cols.items() if c not in existing_cols]
    if missing:
        spark.sql(f"ALTER TABLE {iceberg_gold_table} ADD COLUMNS ({', '.join(missing)})")


def _process_single_date(
    spark,
    iceberg_silver_table: str,
    iceberg_gold_table: str,
    process_date: date,
    area_type: str,
) -> None:
    """
    Build and write area-level daily features for one process_date (idempotent).

    Reads silver incidents for [process_date-14, process_date+1], computes rolling
    windows and the next-day label, then overwrites the process_date partition in
    the gold table using dynamic partition overwrite.
    """
    min_feature_date = process_date - timedelta(days=14)
    max_needed_date = process_date + timedelta(days=1)

    logger.info(
        "Reading silver data from %s for date range %s to %s",
        iceberg_silver_table,
        min_feature_date,
        max_needed_date,
    )

    silver = (
        spark.read.format("iceberg")
        .load(iceberg_silver_table)
        .filter(
            (to_date(col("date")) >= lit(min_feature_date))
            & (to_date(col("date")) <= lit(max_needed_date))
        )
    )
    silver = ensure_crime_taxonomy_columns(silver)

    n = silver.count()
    logger.info("Silver records in window for %s: %s", process_date, n)
    if n == 0:
        logger.info("No silver data in window for %s; skipping write.", process_date)
        return

    daily = build_area_daily_counts(silver, area_type=area_type)

    # ------------------------------------------------------------------
    # Densify area-day rows:
    #   Ensure we have one row per (area_type, area_id, event_date)
    #   for every calendar date in [min_feature_date, max_needed_date].
    #   This makes zero-incident days explicit instead of missing.
    # ------------------------------------------------------------------
    areas_df = daily.select("area_type", "area_id").distinct()

    dates_df = (
        spark.createDataFrame([(1,)], ["dummy"])
        .select(
            explode(
                sequence(
                    lit(min_feature_date),
                    lit(max_needed_date),
                )
            ).alias("event_date")
        )
        .withColumn("date_key", date_format(col("event_date"), "yyyyMMdd").cast("int"))
    )

    calendar_df = areas_df.crossJoin(dates_df)

    daily = (
        calendar_df.join(
            daily,
            on=["area_type", "area_id", "event_date", "date_key"],
            how="left",
        )
        .select(
            "area_type",
            "area_id",
            "event_date",
            "date_key",
            "count_1d",
            "violent_count_1d",
            "property_count_1d",
            "drug_count_1d",
            "weapon_count_1d",
            "arrest_count_1d",
            "domestic_count_1d",
        )
    )

    w3 = (
        Window.partitionBy("area_type", "area_id")
        .orderBy("event_date")
        .rowsBetween(-2, 0)
    )
    w7 = (
        Window.partitionBy("area_type", "area_id")
        .orderBy("event_date")
        .rowsBetween(-6, 0)
    )
    w14 = (
        Window.partitionBy("area_type", "area_id")
        .orderBy("event_date")
        .rowsBetween(-13, 0)
    )

    daily = (
        daily.withColumn("count_3d", spark_sum("count_1d").over(w3))
        .withColumn("count_7d", spark_sum("count_1d").over(w7))
        .withColumn("count_14d", spark_sum("count_1d").over(w14))
        .withColumn("violent_count_7d", spark_sum("violent_count_1d").over(w7))
        .withColumn("property_count_7d", spark_sum("property_count_1d").over(w7))
        .withColumn("drug_count_7d", spark_sum("drug_count_1d").over(w7))
        .withColumn("weapon_count_7d", spark_sum("weapon_count_1d").over(w7))
    )

    daily = daily.withColumn(
        "arrest_count_7d", spark_sum("arrest_count_1d").over(w7)
    ).withColumn(
        "domestic_count_7d", spark_sum("domestic_count_1d").over(w7)
    ).withColumn(
        "arrest_rate_7d",
        when(col("count_7d") > 0, col("arrest_count_7d") / col("count_7d")).otherwise(0.0),
    ).withColumn(
        "domestic_rate_7d",
        when(col("count_7d") > 0, col("domestic_count_7d") / col("count_7d")).otherwise(0.0),
    )

    label_df = daily.select(
        col("area_type").alias("label_area_type"),
        col("area_id").alias("label_area_id"),
        col("event_date").alias("label_event_date"),
        col("count_1d").alias("label_next_day_count"),
        col("violent_count_1d").alias("label_next_day_violent_count"),
        col("property_count_1d").alias("label_next_day_property_count"),
        col("drug_count_1d").alias("label_next_day_drug_count"),
        col("weapon_count_1d").alias("label_next_day_weapon_count"),
    )

    daily = (
        daily.join(
            label_df,
            on=(
                (daily.area_type == label_df.label_area_type)
                & (daily.area_id == label_df.label_area_id)
                & (date_add(daily.event_date, 1) == label_df.label_event_date)
            ),
            how="left",
        )
        .drop("label_area_type", "label_area_id", "label_event_date")
    )

    daily = daily.withColumn("day_of_week", dayofweek(col("event_date"))).withColumn(
        "is_weekend",
        when(col("day_of_week").isin(1, 7), 1).otherwise(0),
    ).withColumn(
        "month",
        spark_month(col("event_date")),
    )
    daily = daily.withColumn(
        "season",
        when(col("month").isin(12, 1, 2), 1)
        .when(col("month").isin(3, 4, 5), 2)
        .when(col("month").isin(6, 7, 8), 3)
        .when(col("month").isin(9, 10, 11), 4)
        .otherwise(1),
    )

    daily = daily.select(
        "area_type",
        "area_id",
        col("event_date").alias("date"),
        "date_key",
        coalesce(col("count_1d"), lit(0)).cast("int").alias("count_1d"),
        coalesce(col("violent_count_1d"), lit(0)).cast("int").alias("violent_count_1d"),
        coalesce(col("property_count_1d"), lit(0)).cast("int").alias("property_count_1d"),
        coalesce(col("drug_count_1d"), lit(0)).cast("int").alias("drug_count_1d"),
        coalesce(col("weapon_count_1d"), lit(0)).cast("int").alias("weapon_count_1d"),
        coalesce(col("count_3d"), lit(0)).cast("int").alias("count_3d"),
        coalesce(col("count_7d"), lit(0)).cast("int").alias("count_7d"),
        coalesce(col("count_14d"), lit(0)).cast("int").alias("count_14d"),
        coalesce(col("violent_count_7d"), lit(0)).cast("int").alias("violent_count_7d"),
        coalesce(col("property_count_7d"), lit(0)).cast("int").alias("property_count_7d"),
        coalesce(col("drug_count_7d"), lit(0)).cast("int").alias("drug_count_7d"),
        coalesce(col("weapon_count_7d"), lit(0)).cast("int").alias("weapon_count_7d"),
        coalesce(col("arrest_count_1d"), lit(0)).cast("int").alias("arrest_count_1d"),
        coalesce(col("domestic_count_1d"), lit(0)).cast("int").alias("domestic_count_1d"),
        coalesce(col("arrest_rate_7d"), lit(0.0)).alias("arrest_rate_7d"),
        coalesce(col("domestic_rate_7d"), lit(0.0)).alias("domestic_rate_7d"),
        "day_of_week",
        "is_weekend",
        "month",
        "season",
        coalesce(col("label_next_day_count"), lit(0)).cast("int").alias("label_next_day_count"),
        coalesce(col("label_next_day_violent_count"), lit(0)).cast("int").alias(
            "label_next_day_violent_count"
        ),
        coalesce(col("label_next_day_property_count"), lit(0)).cast("int").alias(
            "label_next_day_property_count"
        ),
        coalesce(col("label_next_day_drug_count"), lit(0)).cast("int").alias(
            "label_next_day_drug_count"
        ),
        coalesce(col("label_next_day_weapon_count"), lit(0)).cast("int").alias(
            "label_next_day_weapon_count"
        ),
    )

    daily = daily.withColumn("feature_as_of_date", lit(process_date)).withColumn(
        "process_date", lit(process_date)
    )

    daily_out = daily.filter(col("date") == lit(process_date))

    out_count = daily_out.count()
    logger.info("Rows to write for %s: %s", process_date, out_count)
    if out_count == 0:
        logger.info("No area-day rows for process_date=%s; skipping write.", process_date)
        return

    logger.info(
        "Writing area risk features to %s for process_date=%s", iceberg_gold_table, process_date
    )
    (
        daily_out.write.format("iceberg")
        .option("overwrite-mode", "dynamic")
        .mode("overwrite")
        .save(iceberg_gold_table)
    )
    logger.info("Successfully wrote area risk feature table for %s", process_date)


def main(
    iceberg_silver_table: str,
    iceberg_gold_table: str,
    process_date: date,
    start_date: date | None = None,
    area_type: str = "community_area",
) -> None:
    """
    Build area-level daily feature table for all dates in [start_date, process_date].

    When start_date equals process_date (the default for daily runs) only that single
    day is processed.  Passing an earlier start_date enables idempotent backfills: the
    same Spark session iterates through every date in the range and overwrites each
    process_date partition via dynamic partition overwrite, so re-runs are safe.
    """
    effective_start = start_date if start_date is not None else process_date

    if effective_start > process_date:
        raise ValueError(
            f"start_date ({effective_start}) must be <= process_date ({process_date})."
        )

    dates_to_process: list[date] = []
    cur = effective_start
    while cur <= process_date:
        dates_to_process.append(cur)
        cur += timedelta(days=1)

    logger.info(
        "Processing %d date(s) from %s to %s",
        len(dates_to_process),
        effective_start,
        process_date,
    )

    spark = None
    try:
        spark = SparkSession.builder.appName("BuildAreaRiskFeatures").getOrCreate()
        logger.info("Spark session initialized.")

        _ensure_table(spark, iceberg_gold_table)

        for d in dates_to_process:
            logger.info("--- Processing process_date=%s ---", d)
            _process_single_date(
                spark=spark,
                iceberg_silver_table=iceberg_silver_table,
                iceberg_gold_table=iceberg_gold_table,
                process_date=d,
                area_type=area_type,
            )

        logger.info(
            "Completed area risk feature ETL for %d date(s) [%s → %s].",
            len(dates_to_process),
            effective_start,
            process_date,
        )

    except Exception as exc:
        logger.error("Area risk feature job failed: %s", exc)
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
        description="Build area-level daily crime risk feature table for ML.",
    )
    parser.add_argument(
        "--iceberg_silver_table",
        required=True,
        help="Fully qualified silver Iceberg table (e.g. chicago_crime.chicago_crime_silver).",
    )
    parser.add_argument(
        "--iceberg_gold_table",
        required=True,
        help=(
            "Fully qualified gold Iceberg table for area features "
            "(e.g. chicago_crime_gold.feature_crime_risk_area_daily)."
        ),
    )
    parser.add_argument(
        "--process_date",
        type=parse_date,
        required=True,
        help="Latest date (inclusive) to build features for (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--start_date",
        type=parse_date,
        default=None,
        help=(
            "Earliest date (inclusive) to build features for (YYYY-MM-DD). "
            "Defaults to --process_date, meaning only that single day is processed. "
            "Set to an earlier date to backfill the full range idempotently."
        ),
    )
    parser.add_argument(
        "--area_type",
        default="community_area",
        help="Area grain for features (currently only 'community_area' is supported).",
    )

    args = parser.parse_args()
    logger.info(
        "Starting area risk feature ETL — iceberg_silver_table=%s, iceberg_gold_table=%s, "
        "start_date=%s, process_date=%s, area_type=%s",
        args.iceberg_silver_table,
        args.iceberg_gold_table,
        args.start_date or args.process_date,
        args.process_date,
        args.area_type,
    )
    main(
        iceberg_silver_table=args.iceberg_silver_table,
        iceberg_gold_table=args.iceberg_gold_table,
        process_date=args.process_date,
        start_date=args.start_date,
        area_type=args.area_type,
    )

