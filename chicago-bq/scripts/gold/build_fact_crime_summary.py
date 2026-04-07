#!/usr/bin/env python3
import argparse
import base64
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

from google.cloud import bigquery
from google.oauth2 import service_account
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, date_format, hash, hour, lit,
                                   sum, when, coalesce)

from chicago_crime_scripts.crime_taxonomy import ensure_crime_taxonomy_columns


def bq_creds() -> str:
    raw = os.getenv("GCP_BQ_SERVICE_ACCOUNT_JSON", "")
    return base64.b64encode(raw.encode()).decode() if raw else ""


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _make_bq_client(project: str) -> bigquery.Client:
    raw = os.getenv("GCP_BQ_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise RuntimeError(
            "GCP_BQ_SERVICE_ACCOUNT_JSON is not set; cannot authenticate to BigQuery."
        )
    sa_info = json.loads(raw)
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(project=project, credentials=credentials)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(gcp_project_id: str, bq_dataset: str,
         iceberg_table: str, process_date: date):
    spark = (
        SparkSession.builder
        .appName("BuildCrimeSummaryFact")
        .getOrCreate()
    )

    try:
        bq_client = _make_bq_client(gcp_project_id)
        daily_table = f"{gcp_project_id}.{bq_dataset}.fact_crime_daily_summary"
        hourly_table = f"{gcp_project_id}.{bq_dataset}.fact_crime_hourly_summary"

        # Idempotency: replace current process_date slices before append writes.
        bq_client.query(
            f"""
            DELETE FROM `{daily_table}`
            WHERE process_date = DATE('{process_date}')
            """
        ).result()
        bq_client.query(
            f"""
            DELETE FROM `{hourly_table}`
            WHERE process_date = DATE('{process_date}')
            """
        ).result()
        logger.info(
            f"Deleted existing summary rows for process_date={process_date} "
            f"in {daily_table} and {hourly_table}"
        )

        # Read dimension tables from BigQuery
        def read_bq(table: str):
            return (spark.read
                    .format("bigquery")
                    .option("table", table)
                    .option("parentProject", gcp_project_id)
                    .option("credentials", bq_creds())
                    .load())

        dim_date = read_bq(f"{gcp_project_id}.{bq_dataset}.dim_date")
        dim_location = read_bq(f"{gcp_project_id}.{bq_dataset}.dim_location")
        dim_crime_type = read_bq(f"{gcp_project_id}.{bq_dataset}.dim_crime_type")

        dim_date_count = dim_date.count()
        dim_location_count = dim_location.count()
        dim_crime_type_count = dim_crime_type.count()

        logger.info(f"Dimension counts — date: {dim_date_count}, "
                    f"location: {dim_location_count}, crime_type: {dim_crime_type_count}")

        if dim_date_count == 0:
            raise Exception("dim_date is empty. Run dimension tables first.")
        if dim_location_count == 0:
            raise Exception("dim_location is empty. Run dimension tables first.")
        if dim_crime_type_count == 0:
            raise Exception("dim_crime_type is empty. Run dimension tables first.")

        # Read silver crime data for the target date only
        process_date_key = int(process_date.strftime("%Y%m%d"))
        crime_df = (spark.read.format("iceberg")
                    .load(iceberg_table)
                    .filter(date_format(col("date"), "yyyyMMdd").cast("integer") == process_date_key))
        crime_df = ensure_crime_taxonomy_columns(crime_df)

        crime_count = crime_df.count()
        logger.info(f"Crime records for {process_date}: {crime_count}")

        if crime_count == 0:
            logger.info(f"No crime data found for {process_date}. Skipping.")
            return

        # ─────────────────────────────────────────────────────────
        # Prepare shared columns
        # ─────────────────────────────────────────────────────────
        crime_prepared = crime_df.select(
            date_format(col("date"), "yyyyMMdd").cast("integer").alias("date_key"),
            hash(
                coalesce(col("beat"), lit(0)),
                coalesce(col("district"), lit(0)),
                coalesce(col("ward"), lit(0)),
                coalesce(col("community_area"), lit(0)),
                coalesce(col("location_description"), lit("Unknown")),
                coalesce(col("block"), lit("Unknown")),
            ).cast("bigint").alias("location_key"),
            col("crime_type_key").cast("bigint").alias("crime_type_key"),
            col("district").cast("integer").alias("district"),
            col("arrest").cast("boolean").alias("arrest"),
            col("domestic").cast("boolean").alias("domestic"),
            hour(col("date")).alias("hour"),
            col("violent_crime_flag").cast("integer").alias("violent_crime_flag"),
        )

        # ─────────────────────────────────────────────────────────
        # FK validation: ensure all location_keys exist in dim_location
        # ─────────────────────────────────────────────────────────
        missing_location_keys = (
            crime_prepared.select("location_key").distinct()
            .join(dim_location.select("location_key"), on="location_key", how="left_anti")
        )
        missing_count = missing_location_keys.count()
        if missing_count > 0:
            sample = [r.location_key for r in missing_location_keys.limit(10).collect()]
            logger.warning(f"{missing_count} location_key(s) missing from dim_location. "
                           f"Sample: {sample}")
            raise Exception(
                f"{missing_count} location keys missing from dim_location. "
                "Ensure dimension tables are populated before running fact tables."
            )

        # ─────────────────────────────────────────────────────────
        # Daily summary — Transaction 1
        # ─────────────────────────────────────────────────────────
        daily_summary = (crime_prepared
            .groupBy("date_key", "location_key", "crime_type_key", "district")
            .agg(
                count("*").alias("total_incidents"),
                sum(when(col("arrest"), 1).otherwise(0)).alias("total_arrests"),
                sum(when(col("domestic"), 1).otherwise(0)).alias("total_domestic"),
                sum("violent_crime_flag").alias("total_violent_crimes"),
                count("*").alias("incident_count_check"),
            )
        )

        daily_summary = (daily_summary
            .withColumn("arrest_rate",
                when(col("total_incidents") > 0,
                     (col("total_arrests").cast("double") / col("total_incidents")) * 100.0)
                .otherwise(0.0))
            .withColumn("domestic_rate",
                when(col("total_incidents") > 0,
                     (col("total_domestic").cast("double") / col("total_incidents")) * 100.0)
                .otherwise(0.0))
            .withColumn("violent_crime_rate",
                when(col("total_incidents") > 0,
                     (col("total_violent_crimes").cast("double") / col("total_incidents")) * 100.0)
                .otherwise(0.0))
            .withColumn("process_date", lit(process_date))
            .withColumn("summary_type", lit("daily"))
        )

        daily_count = daily_summary.count()
        logger.info(f"Daily summary rows to upsert: {daily_count}")

        (daily_summary.write
            .format("bigquery")
            .option("table", daily_table)
            .option("parentProject", gcp_project_id)
            .option("credentials", bq_creds())
            .option("writeMethod", "direct")
            .mode("append")
            .save())

        logger.info(f"Daily summary write to BigQuery complete: {daily_count} rows to {daily_table}.")

        # ─────────────────────────────────────────────────────────
        # Hourly summary — Transaction 2 (independent of daily)
        # ─────────────────────────────────────────────────────────
        hourly_summary = (crime_prepared
            .groupBy("date_key", "hour", "district")
            .agg(
                count("*").alias("hourly_incidents"),
                sum(when(col("arrest"), 1).otherwise(0)).alias("hourly_arrests"),
                sum("violent_crime_flag").alias("hourly_violent_crimes"),
            )
            .withColumnRenamed("hour", "time_of_day_key")
            .withColumn("time_period",
                when((col("time_of_day_key") >= 5) & (col("time_of_day_key") <= 11), "Morning")
                .when((col("time_of_day_key") >= 12) & (col("time_of_day_key") <= 17), "Afternoon")
                .when((col("time_of_day_key") >= 18) & (col("time_of_day_key") <= 22), "Evening")
                .otherwise("Night"))
            .withColumn("process_date", lit(process_date))
            .withColumn("summary_type", lit("hourly"))
        )

        hourly_count = hourly_summary.count()
        logger.info(f"Hourly summary rows to upsert: {hourly_count}")

        (hourly_summary.write
            .format("bigquery")
            .option("table", hourly_table)
            .option("parentProject", gcp_project_id)
            .option("credentials", bq_creds())
            .option("writeMethod", "direct")
            .mode("append")
            .save())

        logger.info(f"Hourly summary write to BigQuery complete: {hourly_count} rows to {hourly_table}.")

        logger.info(f"build_fact_crime_summary complete for {process_date}.")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Crime Summary Facts")
    parser.add_argument("--gcp_project_id", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--iceberg_table", required=True)
    parser.add_argument("--process_date", type=parse_date, required=True)
    args = parser.parse_args()

    main(args.gcp_project_id, args.bq_dataset,
         args.iceberg_table, args.process_date)

