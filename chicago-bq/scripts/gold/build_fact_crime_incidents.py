#!/usr/bin/env python3
import sys
from pathlib import Path

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, hash, hour, dayofweek, date_format
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
import argparse
import logging
from datetime import date
import base64
import os
import json

from google.cloud import bigquery
from google.oauth2 import service_account

from chicago_crime_scripts.bq_schema import ensure_bq_table_columns
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


_FACT_INCIDENTS_EXTRA_COLS = (
    ("crime_category", "STRING"),
    ("crime_group", "STRING"),
    ("crime_subtype", "STRING"),
    ("property_crime_flag", "INT64"),
    ("drug_crime_flag", "INT64"),
    ("weapon_crime_flag", "INT64"),
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(gcp_project_id: str, bq_dataset: str, iceberg_table: str, process_date: date):
    spark = (
        SparkSession.builder
        .appName("BuildCrimeIncidentsFact")
        .getOrCreate()
    )

    try:
        bq_client = _make_bq_client(gcp_project_id)
        table_name = f"{gcp_project_id}.{bq_dataset}.fact_crime_incidents"
        ensure_bq_table_columns(bq_client, table_name, list(_FACT_INCIDENTS_EXTRA_COLS))

        # Idempotency: replace current process_date slice before append write.
        bq_client.query(
            f"""
            DELETE FROM `{table_name}`
            WHERE process_date = DATE('{process_date}')
            """
        ).result()
        logger.info(f"Deleted existing fact_crime_incidents rows for process_date={process_date}")

        # Read dimension tables from BigQuery (validates presence, but we don't join yet)
        dim_date = (spark.read
                    .format("bigquery")
                    .option("table", f"{gcp_project_id}.{bq_dataset}.dim_date")
                    .option("parentProject", gcp_project_id)
                    .option("credentials", bq_creds())
                    .load())

        dim_location = (spark.read
                        .format("bigquery")
                        .option("table", f"{gcp_project_id}.{bq_dataset}.dim_location")
                        .option("parentProject", gcp_project_id)
                        .option("credentials", bq_creds())
                        .load())

        dim_crime_type = (spark.read
                          .format("bigquery")
                          .option("table", f"{gcp_project_id}.{bq_dataset}.dim_crime_type")
                          .option("parentProject", gcp_project_id)
                          .option("credentials", bq_creds())
                          .load())

        dim_time_of_day = (spark.read
                           .format("bigquery")
                           .option("table", f"{gcp_project_id}.{bq_dataset}.dim_time_of_day")
                           .option("parentProject", gcp_project_id)
                           .option("credentials", bq_creds())
                           .load())

        # Validate dimension tables have data
        dim_date_count = dim_date.count()
        dim_location_count = dim_location.count()
        dim_crime_type_count = dim_crime_type.count()
        dim_time_count = dim_time_of_day.count()

        logger.info(f"Dimension table counts - Date: {dim_date_count}, Location: {dim_location_count}, Crime Type: {dim_crime_type_count}, Time: {dim_time_count}")

        if dim_date_count == 0:
            raise Exception("dim_date table is empty. Please run dimension tables first.")
        if dim_location_count == 0:
            raise Exception("dim_location table is empty. Please run dimension tables first.")
        if dim_crime_type_count == 0:
            raise Exception("dim_crime_type table is empty. Please run dimension tables first.")
        if dim_time_count == 0:
            raise Exception("dim_time_of_day table is empty. Please run dimension tables first.")

        # Read crime data from Iceberg for the specific date
        process_date_key = int(process_date.strftime("%Y%m%d"))
        crime_df = (spark.read.format("iceberg")
                   .load(iceberg_table)
                   .filter(date_format(col("date"), "yyyyMMdd").cast("integer") == process_date_key))
        crime_df = ensure_crime_taxonomy_columns(crime_df)

        if crime_df.count() == 0:
            logger.info(f"No crime incidents found for {process_date}")
            return

        # Create fact table with foreign keys and measures
        fact_incidents = crime_df.select(
            col("id").cast("string").alias("incident_id"),
            col("case_number"),

            # Foreign Keys
            date_format(col("date"), "yyyyMMdd").cast("integer").alias("date_key"),

            # Location key - hash of location attributes
            hash(
                coalesce(col("beat"), lit(0)),
                coalesce(col("district"), lit(0)),
                coalesce(col("ward"), lit(0)),
                coalesce(col("community_area"), lit(0)),
                coalesce(col("location_description"), lit("Unknown")),
                coalesce(col("block"), lit("Unknown"))
            ).cast("bigint").alias("location_key"),

            # Canonical offense key from shared silver taxonomy
            col("crime_type_key").cast("bigint").alias("crime_type_key"),

            # Time of day key
            hour(col("date")).alias("time_of_day_key"),

            # Measures and facts
            col("date").cast("timestamp").alias("incident_datetime"),
            col("arrest").cast("boolean").alias("arrest_made"),
            col("domestic").cast("boolean").alias("domestic_incident"),

            # Location coordinates for mapping and analysis
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),

            # Additional dimensions for detailed analysis
            col("primary_type").alias("crime_type"),
            col("description").alias("crime_description"),
            col("crime_category"),
            col("crime_offense_group").alias("crime_group"),
            col("crime_offense_subtype").alias("crime_subtype"),
            col("location_description"),
            col("beat").cast("integer").alias("beat"),
            col("district").cast("integer").alias("district"),
            col("ward").cast("integer").alias("ward"),
            col("community_area").cast("integer").alias("community_area"),
            col("iucr").alias("iucr_code"),

            # Derived measures and flags
            when(col("arrest") == True, 1).otherwise(0).alias("arrest_flag"),
            when(col("domestic") == True, 1).otherwise(0).alias("domestic_flag"),
            lit(1).alias("incident_count"),
            col("violent_crime_flag").cast("integer").alias("violent_crime_flag"),
            col("property_crime_flag").cast("integer").alias("property_crime_flag"),
            col("drug_crime_flag").cast("integer").alias("drug_crime_flag"),
            col("weapon_crime_flag").cast("integer").alias("weapon_crime_flag"),

            # Time-based analysis fields
            dayofweek(col("date")).alias("day_of_week"),
            date_format(col("date"), "yyyy").cast("integer").alias("year"),
            date_format(col("date"), "MM").cast("integer").alias("month"),
            date_format(col("date"), "dd").cast("integer").alias("day"),
            hour(col("date")).alias("hour"),
        )

        # Add derived location classification
        fact_incidents = fact_incidents.withColumn(
            "location_risk_category",
            when(col("location_description").rlike("(?i)(STREET|SIDEWALK|ALLEY)"), "High Risk")
            .when(col("location_description").rlike("(?i)(RESIDENCE|APARTMENT)"), "Medium Risk")
            .otherwise("Low Risk")
        )

        # Add process metadata
        fact_incidents = fact_incidents.withColumn("process_date", lit(process_date))

        # Deterministic de-duplication keeps reruns stable when source contains
        # accidental duplicates for the same incident id.
        dedupe_window = Window.partitionBy("incident_id").orderBy(
            desc("incident_datetime"),
            desc("date_key"),
            desc("case_number"),
        )
        fact_incidents = (
            fact_incidents
            .withColumn("_row_num", row_number().over(dedupe_window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

        logger.info(f"Writing fact_crime_incidents for {process_date} to BigQuery")

        (
            fact_incidents.write
            .format("bigquery")
            .option("table", table_name)
            .option("parentProject", gcp_project_id)
            .option("credentials", bq_creds())
            .option("writeMethod", "direct")
            .mode("append")
            .save()
        )

        logger.info(f"Successfully wrote fact_crime_incidents for {process_date} to {table_name}")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Crime Incidents Fact")
    parser.add_argument("--gcp_project_id", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--iceberg_table", required=True)
    parser.add_argument("--process_date", type=parse_date, required=True)
    args = parser.parse_args()

    main(args.gcp_project_id, args.bq_dataset,
         args.iceberg_table, args.process_date)

