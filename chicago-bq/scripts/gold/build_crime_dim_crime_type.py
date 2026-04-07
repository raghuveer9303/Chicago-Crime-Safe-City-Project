#!/usr/bin/env python3
import sys
from pathlib import Path

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    desc,
    first,
    lit,
    row_number,
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
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


_DIM_CRIME_TYPE_EXTRA_COLS = (
    ("crime_category", "STRING"),
    ("crime_offense_group", "STRING"),
    ("crime_offense_subtype", "STRING"),
    ("crime_offense_label", "STRING"),
    ("violent_crime_flag", "INT64"),
    ("property_crime_flag", "INT64"),
    ("drug_crime_flag", "INT64"),
    ("weapon_crime_flag", "INT64"),
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(gcp_project_id: str, bq_dataset: str, iceberg_table: str, process_date: date):
    """
    Builds the Crime Type Dimension table by extracting unique crime types
    from the Iceberg table and loading them into PostgreSQL with upsert logic.
    """
    spark = (
        SparkSession.builder
        .appName("BuildCrimeTypeDimension")
        .getOrCreate()
    )

    try:
        bq_client = _make_bq_client(gcp_project_id)
        table_name = f"{gcp_project_id}.{bq_dataset}.dim_crime_type"
        ensure_bq_table_columns(bq_client, table_name, list(_DIM_CRIME_TYPE_EXTRA_COLS))

        # Idempotency: reruns for the same process_date replace that day's slice.
        bq_client.query(
            f"""
            DELETE FROM `{table_name}`
            WHERE updated_date = DATE('{process_date}')
            """
        ).result()
        logger.info(f"Deleted existing dim_crime_type rows for process_date={process_date}")

        # 1. Read crime data from Iceberg source
        logger.info(f"Reading crime data from Iceberg table: {iceberg_table}")
        crime_df = spark.read.format("iceberg").load(iceberg_table)

        # 2. Apply the shared taxonomy once and build one row per canonical offense key.
        logger.info("Building crime type dimension from shared silver taxonomy...")
        crime_df = ensure_crime_taxonomy_columns(crime_df)
        crime_type_dim = (
            crime_df.filter(col("primary_type").isNotNull())
            .groupBy("crime_type_key")
            .agg(
                first("primary_type").alias("primary_type"),
                first("crime_primary_type_clean").alias("primary_type_clean"),
                first("description").alias("description"),
                first("crime_description_clean").alias("crime_description_clean"),
                first("iucr").alias("iucr"),
                first("crime_offense_label").alias("crime_offense_label"),
                first("crime_category").alias("crime_category"),
                first("crime_offense_group").alias("crime_offense_group"),
                first("crime_offense_subtype").alias("crime_offense_subtype"),
                first("crime_severity_level").alias("severity_level"),
                first("crime_fbi_classification").alias("fbi_classification"),
                first("crime_clearance_bucket").alias("typical_clearance_rate"),
                first("violent_crime_flag").alias("violent_crime_flag"),
                first("property_crime_flag").alias("property_crime_flag"),
                first("drug_crime_flag").alias("drug_crime_flag"),
                first("weapon_crime_flag").alias("weapon_crime_flag"),
            )
        )

        # 5. Add metadata and select final columns
        logger.info("Finalizing dimension table structure...")
        crime_type_final = crime_type_dim.select(
            col("crime_type_key"),
            col("crime_category"),
            col("crime_offense_group"),
            col("crime_offense_subtype"),
            col("crime_offense_label"),
            col("primary_type"),
            col("primary_type_clean"),
            col("description").alias("crime_description"),
            col("crime_description_clean"),
            col("iucr").alias("iucr_code"),
            col("severity_level"),
            col("fbi_classification"),
            col("typical_clearance_rate"),
            col("violent_crime_flag"),
            col("property_crime_flag"),
            col("drug_crime_flag"),
            col("weapon_crime_flag"),
            lit(process_date).cast("date").alias("created_date"),
            lit(process_date).cast("date").alias("updated_date"),
        )

        # 4. Dedupe by crime_type_key (one row per canonical offense)
        logger.info("Removing duplicate crime_type_key values...")
        window_spec = Window.partitionBy("crime_type_key").orderBy(
            desc("primary_type"), desc("iucr_code")
        )
        crime_type_final = crime_type_final.withColumn("row_num", row_number().over(window_spec))
        crime_type_final = crime_type_final.filter(col("row_num") == 1).drop("row_num")

        logger.info(f"After deduplication: {crime_type_final.count()} unique crime type records")

        # 7. Write dimension to BigQuery
        logger.info(f"Writing crime type dimension to BigQuery for process_date={process_date}")

        (
            crime_type_final.write
            .format("bigquery")
            .option("table", table_name)
            .option("parentProject", gcp_project_id)
            .option("credentials", bq_creds())
            .option("writeMethod", "direct")
            .mode("append")
            .save()
        )

        logger.info(f"Successfully wrote crime type dimension for {process_date} to {table_name}")

    finally:
        logger.info("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Crime Type Dimension Spark Job")
    parser.add_argument("--gcp_project_id", required=True, help="GCP project id for BigQuery")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset for gold tables")
    parser.add_argument("--iceberg_table", required=True, help="Name of the source Iceberg table")
    parser.add_argument("--process_date", type=parse_date, required=True, help="Processing date in YYYY-MM-DD format")

    args = parser.parse_args()

    main(
        gcp_project_id=args.gcp_project_id,
        bq_dataset=args.bq_dataset,
        iceberg_table=args.iceberg_table,
        process_date=args.process_date,
    )

