#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, hash, trim, upper, concat, row_number, desc
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


def main(gcp_project_id: str, bq_dataset: str, iceberg_table: str, process_date: date):
    """
    Builds the Location Dimension table by extracting unique locations
    from the Iceberg table and loading them into PostgreSQL with upsert logic.
    """
    spark = (
        SparkSession.builder
        .appName("BuildCrimeLocationDimension")
        .getOrCreate()
    )

    try:
        bq_client = _make_bq_client(gcp_project_id)
        table_name = f"{gcp_project_id}.{bq_dataset}.dim_location"

        # Idempotency: reruns for the same process_date replace that day's slice.
        bq_client.query(
            f"""
            DELETE FROM `{table_name}`
            WHERE updated_date = DATE('{process_date}')
            """
        ).result()
        logger.info(f"Deleted existing dim_location rows for process_date={process_date}")

        # 1. Read crime data from Iceberg source
        logger.info(f"Reading crime data from Iceberg table: {iceberg_table}")
        crime_df = spark.read.format("iceberg").load(iceberg_table)

        geo_cols = [
            "beat",
            "district",
            "ward",
            "community_area",
            "location_description",
            "block",
        ]
        for optional in ("district_name", "community_area_name"):
            if optional in crime_df.columns:
                geo_cols.append(optional)

        # 2. Extract unique location combinations to create the dimension
        location_dim = (
            crime_df.select(*geo_cols)
            .filter(col("beat").isNotNull() | col("district").isNotNull())
            .distinct()
        )

        # 3. Generate a unique location_key using a hash of all relevant fields
        # Including 'block' is crucial for uniqueness.
        logger.info("Generating unique location keys...")
        location_dim = location_dim.withColumn(
            "location_key",
            hash(
                coalesce(col("beat"), lit(0)),
                coalesce(col("district"), lit(0)),
                coalesce(col("ward"), lit(0)),
                coalesce(col("community_area"), lit(0)),
                coalesce(col("location_description"), lit("Unknown")),
                coalesce(col("block"), lit("Unknown"))
            ).cast("bigint")
        )

        # 4. Add derived columns for enrichment
        logger.info("Adding derived columns for enrichment...")

        # District display name: prefer Sedona/shapefile-enriched district_name from Silver;
        # fallback to generic label from district id only (no hardcoded district roster).
        if "district_name" in location_dim.columns:
            location_dim = location_dim.withColumn(
                "district_name",
                coalesce(
                    trim(col("district_name")),
                    concat(lit("District "), col("district").cast("string")),
                ),
            )
        else:
            location_dim = location_dim.withColumn(
                "district_name",
                concat(lit("District "), col("district").cast("string")),
            )

        # Location type classification
        location_dim = location_dim.withColumn(
            "location_type",
            when(col("location_description").rlike("(?i)(STREET|SIDEWALK|ALLEY|PARKING)"), "Public/Street")
            .when(col("location_description").rlike("(?i)(RESIDENCE|APARTMENT|HOUSE|HOME)"), "Residential")
            .when(col("location_description").rlike("(?i)(STORE|SHOP|RETAIL|RESTAURANT|BAR)"), "Commercial")
            .when(col("location_description").rlike("(?i)(SCHOOL|COLLEGE|UNIVERSITY)"), "Educational")
            .when(col("location_description").rlike("(?i)(HOSPITAL|MEDICAL)"), "Medical")
            .when(col("location_description").rlike("(?i)(PARK|BEACH|FOREST)"), "Recreation")
            .when(col("location_description").rlike("(?i)(GOVERNMENT|COURT|POLICE)"), "Government")
            .when(col("location_description").rlike("(?i)(CHURCH|TEMPLE|MOSQUE)"), "Religious")
            .otherwise("Other")
        )

        location_dim = location_dim.withColumn("location_description_clean", trim(upper(col("location_description"))))
        location_dim = location_dim.withColumn("block_address", trim(upper(col("block"))))

        location_dim = location_dim.withColumn(
            "location_category",
            when(col("location_type") == "Public/Street", "Public")
            .when(col("location_type") == "Residential", "Private")
            .when(col("location_type").isin(["Commercial", "Educational", "Medical"]), "Semi-Public")
            .otherwise("Other")
        )

        location_dim = location_dim.withColumn(
            "risk_level",
            when(col("location_type") == "Public/Street", "High")
            .when(col("location_type").isin(["Commercial", "Recreation"]), "Medium")
            .when(col("location_type").isin(["Residential", "Educational", "Medical", "Religious"]), "Low")
            .otherwise("Medium")
        )

        # 5. Add metadata and select final columns
        logger.info("Finalizing dimension table structure...")
        location_final = location_dim.select(
            col("location_key"),
            col("beat").cast("integer"),
            col("district").cast("integer"),
            col("district_name"),
            col("ward").cast("integer"),
            col("community_area").cast("integer"),
            col("location_type"),
            col("location_description_clean"),
            col("location_category"),
            col("risk_level"),
            col("block_address"),
            lit(process_date).cast("date").alias("created_date"),
            lit(process_date).cast("date").alias("updated_date")
        )

        # 6. CRITICAL FIX: Remove duplicates by location_key to prevent PostgreSQL constraint violation
        logger.info("Removing duplicate location_key values...")
        window_spec = Window.partitionBy("location_key").orderBy(desc("beat"), desc("district"), desc("ward"))
        location_final = location_final.withColumn("row_num", row_number().over(window_spec))
        location_final = location_final.filter(col("row_num") == 1).drop("row_num")

        logger.info(f"After deduplication: {location_final.count()} unique location records")

        # 7. Write dimension to BigQuery
        logger.info(f"Writing location dimension to BigQuery for process_date={process_date}")

        (
            location_final.write
            .format("bigquery")
            .option("table", table_name)
            .option("parentProject", gcp_project_id)
            .option("credentials", bq_creds())
            .option("writeMethod", "direct")
            .mode("append")
            .save()
        )

        logger.info(f"Successfully wrote location dimension for {process_date} to {table_name}")

    finally:
        logger.info("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Crime Location Dimension Spark Job")
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

