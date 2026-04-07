#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import *
import argparse
import logging
from datetime import date
import base64
import os


def bq_creds() -> str:
    raw = os.getenv("GCP_BQ_SERVICE_ACCOUNT_JSON", "")
    return base64.b64encode(raw.encode()).decode() if raw else ""


def parse_date(s: str) -> date:
    return date.fromisoformat(s)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(gcp_project_id: str, bq_dataset: str, process_date: date):
    """
    Builds the Time of Day Dimension table by generating 24-hour time periods
    and loading them into PostgreSQL with upsert logic.
    """
    spark = (
        SparkSession.builder
        .appName("BuildCrimeTimeOfDayDimension")
        .getOrCreate()
    )

    try:
        # 1. Generate 24-hour time periods
        logger.info(f"Generating time of day dimension for process_date: {process_date}")

        time_list = []
        for hour in range(24):
            time_list.append({
                "time_of_day_key": hour,
                "hour": hour,
                "time_period": get_time_period(hour),
                "business_period": get_business_period(hour),
                "police_shift": get_police_shift(hour),
                "activity_level": get_activity_level(hour),
                "hour_display": f"{hour:02d}:00",
                "hour_24_format": f"{hour:02d}:00",
                "created_date": process_date,
                "updated_date": process_date,
            })

        # 2. Create DataFrame from time list
        schema = StructType([
            StructField("time_of_day_key", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("time_period", StringType(), True),
            StructField("business_period", StringType(), True),
            StructField("police_shift", StringType(), True),
            StructField("activity_level", StringType(), True),
            StructField("hour_display", StringType(), True),
            StructField("hour_24_format", StringType(), True),
            StructField("created_date", DateType(), True),
            StructField("updated_date", DateType(), True),
        ])

        df = spark.createDataFrame(time_list, schema)

        # 3. Write dimension to BigQuery
        logger.info(f"Writing time of day dimension to BigQuery for process_date={process_date}")

        table_name = f"{gcp_project_id}.{bq_dataset}.dim_time_of_day"
        (
            df.write
            .format("bigquery")
            .option("table", table_name)
            .option("parentProject", gcp_project_id)
            .option("credentials", bq_creds())
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save()
        )

        logger.info(f"Successfully wrote time of day dimension for {process_date} to {table_name}")

    finally:
        logger.info("Stopping Spark session.")
        spark.stop()


def get_time_period(hour: int) -> str:
    """Determine time period based on hour."""
    if 5 <= hour <= 11:
        return "Morning"
    elif 12 <= hour <= 17:
        return "Afternoon"
    elif 18 <= hour <= 22:
        return "Evening"
    else:
        return "Night"


def get_business_period(hour: int) -> str:
    """Determine business period based on hour."""
    if 9 <= hour <= 17:
        return "Business Hours"
    elif 18 <= hour <= 22:
        return "Evening Hours"
    elif 23 <= hour or hour <= 5:
        return "Late Night"
    else:
        return "Early Morning"


def get_police_shift(hour: int) -> str:
    """Determine police shift based on hour."""
    if 6 <= hour <= 14:
        return "Day Shift"
    elif 15 <= hour <= 23:
        return "Evening Shift"
    else:
        return "Night Shift"


def get_activity_level(hour: int) -> str:
    """Determine activity level based on hour."""
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        return "High"
    elif 10 <= hour <= 16 or 20 <= hour <= 22:
        return "Medium"
    elif 23 <= hour or hour <= 6:
        return "Low"
    else:
        return "Moderate"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Crime Time of Day Dimension Spark Job")
    parser.add_argument("--gcp_project_id", required=True, help="GCP project id for BigQuery")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset for gold tables")
    parser.add_argument("--process_date", type=parse_date, required=True, help="Processing date in YYYY-MM-DD format")

    args = parser.parse_args()

    main(
        gcp_project_id=args.gcp_project_id,
        bq_dataset=args.bq_dataset,
        process_date=args.process_date,
    )

