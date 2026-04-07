#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    BooleanType,
)
import argparse
import logging
from datetime import date, timedelta
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
    Builds the Date Dimension table by generating a comprehensive date range
    and loading them into PostgreSQL with upsert logic.
    """
    spark = (
        SparkSession.builder
        .appName("BuildCrimeDateDimension")
        .getOrCreate()
    )

    try:
        # 1. Generate date range (5 years back from process_date to 1 year forward)
        logger.info(f"Generating date dimension for process_date: {process_date}")
        
        start_date = process_date - timedelta(days=365*5)  # 5 years back
        end_date = process_date + timedelta(days=365)      # 1 year forward
        
        # Create date list
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append({
                'date': current_date,
                'date_key': int(current_date.strftime("%Y%m%d")),
                'year': current_date.year,
                'quarter': (current_date.month - 1) // 3 + 1,
                'month': current_date.month,
                'day': current_date.day,
                'day_of_week': current_date.weekday() + 1,  # 1=Monday, 7=Sunday
                'week_of_year': current_date.isocalendar()[1],
                'date_string': current_date.strftime("%Y-%m-%d"),
                'month_name': current_date.strftime("%B"),
                'day_name': current_date.strftime("%A"),
                'quarter_name': f"Q{(current_date.month - 1) // 3 + 1}",
                'is_weekend': current_date.weekday() >= 5,  # Saturday=5, Sunday=6
                'is_weekday': current_date.weekday() < 5,
                'season': get_season(current_date.month),
                'fiscal_year': current_date.year if current_date.month < 7 else current_date.year + 1,
                'created_date': process_date,
                'updated_date': process_date
            })
            current_date += timedelta(days=1)
        
        # 2. Create DataFrame from date list
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("week_of_year", IntegerType(), True),
            StructField("date_string", StringType(), True),
            StructField("month_name", StringType(), True),
            StructField("day_name", StringType(), True),
            StructField("quarter_name", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("is_weekday", BooleanType(), True),
            StructField("season", StringType(), True),
            StructField("fiscal_year", IntegerType(), True),
            StructField("created_date", DateType(), True),
            StructField("updated_date", DateType(), True)
        ])
        
        df = spark.createDataFrame(date_list, schema)

        # 3. Write full date dimension to BigQuery (overwrite)
        logger.info(f"Writing date dimension to BigQuery project={gcp_project_id}, dataset={bq_dataset}")

        table_name = f"{gcp_project_id}.{bq_dataset}.dim_date"
        (df.write
         .format("bigquery")
         .option("table", table_name)
         .option("parentProject", gcp_project_id)
         .option("credentials", bq_creds())
         .option("writeMethod", "direct")
         .mode("overwrite")
         .save())

        logger.info(f"Successfully wrote date dimension to {table_name}")

    finally:
        logger.info("Stopping Spark session.")
        spark.stop()

def get_season(month: int) -> str:
    """Determine season based on month."""
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:
        return "Fall"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Crime Date Dimension Spark Job")
    parser.add_argument("--gcp_project_id", required=True, help="GCP project id for BigQuery")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset for gold tables")
    parser.add_argument("--process_date", type=parse_date, required=True, help="Processing date in YYYY-MM-DD format")

    args = parser.parse_args()
    main(
        gcp_project_id=args.gcp_project_id,
        bq_dataset=args.bq_dataset,
        process_date=args.process_date,
    )

