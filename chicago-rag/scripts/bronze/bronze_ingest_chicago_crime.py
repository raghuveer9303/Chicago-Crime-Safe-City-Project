import argparse
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

import boto3
import pandas as pd
from sodapy import Socrata

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Socrata API Configuration ---
SOCRATA_API_KEY_ID = os.getenv("SOCRATA_API_KEY_ID", "")
SOCRATA_API_KEY_SECRET = os.getenv("SOCRATA_API_KEY_SECRET", "")
DATASET_ID = "ijzp-q8t2"
SOCRATA_DOMAIN = "data.cityofchicago.org"

# --- MinIO Configuration ---
# Read at call time (not module import time) so that values set via
# os.environ inside the Airflow task callable are visible.
MINIO_ENDPOINT = "http://minio:9000"  # default; overridden at call time


@dataclass(frozen=True)
class _S3Location:
    bucket: str
    key: str


def _parse_s3_location(output_path: str) -> _S3Location:
    """
    Parse an S3-style URL to bucket + key.

    Supports both `s3://bucket/key` and `s3a://bucket/key`.
    """
    parsed = urlparse(output_path)
    if parsed.scheme not in {"s3", "s3a"}:
        raise ValueError(f"Unsupported output scheme '{parsed.scheme}' in '{output_path}'. Expected s3:// or s3a://")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URL '{output_path}'. Expected s3a://<bucket>/<key>")
    return _S3Location(bucket=bucket, key=key)


def _dedupe_bronze_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deterministically deduplicate by Socrata record id.

    Keeping the latest `updated_on` row per `id` makes reruns stable and
    idempotent, even if the source briefly serves duplicate ids.
    """
    if df.empty:
        return df

    if "id" not in df.columns:
        logger.warning("Source payload has no 'id' column; skipping bronze dedupe.")
        return df

    before = len(df)
    work = df.copy()
    work["id"] = work["id"].astype(str)
    if "updated_on" in work.columns:
        work["_updated_on_ts"] = pd.to_datetime(work["updated_on"], errors="coerce")
    else:
        work["_updated_on_ts"] = pd.NaT

    work = work.sort_values(by=["id", "_updated_on_ts"], ascending=[True, False], kind="stable")
    work = work.drop_duplicates(subset=["id"], keep="first").drop(columns=["_updated_on_ts"])

    removed = before - len(work)
    if removed > 0:
        logger.info(f"Bronze dedupe removed {removed} duplicate records by id.")
    return work


def fetch_data_for_date(date_str: str) -> pd.DataFrame:
    """
    Fetches Chicago crime data for a specific date in a single API call.
    Authenticates using the API Key ID and Secret.
    """
    try:
        # Authenticate using username (API Key ID) and password (API Key Secret)
        client = Socrata(
            SOCRATA_DOMAIN,
            None,  # App token is set to None as we are using Basic Auth
            username=SOCRATA_API_KEY_ID,
            password=SOCRATA_API_KEY_SECRET,
        )

        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        start_of_day = target_date.strftime("%Y-%m-%dT00:00:00.000")
        end_of_day = target_date.strftime("%Y-%m-%dT23:59:59.999")

        where_clause = f"date between '{start_of_day}' and '{end_of_day}'"

        logger.info(f"Fetching data for {date_str} with query: {where_clause}")

        logger.info("Fetching daily records in a single request")
        records = client.get(
            DATASET_ID,
            where=where_clause,
            # For this pipeline daily volume (~400-500) is well below this cap.
            limit=50000,
            order="date,id",
        )

        if records:
            df = pd.DataFrame.from_records(records)
            df = _dedupe_bronze_records(df)
            logger.info(f"Successfully fetched a total of {len(df)} records for {date_str}.")
            return df
        else:
            logger.warning(f"No data found for {date_str}. An empty DataFrame will be used.")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Failed to fetch data for {date_str}: {e}")
        raise


def save_to_minio_as_parquet(df: pd.DataFrame, output_path: str):
    """
    Save a DataFrame to MinIO as Parquet, overwriting if it exists.

    Important: we intentionally do NOT use `pandas -> fsspec -> s3fs` here.
    Airflow constraints can pin `aiobotocore` versions that are incompatible with
    certain `s3fs` releases, which breaks `to_parquet("s3a://...")`.

    Instead we write locally and upload via boto3 to MinIO (S3-compatible).
    """
    try:
        loc = _parse_s3_location(output_path)

        endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "")
        secret_key = os.getenv("MINIO_SECRET_KEY", "")

        if not access_key or not secret_key:
            raise RuntimeError("Missing MINIO_ACCESS_KEY / MINIO_SECRET_KEY in environment for MinIO upload.")

        logger.info("Saving %s records to local parquet then uploading to %s", len(df), output_path)

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        # Write to a local temp file first (atomic upload; avoids multipart edge cases in fsspec).
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "data.parquet")
            df.to_parquet(local_path, index=False, engine="pyarrow")

            # Upload with overwrite semantics.
            extra_args = {"ContentType": "application/octet-stream"}
            s3.upload_file(local_path, loc.bucket, loc.key, ExtraArgs=extra_args)

        logger.info("Successfully uploaded parquet to MinIO at %s", output_path)
    except Exception as e:
        logger.error(f"Failed to save data to MinIO at {output_path}: {e}")
        raise


def run_etl(date_str: str, output_path: str) -> int:
    """
    Main ETL function to fetch data and save it to MinIO.

    Returns:
        Number of records written. 0 means no data was available for date_str
        and nothing was written — the caller should raise AirflowSkipException.
    """
    logger.info(f"Starting ETL for date: {date_str}, output path: {output_path}")
    data_df = fetch_data_for_date(date_str)

    if data_df.empty:
        logger.warning(f"No records returned from the API for {date_str}. Skipping write.")
        return 0

    logger.info(f"Fetched {len(data_df)} records for {date_str}. Writing to MinIO.")
    save_to_minio_as_parquet(data_df, output_path)
    logger.info(f"ETL complete — {len(data_df)} records written to {output_path}.")
    return len(data_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Chicago crime data and load to MinIO.")
    parser.add_argument("--date", required=True, help="The date to fetch data for (YYYY-MM-DD).")
    parser.add_argument("--bucket", required=True, help="The MinIO bucket name (e.g., chicago-crime-data).")
    args = parser.parse_args()

    dt = datetime.strptime(args.date, "%Y-%m-%d")
    s3_output_path = (
        f"s3a://{args.bucket}/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"crime_data_{args.date.replace('-', '')}.parquet"
    )

    run_etl(args.date, s3_output_path)

