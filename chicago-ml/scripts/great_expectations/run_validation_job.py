"""
Spark-submit entrypoint for Great Expectations-style validations.

Why this exists:
- Airflow workers typically do not have a correctly configured SparkSession for
  Iceberg + MinIO (S3A) access.
- Running validations as Spark jobs guarantees access to the same catalog/S3A
  configuration used by the ETL jobs that produced the data.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict


def _bootstrap_local_imports() -> None:
    """Make local DAG packages importable when launched via spark-submit."""
    dags_root = Path(__file__).resolve().parents[2]
    dags_root_str = str(dags_root)
    if dags_root_str not in sys.path:
        sys.path.insert(0, dags_root_str)


_bootstrap_local_imports()


def _maybe_persist_result(
    result: Dict[str, Any],
    layer: str,
    process_date: str,
    idempotency_key: str = "",
) -> None:
    """
    Best-effort persistence of results to MinIO.
    Never fails the validation job if persistence is unavailable.
    """

    try:
        from great_expectations.utils.reporting import ValidationReporter

        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "")
        secret_key = os.getenv("MINIO_SECRET_KEY", "")
        bucket = os.getenv("CHICAGO_BUCKET", "chicago-crime-data")

        reporter = ValidationReporter(
            minio_endpoint=endpoint,
            minio_access_key=access_key,
            minio_secret_key=secret_key,
        )
        reporter.write_result_to_s3(
            result,
            layer=layer,
            process_date=process_date,
            bucket=bucket,
            idempotency_key=idempotency_key,
        )
    except Exception:
        # Intentionally swallow all persistence errors.
        return


def _exit_code_from_result(result: Dict[str, Any], fail_on_error: bool) -> int:
    if not fail_on_error:
        return 0
    checks_failed = int(result.get("checks_failed", 0) or 0)
    return 1 if checks_failed > 0 else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GX validations as a Spark job.")
    parser.add_argument(
        "--validation",
        required=True,
        choices=[
            "bronze_parquet",
            "silver_iceberg",
            "gold_bi",
            "gold_ml_features",
            "ml_predictions",
        ],
    )
    parser.add_argument("--process_date", required=True)

    # bronze_parquet
    parser.add_argument("--input_path", default="")

    # table-based validations
    parser.add_argument("--table_name", default="")

    # gold_bi
    parser.add_argument("--fact_table_name", default="")
    parser.add_argument("--dim_tables_json", default="{}")

    parser.add_argument(
        "--fail_on_error",
        action="store_true",
        help="Exit non-zero when validation has failed checks.",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Best-effort write result JSON to MinIO (gx-results/...).",
    )
    parser.add_argument(
        "--idempotency_key",
        default="",
        help=(
            "Optional stable key for persistence naming. "
            "When set, retries overwrite the same result object."
        ),
    )

    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"gx_validation_{args.validation}").getOrCreate()

    if args.validation == "bronze_parquet":
        if not args.input_path:
            raise SystemExit("--input_path is required for bronze_parquet")
        from great_expectations.expectations.bronze import validate_bronze_parquet

        result = validate_bronze_parquet(input_path=args.input_path, process_date=args.process_date)
        layer = "bronze"

    elif args.validation == "silver_iceberg":
        if not args.table_name:
            raise SystemExit("--table_name is required for silver_iceberg")
        from great_expectations.expectations.silver import validate_silver_iceberg

        result = validate_silver_iceberg(
            table_name=args.table_name,
            process_date=args.process_date,
            spark_session=spark,
        )
        layer = "silver"

    elif args.validation == "gold_bi":
        if not args.fact_table_name:
            raise SystemExit("--fact_table_name is required for gold_bi")
        from great_expectations.expectations.gold import validate_gold_bi

        dim_tables = json.loads(args.dim_tables_json or "{}")
        result = validate_gold_bi(
            fact_table_name=args.fact_table_name,
            dim_tables=dim_tables,
            process_date=args.process_date,
            spark_session=spark,
        )
        layer = "gold"

    elif args.validation == "gold_ml_features":
        if not args.table_name:
            raise SystemExit("--table_name is required for gold_ml_features")
        from great_expectations.expectations.gold import validate_gold_ml_features

        result = validate_gold_ml_features(
            table_name=args.table_name,
            process_date=args.process_date,
            spark_session=spark,
        )
        layer = "gold_ml"

    elif args.validation == "ml_predictions":
        if not args.table_name:
            raise SystemExit("--table_name is required for ml_predictions")
        from great_expectations.expectations.ml import validate_ml_predictions

        result = validate_ml_predictions(
            table_name=args.table_name,
            process_date=args.process_date,
            spark_session=spark,
        )
        layer = "ml"

    else:
        raise SystemExit(f"Unknown validation: {args.validation}")

    print(json.dumps(result, indent=2, default=str))

    if args.persist:
        _maybe_persist_result(
            result,
            layer=layer,
            process_date=args.process_date,
            idempotency_key=args.idempotency_key,
        )

    return _exit_code_from_result(result, fail_on_error=args.fail_on_error)


if __name__ == "__main__":
    sys.exit(main())

