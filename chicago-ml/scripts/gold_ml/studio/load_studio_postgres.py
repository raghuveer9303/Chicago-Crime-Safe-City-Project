#!/usr/bin/env python3
"""
Load AI Studio serving snapshot into Postgres (chicago_crime_analytics.studio_area_snapshot).

Reads all area-risk prediction rows for process_date (one row per area per prediction_date
in the batch — e.g. 11 days when scoring ds..ds+10), top-3 crime_category from
crime_ml_features over a lookback window, optional 7-day counts from
feature_crime_risk_area_daily joined on prediction_date, then JDBC append after deleting
rows for that process_date.
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count as spark_count,
    desc,
    lit,
    max as spark_max,
    row_number,
    when,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _db_creds_from_airflow_sqlalchemy_conn() -> tuple[str, str]:
    conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    if not conn:
        return "", ""
    parsed = urlparse(conn)
    return parsed.username or "", parsed.password or ""


def _ensure_postgres_schema(spark, jdbc_url: str, user: str, password: str) -> None:
    """Ensure new columns exist on studio_area_snapshot (idempotent, ADD COLUMN IF NOT EXISTS)."""
    jvm = spark._jvm
    cl = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    driver_cls = jvm.java.lang.Class.forName("org.postgresql.Driver", True, cl)
    driver = driver_cls.newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    conn = driver.connect(jdbc_url, props)
    if conn is None:
        raise RuntimeError("PostgreSQL driver returned null for schema check connection")
    try:
        st = conn.createStatement()
        for col_def in [
            "predicted_count_low DOUBLE PRECISION",
            "predicted_count_high DOUBLE PRECISION",
        ]:
            try:
                st.executeUpdate(
                    f"ALTER TABLE studio_area_snapshot ADD COLUMN IF NOT EXISTS {col_def}"
                )
                logger.info("Ensured column: %s", col_def.split()[0])
            except Exception as exc:
                logger.warning("Could not add column %s: %s", col_def.split()[0], exc)
        st.close()
    finally:
        conn.close()


def _jdbc_delete(spark, jdbc_url: str, user: str, password: str, process_date: date) -> None:
    """Delete existing rows for this batch (idempotent reload)."""
    # postgresql.jar is on Spark's classloader (--jars). DriverManager.getConnection
    # often still fails to resolve the driver from PySpark; use Driver.connect on an
    # instance loaded with Spark's classloader instead.
    jvm = spark._jvm
    cl = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    driver_cls = jvm.java.lang.Class.forName("org.postgresql.Driver", True, cl)
    driver = driver_cls.newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    conn = driver.connect(jdbc_url, props)
    if conn is None:
        raise RuntimeError("PostgreSQL JDBC driver rejected URL (driver.connect returned null)")
    try:
        st = conn.createStatement()
        sql = f"DELETE FROM studio_area_snapshot WHERE process_date = DATE '{process_date.isoformat()}'"
        n = st.executeUpdate(sql)
        st.close()
        logger.info("JDBC delete for process_date=%s affected rows (reported): %s", process_date, n)
    finally:
        conn.close()


def main(
    process_date: date,
    predictions_table: str,
    ml_features_table: str,
    feature_daily_table: str,
    community_areas_path: str | None,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    lookback_days: int = 90,
) -> None:
    spark = SparkSession.builder.appName("LoadStudioPostgres").getOrCreate()
    try:
        pred = (
            spark.read.format("iceberg")
            .load(predictions_table)
            .filter(col("process_date") == lit(process_date))
            .filter(col("area_type") == lit("community_area"))
        )
        if "safety_score" not in pred.columns:
            pred = pred.withColumn("safety_score", lit(None).cast("double"))
        n = pred.count()
        if n == 0:
            logger.warning("No prediction rows for process_date=%s; skipping studio load.", process_date)
            return

        start_ml = process_date - timedelta(days=lookback_days)
        ml = (
            spark.read.format("iceberg")
            .load(ml_features_table)
            .filter(col("process_date") >= lit(start_ml))
            .filter(col("process_date") <= lit(process_date))
            .filter(col("community_area") > 0)
        )
        cat_counts = ml.groupBy("community_area", "crime_category").agg(spark_count("*").alias("cnt"))
        rw = Window.partitionBy("community_area").orderBy(desc("cnt"))
        ranked = cat_counts.withColumn("rn", row_number().over(rw)).filter(col("rn") <= 3)
        top_pivot = ranked.groupBy("community_area").agg(
            spark_max(when(col("rn") == 1, col("crime_category"))).alias("top_crime_1"),
            spark_max(when(col("rn") == 2, col("crime_category"))).alias("top_crime_2"),
            spark_max(when(col("rn") == 3, col("crime_category"))).alias("top_crime_3"),
        )

        feat = (
            spark.read.format("iceberg")
            .load(feature_daily_table)
            .filter(col("process_date") == lit(process_date))
            .filter(col("area_type") == lit("community_area"))
            .select(
                col("area_id"),
                col("date").alias("feature_date"),
                col("count_7d"),
                col("violent_count_7d"),
            )
        )

        j1 = pred.join(
            top_pivot,
            pred.area_id == top_pivot.community_area,
            "left",
        ).drop("community_area")

        feat_keys = feat.select(
            col("area_id").alias("_fa_id"),
            col("count_7d"),
            col("violent_count_7d"),
        )
        joined = j1.join(
            feat_keys,
            j1.area_id == col("_fa_id"),
            "left",
        ).drop("_fa_id")

        # Canonical community-area names (so `studio_area_snapshot.area_name` is not NULL).
        if community_areas_path:
            ref = spark.read.parquet(community_areas_path)
            # This parquet sometimes comes from different upstream shapes.
            # Support both canonical and raw community-areas columns.
            id_candidates = [
                "area_id",
                "community_area",
                "community_area_id",
                "id",
                "area_numbe",
                "area_num_1",
            ]
            name_candidates = [
                "area_name",
                "name",
                "community_area_name",
                "community_area",
                "community",
                "area",
            ]

            ref_id_col = next((c for c in id_candidates if c in ref.columns), None)
            ref_name_col = next((c for c in name_candidates if c in ref.columns), None)

            joined_ref_name = False
            if ref_id_col and ref_name_col:
                logger.info(
                    "Joining community-area names: id_col=%s name_col=%s from %s",
                    ref_id_col,
                    ref_name_col,
                    community_areas_path,
                )
                ref_sel = ref.select(
                    col(ref_id_col).cast("double").cast("int").alias("_ref_area_id"),
                    col(ref_name_col).cast("string").alias("_ref_area_name"),
                )
                joined = (
                    joined
                    .join(ref_sel, joined.area_id == col("_ref_area_id"), "left")
                    .drop("_ref_area_id")
                )
                joined_ref_name = True
            else:
                logger.warning(
                    "Could not populate area_name from %s (missing expected columns; found columns=%s)",
                    community_areas_path,
                    ref.columns,
                )

            # Keep schema stable: later `out = joined.select(...)` references `_ref_area_name`.
            # If we couldn't join reference names, set it to NULL explicitly.
            if not joined_ref_name:
                joined = joined.withColumn("_ref_area_name", lit(None).cast("string"))

        joined = joined.withColumn(
            "safety_score",
            coalesce(col("safety_score"), lit(0.0)),
        )

        out = joined.select(
            col("area_type"),
            col("area_id"),
            coalesce(col("_ref_area_name"), lit(None).cast("string")).alias("area_name"),
            col("predicted_count").cast("double"),
            coalesce(col("predicted_count_low"), lit(0.0)).cast("double").alias("predicted_count_low"),
            coalesce(col("predicted_count_high"), col("predicted_count")).cast("double").alias("predicted_count_high"),
            col("safety_score").cast("double"),
            col("top_crime_1"),
            col("top_crime_2"),
            col("top_crime_3"),
            col("count_7d").cast("int"),
            col("violent_count_7d").cast("int"),
            col("prediction_date"),
            col("target_date"),
            col("process_date"),
            col("model_name"),
            col("model_version"),
            col("run_id"),
            col("created_at").alias("source_prediction_ts"),
        )

        _ensure_postgres_schema(spark, jdbc_url, jdbc_user, jdbc_password)
        _jdbc_delete(spark, jdbc_url, jdbc_user, jdbc_password, process_date)

        props = {
            "user": jdbc_user,
            "password": jdbc_password,
            "driver": "org.postgresql.Driver",
            "batchsize": "500",
        }
        out.write.jdbc(
            url=jdbc_url,
            table="studio_area_snapshot",
            mode="append",
            properties=props,
        )
        logger.info("Wrote %d rows to studio_area_snapshot for process_date=%s", out.count(), process_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    p = argparse.ArgumentParser(description="Load studio_area_snapshot from Iceberg to Postgres")
    p.add_argument("--process_date", type=parse_date, required=True)
    p.add_argument("--predictions_table", default="chicago_crime_gold.fact_area_risk_predictions")
    p.add_argument("--ml_features_table", default="chicago_crime_gold.crime_ml_features")
    p.add_argument("--feature_daily_table", default="chicago_crime_gold.feature_crime_risk_area_daily")
    p.add_argument(
        "--community_areas_path",
        default=None,
        help="Optional Parquet path with canonical community-area names (must include area/community_area id + name columns).",
    )
    p.add_argument(
        "--jdbc_url",
        default=None,
        help="jdbc:postgresql://host:5432/chicago_crime_analytics",
    )
    p.add_argument("--jdbc_user", default=None)
    p.add_argument("--jdbc_password", default=None)
    p.add_argument("--lookback_days", type=int, default=90)
    args = p.parse_args()

    def _default_jdbc() -> str:
        if os.getenv("CHICAGO_ANALYTICS_JDBC_URL"):
            return os.environ["CHICAGO_ANALYTICS_JDBC_URL"]
        h = os.getenv("CHICAGO_ANALYTICS_DB_HOST", "postgres_db")
        p = os.getenv("CHICAGO_ANALYTICS_DB_PORT", "5432")
        db = os.getenv("CHICAGO_ANALYTICS_DB_NAME", "carvana_db")
        return f"jdbc:postgresql://{h}:{p}/{db}"

    airflow_db_user, airflow_db_password = _db_creds_from_airflow_sqlalchemy_conn()
    jdbc_url = args.jdbc_url or _default_jdbc()
    jdbc_user = args.jdbc_user or os.getenv("CHICAGO_ANALYTICS_DB_USER", airflow_db_user or "admin")
    jdbc_password = args.jdbc_password or os.getenv("CHICAGO_ANALYTICS_DB_PASSWORD", airflow_db_password)
    if not jdbc_password:
        raise ValueError(
            "Missing JDBC password: set CHICAGO_ANALYTICS_DB_PASSWORD, "
            "or provide AIRFLOW__DATABASE__SQL_ALCHEMY_CONN, or pass --jdbc_password."
        )

    main(
        process_date=args.process_date,
        predictions_table=args.predictions_table,
        ml_features_table=args.ml_features_table,
        feature_daily_table=args.feature_daily_table,
        community_areas_path=args.community_areas_path,
        jdbc_url=jdbc_url,
        jdbc_user=jdbc_user,
        jdbc_password=jdbc_password,
        lookback_days=args.lookback_days,
    )
