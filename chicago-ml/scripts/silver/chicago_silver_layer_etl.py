#!/usr/bin/env python3
import argparse
import logging
import sys
from datetime import date
from pathlib import Path

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import broadcast, col, desc, expr, lit, row_number
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from chicago_crime_scripts.crime_taxonomy import with_crime_taxonomy

logger = logging.getLogger(__name__)

ICEBERG_CATALOG = "chicago_crime"

SILVER_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("case_number", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("block", StringType(), True),
        StructField("iucr", StringType(), True),
        StructField("primary_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("location_description", StringType(), True),
        StructField("arrest", BooleanType(), True),
        StructField("domestic", BooleanType(), True),
        StructField("beat", StringType(), True),
        StructField("district", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("community_area", StringType(), True),
        StructField("community_area_name", StringType(), True),
        StructField("district_name", StringType(), True),
        StructField("fbi_code", StringType(), True),
        StructField("x_coordinate", DoubleType(), True),
        StructField("y_coordinate", DoubleType(), True),
        StructField("year", LongType(), True),
        StructField("updated_on", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("crime_type_key", LongType(), True),
        StructField("crime_primary_type_clean", StringType(), True),
        StructField("crime_description_clean", StringType(), True),
        StructField("crime_offense_label", StringType(), True),
        StructField("crime_category", StringType(), True),
        StructField("crime_offense_group", StringType(), True),
        StructField("crime_offense_subtype", StringType(), True),
        StructField("crime_severity_level", StringType(), True),
        StructField("crime_fbi_classification", StringType(), True),
        StructField("crime_clearance_bucket", StringType(), True),
        StructField("violent_crime_flag", LongType(), True),
        StructField("property_crime_flag", LongType(), True),
        StructField("drug_crime_flag", LongType(), True),
        StructField("weapon_crime_flag", LongType(), True),
        StructField("process_date", TimestampType(), True),
    ]
)


def parse_date(s: str) -> date:
    """Convert an ISO-8601 string (YYYY-MM-DD) into a datetime.date."""
    return date.fromisoformat(s)


def _pick_name_col(cols: list[str], id_col: str, geom_col: str) -> str | None:
    """
    Pick a human-readable label column from boundary Parquet (shapefile export).
    Skips id, geometry, and numeric code columns.
    """
    excluded_lower = {
        id_col.lower(),
        geom_col.lower(),
        "geometry",
        "geom",
        "objectid",
        "fid",
        "shape_area",
        "shape_leng",
        "perimeter",
        "area",
    }
    preferred = [
        "community_name",
        "community",
        "name",
        "label",
        "long_name",
        "place_name",
    ]
    cols_lower = {c.lower(): c for c in cols}
    for p in preferred:
        if p in cols_lower and cols_lower[p].lower() not in excluded_lower:
            return cols_lower[p]
    for c in cols:
        cl = c.lower()
        if cl in excluded_lower:
            continue
        if any(tok in cl for tok in ("name", "label", "title")):
            if any(tok in cl for tok in ("num", "number", "code", "id")) and "name" not in cl:
                continue
            return c
    return None


def _pick_col(cols: list[str], preferred: list[str], contains_any: list[str]) -> str | None:
    cols_lower = {c.lower(): c for c in cols}
    for p in preferred:
        if p.lower() in cols_lower:
            return cols_lower[p.lower()]
    for c in cols:
        c_low = c.lower()
        if any(tok in c_low for tok in contains_any):
            return c
    return None


def _ensure_silver_optional_columns(spark, table: str) -> None:
    """
    Add new columns to an existing Iceberg table when the deployed catalog
    predates schema changes (idempotent).
    """
    try:
        existing = set(spark.table(table).columns)
    except Exception:
        return
    for name, typ in (
        ("community_area_name", "STRING"),
        ("district_name", "STRING"),
        ("crime_type_key", "BIGINT"),
        ("crime_primary_type_clean", "STRING"),
        ("crime_description_clean", "STRING"),
        ("crime_offense_label", "STRING"),
        ("crime_category", "STRING"),
        ("crime_offense_group", "STRING"),
        ("crime_offense_subtype", "STRING"),
        ("crime_severity_level", "STRING"),
        ("crime_fbi_classification", "STRING"),
        ("crime_clearance_bucket", "STRING"),
        ("violent_crime_flag", "BIGINT"),
        ("property_crime_flag", "BIGINT"),
        ("drug_crime_flag", "BIGINT"),
        ("weapon_crime_flag", "BIGINT"),
    ):
        if name not in existing:
            try:
                spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
            except Exception as exc:
                logger.warning(
                    "Could not ALTER TABLE %s ADD COLUMN %s (%s); continuing.",
                    table,
                    name,
                    exc,
                )


def _geom_expr(df_cols: list[str], geom_col: str) -> str:
    """
    Return a Sedona SQL expression that converts a Parquet geometry column
    into a Geometry. Handles either WKB (binary) or WKT (string) encodings.
    """
    # We can't reliably inspect Spark types here without a DataFrame; so use
    # defensive SQL: try WKB first for binary-like columns, else WKT.
    # Users can override by providing a pre-converted 'geom' column upstream.
    # NOTE: In Sedona, ST_GeomFromWKB expects BINARY; ST_GeomFromWKT expects STRING.
    return (
        f"CASE WHEN typeof({geom_col}) = 'binary' "
        f"THEN ST_GeomFromWKB({geom_col}) ELSE ST_GeomFromWKT({geom_col}) END"
    )


def main(
    input_path: str,
    table: str,
    process_date: date,
    wards_path: str | None,
    community_areas_path: str | None,
    police_districts_path: str | None,
    police_beats_path: str | None,
):
    """
    Reads a parquet file from MinIO, cleans, validates, schema-enforces,
    deduplicates, and overwrites the Silver Iceberg partition for process_date.

    Idempotency: uses Iceberg dynamic partition overwrite so re-running for
    the same process_date replaces rather than appends.
    """
    spark = None
    try:
        spark = SparkSession.builder.appName("ChicagoCrimeSilverETL").getOrCreate()
        logger.info("Spark session initialized.")

        logger.info(f"Reading data from: {input_path}")
        df = spark.read.parquet(input_path)
        input_count = df.count()
        logger.info(f"Read {input_count} records from input file.")

        if input_count == 0:
            logger.warning("Input file is empty — skipping processing.")
            return

        # Normalise column names to lowercase
        df = df.toDF(*[c.lower() for c in df.columns])

        # Attach partition column and deterministically deduplicate on natural key.
        # We keep the latest record per id so reruns are stable.
        df = df.withColumn("process_date", lit(process_date).cast(TimestampType()))
        dedupe_window = Window.partitionBy("id").orderBy(
            desc("updated_on"),
            desc("date"),
            desc("case_number"),
        )
        df = (
            df.withColumn("_row_num", row_number().over(dedupe_window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

        df = (
            df.withColumn("community_area_name", lit(None).cast("string"))
            .withColumn("district_name", lit(None).cast("string"))
        )

        # Spatial enrichment (districts, beats, wards, community areas) via Sedona + boundary Parquet.
        # Geographic labels come only from the reference geometries, not hardcoded maps.
        if (
            wards_path
            or community_areas_path
            or police_districts_path
            or police_beats_path
        ):
            logger.info("Performing spatial enrichment using administrative boundaries.")

            # Crimes point geometry from lon/lat (WGS84)
            crimes = df
            crimes = crimes.withColumn(
                "_crime_pt",
                expr(
                    "ST_Point(CAST(longitude AS DECIMAL(24, 15)), "
                    "CAST(latitude AS DECIMAL(24, 15)))"
                ),
            )

            if police_districts_path:
                pd_raw = spark.read.parquet(police_districts_path)
                pd_raw = pd_raw.toDF(*[c.lower() for c in pd_raw.columns])
                dist_id_col = _pick_col(
                    pd_raw.columns,
                    preferred=[
                        "district",
                        "district_num",
                        "dist_num",
                        "dist",
                        "district_n",
                    ],
                    contains_any=["district", "dist"],
                )
                dist_geom_col = _pick_col(
                    pd_raw.columns,
                    preferred=["geometry", "geom"],
                    contains_any=["geom"],
                )
                if not dist_id_col or not dist_geom_col:
                    raise ValueError(
                        "Could not infer district id/geometry columns from police "
                        f"districts dataset. Columns={pd_raw.columns}"
                    )
                dist_name_col = _pick_name_col(
                    pd_raw.columns, dist_id_col, dist_geom_col
                )
                dist_cols = [
                    col(dist_id_col).cast("string").alias("_district_from_boundary"),
                ]
                if dist_name_col:
                    dist_cols.append(
                        col(dist_name_col).cast("string").alias(
                            "_district_name_from_boundary"
                        )
                    )
                else:
                    dist_cols.append(
                        lit(None).cast("string").alias("_district_name_from_boundary")
                    )
                dist_cols.append(
                    expr(_geom_expr(pd_raw.columns, dist_geom_col)).alias(
                        "_district_geom"
                    )
                )
                districts = pd_raw.select(*dist_cols)

                crimes = (
                    crimes.alias("c")
                    .join(
                        broadcast(districts).alias("d"),
                        expr("ST_Contains(d._district_geom, c._crime_pt)"),
                        how="left",
                    )
                    .withColumn(
                        "district",
                        expr(
                            "COALESCE(NULLIF(TRIM(c.district), ''), "
                            "d._district_from_boundary)"
                        ),
                    )
                    .withColumn(
                        "district_name",
                        expr(
                            "COALESCE(NULLIF(TRIM(c.district_name), ''), "
                            "d._district_name_from_boundary)"
                        ),
                    )
                    .drop(
                        "_district_geom",
                        "_district_from_boundary",
                        "_district_name_from_boundary",
                    )
                )

            if police_beats_path:
                beats_raw = spark.read.parquet(police_beats_path)
                beats_raw = beats_raw.toDF(*[c.lower() for c in beats_raw.columns])
                beat_id_col = _pick_col(
                    beats_raw.columns,
                    preferred=["beat", "beat_num", "beat_numbe"],
                    contains_any=["beat"],
                )
                beat_geom_col = _pick_col(
                    beats_raw.columns,
                    preferred=["geometry", "geom"],
                    contains_any=["geom"],
                )
                if not beat_id_col or not beat_geom_col:
                    raise ValueError(
                        "Could not infer beat id/geometry columns from police beats "
                        f"dataset. Columns={beats_raw.columns}"
                    )
                beat_sel = [
                    col(beat_id_col).cast("string").alias("_beat_from_boundary"),
                    expr(_geom_expr(beats_raw.columns, beat_geom_col)).alias(
                        "_beat_geom"
                    ),
                ]
                beats = beats_raw.select(*beat_sel)

                crimes = (
                    crimes.alias("c")
                    .join(
                        broadcast(beats).alias("b"),
                        expr("ST_Contains(b._beat_geom, c._crime_pt)"),
                        how="left",
                    )
                    .withColumn(
                        "beat",
                        expr(
                            "COALESCE(NULLIF(TRIM(c.beat), ''), b._beat_from_boundary)"
                        ),
                    )
                    .drop("_beat_geom", "_beat_from_boundary")
                )

            if wards_path:
                wards_raw = spark.read.parquet(wards_path)
                wards_raw = wards_raw.toDF(*[c.lower() for c in wards_raw.columns])
                ward_id_col = _pick_col(
                    wards_raw.columns, preferred=["ward"], contains_any=["ward"]
                )
                ward_geom_col = _pick_col(
                    wards_raw.columns,
                    preferred=["geometry", "geom"],
                    contains_any=["geom"],
                )
                if not ward_id_col or not ward_geom_col:
                    raise ValueError(
                        "Could not infer ward id/geometry columns from wards dataset. "
                        f"Columns={wards_raw.columns}"
                    )

                ward_cols = [
                    col(ward_id_col).cast("string").alias("_ward_from_boundary"),
                    expr(_geom_expr(wards_raw.columns, ward_geom_col)).alias(
                        "_ward_geom"
                    ),
                ]
                wards = wards_raw.select(*ward_cols)

                crimes = (
                    crimes.alias("c")
                    .join(
                        broadcast(wards).alias("w"),
                        expr("ST_Contains(w._ward_geom, c._crime_pt)"),
                        how="left",
                    )
                    .withColumn(
                        "ward",
                        expr(
                            "COALESCE(NULLIF(TRIM(c.ward), ''), w._ward_from_boundary)"
                        ),
                    )
                    .drop("_ward_geom", "_ward_from_boundary")
                )

            if community_areas_path:
                ca_raw = spark.read.parquet(community_areas_path)
                ca_raw = ca_raw.toDF(*[c.lower() for c in ca_raw.columns])
                ca_id_col = _pick_col(
                    ca_raw.columns,
                    preferred=[
                        "community_area",
                        "area_numbe",
                        "area_number",
                        "community",
                        "communityarea",
                    ],
                    contains_any=["community", "area_num", "area_number", "area_numbe"],
                )
                ca_geom_col = _pick_col(
                    ca_raw.columns,
                    preferred=["geometry", "geom"],
                    contains_any=["geom"],
                )
                if not ca_id_col or not ca_geom_col:
                    raise ValueError(
                        "Could not infer community area id/geometry columns from "
                        f"community areas dataset. Columns={ca_raw.columns}"
                    )

                ca_name_col = _pick_name_col(
                    ca_raw.columns, ca_id_col, ca_geom_col
                )
                ca_cols = [
                    col(ca_id_col).cast("string").alias("_community_area_from_boundary"),
                ]
                if ca_name_col:
                    ca_cols.append(
                        col(ca_name_col).cast("string").alias(
                            "_community_name_from_boundary"
                        )
                    )
                else:
                    ca_cols.append(
                        lit(None).cast("string").alias("_community_name_from_boundary")
                    )
                ca_cols.append(
                    expr(_geom_expr(ca_raw.columns, ca_geom_col)).alias(
                        "_community_geom"
                    )
                )
                cas = ca_raw.select(*ca_cols)

                crimes = (
                    crimes.alias("c")
                    .join(
                        broadcast(cas).alias("a"),
                        expr("ST_Contains(a._community_geom, c._crime_pt)"),
                        how="left",
                    )
                    .withColumn(
                        "community_area",
                        expr(
                            "COALESCE(NULLIF(TRIM(c.community_area), ''), "
                            "a._community_area_from_boundary)"
                        ),
                    )
                    .withColumn(
                        "community_area_name",
                        expr(
                            "COALESCE(NULLIF(TRIM(c.community_area_name), ''), "
                            "a._community_name_from_boundary)"
                        ),
                    )
                    .drop(
                        "_community_geom",
                        "_community_area_from_boundary",
                        "_community_name_from_boundary",
                    )
                )

            df = crimes.drop("_crime_pt")
            logger.info("Spatial enrichment completed.")

        dedup_count = df.count()
        logger.info(
            f"Records after deduplication: {dedup_count} "
            f"({input_count - dedup_count} duplicates removed)."
        )

        df = with_crime_taxonomy(df)

        # Enforce schema — explicit cast to every target column
        df_typed = df.select(
            col("id").cast("bigint"),
            col("case_number").cast("string"),
            col("date").cast("timestamp"),
            col("block").cast("string"),
            col("iucr").cast("string"),
            col("primary_type").cast("string"),
            col("description").cast("string"),
            col("location_description").cast("string"),
            col("arrest").cast("boolean"),
            col("domestic").cast("boolean"),
            col("beat").cast("string"),
            col("district").cast("string"),
            col("ward").cast("string"),
            col("community_area").cast("string"),
            col("community_area_name").cast("string"),
            col("district_name").cast("string"),
            col("fbi_code").cast("string"),
            col("x_coordinate").cast("double"),
            col("y_coordinate").cast("double"),
            col("year").cast("bigint"),
            col("updated_on").cast("timestamp"),
            col("latitude").cast("double"),
            col("longitude").cast("double"),
            col("location").cast("string"),
            col("crime_type_key").cast("bigint"),
            col("crime_primary_type_clean").cast("string"),
            col("crime_description_clean").cast("string"),
            col("crime_offense_label").cast("string"),
            col("crime_category").cast("string"),
            col("crime_offense_group").cast("string"),
            col("crime_offense_subtype").cast("string"),
            col("crime_severity_level").cast("string"),
            col("crime_fbi_classification").cast("string"),
            col("crime_clearance_bucket").cast("string"),
            col("violent_crime_flag").cast("bigint"),
            col("property_crime_flag").cast("bigint"),
            col("drug_crime_flag").cast("bigint"),
            col("weapon_crime_flag").cast("bigint"),
            col("process_date").cast("timestamp"),
        )

        # Create table if it doesn't exist yet
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGINT,
                case_number STRING,
                date TIMESTAMP,
                block STRING,
                iucr STRING,
                primary_type STRING,
                description STRING,
                location_description STRING,
                arrest BOOLEAN,
                domestic BOOLEAN,
                beat STRING,
                district STRING,
                ward STRING,
                community_area STRING,
                community_area_name STRING,
                district_name STRING,
                fbi_code STRING,
                x_coordinate DOUBLE,
                y_coordinate DOUBLE,
                year BIGINT,
                updated_on TIMESTAMP,
                latitude DOUBLE,
                longitude DOUBLE,
                location STRING,
                crime_type_key BIGINT,
                crime_primary_type_clean STRING,
                crime_description_clean STRING,
                crime_offense_label STRING,
                crime_category STRING,
                crime_offense_group STRING,
                crime_offense_subtype STRING,
                crime_severity_level STRING,
                crime_fbi_classification STRING,
                crime_clearance_bucket STRING,
                violent_crime_flag BIGINT,
                property_crime_flag BIGINT,
                drug_crime_flag BIGINT,
                weapon_crime_flag BIGINT,
                process_date TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (days(process_date))
        """
        )

        _ensure_silver_optional_columns(spark, table)

        logger.info(
            f"Writing {dedup_count} records to {table} for process_date={process_date}."
        )

        # Dynamic partition overwrite: replaces only the process_date partition,
        # making every run fully idempotent.
        (
            df_typed.write.format("iceberg")
            .option("overwrite-mode", "dynamic")
            .mode("overwrite")
            .save(table)
        )

        logger.info(f"Successfully wrote {dedup_count} records to {table}.")

    except Exception as e:
        logger.error(f"Silver ETL failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Run the Silver Layer ETL for Chicago Crime data."
    )
    parser.add_argument(
        "--input",
        dest="input_path",
        required=True,
        help="Path to input parquet file (s3a://...)",
    )
    parser.add_argument(
        "--table",
        dest="table",
        required=True,
        default="chicago_crime.chicago_crime_silver",
        help="Fully-qualified Iceberg table name.",
    )
    parser.add_argument(
        "--process_date",
        dest="process_date",
        type=parse_date,
        required=True,
        help="Date of the data being processed (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--wards_path",
        dest="wards_path",
        default=None,
        help=(
            "Optional: Parquet path to wards boundaries dataset "
            "(s3a://... or file://...)."
        ),
    )
    parser.add_argument(
        "--community_areas_path",
        dest="community_areas_path",
        default=None,
        help=(
            "Optional: Parquet path to community areas boundaries dataset "
            "(s3a://... or file://...)."
        ),
    )
    parser.add_argument(
        "--police_districts_path",
        dest="police_districts_path",
        default=None,
        help=(
            "Optional: Parquet path to Chicago police district boundaries (shapefile export) "
            "for Sedona ST_Contains enrichment of district + district_name."
        ),
    )
    parser.add_argument(
        "--police_beats_path",
        dest="police_beats_path",
        default=None,
        help=(
            "Optional: Parquet path to Chicago police beat boundaries for Sedona enrichment "
            "of beat (code from polygon join)."
        ),
    )

    args = parser.parse_args()
    logger.info(
        "Starting Chicago Crime Silver ETL — "
        f"input={args.input_path}, table={args.table}, process_date={args.process_date}"
    )
    main(
        args.input_path,
        args.table,
        args.process_date,
        args.wards_path,
        args.community_areas_path,
        args.police_districts_path,
        args.police_beats_path,
    )

