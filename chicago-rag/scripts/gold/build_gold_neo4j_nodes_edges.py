import argparse
import sys
from datetime import date
from pathlib import Path

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    lit,
    trim,
)

from chicago_crime_scripts.crime_taxonomy import ensure_crime_taxonomy_columns


def _ensure_silver_columns(df):
    """Backfill optional silver columns when re-running against an older Iceberg snapshot."""
    for name in (
        "district_name",
        "community_area_name",
        "beat",
        "district",
        "ward",
        "community_area",
    ):
        if name not in df.columns:
            df = df.withColumn(name, lit(None).cast("string"))
    return df


def _location_text_key_col():
    """Text-only identity for Location nodes (names + street context; no hash)."""
    def nz(c):
        return coalesce(trim(c), lit("Unknown"))

    return concat_ws(
        "|",
        nz(col("community_area_name")),
        nz(col("district_name")),
        nz(col("block")),
        nz(col("location_description")),
    )


def load_data_to_neo4j(iceberg_table, neo4j_uri, neo4j_user, neo4j_password, process_date_str):
    """
    Loads data from the Silver Iceberg table into Neo4j. Idempotent — safe to re-run.

    All node writes use mode("Append") with node.keys, which the Neo4j Spark connector
    translates to MERGE on the key properties. All relationship writes use
    relationship.save.strategy=keys, which MERGEs on (source key, type, target key).
    Re-running for the same process_date will update existing nodes/relationships in place
    without creating duplicates.

    Node keys:
      Crime       → case_number
      Date        → date_label (ISO text, e.g. "2025-06-05")
      Location    → location_text_key (community_area|district|block|description)
      CrimeType   → crime_type_key
    """
    spark = SparkSession.builder.appName("Iceberg to Neo4j Loader").getOrCreate()

    process_date = date.fromisoformat(process_date_str)
    process_date_key = process_date.strftime("%Y%m%d")

    silver_df = (
        spark.read.format("iceberg")
        .load(iceberg_table)
        .filter(date_format(col("date"), "yyyyMMdd") == lit(process_date_key))
    )
    silver_df = ensure_crime_taxonomy_columns(_ensure_silver_columns(silver_df))
    silver_df = silver_df.withColumn("location_text_key", _location_text_key_col())

    # One Date node per daily run. Past dates were already MERGEd by their own runs.
    dim_date = spark.createDataFrame(
        [(
            process_date.isoformat(),
            process_date.strftime("%Y%m%d"),
            process_date,
            process_date.weekday(),
            process_date.month,
            process_date.year,
        )],
        ["date_label", "yyyymmdd_key", "full_date", "day_of_week", "month", "year"],
    )

    dim_location = (
        silver_df.select(
            "location_text_key",
            "latitude",
            "longitude",
            "location_description",
            "block",
            col("beat").alias("beat_id"),
            col("district").alias("district_id"),
            col("ward").alias("ward_id"),
            col("community_area").alias("community_area_id"),
            "district_name",
            "community_area_name",
        )
        .distinct()
        .dropDuplicates(["location_text_key"])
    )

    dim_crime_type = (
        silver_df.select(
            "crime_type_key",
            "crime_offense_label",
            "crime_category",
            "crime_offense_group",
            "crime_offense_subtype",
            "violent_crime_flag",
            "property_crime_flag",
            "drug_crime_flag",
            "weapon_crime_flag",
        )
        .distinct()
        .dropDuplicates(["crime_type_key"])
    )

    fact_crime_incidents = (
        silver_df.select(
            "case_number",
            "primary_type",
            "description",
            "arrest",
            "domestic",
            "date",
            "crime_type_key",
            "crime_offense_label",
            "crime_category",
            "crime_offense_group",
            "crime_offense_subtype",
            "location_text_key",
            col("beat").alias("beat_id"),
            col("district").alias("district_id"),
            col("ward").alias("ward_id"),
            col("community_area").alias("community_area_id"),
            "district_name",
            "community_area_name",
        )
        .withColumn("date_label", date_format(col("date"), "yyyy-MM-dd"))
        .dropDuplicates(["case_number"])
    )

    neo4j_options = {
        "url": neo4j_uri,
        "authentication.type": "basic",
        "authentication.basic.username": neo4j_user,
        "authentication.basic.password": neo4j_password,
    }

    # --- Nodes ---
    fact_crime_incidents.select(
        "case_number",
        "primary_type",
        "description",
        "crime_category",
        "crime_offense_group",
        "crime_offense_subtype",
        "arrest",
        "domestic",
        "beat_id",
        "district_id",
        "district_name",
        "ward_id",
        "community_area_id",
        "community_area_name",
    ).write.format("org.neo4j.spark.DataSource").options(**neo4j_options).option(
        "labels", "Crime"
    ).option("node.keys", "case_number").mode("Overwrite").save()

    dim_date.select(
        "date_label",
        "yyyymmdd_key",
        "full_date",
        "day_of_week",
        "month",
        "year",
    ).write.format("org.neo4j.spark.DataSource").options(**neo4j_options).option(
        "labels", "Date"
    ).option("node.keys", "date_label").mode("Overwrite").save()

    dim_location.select(
        "location_text_key",
        "latitude",
        "longitude",
        "location_description",
        "block",
        "beat_id",
        "district_id",
        "district_name",
        "ward_id",
        "community_area_id",
        "community_area_name",
    ).write.format("org.neo4j.spark.DataSource").options(**neo4j_options).option(
        "labels", "Location"
    ).option("node.keys", "location_text_key").mode("Overwrite").save()

    dim_crime_type.write.format("org.neo4j.spark.DataSource").options(**neo4j_options).option(
        "labels", "CrimeType"
    ).option("node.keys", "crime_type_key").mode("Overwrite").save()

    # --- Relationships (text keys only) ---
    fact_crime_incidents.select("case_number", "date_label").dropDuplicates().write.format(
        "org.neo4j.spark.DataSource"
    ).options(**neo4j_options).option("relationship", "OCCURRED_ON").option(
        "relationship.save.strategy", "keys"
    ).option("relationship.source.labels", "Crime").option(
        "relationship.source.node.keys", "case_number:case_number"
    ).option("relationship.target.labels", "Date").option(
        "relationship.target.node.keys", "date_label:date_label"
    ).mode("Append").save()

    fact_crime_incidents.select("case_number", "location_text_key").dropDuplicates().write.format(
        "org.neo4j.spark.DataSource"
    ).options(**neo4j_options).option("relationship", "OCCURRED_AT").option(
        "relationship.save.strategy", "keys"
    ).option("relationship.source.labels", "Crime").option(
        "relationship.source.node.keys", "case_number:case_number"
    ).option("relationship.target.labels", "Location").option(
        "relationship.target.node.keys", "location_text_key:location_text_key"
    ).mode("Append").save()

    fact_crime_incidents.select("case_number", "crime_type_key").dropDuplicates().write.format(
        "org.neo4j.spark.DataSource"
    ).options(**neo4j_options).option("relationship", "IS_OF_TYPE").option(
        "relationship.save.strategy", "keys"
    ).option("relationship.source.labels", "Crime").option(
        "relationship.source.node.keys", "case_number:case_number"
    ).option("relationship.target.labels", "CrimeType").option(
        "relationship.target.node.keys", "crime_type_key:crime_type_key"
    ).mode("Append").save()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iceberg_table", required=True, help="Name of the source Iceberg table")
    parser.add_argument("--process_date", required=True, help="Processing date in YYYY-MM-DD format")
    parser.add_argument("--neo4j_uri", required=True, help="Neo4j connection URI")
    parser.add_argument("--neo4j_user", required=True, help="Neo4j username")
    parser.add_argument("--neo4j_password", required=True, help="Neo4j password")
    args = parser.parse_args()

    load_data_to_neo4j(
        args.iceberg_table,
        args.neo4j_uri,
        args.neo4j_user,
        args.neo4j_password,
        args.process_date,
    )
