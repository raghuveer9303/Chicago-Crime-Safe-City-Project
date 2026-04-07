from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    hash,
    initcap,
    lit,
    lower,
    trim,
    upper,
    when,
)


TAXONOMY_COLUMNS = (
    "crime_type_key",
    "crime_primary_type_clean",
    "crime_description_clean",
    "crime_offense_label",
    "crime_category",
    "crime_offense_group",
    "crime_offense_subtype",
    "crime_severity_level",
    "crime_fbi_classification",
    "crime_clearance_bucket",
    "violent_crime_flag",
    "property_crime_flag",
    "drug_crime_flag",
    "weapon_crime_flag",
)

def with_crime_taxonomy(df):
    """
    Add a consistent offense taxonomy derived from raw offense fields.

    Intuition:
    - primary_type is the broad chapter title
    - description is the subtype detail inside that chapter
    - iucr is the stable legal code anchor
    """

    pt = upper(trim(coalesce(col("primary_type"), lit("Unknown"))))
    desc = upper(trim(coalesce(col("description"), lit("Unknown"))))
    iucr = upper(trim(coalesce(col("iucr"), lit("0000"))))

    primary_clean = trim(initcap(lower(coalesce(col("primary_type"), lit("Unknown")))))
    description_clean = trim(initcap(lower(coalesce(col("description"), lit("Unknown")))))

    violent_types = [
        "HOMICIDE",
        "ROBBERY",
        "BATTERY",
        "ASSAULT",
        "KIDNAPPING",
        "CRIMINAL SEXUAL ASSAULT",
        "HUMAN TRAFFICKING",
        "INTIMIDATION",
        "STALKING",
    ]
    property_types = [
        "THEFT",
        "MOTOR VEHICLE THEFT",
        "BURGLARY",
        "CRIMINAL DAMAGE",
        "CRIMINAL TRESPASS",
        "ARSON",
        "DECEPTIVE PRACTICE",
        "STOLEN PROPERTY",
        "OTHER OFFENSE",
    ]
    drug_types = [
        "NARCOTICS",
        "OTHER NARCOTIC VIOLATION",
        "CANNABIS",
        "LIQUOR LAW VIOLATION",
    ]
    weapon_types = [
        "WEAPONS VIOLATION",
        "CONCEALED CARRY LICENSE VIOLATION",
    ]
    public_order_types = [
        "PUBLIC PEACE VIOLATION",
        "INTERFERENCE WITH PUBLIC OFFICER",
        "OBSCENITY",
        "GAMBLING",
        "RITUALISM",
        "NON-CRIMINAL",
        "NON-CRIMINAL (SUBJECT SPECIFIED)",
        "NON - CRIMINAL",
    ]
    sex_types = [
        "CRIMINAL SEXUAL ASSAULT",
        "SEX OFFENSE",
        "PROSTITUTION",
        "OFFENSE INVOLVING CHILDREN",
    ]

    crime_category = (
        when(col("primary_type").isNull(), lit("Unknown"))
        .when(pt.isin(*violent_types), lit("Violent"))
        .when(pt.isin(*sex_types), lit("Sex offense"))
        .when(pt.isin(*weapon_types), lit("Weapons"))
        .when(pt.isin(*drug_types), lit("Drug"))
        .when(pt.isin(*property_types), lit("Property"))
        .when(pt.isin(*public_order_types), lit("Public order"))
        .otherwise(lit("Other"))
    )

    crime_offense_group = (
        when(pt == "HOMICIDE", lit("Homicide"))
        .when(pt == "ROBBERY", lit("Robbery"))
        .when((pt == "BATTERY") & desc.rlike("AGGRAVATED"), lit("Aggravated battery"))
        .when(pt == "BATTERY", lit("Battery"))
        .when((pt == "ASSAULT") & desc.rlike("AGGRAVATED"), lit("Aggravated assault"))
        .when(pt == "ASSAULT", lit("Assault"))
        .when(pt == "CRIMINAL SEXUAL ASSAULT", lit("Sexual assault"))
        .when(pt == "SEX OFFENSE", lit("Sex offense"))
        .when(pt == "BURGLARY", lit("Burglary"))
        .when(pt == "THEFT", lit("Theft"))
        .when(pt == "MOTOR VEHICLE THEFT", lit("Motor vehicle theft"))
        .when(pt == "DECEPTIVE PRACTICE", lit("Fraud / deception"))
        .when(pt == "CRIMINAL DAMAGE", lit("Criminal damage"))
        .when(pt == "CRIMINAL TRESPASS", lit("Criminal trespass"))
        .when(pt == "ARSON", lit("Arson"))
        .when(pt.isin(*drug_types), lit("Drug offense"))
        .when(pt.isin(*weapon_types), lit("Weapons offense"))
        .when(pt == "OFFENSE INVOLVING CHILDREN", lit("Child-related offense"))
        .when(pt == "PROSTITUTION", lit("Commercial sex offense"))
        .when(pt.isin(*public_order_types), lit("Public order offense"))
        .otherwise(primary_clean)
    )

    crime_severity_level = (
        when(pt.isin("HOMICIDE", "CRIMINAL SEXUAL ASSAULT", "HUMAN TRAFFICKING"), lit("High"))
        .when(
            pt.isin(
                "ROBBERY",
                "BATTERY",
                "ASSAULT",
                "KIDNAPPING",
                "WEAPONS VIOLATION",
                "CONCEALED CARRY LICENSE VIOLATION",
                "ARSON",
            ),
            lit("Medium"),
        )
        .when(
            pt.isin(
                "THEFT",
                "MOTOR VEHICLE THEFT",
                "BURGLARY",
                "CRIMINAL DAMAGE",
                "CRIMINAL TRESPASS",
                "DECEPTIVE PRACTICE",
                "NARCOTICS",
                "OTHER NARCOTIC VIOLATION",
                "CANNABIS",
                "LIQUOR LAW VIOLATION",
            ),
            lit("Low"),
        )
        .otherwise(lit("Variable"))
    )

    crime_fbi_classification = (
        when(crime_category == "Violent", lit("Violent Crime"))
        .when(crime_category == "Property", lit("Property Crime"))
        .when(crime_category == "Drug", lit("Drug Crime"))
        .when(crime_category == "Weapons", lit("Weapons Crime"))
        .when(crime_category == "Sex offense", lit("Sex Crime"))
        .when(crime_category == "Public order", lit("Public Order Crime"))
        .otherwise(lit("Other Crime"))
    )

    crime_clearance_bucket = (
        when(pt.isin("HOMICIDE", "CRIMINAL SEXUAL ASSAULT"), lit("High"))
        .when(pt.isin("ROBBERY", "BATTERY", "ASSAULT", "BURGLARY"), lit("Medium"))
        .when(
            pt.isin(
                "THEFT",
                "MOTOR VEHICLE THEFT",
                "CRIMINAL DAMAGE",
                "DECEPTIVE PRACTICE",
            ),
            lit("Low"),
        )
        .otherwise(lit("Variable"))
    )

    return (
        df.withColumn("crime_type_key", hash(iucr, pt, desc).cast("bigint"))
        .withColumn("crime_primary_type_clean", primary_clean)
        .withColumn("crime_description_clean", description_clean)
        .withColumn("crime_offense_label", concat_ws(" | ", iucr, pt, desc))
        .withColumn("crime_category", crime_category)
        .withColumn("crime_offense_group", crime_offense_group)
        .withColumn("crime_offense_subtype", description_clean)
        .withColumn("crime_severity_level", crime_severity_level)
        .withColumn("crime_fbi_classification", crime_fbi_classification)
        .withColumn("crime_clearance_bucket", crime_clearance_bucket)
        .withColumn("violent_crime_flag", when(crime_category == "Violent", lit(1)).otherwise(lit(0)))
        .withColumn("property_crime_flag", when(crime_category == "Property", lit(1)).otherwise(lit(0)))
        .withColumn("drug_crime_flag", when(crime_category == "Drug", lit(1)).otherwise(lit(0)))
        .withColumn("weapon_crime_flag", when(crime_category == "Weapons", lit(1)).otherwise(lit(0)))
    )


def ensure_crime_taxonomy_columns(df):
    """Backfill taxonomy columns when reading older silver snapshots."""
    missing = [name for name in TAXONOMY_COLUMNS if name not in df.columns]
    if not missing:
        return df
    # Partial schema (failed migration or manual edits): drop any taxonomy cols and recompute once.
    out = df
    for name in TAXONOMY_COLUMNS:
        if name in out.columns:
            out = out.drop(name)
    return with_crime_taxonomy(out)
