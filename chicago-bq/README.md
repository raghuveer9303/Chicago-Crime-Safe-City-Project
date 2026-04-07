# Chicago Crime Analytical Data Warehouse
![Build Status](https://img.shields.io/badge/pipeline-passing-brightgreen) ![Runtime](https://img.shields.io/badge/data-BigQuery-blue) ![License](https://img.shields.io/badge/license-MIT-blue)

*Robust star-schema dimensional modeling orchestrating gigabytes of data into Google BigQuery.*

## Table of Contents 📑
- [About the Project](#about-the-project-)
- [Screenshots / Demo](#screenshots--demo-)
- [Technologies Used](#technologies-used-%EF%B8%8F--)
- [Setup / Installation](#setup--installation-)
- [Approach](#approach-)
- [Example Usage / Output](#example-usage--output)
- [Project Structure](#project-structure-)
- [Status](#status-)
- [Limitations](#limitations-%EF%B8%8F)
- [Improvements / Roadmap](#improvements--roadmap-)
- [Credits](#credits-)
- [Author](#author)

## About the Project 📚
**What it does:** Converts normalized, flat Chicago crime datasets into an optimized, highly-performant Star Schema targeting Google BigQuery. 
**Why it was built:** Analytical queries (e.g., "Group by block and year where arrests were made") are exceptionally slow and expensive across massive flat files. Dimensional modeling guarantees millisecond BI dashboarding.
**Who it is for:** Data Analysts, BI Developers, and product leadership requiring reporting tables.

## Screenshots / Demo 📷
*![BigQuery Schema View](https://via.placeholder.com/800x400?text=Google+BigQuery+Star+Schema+Overview)*

## Technologies Used ☕️ 🐍 ⚛️
- **Orchestration**: Apache Airflow
- **Data Warehouse**: Google BigQuery
- **Data Processing**: Apache Spark / PySpark 
- **Format**: Apache Iceberg

## Setup / Installation 💻
Setup assumes working Google Cloud credentials:
```bash
git clone <repository_url>
cd chicago-bq
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
airflow dags trigger chicago_crime_dimensional
```

## Approach 🚶
This data pipeline conforms structurally to a **Kimball Dimensional Architecture**.
- We split the Silver Iceberg tables into distinct dimensions (`dim_date`, `dim_location`, `dim_crime_type`, `dim_time_of_day`) and rapidly changing facts (`fact_crime_incidents`, `fact_crime_summary`).
- Airflow dynamically maps the schemas using `bq_schema.py` to ensure BQ destination tables align perfectly before data injection.
- Heavy transformations and surrogate key generation execute entirely within processing clusters, shipping pre-computed facts mapping natively to IDs.

## Example Usage / Output
**Dimensional Query directly in BI / BQ**
```sql
SELECT 
    l.community, 
    d.year, 
    COUNT(f.incident_id) as total,
    SUM(CASE WHEN c.is_violent = TRUE THEN 1 ELSE 0 END) as violent_crimes
FROM `project_id.dataset.fact_crime_incidents` f
JOIN `project_id.dataset.dim_location` l ON f.location_id = l.id
JOIN `project_id.dataset.dim_date` d ON f.date_id = d.id
JOIN `project_id.dataset.dim_crime_type` c ON f.crime_type_id = c.id
GROUP BY l.community, d.year
ORDER BY d.year DESC;
```

## Project Structure 📁
```text
.
├── dags/               # Main orchestration (chicago_crime_dimensional_dag.py)
├── scripts/gold/       # Dedicated builders for fact and dimension partitions
├── scripts/bronze/     # Supporting base raw extractions
├── scripts/silver/     # Intermediary curation and validation logic
└── README.md
```

## Status 📶
**Stable**. The ETL strategy and table relationships map reliably with 100% data preservation and no duplicates.

## Limitations ⚠️
- **Full Refresh Costs:** Due to the complexity of the Chicago Crime dataset edits, certain dimensions undergo drop-and-replace updates which costs more compute than incremental inserts.
- **Latency:** BigQuery commits operate in micro-batches, leaving a 24-hour lag from live dashboard tracking.

## Improvements / Roadmap 🚀
- Migrate full-table overwrite scripts into MERGE-based incremental upserts using BQ Delta tables.
- Introduce dbt (Data Build Tool) instead of pure PySpark to manage dimension definitions natively with SQL.
- Enable automatic partition expiration on deep-history facts to save storage costs.

## Credits 📝
- Modeling architectures based strictly on the Ralph Kimball Data Warehouse Toolkit.

## Author
Raghuveer Venkatesh • [LinkedIn](https://linkedin.com) • [GitHub](https://github.com/raghuveer9303)
