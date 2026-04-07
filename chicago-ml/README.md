# Chicago Crime Machine Learning Pipeline
![Build Status](https://img.shields.io/badge/pipeline-passing-brightgreen) ![Runtime](https://img.shields.io/badge/engine-Airflow%20%7C%20MLflow-blue) ![License](https://img.shields.io/badge/license-MIT-blue)

*End-to-end predictive modeling isolating high-risk geospatial regions in Chicago based on 20 years of incident data.*

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
**What it does:** Orchestrates data flow from raw data APIs into an Iceberg data lake, engineers spatio-temporal features, and trains Tree-based (LightGBM) models to forecast risk scores per area.
**Why it was built:** Crime is highly deterministic across space and time. This pipeline automates the complex window functions and lagged feature engineering needed to provide meaningful insights reliably at scale.
**Who it is for:** MLOps engineers, Data Scientists maintaining the backend, and downstream web apps relying on predictive scoring.

## Screenshots / Demo 📷
*![Airflow DAG execution](https://via.placeholder.com/800x400?text=Airflow+ML+DAG+Graph)*
*![MLflow tracking](https://via.placeholder.com/800x400?text=MLflow+Model+Registry+Metrics)*

## Technologies Used ☕️ 🐍 ⚛️
- **Orchestration**: Apache Airflow
- **Processing**: Apache Spark (PySpark), Apache Iceberg
- **Machine Learning**: LightGBM, Pandas, Scikit-learn
- **Experiment Tracking**: MLflow
- **Data Quality**: Great Expectations

## Setup / Installation 💻
Assuming you have a local Airflow environment running:
```bash
git clone <repository_url>
cd chicago-ml
export AIRFLOW_HOME=$(pwd)
airflow standalone
```
Place all models under MLFlow configurations defined in the `.env` context.

## Approach 🚶
We utilize a **Medallion Architecture (Bronze/Silver/Gold)**:
- **Bronze**: Append-only ingestion from the Chicago Crime API into raw tables.
- **Silver**: Deduplication, schema validation, and partitioning structure logic deployed using Great Expectations for data quality constraints.
- **Gold ML**: We implement isolated feature engineering dags (`area_risk_features`) separate from scoring dags (`daily_score`) to avoid re-computing heavy spatial lookups. Models are tracked and version-controlled via MLflow prior to generating inferences.

## Example Usage / Output
**Spark DataFrame Output for Feature Matrix**
```python
# Output of build_area_risk_features.py
+-------------+----------+---------------+----------------+-----------------+
| community   | date     | risk_score 7d | temp_avg_lag1  | arrests_lag72h  |
+-------------+----------+---------------+----------------+-----------------+
| Near North  | 2026-04  | 0.82          | 45             | 14              |
| Loop        | 2026-04  | 0.91          | 46             | 39              |
+-------------+----------+---------------+----------------+-----------------+
```

## Project Structure 📁
```text
.
├── dags/               # Airflow definition files for execution graphs
├── scripts/
│   ├── bronze/         # Raw dataset extraction logic
│   ├── silver/         # Cleansing and structural enforcement 
│   └── gold_ml/        # Feature extraction and LightGBM model training logic
└── README.md
```

## Status 📶
**Active**. Feature layers are highly stable. The transition from pure LightGBM to integrating LSTM topologies is heavily in progress.

## Limitations ⚠️
- **Cold Starts:** Tracts with extremely sparse records naturally receive biased "low risk" scores regardless of macroeconomic indicators.
- **Processing Time:** Full re-training of the 20-year history on local machines takes upwards of 3 hours without a distributed Spark cluster.

## Improvements / Roadmap 🚀
- Establish model drift detection alerts within Airflow.
- Containerize Spark tasks via KubernetesPodOperator instead of running inside the Airflow worker.
- Replace manual parameter grid searches with an automated Hyperopt tuning stage in the pipeline.

## Credits 📝
- Feature-store topologies adapted from Databricks medaillion architectures.

## Author
Raghuveer Venkatesh • [LinkedIn](https://linkedin.com) • [GitHub](https://github.com/raghuveer9303)
