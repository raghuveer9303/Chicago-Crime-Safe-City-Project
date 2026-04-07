# Chicago Crime Safe City Platform
![Build Status](https://img.shields.io/badge/build-passing-brightgreen) ![Language](https://img.shields.io/badge/language-Python%20%7C%20TypeScript-blue) ![License](https://img.shields.io/badge/license-MIT-blue)

**[🌐 View Live Project: chicagocrimeproject.raghuveervenkatesh.us](https://chicagocrimeproject.raghuveervenkatesh.us/)**

*An end-to-end ML, data infrastructure, and full-stack platform transforming 20 years of raw Chicago crime datasets into real-time predictive geographic risk models.*

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
**What it does:** The Chicago Crime Safe City Platform is a comprehensive data and artificial intelligence ecosystem. It encompasses four major pillars: an optimized BigQuery data warehouse, a LightGBM predictive pipeline scoring geographic area risks, a Neo4j + LangGraph RAG Agent for deep semantic crime querying, and a high-performance React/FastAPI web dashboard for interactive visualization.

**Why it was built:** Traditional BI dashboards and raw relational models fail to cleanly present overlapping spatio-temporal datasets comprising over 20 years of complex incident data (gigabytes in size). This systemic platform resolves heavy analytical bottlenecks by decoupling ingestion into a Medallion Architecture, providing millisecond-latency API inferences.

**Who it is for:** MLOps engineers, data scientists, public policy researchers, and analysts evaluating robust, full-scale end-to-end data architectures.

## Screenshots / Demo 📷

<img width="1233" height="716" alt="image" src="https://github.com/user-attachments/assets/39cf80ec-b411-4aef-a527-e1e30f4a509e" />

<img width="1709" height="939" alt="image" src="https://github.com/user-attachments/assets/7a8cc687-e89c-47d6-b2b5-5df8f1f5b280" />

<img width="1720" height="943" alt="image" src="https://github.com/user-attachments/assets/721d2fb0-e167-46aa-aed8-cdbb5f8d7ffb" />

## Technologies Used ☕️ 🐍 ⚛️
This is a comprehensive, multi-module ecosystem leveraging best-in-class components:
- **Data Engineering & Warehousing**: Google BigQuery, Apache Airflow, Apache Spark (PySpark), Apache Iceberg
- **Machine Learning**: LightGBM, MLflow, Great Expectations, Pandas, Scikit-learn
- **AI & Graph RAG**: Neo4j, LangGraph, LangChain, OpenAI API
- **Web App Full-Stack**: React, TypeScript, Tailwind CSS, Vite, FastAPI, PostgreSQL

## Setup / Installation 💻
As an enterprise-scale distributed system, each module is containerized and requires distinct credentials.

```bash
git clone https://github.com/raghuveer9303/Chicago-Crime-Safe-City-Project.git
cd Chicago-Crime-Safe-City-Project

# Spin up web dashboard and backend database
cd chicago-crime-web-app
docker-compose up --build -d

# Spin up local orchestrators for data models
cd ../chicago-ml
export AIRFLOW_HOME=$(pwd)
airflow standalone
```
*Note: Refer to specific ecosystem components for detailed `.env` API setups.*

## Approach 🚶
This platform tackles systemic urban intelligence via decoupled, scalable modules using a **Medallion Data Architecture (Bronze/Silver/Gold)**:

1. **[Analytical Data Warehouse (chicago-bq)](./chicago-bq/README.md)**: Transforms raw API telemetry into a Kimball Star Schema in BigQuery.
2. **[Machine Learning Pipeline (chicago-ml)](./chicago-ml/README.md)**: Constructs engineered feature stores mapping 20 years of history, powering MLflow-tracked LightGBM spatial risk engines.
3. **[Graph RAG Copilot (chicago-rag)](./chicago-rag/README.md)**: An intelligent LangGraph multi-agent converting intent directly into Neo4j Cypher algorithms to trace underlying offender/incident network structures.
4. **[Interactive Dashboard (chicago-crime-web-app)](./chicago-crime-web-app/README.md)**: A glassmorphic React visualizer rendering geometric tile arrays connected to a low-latency FastAPI aggregated backend.

## Example Usage / Output
**Holistic Intelligence Workflow Analysis**

*1. Querying incident predictive risk via Backend APIs:*
```json
// input → GET /api/v1/risk/tract/17031010100
// output
{
  "tract_id": "17031010100",
  "daily_risk_score": 0.84,
  "confidence_interval": [0.79, 0.89],
  "dominant_crime_types": ["THEFT", "BATTERY"]
}
```

*2. Graph RAG semantic exploration of relational incident logic:*
```text
input → "How many coordinated robberies happened in the Loop community?"

output → System executes MATCH (i:Incident {type: 'ROBBERY'})-[:OCCURRED_AT]->(l:Location {community: 'LOOP'})
"Based on the graph, there are 14 coordinated robbery incidents in the Loop involving multiple suspects."
```

## Project Structure 📁
```text
.
├── chicago-bq/             # BigQuery dimensional modeling & ETL Airflow pipelines
├── chicago-crime-web-app/  # React/TS map visualizer & FastAPI inference API
├── chicago-ml/             # Medallion lake features, MLflow tracking, LightGBM models
├── chicago-rag/            # Neo4j localized topology schemas & LangGraph agents
└── README.md               # You are here!
```

## Status 📶
**Active**. Core data models, analytical pipelines, schema injections, and visualization layouts are extremely stable. ML modeling layers are actively expanding support for sequential modeling via LSTM structures.

## Limitations ⚠️
- **Memory Caps & Storage Constraints**: Syncing highly connected multi-million node graphs into local Neo4j instances can trigger memory starvation and necessitates clustered cloud deployments.
- **Latency Overlays**: Heavy React geographic tract rendering with dense incident nodes creates tiling latency independent of API speed (which operates under sub-200ms).
- **Macro-level Processing Lags**: Base historical features undergo batch Airflow evaluation rather than live stream updates, resulting in short (24-hour) lag windows compared to true live events.
- **LLM Context Hallucinations**: Extremely complex relational prompt vectors may corrupt the strict Graph Agent Cypher generators.

## Improvements / Roadmap 🚀
- Migrate Airflow data pipeline tasks dynamically via `KubernetesPodOperator` for distributed worker clusters.
- Establish robust multi-modal search combining Neo4j graph results with direct Faiss vector similarity indexing for the RAG Copilot.
- Deploy Vector rendering tools (Mapbox GL JS) into the dashboard to alleviate dynamic zooming lag seen from traditional point clustering.
- Automate complete parameter optimization (Hyperopt) integrations directly into the `chicago-ml` CI/CD track.

## Credits 📝
- Inspired by enterprise data architectures and data platform schemas established by Databricks and LangChain methodologies.
- The fundamental data schema stems from the open records supplied by the [Chicago Data Portal](https://data.cityofchicago.org/).

## Author
Raghuveer Venkatesh • [LinkedIn](https://linkedin.com) • [GitHub](https://github.com/raghuveer9303)
