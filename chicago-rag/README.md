# Chicago Crime Graph RAG Copilot
![Build Status](https://img.shields.io/badge/build-passing-brightgreen) ![Runtime](https://img.shields.io/badge/Graph-Neo4j-blue) ![License](https://img.shields.io/badge/license-MIT-blue)

*An LLM-driven Graph Retrieval-Augmented Generation (RAG) system answering profound network topology questions about Chicago crime.*

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
**What it does:** Extracts flat data, loads it into a Neo4j Graph schema (Locations, Dates, Suspects), and wraps it with a LangGraph AI chatbot capable of executing Cypher queries dynamically from natural text.
**Why it was built:** Traditional BI dashboards fail to answer unstructured questions like "What is the network of offenses tied to this block on weekends?". Graph + RAG bridges this gap.
**Who it is for:** Investigators, analysts, and researchers needing semantic understanding of deep data networks.

## Screenshots / Demo 📷
*![Chatbot Interface](https://via.placeholder.com/800x400?text=RAG+Chatbot+Semantic+Responses)*
*![Neo4j Node Topology](https://via.placeholder.com/800x400?text=Neo4j+Cypher+Graph+Visualization)*

## Technologies Used ☕️ 🐍 ⚛️
- **Graph Database**: Neo4j
- **AI / LLM Framework**: LangGraph, LangChain, OpenAI API
- **Backend**: FastAPI
- **Pipeline Processing**: Apache Airflow, Python scripts

## Setup / Installation 💻
Ensure Neo4j is running locally or via Docker:
```bash
git clone <repository_url>
cd chicago-rag
export OPENAI_API_KEY="sk-..."
export NEO4J_URI="bolt://localhost:7687"
# Populate database
airflow dags trigger chicago_crime_neo4j_load
# Boot the backend
uvicorn chatbot.main:app --reload
```

## Approach 🚶
This repo solves the challenge of relational density:
- **Data Engineering (The Graph Build):** Uses the `build_gold_neo4j_nodes_edges.py` via Airflow to map the silver-tier tabular dataset into graph relationships (`[Location] <-[:OCCURRED_AT]- [Incident]`).
- **Query Resolution (LangGraph Agent):** The backend chatbot utilizes a multi-stage LangGraph workflow:
  1. Determine intent.
  2. Generate Cypher query.
  3. Execute safe Cypher against Neo4j.
  4. Sythesize and respond in human language.

## Example Usage / Output
**Human Query -> Agent Payload**
```text
User: "How many burglaries happened on Halsted St in the Loop connected to multiple offenders?"

System (Cypher Gen): 
MATCH (i:Incident {type: 'BURGLARY'})-[:OCCURRED_AT]->(l:Location {street: 'HALSTED', community: 'LOOP'})
MATCH (i)-[:COMMITTED_BY]->(o:Offender)
WITH i, count(o) as offenders WHERE offenders > 1
RETURN count(i) as total_incidents

Chatbot Output: "Based on the graph, there are 14 distinct burglary incidents on Halsted St within the Loop involving multiple coordinated suspects."
```

## Project Structure 📁
```text
.
├── chatbot/            # FastAPI and LangGraph conversational implementation
├── dags/               # Orchestrates dropping/reloading graph databases periodically
├── scripts/gold/       # Data pipelines mapping flat data to Neo4j graph nodes/edges
├── scripts/bronze/     # Reused foundation ingestion
└── README.md
```

## Status 📶
**Experimental**. The schema generation is fully functional, but LLM Cypher-injection accuracy requires refinement under complex relational queries.

## Limitations ⚠️
- **LLM Hallucination:** Even with schema injection, the agent will occasionally generate structurally invalid Cypher queries.
- **Graph Scale:** Loading 8 million rows into Neo4j locally causes significant memory starvation; queries must be limited by temporal windows.

## Improvements / Roadmap 🚀
- Implement Vector Store embeddings (FAISS/Pinecone) alongside the Graph DB to accomplish hybrid search.
- Implement strict Pydantic Cypher validators prior to execution.
- Create automated Neo4j index optimizers based on recent query histories.

## Credits 📝
- Architectural patterns motivated by Andrew Ng's agentic workflows and LangChain's Graph RAG documentation.

## Author
Raghuveer Venkatesh • [LinkedIn](https://linkedin.com) • [GitHub](https://github.com/raghuveer9303)
