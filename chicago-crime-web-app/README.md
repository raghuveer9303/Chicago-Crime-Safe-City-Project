# Chicago Crime Web Dashboard
![Build Status](https://img.shields.io/badge/build-passing-brightgreen) ![Runtime](https://img.shields.io/badge/runtime-Node.js%20%7C%20Python-blue) ![License](https://img.shields.io/badge/license-MIT-blue)

*Providing real-time insights and predictive risk mapping for over 20 years of Chicago crime data through an interactive dashboard.*

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
**What it does:** An aesthetic, highly responsive dashboard rendering real-time crime risk probabilities, historical metric drill-downs, and geographic boundaries for city precincts. 
**Why it was built:** Law enforcement analysts and community planners need an immediate, visual way to consume dense datasets without running raw queries against a multi-terabyte data warehouse.
**Who it is for:** Civic analysts, public policy researchers, and data-driven citizens.

## Screenshots / Demo 📷
*![Chicago Crime Map Dashboard](https://via.placeholder.com/800x400?text=Interactive+Tract-Level+Crime+Risk+Map)* 
*![Dimensional Drill-Downs](https://via.placeholder.com/800x400?text=Weekly+Temporal+Crime+Distribution)*

## Technologies Used ☕️ 🐍 ⚛️
- **Frontend**: React, TypeScript, Vite, Tailwind CSS, Recharts, Leaflet/Mapbox
- **Backend / API**: FastAPI, Python 3.10
- **Database / Cache**: PostgreSQL, Redis (via backend-service)
- **Deployment**: Docker, Docker Compose

## Setup / Installation 💻
Launch the entire frontend + backend stack using Docker Compose:
```bash
git clone <repository_url>
cd chicago-crime-web-app
docker-compose up --build
```
Navigate to `http://localhost:3000` to interact with the dashboard.

## Approach 🚶
This project leverages a decoupled **Microservice Architecture**. 
- The React frontend handles high-density data visualizations via Recharts and map primitives. We optimized the UX with dynamic layouts (e.g. glassmorphism) tailored for geographic density maps.
- The FastAPI backend sits in front of the PostgreSQL dimensional schema, executing pre-aggregated queries to guarantee sub-200ms latency independent of the dashboard's rendering frame rates.
- Caching layers at edge endpoints ensure zero re-computation for daily static risk probabilities.

## Example Usage / Output
**Querying the Risk API (Backend)**
```json
// GET /api/v1/risk/tract/17031010100
{
  "tract_id": "17031010100",
  "daily_risk_score": 0.84,
  "confidence_interval": [0.79, 0.89],
  "dominant_crime_types": ["THEFT", "BATTERY"]
}
```

## Project Structure 📁
```text
.
├── backend/            # FastAPI microservice for querying metrics and risk scores
├── frontend/           # React TS frontend with spatial mapping and charts
├── docker-compose.yaml # Local development instrumentation
└── README.md
```

## Status 📶
**Active**. The core API contracts and UI layouts are stable. Real-time model endpoint integrations are currently *experimental*.

## Limitations ⚠️
- **Heavy Map Rendering**: Rendering >50k individual incidents causes map tiling lag; currently clustered via backend optimizations.
- **Cache Invalidation**: Latency hits zero after initial warmup, but the 12:00 AM trigger can cause short 2-3s load spikes on the dashboard. 

## Improvements / Roadmap 🚀
- Migrate Leaflet points to Mapbox GL JS vectors for seamless zooming over native WebGL.
- Implement socketed connections for live 911 dispatch event markers.
- Reduce Docker image size by building multi-stage Rust/Alpine bases for the backend.

## Credits 📝
- Inspired by the open dataset provided by the [Chicago Data Portal](https://data.cityofchicago.org/).

## Author
Raghuveer Venkatesh • [LinkedIn](https://linkedin.com) • [GitHub](https://github.com/raghuveer9303)
