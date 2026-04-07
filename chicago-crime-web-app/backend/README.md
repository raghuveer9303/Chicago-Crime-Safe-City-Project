# Backend-Service (AI Studio map API)

Reads `studio_area_snapshot` from Postgres (`chicago_crime_analytics`) using the same credentials pattern as Airflow (`CHICAGO_ANALYTICS_*`).

## Run locally

```bash
cd Backend-Service
pip install -e .
export CHICAGO_ANALYTICS_DB_HOST=localhost
export CHICAGO_ANALYTICS_DB_USER=admin
export CHICAGO_ANALYTICS_DB_PASSWORD=...
export CHICAGO_ANALYTICS_DB_NAME=carvana_db
uvicorn backend_service.main:app --host 0.0.0.0 --port 8090
```

## GeoJSON

Bundled placeholder: `data/geo/community_areas.geojson` (3 sample areas). Replace with full Chicago community areas from the [City open data portal](https://data.cityofchicago.org/) for production maps.

## Docker

```bash
docker build -t backend-service .
docker run -p 8090:8090 -e CHICAGO_ANALYTICS_DB_HOST=postgres_db ... backend-service
```
