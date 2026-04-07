FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pip setuptools wheel hatchling

COPY pyproject.toml README.md ./
COPY backend_service ./backend_service
COPY data ./data

RUN pip install --no-cache-dir .

ENV HOST=0.0.0.0
ENV PORT=8090
EXPOSE 8090

CMD ["uvicorn", "backend_service.main:app", "--host", "0.0.0.0", "--port", "8090"]
