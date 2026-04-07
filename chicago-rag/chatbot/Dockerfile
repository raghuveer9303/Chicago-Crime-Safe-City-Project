FROM python:3.11-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/
COPY rag_chatbot /app/rag_chatbot

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

EXPOSE 8080

CMD ["uvicorn", "rag_chatbot.main:app", "--host", "0.0.0.0", "--port", "8080"]
