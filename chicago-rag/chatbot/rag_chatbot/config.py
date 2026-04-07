from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # LLM providers (keep Groq; add Mistral)
    groq_api_key: str = ""
    mistral_api_key: str = ""
    langsearch_api_key: str = ""

    neo4j_username: str = "neo4j"
    neo4j_password: str = ""

    # Neo4j MCP (HTTP); tools loaded at startup. Basic auth defaults to Neo4j DB creds.
    neo4j_mcp_url: str = "http://localhost:9519/mcp/"
    neo4j_mcp_http_user: str = ""
    neo4j_mcp_http_password: str = ""

    # LangGraph checkpoints (optional). When unset, uses in-memory saver.
    checkpoint_postgres_uri: str = ""

    groq_model_small: str = "openai/gpt-oss-20b"
    groq_model_large: str = "openai/gpt-oss-120b"

    mistral_model_small: str = "mistral-small-latest"
    mistral_model_medium: str = "mistral-medium-latest"
    mistral_model_large: str = "mistral-large-latest"

    langfuse_host: str = "http://localhost:9507"
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    # Langfuse Python SDK 3.x sends traces via OTLP to /api/public/otel/v1/traces (requires Langfuse v3+).
    # Docker/langfuse-docker.yaml deploys v3; set false only if you point at a legacy v2 server.
    langfuse_langchain_callbacks: bool = True
    # OpenTelemetry resource (fixes "unknown_service" on Langfuse spans). Env: OTEL_SERVICE_NAME.
    otel_service_name: str = "rag-api"


@lru_cache
def get_settings() -> Settings:
    return Settings()
