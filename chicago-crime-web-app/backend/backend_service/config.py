from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    chicago_analytics_db_host: str = Field(default="postgres_db", alias="CHICAGO_ANALYTICS_DB_HOST")
    chicago_analytics_db_port: int = Field(default=5432, alias="CHICAGO_ANALYTICS_DB_PORT")
    chicago_analytics_db_name: str = Field(
        default="carvana_db",
        alias="CHICAGO_ANALYTICS_DB_NAME",
    )
    chicago_analytics_db_user: str = Field(default="admin", alias="CHICAGO_ANALYTICS_DB_USER")
    chicago_analytics_db_password: str = Field(default="", alias="CHICAGO_ANALYTICS_DB_PASSWORD")

    geojson_path: str = Field(default="data/geo/community_areas.geojson", alias="GEOJSON_PATH")
    cors_origins: str = Field(default="*", alias="CORS_ORIGINS")

    @property
    def database_url(self) -> str:
        u = self.chicago_analytics_db_user
        p = self.chicago_analytics_db_password
        h = self.chicago_analytics_db_host
        port = self.chicago_analytics_db_port
        db = self.chicago_analytics_db_name
        return f"postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
