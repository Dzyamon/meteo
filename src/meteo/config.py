from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Location(BaseModel):
    id: str
    name: str
    latitude: float
    longitude: float
    timezone: str = "UTC"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://meteo:meteo@localhost:5432/meteo"
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "meteo-bronze"
    use_local_bronze: bool = False
    local_bronze_path: Path = Path("./data/bronze")
    openweather_api_key: str = ""
    locations_config: Path = Path("config/locations.yaml")
    ingest_lookback_hours: int = 48
    forecast_hours: int = 6


@lru_cache
def get_settings() -> Settings:
    return Settings()


def load_locations(config_path: Path | None = None) -> list[Location]:
    settings = get_settings()
    path = config_path or settings.locations_config
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [Location.model_validate(item) for item in data["locations"]]
