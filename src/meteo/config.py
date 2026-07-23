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
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic_observations: str = "weather.observations"
    kafka_consumer_group: str = "meteo-stream-consumer"
    kafka_consumer_group_alerts: str = "meteo-stream-alerter"
    kafka_topic_alerts: str = "weather.alerts"
    producer_poll_interval_seconds: int = 600
    alerts_config: Path = Path("config/alerts.yaml")
    alert_cooldown_seconds: int = 1800
    alert_webhook_url: str = ""
    # Approach 3 (model-centric)
    gfs_s3_bucket: str = "noaa-gfs-bdp-pds"
    gfs_model: str = "gfs"
    nwp_forecast_hours: int = 48
    model_dir: Path = Path("./models")
    bias_correction_min_samples: int = 300
    validation_holdout_fraction: float = 0.2  # most-recent share held out for out-of-sample MAE
    # AI model comparison (Open-Meteo-served ECMWF AIFS)
    aifs_openmeteo_model: str = "ecmwf_aifs025_single"
    aifs_model_name: str = "aifs"


@lru_cache
def get_settings() -> Settings:
    return Settings()


def load_locations(config_path: Path | None = None) -> list[Location]:
    settings = get_settings()
    path = config_path or settings.locations_config
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [Location.model_validate(item) for item in data["locations"]]
