from __future__ import annotations

from datetime import datetime, timedelta, timezone

from meteo.clients.weather import OpenMeteoClient, OpenWeatherClient
from meteo.config import Location, get_settings, load_locations
from meteo.features.engineering import build_feature_rows
from meteo.model.baseline import BaselineNowcaster
from meteo.storage.bronze import BronzeStore
from meteo.storage.timescale import TimescaleStore


def ingest_location(
    location: Location,
    open_meteo: OpenMeteoClient,
    bronze: BronzeStore,
    db: TimescaleStore,
) -> dict:
    settings = get_settings()
    stats = {"location_id": location.id, "observations": 0, "features": 0, "predictions": 0}

    payload, rows = open_meteo.fetch_current_and_recent(
        location,
        lookback_hours=settings.ingest_lookback_hours,
    )
    bronze.save_json("open_meteo", location.id, payload)
    stats["observations"] += db.upsert_observations(rows)

    if settings.openweather_api_key:
        try:
            ow = OpenWeatherClient(settings.openweather_api_key)
            ow_payload, ow_row = ow.fetch_current(location)
            bronze.save_json("openweather", location.id, ow_payload)
            stats["observations"] += db.upsert_observations([ow_row])
            ow.close()
        except Exception as exc:  # noqa: BLE001 - backup source must not fail the run
            stats["openweather_error"] = str(exc)

    history = db.fetch_observations_for_features(location.id)
    feature_rows = build_feature_rows(history)
    stats["features"] += db.upsert_features(feature_rows)

    nowcaster = BaselineNowcaster()
    forecast_payload = open_meteo.fetch_forecast(location, forecast_hours=settings.forecast_hours)
    predictions = nowcaster.predict_from_forecast(location.id, forecast_payload)
    stats["predictions"] += db.save_predictions(predictions)

    return stats


def run_ingest_cycle() -> list[dict]:
    settings = get_settings()
    locations = load_locations()
    open_meteo = OpenMeteoClient()
    bronze = BronzeStore()
    db = TimescaleStore()

    results: list[dict] = []
    try:
        for location in locations:
            results.append(ingest_location(location, open_meteo, bronze, db))
    finally:
        open_meteo.close()
        db.close()

    return results
