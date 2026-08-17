from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo.clients.weather import OpenMeteoClient
from meteo.config import get_settings, load_locations
from meteo.storage.bronze import BronzeStore
from meteo.storage.timescale import TimescaleStore
from meteo_model.sources import openmeteo_ai

logger = logging.getLogger(__name__)


def parse_models(spec: str) -> list[tuple[str, str]]:
    """'ecmwf_aifs025_single:aifs,icon_seamless:icon' -> [(id, name), ...]."""
    out = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        model_id, _, name = item.partition(":")
        out.append((model_id, name or model_id))
    return out


def process_model(location, model_id: str, model_name: str, client, bronze, db) -> dict:
    run_time, rows = openmeteo_ai.fetch_forecast(location, model_id, model_name, client=client)
    if not rows:
        return {"location_id": location.id, "model": model_name, "nwp_forecasts": 0, "predictions": 0}

    dict_rows = [r.as_dict() for r in rows]
    bronze.save_json(model_name, location.id, {"run_time": run_time.isoformat(), "forecasts": dict_rows})
    nwp_count = db.upsert_nwp_forecasts(dict_rows)

    created_at = datetime.now(timezone.utc)
    predictions = [
        {
            "created_at": created_at,
            "valid_time": r.valid_time,
            "location_id": location.id,
            "horizon_hours": r.horizon_hours,
            "temperature_c": r.temperature_c,
            "precipitation_mm": r.precipitation_mm,
            "wind_speed_ms": r.wind_speed_ms,
            "model_version": f"{model_name}_raw",
        }
        for r in rows
    ]
    pred_count = db.save_predictions(predictions)
    return {"location_id": location.id, "model": model_name, "nwp_forecasts": nwp_count, "predictions": pred_count}


def run_cycle() -> list[dict]:
    settings = get_settings()
    models = parse_models(settings.openmeteo_models)
    locations = load_locations()
    client = OpenMeteoClient()
    bronze = BronzeStore()
    db = TimescaleStore()
    results: list[dict] = []
    try:
        for location in locations:
            for model_id, model_name in models:
                try:
                    results.append(process_model(location, model_id, model_name, client, bronze, db))
                except Exception:  # noqa: BLE001 - one bad model/location must not kill the cycle
                    logger.exception("Failed Open-Meteo model %s for %s", model_name, location.id)
    finally:
        client.close()
        db.close()
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = run_cycle()
    logger.info("Open-Meteo model cycle complete: %s", results)


if __name__ == "__main__":
    main()
