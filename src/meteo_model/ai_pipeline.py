from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo.clients.weather import OpenMeteoClient
from meteo.config import get_settings, load_locations
from meteo.storage.bronze import BronzeStore
from meteo.storage.timescale import TimescaleStore
from meteo_model.sources import openmeteo_ai

logger = logging.getLogger(__name__)

RAW_VERSION = "aifs_raw"


def process_location(location, client, bronze, db) -> dict:
    run_time, rows = openmeteo_ai.fetch_forecast(location, client=client)
    if not rows:
        return {"location_id": location.id, "nwp_forecasts": 0, "predictions": 0}

    dict_rows = [r.as_dict() for r in rows]
    bronze.save_json("aifs", location.id, {"run_time": run_time.isoformat(), "forecasts": dict_rows})
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
            "model_version": RAW_VERSION,
        }
        for r in rows
    ]
    pred_count = db.save_predictions(predictions)
    return {"location_id": location.id, "nwp_forecasts": nwp_count, "predictions": pred_count}


def run_cycle() -> list[dict]:
    locations = load_locations()
    client = OpenMeteoClient()
    bronze = BronzeStore()
    db = TimescaleStore()
    results: list[dict] = []
    try:
        for location in locations:
            try:
                results.append(process_location(location, client, bronze, db))
            except Exception:  # noqa: BLE001 - one bad location must not kill the cycle
                logger.exception("Failed AIFS cycle for %s", location.id)
    finally:
        client.close()
        db.close()
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = run_cycle()
    logger.info("AIFS cycle complete: %s", results)


if __name__ == "__main__":
    main()
