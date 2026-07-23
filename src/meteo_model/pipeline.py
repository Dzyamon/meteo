from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo.config import Location, get_settings, load_locations
from meteo.storage.bronze import BronzeStore
from meteo.storage.timescale import TimescaleStore
from meteo_model.correct.predict import predict_location
from meteo_model.sources.gfs import fetch_forecast

logger = logging.getLogger(__name__)


def process_location(
    location: Location,
    now: datetime,
    bronze: BronzeStore,
    db: TimescaleStore,
) -> dict:
    settings = get_settings()
    run_time, rows = fetch_forecast(location, now, settings.nwp_forecast_hours, model=settings.gfs_model)

    dict_rows = [r.as_dict() for r in rows]
    bronze.save_json(
        "gfs",
        location.id,
        {"run_time": run_time.isoformat(), "forecasts": dict_rows},
    )
    nwp_count = db.upsert_nwp_forecasts(dict_rows)

    prediction_stats = predict_location(location.id, db)
    return {
        "location_id": location.id,
        "run_time": run_time.isoformat(),
        "nwp_forecasts": nwp_count,
        **prediction_stats,
    }


def run_cycle() -> list[dict]:
    now = datetime.now(timezone.utc)
    locations = load_locations()
    bronze = BronzeStore()
    db = TimescaleStore()

    results: list[dict] = []
    try:
        for location in locations:
            try:
                results.append(process_location(location, now, bronze, db))
            except Exception:  # noqa: BLE001 - one bad location must not kill the cycle
                logger.exception("Failed GFS cycle for %s", location.id)
    finally:
        db.close()
    return results
