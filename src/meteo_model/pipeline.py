from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo.config import get_settings, load_locations
from meteo.storage.bronze import BronzeStore
from meteo.storage.timescale import TimescaleStore
from meteo_model.correct.predict import predict_location
from meteo_model.sources.gfs import fetch_forecasts

logger = logging.getLogger(__name__)


def run_cycle() -> list[dict]:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    locations = load_locations()
    bronze = BronzeStore()
    db = TimescaleStore()

    results: list[dict] = []
    try:
        # Fetch GFS once for all locations (each horizon downloaded a single time).
        run_time, rows_by_loc = fetch_forecasts(
            locations, now, settings.nwp_forecast_hours, model=settings.gfs_model
        )
        for location in locations:
            try:
                rows = rows_by_loc.get(location.id, [])
                dict_rows = [r.as_dict() for r in rows]
                bronze.save_json(
                    "gfs", location.id, {"run_time": run_time.isoformat(), "forecasts": dict_rows}
                )
                nwp_count = db.upsert_nwp_forecasts(dict_rows)
                prediction_stats = predict_location(location.id, db)
                results.append(
                    {
                        "location_id": location.id,
                        "run_time": run_time.isoformat(),
                        "nwp_forecasts": nwp_count,
                        **prediction_stats,
                    }
                )
            except Exception:  # noqa: BLE001 - one bad location must not kill the cycle
                logger.exception("Failed GFS store/predict for %s", location.id)
    except Exception:  # noqa: BLE001 - a fetch failure logs and yields an empty cycle
        logger.exception("GFS fetch failed for this cycle")
    finally:
        db.close()
    return results
