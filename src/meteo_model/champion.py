from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo.config import load_locations
from meteo.storage.timescale import TimescaleStore

logger = logging.getLogger(__name__)

# variable -> (mae field, n field) as returned by TimescaleStore.evaluate_models
_VARS = {
    "temperature_c": ("temp_mae", "temp_n"),
    "wind_speed_ms": ("wind_mae", "wind_n"),
    "precipitation_mm": ("precip_mae", "precip_n"),
}
MIN_SCORED = 10  # don't crown a champion on a handful of points


def select_location(location_id: str, db: TimescaleStore, hours: int = 336) -> list[dict]:
    """Pick the lowest-MAE model_version per variable and persist it."""
    scores = db.evaluate_models(location_id, since_hours=hours)
    now = datetime.now(timezone.utc)
    champions: list[dict] = []
    for variable, (mae_field, n_field) in _VARS.items():
        best = None
        for row in scores:
            mae, n = row[mae_field], row[n_field]
            if mae is None or (n or 0) < MIN_SCORED:
                continue
            if best is None or mae < best[mae_field]:
                best = row
        if best is None:
            continue
        champions.append({
            "location_id": location_id,
            "variable": variable,
            "model_version": best["model_version"],
            "mae": round(float(best[mae_field]), 4),
            "n_scored": int(best[n_field]),
            "window_hours": hours,
            "evaluated_at": now,
        })
    db.upsert_champions(champions)
    logger.info(
        "Champions for %s: %s",
        location_id,
        {c["variable"]: f"{c['model_version']} ({c['mae']})" for c in champions},
    )
    return champions


def select_all(hours: int = 336) -> list[dict]:
    db = TimescaleStore()
    try:
        out = []
        for loc in load_locations():
            out.extend(select_location(loc.id, db, hours=hours))
        return out
    finally:
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Champion selection complete: %s", select_all())


if __name__ == "__main__":
    main()
