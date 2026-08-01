from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np

from meteo.config import load_locations
from meteo.storage.timescale import TimescaleStore
from meteo_model.ensemble.dataset import (
    ENSEMBLE_MEMBERS,
    ENSEMBLE_TARGETS,
    _row_features,
    build_predict_rows,
)
from meteo_model.ensemble.storage import load_ensemble

logger = logging.getLogger(__name__)

ENSEMBLE_VERSION = "ensemble"
_NON_NEGATIVE = {"precipitation_mm", "wind_speed_ms"}


def predict_location(location_id: str, db: TimescaleStore) -> dict:
    models = {t: load_ensemble(location_id, t) for t in ENSEMBLE_TARGETS}
    models = {t: b for t, b in models.items() if b is not None}
    if not models:
        return {"location_id": location_id, "predictions": 0, "ensemble_active": False}

    rows = db.fetch_latest_member_forecasts(location_id, ENSEMBLE_MEMBERS)
    pivot = build_predict_rows(rows)
    created_at = datetime.now(timezone.utc)

    out: list[dict] = []
    for vt in sorted(pivot):
        if vt <= created_at:
            continue  # forecast future valid_times only (distinct horizons, no PK clash)
        slot = pivot[vt]
        rec = {
            "created_at": created_at,
            "valid_time": vt,
            "location_id": location_id,
            "horizon_hours": slot["horizon"] or 0,
            "model_version": ENSEMBLE_VERSION,
            "temperature_c": None,
            "precipitation_mm": None,
            "wind_speed_ms": None,
        }
        wrote_any = False
        for target, bundle in models.items():
            feats = np.asarray([_row_features(slot["members"], target, slot["horizon"], vt)], dtype=float)
            if np.all(np.isnan(feats[0, : len(ENSEMBLE_MEMBERS)])):
                continue  # no member forecast for this target at this time
            value = float(bundle["model"].predict(feats)[0])
            if target in _NON_NEGATIVE:
                value = max(value, 0.0)
            rec[target] = value
            wrote_any = True
        if wrote_any:
            out.append(rec)

    n = db.save_predictions(out)
    return {"location_id": location_id, "predictions": n, "ensemble_active": True}


def predict_all() -> list[dict]:
    db = TimescaleStore()
    try:
        return [predict_location(loc.id, db) for loc in load_locations()]
    finally:
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Ensemble predict complete: %s", predict_all())


if __name__ == "__main__":
    main()
