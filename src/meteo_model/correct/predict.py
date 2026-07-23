from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np

from meteo.storage.timescale import TimescaleStore
from meteo_model.correct.dataset import feature_vector
from meteo_model.correct.storage import load_model
from meteo_model.schemas import CORRECTION_TARGETS

logger = logging.getLogger(__name__)

RAW_VERSION = "gfs_raw"
CORRECTED_VERSION = "gfs_corrected"
# Targets whose corrected value must never go negative.
_NON_NEGATIVE = {"precipitation_mm", "wind_speed_ms"}


def _load_models(location_id: str) -> dict:
    models = {}
    for target in CORRECTION_TARGETS:
        bundle = load_model(location_id, target)
        if bundle is not None:
            models[target] = bundle
    return models


def build_predictions(location_id: str, db: TimescaleStore) -> tuple[list[dict], list[dict]]:
    """Return (raw, corrected) prediction rows from the latest GFS run."""
    forecast_rows = db.fetch_latest_nwp_forecast(location_id, model="gfs")
    if not forecast_rows:
        return [], []

    models = _load_models(location_id)
    created_at = datetime.now(timezone.utc)
    raw_rows: list[dict] = []
    corrected_rows: list[dict] = []

    for fc in forecast_rows:
        base = {
            "created_at": created_at,
            "valid_time": fc["valid_time"],
            "location_id": location_id,
            "horizon_hours": fc["horizon_hours"],
        }
        raw_rows.append(
            {
                **base,
                "temperature_c": fc["temperature_c"],
                "precipitation_mm": fc["precipitation_mm"],
                "wind_speed_ms": fc["wind_speed_ms"],
                "model_version": RAW_VERSION,
            }
        )

        features = np.asarray(
            [
                feature_vector(
                    temperature_c=fc["temperature_c"],
                    precipitation_mm=fc["precipitation_mm"],
                    wind_speed_ms=fc["wind_speed_ms"],
                    humidity_pct=fc["humidity_pct"],
                    pressure_hpa=fc["pressure_hpa"],
                    horizon_hours=fc["horizon_hours"],
                    valid_time=fc["valid_time"],
                )
            ],
            dtype=float,
        )

        corrected = {**base, "model_version": CORRECTED_VERSION}
        for target in CORRECTION_TARGETS:
            raw_value = fc[target]
            bundle = models.get(target)
            if bundle is None or raw_value is None:
                corrected[target] = raw_value  # cold-start passthrough
                continue
            residual = float(bundle["model"].predict(features)[0])
            value = raw_value + residual
            if target in _NON_NEGATIVE:
                value = max(value, 0.0)
            corrected[target] = value
        corrected_rows.append(corrected)

    return raw_rows, corrected_rows


def predict_location(location_id: str, db: TimescaleStore) -> dict:
    raw_rows, corrected_rows = build_predictions(location_id, db)
    raw_n = db.save_predictions(raw_rows)
    corrected_n = db.save_predictions(corrected_rows)
    corrected_active = any(load_model(location_id, t) is not None for t in CORRECTION_TARGETS)
    return {
        "location_id": location_id,
        "raw": raw_n,
        "corrected": corrected_n,
        "correction_active": corrected_active,
    }
