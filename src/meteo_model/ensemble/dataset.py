from __future__ import annotations

import math
from datetime import datetime

import numpy as np

from meteo_model.schemas import CORRECTION_TARGETS

# Member forecasts blended by the ensemble. Order fixes the feature-column order.
# gfs_corrected is derived from gfs_raw, but stacking can still weight them.
# Diverse sources (GFS physics, AIFS AI, ICON physics) give the stack complementary
# errors to exploit.
ENSEMBLE_MEMBERS = ["gfs_corrected", "gfs_raw", "aifs_raw", "icon_raw", "open_meteo_baseline_v0"]

# Ensemble predicts each observed variable from the members' forecasts of it.
ENSEMBLE_TARGETS = list(CORRECTION_TARGETS.keys())


def feature_columns(target: str) -> list[str]:
    """Per-target feature names: each member's forecast of that variable + time-of-day."""
    return [f"{m}:{target}" for m in ENSEMBLE_MEMBERS] + ["horizon_hours", "hour_sin", "hour_cos"]


def _pivot(rows: list[dict]) -> dict:
    """Long rows -> {valid_time: {"members": {model: {var: val}}, "horizon": h, "obs": {var: val}}}."""
    by_time: dict = {}
    for r in rows:
        vt = r["valid_time"]
        slot = by_time.setdefault(vt, {"members": {}, "horizon": r.get("horizon_hours"), "obs": {}})
        slot["members"][r["model_version"]] = {
            "temperature_c": r.get("temperature_c"),
            "precipitation_mm": r.get("precipitation_mm"),
            "wind_speed_ms": r.get("wind_speed_ms"),
        }
        if slot["horizon"] is None:
            slot["horizon"] = r.get("horizon_hours")
        if "obs_temperature_c" in r:
            slot["obs"] = {
                "temperature_c": r.get("obs_temperature_c"),
                "precipitation_mm": r.get("obs_precipitation_mm"),
                "wind_speed_ms": r.get("obs_wind_speed_ms"),
            }
    return by_time


def _row_features(members: dict, target: str, horizon, valid_time: datetime) -> list[float]:
    hour = valid_time.hour
    feats = []
    for m in ENSEMBLE_MEMBERS:
        val = members.get(m, {}).get(target)
        feats.append(np.nan if val is None else float(val))
    feats.append(np.nan if horizon is None else float(horizon))
    feats.append(math.sin(2 * math.pi * hour / 24))
    feats.append(math.cos(2 * math.pi * hour / 24))
    return feats


def build_training_frame(rows: list[dict], target: str) -> tuple[np.ndarray, np.ndarray, list[datetime]]:
    """X (member forecasts + time), y (observed target). Keeps rows where obs and at
    least one member forecast of the target exist; missing members are NaN."""
    pivot = _pivot(rows)
    X, y, times = [], [], []
    for vt in sorted(pivot):
        slot = pivot[vt]
        obs = slot["obs"].get(target)
        if obs is None:
            continue
        feats = _row_features(slot["members"], target, slot["horizon"], vt)
        member_vals = feats[: len(ENSEMBLE_MEMBERS)]
        if all(np.isnan(v) for v in member_vals):
            continue  # no member forecast for this target/time -> unusable
        X.append(feats)
        y.append(float(obs))
        times.append(vt)
    return np.asarray(X, dtype=float).reshape(-1, len(feature_columns(target))), np.asarray(y, dtype=float), times


def build_predict_rows(rows: list[dict]) -> dict:
    """{valid_time: {"members":..., "horizon":...}} for the latest forecasts (no obs)."""
    return _pivot(rows)
