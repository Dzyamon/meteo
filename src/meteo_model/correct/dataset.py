from __future__ import annotations

import math
from datetime import datetime

import numpy as np

from meteo_model.schemas import CORRECTION_TARGETS

# Predictors shared by every per-target bias model. The raw NWP value for the
# target is always included, alongside the other NWP fields and time-of-day.
FEATURE_COLUMNS = [
    "nwp_temperature_c",
    "nwp_precipitation_mm",
    "nwp_wind_speed_ms",
    "nwp_humidity_pct",
    "nwp_pressure_hpa",
    "horizon_hours",
    "hour_sin",
    "hour_cos",
]


def _as_float(value) -> float:
    return np.nan if value is None else float(value)


def feature_vector(
    *,
    temperature_c,
    precipitation_mm,
    wind_speed_ms,
    humidity_pct,
    pressure_hpa,
    horizon_hours,
    valid_time: datetime,
) -> list[float]:
    """Ordered feature row (matching FEATURE_COLUMNS) for train and predict."""
    hour = valid_time.hour
    return [
        _as_float(temperature_c),
        _as_float(precipitation_mm),
        _as_float(wind_speed_ms),
        _as_float(humidity_pct),
        _as_float(pressure_hpa),
        _as_float(horizon_hours),
        math.sin(2 * math.pi * hour / 24),
        math.cos(2 * math.pi * hour / 24),
    ]


def build_training_frame(pairs: list[dict]) -> tuple[np.ndarray, dict[str, np.ndarray], dict[str, np.ndarray]]:
    """
    Turn joined (nwp, obs) rows into a feature matrix X and, per target, the
    residual target y (observed - nwp) plus the raw NWP baseline used to
    reconstruct the corrected value at predict time.

    Rows are kept per target only where both the obs and the raw NWP exist.
    """
    features: list[list[float]] = []
    residuals: dict[str, list[float]] = {t: [] for t in CORRECTION_TARGETS}
    raw_baseline: dict[str, list[float]] = {t: [] for t in CORRECTION_TARGETS}
    keep_mask: dict[str, list[int]] = {t: [] for t in CORRECTION_TARGETS}

    for idx, row in enumerate(pairs):
        features.append(
            feature_vector(
                temperature_c=row["nwp_temperature_c"],
                precipitation_mm=row["nwp_precipitation_mm"],
                wind_speed_ms=row["nwp_wind_speed_ms"],
                humidity_pct=row["nwp_humidity_pct"],
                pressure_hpa=row["nwp_pressure_hpa"],
                horizon_hours=row["horizon_hours"],
                valid_time=row["valid_time"],
            )
        )
        for target, nwp_col in CORRECTION_TARGETS.items():
            obs = row.get(f"obs_{target}")
            nwp = row.get(nwp_col)
            if obs is None or nwp is None:
                continue
            residuals[target].append(float(obs) - float(nwp))
            raw_baseline[target].append(float(nwp))
            keep_mask[target].append(idx)

    x_all = np.asarray(features, dtype=float)
    x_by_target = {t: x_all[keep_mask[t]] if keep_mask[t] else np.empty((0, len(FEATURE_COLUMNS))) for t in CORRECTION_TARGETS}
    y_by_target = {t: np.asarray(residuals[t], dtype=float) for t in CORRECTION_TARGETS}
    return x_by_target, y_by_target, {t: np.asarray(raw_baseline[t], dtype=float) for t in CORRECTION_TARGETS}
