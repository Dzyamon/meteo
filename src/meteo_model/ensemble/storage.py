from __future__ import annotations

from pathlib import Path

import joblib

from meteo.config import get_settings


def ensemble_path(location_id: str, target: str) -> Path:
    return get_settings().model_dir / location_id / f"ensemble_{target}.pkl"


def save_ensemble(location_id: str, target: str, bundle: dict) -> Path:
    path = ensemble_path(location_id, target)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)
    return path


def load_ensemble(location_id: str, target: str) -> dict | None:
    path = ensemble_path(location_id, target)
    return joblib.load(path) if path.exists() else None
