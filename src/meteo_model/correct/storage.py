from __future__ import annotations

from pathlib import Path

import joblib

from meteo.config import get_settings


def model_path(location_id: str, target: str) -> Path:
    settings = get_settings()
    return settings.model_dir / location_id / f"gfs_correction_{target}.pkl"


def save_model(location_id: str, target: str, bundle: dict) -> Path:
    path = model_path(location_id, target)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)
    return path


def load_model(location_id: str, target: str) -> dict | None:
    path = model_path(location_id, target)
    if not path.exists():
        return None
    return joblib.load(path)
