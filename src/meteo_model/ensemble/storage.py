from __future__ import annotations

from meteo_model import model_store


def _key(location_id: str, target: str) -> str:
    return f"{location_id}/ensemble_{target}"


def save_ensemble(location_id: str, target: str, bundle: dict) -> str:
    key = _key(location_id, target)
    model_store.save(key, bundle)
    return key


def load_ensemble(location_id: str, target: str) -> dict | None:
    return model_store.load(_key(location_id, target))
