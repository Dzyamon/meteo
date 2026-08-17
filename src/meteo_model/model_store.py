from __future__ import annotations

import io

import joblib

from meteo.config import get_settings

"""Persist trained model bundles to either local disk or the database.

Disk (default) suits the container. `model_store="db"` stores joblib bytes in the
`model_artifacts` table — the serverless-safe option, since a Vercel function's
disk is ephemeral and lost between invocations.
"""


def _serialize(obj) -> bytes:
    buf = io.BytesIO()
    joblib.dump(obj, buf)
    return buf.getvalue()


def _deserialize(data: bytes):
    return joblib.load(io.BytesIO(data))


def save(key: str, obj) -> None:
    settings = get_settings()
    if settings.model_store == "db":
        from meteo.storage.timescale import TimescaleStore

        db = TimescaleStore()
        try:
            db.save_model_artifact(key, _serialize(obj))
        finally:
            db.close()
    else:
        path = settings.model_dir / f"{key}.pkl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_serialize(obj))


def load(key: str):
    settings = get_settings()
    if settings.model_store == "db":
        from meteo.storage.timescale import TimescaleStore

        db = TimescaleStore()
        try:
            data = db.load_model_artifact(key)
        finally:
            db.close()
        return _deserialize(data) if data else None

    path = settings.model_dir / f"{key}.pkl"
    return _deserialize(path.read_bytes()) if path.exists() else None
