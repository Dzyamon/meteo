from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse

from meteo.config import get_settings, load_locations
from meteo.storage.timescale import TimescaleStore

app = FastAPI(title="Meteo Nowcasting API", version="0.1.0")

_STATIC = Path(__file__).parent / "static"


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    """Model-comparison dashboard (physics vs AI vs corrected vs ensemble)."""
    return FileResponse(_STATIC / "index.html")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/locations")
def list_locations() -> list[dict]:
    return [loc.model_dump() for loc in load_locations()]


@app.get("/predict/{location_id}")
def get_predictions(location_id: str, model_version: str | None = None) -> dict:
    db = TimescaleStore()
    try:
        features = db.latest_features(location_id)
        predictions = db.latest_predictions(location_id, model_version=model_version)
        if not predictions:
            raise HTTPException(status_code=404, detail=f"No predictions for {location_id}")
        return {
            "location_id": location_id,
            "model_version": model_version,
            "features_as_of": features["time"] if features else None,
            "predictions": predictions,
        }
    finally:
        db.close()


@app.get("/forecast/{location_id}")
def get_model_forecast(location_id: str) -> dict:
    """Approach 3: raw GFS vs. locally bias-corrected forecast, side by side."""
    db = TimescaleStore()
    try:
        raw = db.latest_predictions(location_id, model_version="gfs_raw")
        corrected = db.latest_predictions(location_id, model_version="gfs_corrected")
        if not raw and not corrected:
            raise HTTPException(status_code=404, detail=f"No model forecast for {location_id}")

        # Correction is active only if a corrected value actually diverges from
        # raw for some horizon (compare forecast values, not row metadata).
        def _values(rows: list[dict]) -> dict:
            return {
                r["horizon_hours"]: (r["temperature_c"], r["precipitation_mm"], r["wind_speed_ms"])
                for r in rows
            }

        raw_v, corrected_v = _values(raw), _values(corrected)
        correction_active = any(raw_v.get(h) != v for h, v in corrected_v.items())

        return {
            "location_id": location_id,
            "gfs_raw": raw,
            "gfs_corrected": corrected,
            "correction_active": correction_active,
        }
    finally:
        db.close()


@app.get("/alerts")
def list_alerts(location_id: str | None = None, limit: int = 50) -> list[dict]:
    db = TimescaleStore()
    try:
        return db.list_alerts(location_id=location_id, limit=limit)
    finally:
        db.close()


def _round(value, ndigits: int = 3):
    return None if value is None else round(float(value), ndigits)


@app.get("/eval/{location_id}")
def evaluate(location_id: str, hours: int = 168) -> dict:
    """Fair per-model forecast accuracy vs observations over the last `hours`.

    Scores the latest forecast per (model_version, valid_time) so short-lead
    re-forecasts don't distort the comparison. Lower MAE/RMSE is better; bias is
    signed mean error (forecast − observed).
    """
    db = TimescaleStore()
    try:
        rows = db.evaluate_models(location_id, since_hours=hours)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No scored forecasts for {location_id}")
        models = [
            {
                "model_version": r["model_version"],
                "scored": r["scored"],
                "temperature": {
                    "mae": _round(r["temp_mae"]),
                    "rmse": _round(r["temp_rmse"]),
                    "bias": _round(r["temp_bias"]),
                    "n": r["temp_n"],
                },
                "wind_speed": {"mae": _round(r["wind_mae"]), "n": r["wind_n"]},
                "precipitation": {"mae": _round(r["precip_mae"]), "n": r["precip_n"]},
            }
            for r in rows
        ]
        ranked = [m for m in models if m["temperature"]["mae"] is not None]
        best = ranked[0]["model_version"] if ranked else None
        return {
            "location_id": location_id,
            "window_hours": hours,
            "scored_against": "observations (open_meteo)",
            "best_temperature_mae": best,
            "models": models,
        }
    finally:
        db.close()


@app.get("/champions/{location_id}")
def get_champions(location_id: str) -> dict:
    """The current best model_version per variable (from out-of-sample scoring)."""
    db = TimescaleStore()
    try:
        rows = db.fetch_champions(location_id)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No champions selected for {location_id}")
        return {
            "location_id": location_id,
            "champions": {
                r["variable"]: {
                    "model_version": r["model_version"],
                    "mae": r["mae"],
                    "n_scored": r["n_scored"],
                    "window_hours": r["window_hours"],
                    "evaluated_at": r["evaluated_at"],
                }
                for r in rows
            },
        }
    finally:
        db.close()


@app.get("/best/{location_id}")
def best_forecast(location_id: str) -> dict:
    """Best-available forecast: each variable served from its champion model_version."""
    db = TimescaleStore()
    try:
        champs = db.fetch_champions(location_id)
        if not champs:
            raise HTTPException(status_code=404, detail=f"No champions for {location_id}; run selection first")

        by_variable = {c["variable"]: c["model_version"] for c in champs}
        preds_by_version = {
            v: db.latest_predictions(location_id, model_version=v)
            for v in set(by_variable.values())
        }

        by_vt: dict = {}
        for variable, version in by_variable.items():
            for p in preds_by_version.get(version, []):
                vt = p["valid_time"]
                slot = by_vt.setdefault(
                    vt,
                    {"valid_time": vt, "horizon_hours": p["horizon_hours"],
                     "temperature_c": None, "wind_speed_ms": None, "precipitation_mm": None, "sources": {}},
                )
                slot[variable] = p[variable]
                slot["sources"][variable] = version

        forecast = [by_vt[vt] for vt in sorted(by_vt)]
        if not forecast:
            raise HTTPException(status_code=404, detail=f"No forecasts available for {location_id}")
        return {"location_id": location_id, "champions": by_variable, "forecast": forecast}
    finally:
        db.close()


@app.get("/observations/{location_id}/latest")
def latest_observation(location_id: str) -> dict:
    db = TimescaleStore()
    try:
        rows = db.fetch_observations_for_features(location_id, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No observations for {location_id}")
        return rows[-1]
    finally:
        db.close()


def _check_cron(authorization: str | None) -> None:
    """Reject cron requests without the shared secret (when one is configured)."""
    secret = get_settings().cron_secret
    if secret and authorization != f"Bearer {secret}":
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/cron/fetch", include_in_schema=False)
def cron_fetch(authorization: str | None = Header(default=None)) -> dict:
    """Vercel Cron: pull the Open-Meteo models (gfs/aifs/icon) and re-correct."""
    _check_cron(authorization)
    from meteo_model.ai_pipeline import run_cycle
    from meteo_model.correct.predict import predict_all

    fetched = run_cycle()
    corrected = predict_all()
    return {"fetched": fetched, "corrected": corrected}


@app.get("/cron/train", include_in_schema=False)
def cron_train(authorization: str | None = Header(default=None)) -> dict:
    """Vercel Cron: retrain correction + ensemble, then re-select champions."""
    _check_cron(authorization)
    from meteo_model.champion import select_all
    from meteo_model.correct.train import train_all as correction_train
    from meteo_model.ensemble.predict import predict_all as ensemble_predict
    from meteo_model.ensemble.train import train_all as ensemble_train

    correction = correction_train()
    ensemble = ensemble_train()
    ens_pred = ensemble_predict()
    champions = select_all()
    return {"correction": correction, "ensemble": ensemble, "ensemble_predict": ens_pred, "champions": champions}


def serve() -> None:
    import uvicorn

    uvicorn.run("meteo.api.main:app", host="0.0.0.0", port=8000, reload=False)
