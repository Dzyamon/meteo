from __future__ import annotations

import logging
from datetime import datetime, timezone

try:
    from prefect import flow
except ImportError:  # pragma: no cover - allows import without prefect installed
    def flow(fn=None, **kwargs):
        def decorator(f):
            return f
        return decorator(fn) if fn else decorator


# GRIB-heavy imports are done lazily inside the flow bodies so this module
# imports cleanly on the host (no [model] extra) — `prefect deploy` needs that,
# while the containerized worker resolves the heavy deps at run time.


@flow(name="meteo-model-centric", log_prints=True)
def meteo_model_flow() -> list[dict]:
    """Fetch GFS forecasts, store raw, apply local bias correction."""
    from meteo_model.pipeline import run_cycle  # needs GRIB tooling (Herbie/cfgrib)

    started = datetime.now(timezone.utc)
    results = run_cycle()
    print(f"GFS cycle at {started.isoformat()}: {results}")
    return results


@flow(name="meteo-aifs-fetch", log_prints=True)
def aifs_flow() -> list[dict]:
    """Fetch the ECMWF AIFS AI-model forecast (Open-Meteo JSON) as aifs_raw."""
    from meteo_model.ai_pipeline import run_cycle

    results = run_cycle()
    print(f"AIFS cycle: {results}")
    return results


@flow(name="meteo-model-train", log_prints=True)
def train_flow() -> dict:
    """Retrain bias-correction models (holdout-validated, gated) and re-predict."""
    from meteo_model.correct.predict import predict_all
    from meteo_model.correct.train import train_all

    trained = train_all()
    predicted = predict_all()
    print(f"train={trained} predict={predicted}")
    return {"train": trained, "predict": predicted}


@flow(name="meteo-ensemble", log_prints=True)
def ensemble_flow() -> dict:
    """Train the stacking ensemble, produce ensemble predictions, then re-select
    the champion model per (location, variable)."""
    from meteo_model.champion import select_all
    from meteo_model.ensemble.predict import predict_all as ensemble_predict
    from meteo_model.ensemble.train import train_all as ensemble_train

    trained = ensemble_train()
    predicted = ensemble_predict()
    champions = select_all()
    print(f"ensemble train={trained} predict={predicted} champions={champions}")
    return {"train": trained, "predict": predicted, "champions": champions}


def run_once() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    meteo_model_flow()


if __name__ == "__main__":
    run_once()
