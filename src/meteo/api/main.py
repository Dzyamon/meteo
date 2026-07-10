from __future__ import annotations

from fastapi import FastAPI, HTTPException

from meteo.config import load_locations
from meteo.storage.timescale import TimescaleStore

app = FastAPI(title="Meteo Nowcasting API", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/locations")
def list_locations() -> list[dict]:
    return [loc.model_dump() for loc in load_locations()]


@app.get("/predict/{location_id}")
def get_predictions(location_id: str) -> dict:
    db = TimescaleStore()
    try:
        features = db.latest_features(location_id)
        predictions = db.latest_predictions(location_id)
        if not predictions:
            raise HTTPException(status_code=404, detail=f"No predictions for {location_id}")
        return {
            "location_id": location_id,
            "features_as_of": features["time"] if features else None,
            "predictions": predictions,
        }
    finally:
        db.close()


def serve() -> None:
    import uvicorn

    uvicorn.run("meteo.api.main:app", host="0.0.0.0", port=8000, reload=False)
