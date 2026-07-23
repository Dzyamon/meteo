from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo_model.pipeline import run_cycle

try:
    from prefect import flow, task
except ImportError:  # pragma: no cover - allows running without prefect installed
    def task(fn=None, **kwargs):
        def decorator(f):
            return f
        return decorator(fn) if fn else decorator

    def flow(fn=None, **kwargs):
        def decorator(f):
            return f
        return decorator(fn) if fn else decorator


@task(name="gfs-fetch-correct", retries=2, retry_delay_seconds=60)
def gfs_task() -> list[dict]:
    return run_cycle()


@flow(name="meteo-model-centric", log_prints=True)
def meteo_model_flow() -> list[dict]:
    """Fetch GFS forecasts, store raw, apply local bias correction, serve via predictions."""
    started = datetime.now(timezone.utc)
    results = gfs_task()
    print(f"Completed GFS cycle at {started.isoformat()}: {results}")
    return results


def run_once() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    meteo_model_flow()


if __name__ == "__main__":
    run_once()
