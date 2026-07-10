from __future__ import annotations

from datetime import datetime, timedelta, timezone

from meteo.pipeline.ingest import run_ingest_cycle

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


@task(name="ingest-all-locations", retries=2, retry_delay_seconds=30)
def ingest_task() -> list[dict]:
    return run_ingest_cycle()


@flow(name="meteo-micro-batch", log_prints=True)
def meteo_micro_batch_flow() -> list[dict]:
    """Bronze -> silver -> gold -> baseline predictions every 5-15 minutes."""
    started = datetime.now(timezone.utc)
    results = ingest_task()
    print(f"Completed ingest cycle at {started.isoformat()}: {results}")
    return results


def run_once() -> None:
    meteo_micro_batch_flow()


if __name__ == "__main__":
    run_once()
