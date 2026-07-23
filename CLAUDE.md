# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Weather nowcasting pipeline built as **two interchangeable ingest paths that share the same storage, features, model, and API**:

- **Approach 1 — micro-batch** (`src/meteo/`): Prefect cron pulls Open-Meteo every 10 min → TimescaleDB → FastAPI.
- **Approach 2 — streaming** (`src/meteo_stream/`, `feature/streaming` branch): a producer polls Open-Meteo and publishes to Redpanda/Kafka; separate consumer and alerter services read the topic.

Critical rule: **the two paths both write `observations`/`features`, so never run both ingest paths at once** — pick one or you get duplicate writes.

`meteo_stream` deliberately reuses `meteo`'s building blocks (`OpenMeteoClient`, `BronzeStore`, `TimescaleStore`, `build_feature_rows`, `config`). When editing anything under `src/meteo/`, remember both paths depend on it.

## Commands

```bash
pip install -e ".[dev]"        # install with dev extras (pytest, ruff)
ruff check .                    # lint
ruff format .                  # format
pytest                         # tests (no test suite exists yet)
```

Entrypoints (defined in `pyproject.toml [project.scripts]`):

```bash
meteo-ingest              # Approach 1: run one full ETL cycle (meteo.flows.ingest:run_once)
meteo-serve              # FastAPI on :8000 (both approaches)
meteo-stream-producer    # Approach 2: poll → Kafka; add --once for a single cycle
meteo-stream-consumer    # Approach 2: Kafka → observations + features
meteo-stream-alerter     # Approach 2: Kafka → evaluate rules → alerts table + weather.alerts
```

Infrastructure:

```bash
docker compose up -d                                                   # Approach 1: TimescaleDB, MinIO, Prefect
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d  # + Redpanda (:19092, console :8080) + Grafana (:3000)
```

DB schema is applied via `scripts/init_db.sql`; migrations for existing DBs live in `scripts/migrate_alerts.sql` (pipe into `docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo`).

## Data flow (medallion)

```
Open-Meteo/OpenWeather → Bronze (raw JSON: MinIO or ./data/bronze) → Silver (observations) → Gold (features) → predictions/alerts → FastAPI
```

- **Bronze** = raw payloads; set `USE_LOCAL_BRONZE=true` to skip MinIO and write to disk.
- **Silver** = `observations`, keyed `(time, location_id, source)`, upserted idempotently.
- **Gold** = `features`, keyed `(time, location_id)`. `build_feature_rows` computes lag/rolling/diff columns with Polars but **returns only the single latest row** per call — it is a per-cycle incremental feature builder, not a backfill.
- **predictions** currently come from `BaselineNowcaster`, which passes through Open-Meteo's own forecast (`model_version='baseline'`). Real ML (LightGBM per target) is planned once enough history accumulates — see README "Next steps".

All TimescaleDB tables are hypertables (see `scripts/init_db.sql`). DB access goes through `TimescaleStore` (psycopg connection pool, `dict_row`); every endpoint/service constructs and `close()`s its own store.

## Conventions that matter

- **Config is centralized** in `src/meteo/config.py`: `get_settings()` (pydantic-settings, reads `.env`, `lru_cache`d) and `load_locations()` (reads `config/locations.yaml`). Add new tunables as `Settings` fields, not ad-hoc `os.getenv`.
- **`ObservationRow`** (`clients/weather.py`) is the canonical observation dataclass. The streaming path wraps it in Pydantic `ObservationMessage`/`ObservationBatchEvent` (`meteo_stream/schemas.py`) for over-the-wire validation (range checks on temperature/wind); `to_row()`/`row_to_message()` convert between them.
- **Kafka semantics**: consumers use manual commit and **commit even on validation failure** (bad messages are logged and skipped, not retried). Messages are keyed by `location_id` for per-location ordering. Alerter uses a separate consumer group so it reads the same topic independently of the ETL consumer.
- **Alerts**: rules load from `config/alerts.yaml` (`meteo_stream/rules.py`); each fire is de-duplicated by a DB cooldown check (`recent_alert_exists`, `ALERT_COOLDOWN_SECONDS`) before writing to `alerts` + publishing to `weather.alerts` + optional webhook.
- **OpenWeather is a best-effort backup source** — failures in Approach 1 are caught and recorded in stats, never fail the run.
- Windows is the primary dev environment; the README's runbooks use PowerShell.
