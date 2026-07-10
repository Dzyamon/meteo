# Meteo — Approach 1 MVP

Micro-batch weather nowcasting pipeline: Open-Meteo ingestion, medallion-style storage, feature engineering, and FastAPI serving.

## Approach comparison


| Dimension        | **1. Micro-batch + local ML** (this repo)        | **2. Streaming (Kafka/Redpanda)**      | **3. Model-centric (GraphCast/GFS)**               |
| ---------------- | ------------------------------------------------ | -------------------------------------- | -------------------------------------------------- |
| Time to MVP      | **Days–1 week**                                  | 2–3 weeks                              | 3–6+ weeks                                         |
| Ops complexity   | **Low** (Docker Compose, Prefect cron)           | Medium–high (brokers, consumer groups) | High (GPU, NetCDF/Zarr, downscaling)               |
| Latency          | **5–15 min** (scheduled micro-batches)           | **≤15 min** (continuous)               | 15 min–hours (model run cost)                      |
| Data sources     | Open-Meteo, OpenWeather, optional GFS            | Same + event-driven radar/stations     | ERA5, GFS, GraphCast weights                       |
| Best for         | Prototype, 1–20 locations, local bias correction | Live dashboards, many sources, alerts  | Research-grade, global backbone + local correction |
| Cost (free tier) | **Single VM / laptop**                           | Same + broker overhead                 | GPU strongly recommended                           |


**Recommendation:** Start with Approach 1 (this repo). Add streaming only if you need sub-batch reactivity or many concurrent consumers. Add Approach 3 when you have baseline metrics and want to beat NWP with global AI + local correction.

## Architecture

```
Open-Meteo / OpenWeather
        │
        ▼  every 5–15 min (Prefect)
   ┌────────────┐
   │ Prefect    │
   │ micro-batch│
   └─────┬──────┘
         │
    Bronze (MinIO/disk) ── raw JSON
         │
    Silver (TimescaleDB) ── observations
         │
    Gold (TimescaleDB)   ── rolling/lag features
         │
    Predictions          ── baseline → LightGBM later
         │
    FastAPI              ── GET /predict/{location_id}
```



## Quick start



### 1. Infrastructure

```bash
cp .env.example .env
docker compose up -d
```

Services: TimescaleDB `:5432`, MinIO `:9000` (console `:9001`), Prefect `:4200`.

### 2. Python environment

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -e .
```

Edit `config/locations.yaml` for your coordinates. Optionally set `OPENWEATHER_API_KEY` in `.env`.

### 3. Run one ingest cycle (manual)

```bash
meteo-ingest
# or: python -m meteo.flows.ingest
```

This will:

1. Fetch Open-Meteo history + current for each location
2. Store raw JSON in MinIO (or `./data/bronze` if `USE_LOCAL_BRONZE=true`)
3. Upsert cleaned rows into `observations`
4. Compute lag/rolling features into `features`
5. Write baseline 0–6h predictions (Open-Meteo forecast passthrough until ML model is trained)



### 4. Schedule with Prefect

**Order matters:** create the work pool first, then deploy, then start a worker (keep that terminal open).

PowerShell:

```powershell
$env:PREFECT_API_URL = "http://localhost:4200/api"

# 1) Create work pool (once)
prefect work-pool create --type process default-agent-pool

# 2) Register deployment + cron schedule (every 10 min)
# Schedule comes from prefect.yaml — do NOT pass --cron here (would duplicate it)
prefect deploy --name meteo-every-10min

# 3) Start worker in a separate terminal (must stay running)
prefect worker start --pool default-agent-pool
```

Verify in UI: [http://localhost:4200/deployments](http://localhost:4200/deployments) — status should be **Ready** when the worker is connected.

Manual test run:

```powershell
prefect deployment run "meteo-micro-batch/meteo-every-10min"
```

For local dev without Prefect, use Windows Task Scheduler with `meteo-ingest` every 10 minutes.

### 5. Serve predictions

```bash
meteo-serve
# GET http://localhost:8000/predict/vilnius
```

---



## Approach 2: Streaming (feature/streaming branch)

Event-driven pipeline using Redpanda (Kafka-compatible). Reuses Approach 1 clients, storage, and features; replaces Prefect scheduling with a message bus.

### Architecture

```
Open-Meteo
    │
    ▼  poll every 10 min
┌─────────────┐     weather.observations     ┌──────────────┐
│  Producer   │ ───────────────────────────► │   Consumer   │
└─────────────┘         (Redpanda)           └──────┬───────┘
                                                    │
                              TimescaleDB ◄─────────┘
                              observations + features
                                                    │
                              FastAPI (unchanged) ◄─┘
```



### 1. Start infrastructure (base + Redpanda)

```powershell
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d
```

Adds Redpanda on Kafka port `19092`, admin API on `19644`, and **Console UI** on http://localhost:8080.

### 2. Configure `.env`

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC_OBSERVATIONS=weather.observations
PRODUCER_POLL_INTERVAL_SECONDS=600
```



### 3. Run streaming pipeline

Three terminals:

```powershell
# Terminal 1 — consumer (must start first to process messages)
meteo-stream-consumer

# Terminal 2 — producer (poll Open-Meteo, publish to Kafka)
meteo-stream-producer --once   # test one cycle
meteo-stream-producer          # continuous, every 10 min

# Terminal 3 — API (unchanged)
meteo-serve
```



### 4. Verify

```powershell
# Check consumer processed data
docker exec meteo-timescaledb-1 psql -U meteo -d meteo -c "SELECT COUNT(*) FROM observations;"
docker exec meteo-timescaledb-1 psql -U meteo -d meteo -c "SELECT * FROM features ORDER BY time DESC LIMIT 3;"
```

Redpanda topic (CLI):

```powershell
docker exec meteo-redpanda rpk topic consume weather.observations -n 1
```

Redpanda Console (web UI): http://localhost:8080 — browse topics, messages, and consumer groups.



### Approach 1 vs 2 in this repo


|               | Approach 1                                          | Approach 2                                       |
| ------------- | --------------------------------------------------- | ------------------------------------------------ |
| Orchestration | Prefect cron                                        | Redpanda + producer/consumer                     |
| Entrypoints   | `meteo-ingest`                                      | `meteo-stream-producer`, `meteo-stream-consumer` |
| Serving       | `meteo-serve`                                       | `meteo-serve` (same)                             |
| Run together? | No — pick one ingest path to avoid duplicate writes |                                                  |


---



## Next steps (ML training)

Once you have ~2–4 weeks of hourly data in `observations`:

1. Build training dataset: features at `t` → targets at `t+1…t+6`
2. Train LightGBM per target (`temperature_c`, `precipitation_mm`, `wind_speed_ms`)
3. Swap `BaselineNowcaster` with loaded models in `src/meteo/model/`
4. Optional: add GFS features as extra columns in `features`



## Project layout

```
config/locations.yaml      # target sites
docker-compose.yml         # TimescaleDB, MinIO, Prefect
docker-compose.streaming.yml  # Redpanda overlay (Approach 2)
scripts/init_db.sql        # schema
src/meteo/                 # Approach 1 — micro-batch
  clients/weather.py       # Open-Meteo, OpenWeather
  storage/bronze.py        # raw JSON
  storage/timescale.py     # silver + gold tables
  features/engineering.py  # Polars feature builder
  pipeline/ingest.py       # core ETL logic
  flows/ingest.py          # Prefect flow wrapper
  model/baseline.py        # MVP predictor
  api/main.py              # FastAPI
src/meteo_stream/          # Approach 2 — streaming
  producer.py              # poll APIs → Kafka
  consumer.py              # Kafka → TimescaleDB + features
  schemas.py               # message validation
  kafka_client.py          # producer/consumer factories
```



## Environment variables

See `.env`. Key settings:

- `USE_LOCAL_BRONZE=true` — skip MinIO, write to disk (simplest local dev)
- `INGEST_LOOKBACK_HOURS` — history window for features (default 48)
- `OPENWEATHER_API_KEY` — optional backup source
- `KAFKA_BOOTSTRAP_SERVERS` — Redpanda/Kafka address (Approach 2)
- `PRODUCER_POLL_INTERVAL_SECONDS` — producer poll interval, default 600 (10 min)

