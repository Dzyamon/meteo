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

Adds Redpanda on Kafka port `19092`, admin API on `19644`, **Console** on http://localhost:8080, and **Grafana** on http://localhost:3000.

### 2. Configure `.env`

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC_OBSERVATIONS=weather.observations
PRODUCER_POLL_INTERVAL_SECONDS=600
```



### 3. Run streaming pipeline

Four terminals:

```powershell
# Terminal 1 — ETL consumer
meteo-stream-consumer

# Terminal 2 — producer (poll Open-Meteo, publish to Kafka)
meteo-stream-producer --once   # test one cycle
meteo-stream-producer          # continuous, every 10 min

# Terminal 3 — alert evaluator
meteo-stream-alerter

# Terminal 4 — API
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
| Dashboards    | —                                                   | Grafana http://localhost:3000                    |
| Alerting      | —                                                   | `meteo-stream-alerter` + `weather.alerts`      |
| Run together? | No — pick one ingest path to avoid duplicate writes |                                                  |


### 5. Live dashboards (Grafana)

Grafana is included in the streaming compose overlay. It reads directly from TimescaleDB and auto-refreshes every 30 seconds.

```powershell
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d
```

Open http://localhost:3000 — login `admin` / `meteo`.

Pre-built dashboard: **Meteo Live** — temperature, wind, rain, humidity, and recent alerts per location.

For existing databases created before this feature, run:

```powershell
Get-Content scripts/migrate_alerts.sql | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo
```


### 6. Alerting service

A separate Kafka consumer evaluates threshold rules on each observation batch and writes to the `alerts` table + `weather.alerts` topic.

```powershell
# Terminal 4 — alert evaluator (parallel to ETL consumer)
meteo-stream-alerter
```

Edit rules in `config/alerts.yaml`:

```yaml
rules:
  - id: heavy_rain
    metric: precipitation_mm
    operator: gt
    threshold: 2.0
    severity: warning
    message: "Heavy rain detected"
```

Optional webhook notification in `.env`:

```env
ALERT_WEBHOOK_URL=https://your-service.example/alerts
ALERT_COOLDOWN_SECONDS=1800
```

Query alerts via API:

```powershell
Invoke-RestMethod http://localhost:8000/alerts
Invoke-RestMethod http://localhost:8000/alerts?location_id=minsk
```

Inspect alert events in Redpanda Console → topic `weather.alerts`.

### Extended streaming architecture

```
Open-Meteo
    │
    ▼
 Producer ──► weather.observations (Redpanda)
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
    ETL Consumer          Alert service
         │                     │
         ▼                     ├──► weather.alerts
    TimescaleDB ◄──────────────┘         │
         │                          webhook (optional)
         ├──► Grafana (live dashboards)
         └──► FastAPI (/alerts, /observations/.../latest)
```

---



## Approach 3: Model-centric (feature/model-centric branch)

Pull **gridded NWP output** (NOAA GFS 0.25°) instead of point forecasts, extract the grid cell over each location, then **beat raw GFS with local bias correction** — a LightGBM model trained on the observations Approach 1/2 already collected. Reuses `config`, `BronzeStore`, `TimescaleStore`, `build_feature_rows`, the `predictions` table (via `model_version`), and the FastAPI app.

Everything is free and needs no GPU. GFS comes from AWS Open Data (`noaa-gfs-bdp-pds`, no auth) via [Herbie](https://herbie.readthedocs.io) byte-range subsetting, so each cycle downloads KBs, not the full ~400 MB GRIB.

### Architecture

```
NOAA GFS 0.25° GRIB2 (AWS Open Data, anonymous)
    │  fetch latest run (00/06/12/18Z), byte-range subset
    ▼
 extract nearest grid cell per location
    │
 Bronze (raw values) ──► nwp_forecasts (raw GFS per run/valid_time/horizon)
    │                              │
    │        observations + features (already collected by Approach 1/2)
    │                              │
    ▼                              ▼
 LightGBM bias correction (residual = observed − GFS, per target)
    │
 predictions (model_version = 'gfs_raw' & 'gfs_corrected')
    │
 FastAPI  GET /forecast/{location_id}   (raw vs corrected side by side)
```

### 1. Start infrastructure (base + model runner)

GRIB decoding (`eccodes`/`cfgrib`) runs inside a Linux container, so the Windows host never needs the native library:

```powershell
docker compose -f docker-compose.yml -f docker-compose.model.yml up -d --build
```

The `meteo-model` service fetches the latest GFS run, stores it, and applies bias correction every hour (`MODEL_FETCH_INTERVAL`). Bronze → `./data`, trained models → `./models` (bind-mounted).

For existing databases created before this feature, run:

```powershell
Get-Content scripts/migrate_nwp.sql | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo
```

### 2. Run manually (optional)

One fetch+correct cycle on demand (needs `pip install -e ".[model]"` locally, or run in the container):

```powershell
docker compose -f docker-compose.yml -f docker-compose.model.yml run --rm meteo-model meteo-model-fetch
```

### 3. Train the bias-correction models

Correction is a **cold start**: until `BIAS_CORRECTION_MIN_SAMPLES` matched (GFS forecast, observation) pairs exist, `gfs_corrected` passes raw GFS through unchanged. Keep Approach 1 **or** 2 running to accumulate observations, then:

```powershell
# LightGBM (already a core dependency) needs no GRIB tooling, so training runs on the host too
meteo-model-train
```

Models are saved per location + target to `./models/<location_id>/gfs_correction_<target>.pkl`. Re-run periodically as data grows.

### 4. Serve

```powershell
meteo-serve
Invoke-RestMethod http://localhost:8000/forecast/minsk       # raw vs corrected
Invoke-RestMethod http://localhost:8000/predict/minsk?model_version=gfs_corrected
Invoke-RestMethod "http://localhost:8000/eval/minsk?hours=240" # fair per-model accuracy
```

### 5. Compare against an AI model (ECMWF AIFS)

Pull ECMWF's AIFS AI model (served free by Open-Meteo) as another `model_version` — no GPU, no GRIB, host-runnable:

```powershell
meteo-ai-fetch                                # -> nwp_forecasts(aifs) + predictions(aifs_raw)
Invoke-RestMethod "http://localhost:8000/eval/minsk"   # aifs_raw vs gfs_raw vs gfs_corrected vs baseline
```

`GET /eval/{location}?hours=` scores the **latest forecast per (model, valid_time)** against observations (MAE / RMSE / signed bias per variable), so short-lead re-forecasts don't distort the ranking. This turns the shared `model_version` schema into a real multi-model benchmark: physics (GFS) vs AI (AIFS) vs locally-corrected vs Open-Meteo blend.

### Approaches 1/2 vs 3

|               | Approach 1/2                     | Approach 3                                      |
| ------------- | -------------------------------- | ----------------------------------------------- |
| Source        | Open-Meteo point forecast (JSON) | GFS gridded model output (GRIB2)                 |
| Prediction    | forecast passthrough (baseline)  | GFS + trained local bias correction             |
| Entrypoints   | `meteo-ingest` / stream-\*       | `meteo-model-fetch`, `meteo-model-train`        |
| Serving       | `/predict`                       | `/forecast` (+ `/predict?model_version=`)       |
| Run together? | pick one ingest path             | complementary — needs 1/2's observations to train |

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
  alerter.py               # rule-based alerts → DB + weather.alerts
  rules.py                 # alert rule loader/evaluator
  schemas.py               # message validation
  kafka_client.py          # producer/consumer factories
config/alerts.yaml         # alert thresholds
grafana/                   # live dashboard + TimescaleDB datasource
src/meteo_model/           # Approach 3 — model-centric
  sources/gfs.py           # GFS GRIB fetch (Herbie) + nearest grid cell
  extract.py               # unit/wind conversions (pure, testable)
  correct/dataset.py       # join nwp<->obs -> residual training frame
  correct/train.py         # LightGBM bias models per location/target
  correct/predict.py       # apply correction -> gfs_raw + gfs_corrected
  pipeline.py / flows.py   # one fetch+correct cycle (Prefect-wrapped)
docker-compose.model.yml   # GFS runner overlay (Approach 3)
Dockerfile.model           # Linux image with eccodes for GRIB decoding
```



## Environment variables

See `.env`. Key settings:

- `USE_LOCAL_BRONZE=true` — skip MinIO, write to disk (simplest local dev)
- `INGEST_LOOKBACK_HOURS` — history window for features (default 48)
- `OPENWEATHER_API_KEY` — optional backup source
- `KAFKA_BOOTSTRAP_SERVERS` — Redpanda/Kafka address (Approach 2)
- `PRODUCER_POLL_INTERVAL_SECONDS` — producer poll interval, default 600 (10 min)
- `ALERT_WEBHOOK_URL` — optional HTTP POST on alert fire
- `ALERT_COOLDOWN_SECONDS` — suppress duplicate alerts per rule/location (default 1800)

