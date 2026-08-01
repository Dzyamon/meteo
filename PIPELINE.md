# Full pipeline — from zero to best-available forecast

End-to-end steps for the complete system: observations (Approach 1/2) → NWP + AI
forecasts (Approach 3 + AIFS) → local bias correction → **stacking ensemble** →
**champion selection** → serving. Commands are PowerShell (Windows dev host).

```
Open-Meteo obs ─┐
GFS (physics) ──┤   nwp_forecasts ─► bias correction ─► gfs_corrected ─┐
AIFS (AI) ──────┘                                        aifs_raw ──────┼─► ENSEMBLE (stacking) ─► ensemble
                                                         gfs_raw ───────┤
observations (truth) ──────────────────────────────────► baseline ─────┘        │
        │                                                                        ▼
        └────────────────────────► /eval scores every model ─────► CHAMPION per (location, variable)
                                                                                 │
                                                                    /best  ◄──────┘  (serve the winner per variable)
```

## 0. Prerequisites
```powershell
Copy-Item .env.example .env      # first time
pip install -e ".[dev]"          # host: train/predict/serve (no GRIB needed)
# GFS fetch needs the [model] extra — it runs in the container, so you don't install it locally
```

## 1. Infrastructure
```powershell
docker compose -f docker-compose.yml -f docker-compose.model.yml up -d --build
```
Brings up TimescaleDB (:5432), MinIO (:9000, console :9001), Prefect server (:4200), and the model worker. Schema auto-applies on a fresh DB volume. For an existing volume, run the migrations:
```powershell
Get-Content scripts/migrate_nwp.sql       | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo
Get-Content scripts/migrate_ensemble.sql  | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo
```

## 2. Bootstrap training history
The correction and ensemble need matched (forecast, observation) pairs. AWS keeps only ~10 days of GFS, so backfill what's available and then let it accumulate forward.
```powershell
# observations (ERA5 archive, host, fast) — years available
python scripts/backfill_observations.py --days 60
# historical GFS forecasts (container, ~10-day AWS limit, slow)
docker compose -f docker-compose.yml -f docker-compose.model.yml run --rm `
    meteo-model-worker python scripts/backfill_gfs.py --days 8 --horizons 48
```

## 3. Register the schedules (once)
```powershell
$env:PREFECT_API_URL = "http://localhost:4200/api"
prefect deploy --all
```
Standing cadence (UTC), all on `model-pool` (the container worker):

| Deployment | Cron | Produces |
|---|---|---|
| `obs-ingest-6h` | `0 5,11,17,23` | observations, features, baseline |
| `gfs-fetch-6h` | `30 5,11,17,23` | nwp_forecasts(gfs), gfs_raw, gfs_corrected |
| `aifs-fetch-6h` | `35 5,11,17,23` | nwp_forecasts(aifs), aifs_raw |
| `model-train-daily` | `0 6` | retrain correction (holdout + gate) |
| `ensemble-daily` | `20 6` | ensemble train + predict + **champion select** |

That's the whole pipeline running unattended. The steps below are the same stages run **manually** (for understanding / one-offs).

## 4. Run each stage manually
```powershell
# --- ingest observations (ground truth) ---
docker compose -f docker-compose.yml -f docker-compose.model.yml run --rm meteo-model-worker meteo-ingest

# --- Approach 3: fetch model forecasts (container) ---
docker compose ... run --rm meteo-model-worker meteo-model-fetch    # GFS -> gfs_raw + gfs_corrected
meteo-ai-fetch                                                       # AIFS -> aifs_raw (host, no GRIB)

# --- bias correction (host, no GRIB) ---
python -m meteo_model.correct.train      # holdout-validated; deploys per-target only if it beats raw
python -m meteo_model.correct.predict    # re-apply correction to the latest forecast

# --- ensemble + champion (host, no GRIB) ---
python -m meteo_model.ensemble.train     # stack members; deploys only if it beats the best single member
python -m meteo_model.ensemble.predict   # write model_version=ensemble for future horizons
python -m meteo_model.champion           # score all models, persist champion per (location, variable)
```

## 5. Serve & inspect
```powershell
meteo-serve   # http://localhost:8000/docs
Invoke-RestMethod "http://localhost:8000/best/minsk"                      # champion per variable, merged
Invoke-RestMethod "http://localhost:8000/champions/minsk"                 # who won each variable + MAE
Invoke-RestMethod "http://localhost:8000/eval/minsk?hours=336"            # full leaderboard (MAE/RMSE/bias)
Invoke-RestMethod "http://localhost:8000/forecast/minsk"                  # gfs_raw vs gfs_corrected
Invoke-RestMethod "http://localhost:8000/predict/minsk?model_version=ensemble"
```
Other UIs: Prefect **http://localhost:4200** (runs/schedules), MinIO **http://localhost:9001** (bronze), and:
```powershell
docker exec meteo-timescaledb-1 psql -U meteo -d meteo -c "SELECT model_version,count(*) FROM predictions GROUP BY model_version;"
docker exec meteo-timescaledb-1 psql -U meteo -d meteo -c "SELECT * FROM model_champions;"
```

## Principles recap
- **gfs_raw** — physics NWP (NOAA), downloaded as GRIB.
- **gfs_corrected** — gfs_raw + LightGBM local bias correction trained on your observations.
- **aifs_raw** — ECMWF's AI model (Open-Meteo), a genuine neural-net forecast.
- **ensemble** — a stacking model that blends the above; wins when members have complementary errors.
- **champion** — the lowest-MAE model per (location, variable), chosen from `/eval` and served by `/best`.

Every model lands in the shared `predictions` table keyed by `model_version`, scored against `observations`. That single design is what makes ensembling, benchmarking, and champion selection possible.
