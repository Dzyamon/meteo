# Deploy — fully managed on Vercel + Supabase (Open-Meteo everywhere)

A **serverless, zero-VM** deployment: Supabase hosts Postgres (+ optional Storage),
Vercel hosts the API/dashboard and runs the scheduled jobs via Cron. **Every model
is pulled from the Open-Meteo API** (GFS, AIFS, ICON) — no GRIB, no `eccodes`, no
Prefect worker, no container that has to stay running.

> This trades the raw-grid GFS pipeline (Approach 3's GRIB extraction) for
> Open-Meteo's point forecasts. For point predictions the values are equivalent;
> you lose only multi-point / spatial-grid operations. In exchange the whole system
> becomes managed and reboot-proof — the recurring "laptop was off, everything's
> Exited" problem disappears.

```
                 ┌──────────── Vercel ────────────┐
 Open-Meteo API →│ Cron: fetch (6h)  → functions  │→ Supabase Postgres
 (gfs/aifs/icon) │ Cron: train (1/day)            │      ▲
                 │ FastAPI /forecast /eval /best   │──────┘ (read)
                 │ static dashboard  (/)           │
                 └────────────────────────────────┘
```

## What changes vs. the container stack

| Container stack | This deployment |
|---|---|
| TimescaleDB container | **Supabase Postgres** (plain tables — hypertables optional) |
| MinIO bronze | **Supabase Storage** (S3) — or disable bronze |
| GFS via GRIB (`meteo-model-worker`, eccodes) | **GFS via Open-Meteo** (`gfs_seamless`) — a JSON call |
| Prefect server + worker + cron | **Vercel Cron** → serverless function endpoints |
| `meteo-serve` container | **Vercel Python serverless** (FastAPI) + static dashboard |

---

## Part 0 — Required code adaptations (small, one-time)

The repo is container-first; a few changes make it serverless-ready. Do these on a
deploy branch:

1. **GFS from Open-Meteo.** No code change needed beyond config — set the env var
   (Part 2). `ai_pipeline` already fetches any Open-Meteo model via
   `openmeteo_models`; adding `gfs_seamless:gfs` makes it write `nwp_forecasts(model='gfs')`
   + `predictions('gfs_raw')`, which the existing correction/ensemble/champion code
   consumes unchanged. **Do not run** `meteo-model-fetch` (the GRIB path).
2. **Guard the hypertable calls.** Supabase may not have `timescaledb`. In
   `scripts/init_db.sql`, the tables are standard Postgres; run the `CREATE TABLE`
   statements and simply skip the `SELECT create_hypertable(...)` lines (or wrap them
   so a missing extension is non-fatal). Everything works as plain tables.
3. **Connection pooling for serverless.** Point `DATABASE_URL` at Supabase's
   **pooler** (Supavisor, transaction mode, port `6543`), and keep pool sizes tiny —
   in `TimescaleStore` set `min_size=0, max_size=1` (each invocation is ephemeral).
4. **A Vercel entrypoint for FastAPI** — `api/index.py`:
   ```python
   from meteo.api.main import app  # Vercel's Python runtime serves this ASGI app
   ```
5. **Cron handler functions** (thin wrappers Vercel Cron can hit) — e.g. `api/cron/fetch.py`
   and `api/cron/train.py`:
   ```python
   # api/cron/fetch.py  -> fetch all Open-Meteo models, then re-correct
   from meteo_model.ai_pipeline import run_cycle as fetch_all
   from meteo_model.correct.predict import predict_all
   def handler(request):
       fetch_all(); predict_all()
       return {"statusCode": 200, "body": "ok"}
   ```
   ```python
   # api/cron/train.py  -> retrain correction + ensemble, re-select champions
   from meteo_model.correct.train import train_all as corr_train
   from meteo_model.ensemble.train import train_all as ens_train
   from meteo_model.ensemble.predict import predict_all as ens_predict
   from meteo_model.champion import select_all
   def handler(request):
       corr_train(); ens_train(); ens_predict(); select_all()
       return {"statusCode": 200, "body": "ok"}
   ```
   (Exact handler signature follows Vercel's current Python runtime docs.)
6. **Model persistence.** Trained models are written to `MODEL_DIR` (local disk),
   which is ephemeral on serverless. Store them in **Supabase Storage** instead (a
   small change to `correct/storage.py` / `ensemble/storage.py` to read/write the
   bucket), or fold prediction into the train job so a model is used in the same
   invocation it's trained. Simplest: the daily `train` cron trains **and** predicts
   in one run, so nothing needs to persist between invocations.

> Item 6 is the only non-trivial one; the rest are config or ~3-line files.

---

## Part 1 — Supabase

1. Create a project → note the **connection string** (Project Settings → Database).
   Use the **Transaction pooler** URI (host `...pooler.supabase.com`, port `6543`).
2. Apply the schema (tables only). From the SQL editor, paste the `CREATE TABLE`
   blocks from `scripts/init_db.sql` and `scripts/migrate_nwp.sql` /
   `scripts/migrate_ensemble.sql`, omitting the `create_hypertable` lines.
3. *(Optional)* Create a Storage bucket `meteo-bronze` for raw payloads and model
   artifacts; grab the S3 credentials (Storage → Settings).
4. *(Optional)* Enable `pg_cron` if you prefer DB-side scheduling over Vercel Cron.

## Part 2 — Vercel

1. Import the Git repo. Framework preset: **Other**; root is the repo.
2. Add a `vercel.json` (crons + Python function budgets):
   ```json
   {
     "functions": { "api/**/*.py": { "runtime": "python3.12", "maxDuration": 60 } },
     "crons": [
       { "path": "/api/cron/fetch", "schedule": "30 5,11,17,23 * * *" },
       { "path": "/api/cron/train", "schedule": "0 6 * * *" }
     ]
   }
   ```
3. Set environment variables (Part 3).
4. Deploy. The dashboard is served at `/` (FastAPI's `GET /`), the API under `/api/...`.

## Part 3 — Environment variables (Vercel project settings)

```
DATABASE_URL=postgresql://postgres.<ref>:<pw>@aws-0-<region>.pooler.supabase.com:6543/postgres
OPENMETEO_MODELS=ecmwf_aifs025_single:aifs,icon_seamless:icon,gfs_seamless:gfs
NWP_FORECAST_HOURS=48
USE_LOCAL_BRONZE=false          # or point MINIO_* at Supabase Storage's S3 endpoint
MODEL_DIR=/tmp/models           # ephemeral; see Part 0 item 6
BIAS_CORRECTION_MIN_SAMPLES=300
```

## Part 4 — Bootstrap history

Serverless can't run the slow GRIB backfill, and Open-Meteo doesn't serve deep
historical *forecasts*. Bootstrap by:
- Backfilling **observations** (ground truth) once via the archive — run
  `scripts/backfill_observations.py --days 60` locally against the Supabase
  `DATABASE_URL` (host-runnable, no GRIB).
- Letting the **fetch cron accumulate model forecasts forward** — the correction
  needs ~300 (forecast, obs) pairs, reached in a couple of weeks of 6-hourly runs.
  (Correction/ensemble stay in cold-start passthrough until then — by design.)

## Part 5 — Verify

```
curl https://<app>.vercel.app/health
curl https://<app>.vercel.app/eval/minsk?hours=336
curl https://<app>.vercel.app/best/minsk
# open https://<app>.vercel.app/  for the dashboard
```
Check Vercel → Cron logs that `fetch`/`train` runs return 200.

## Part 6 — Limits, caveats, cost

- **Free tiers:** Supabase free (500 MB DB, pauses after ~1 week idle — the crons
  keep it active) and Vercel Hobby cover a single location comfortably. **Cron on
  Hobby is limited** (few jobs, coarse cadence); 6-hourly fetch wants Vercel **Pro**,
  or use Supabase `pg_cron` instead.
- **Function duration:** Open-Meteo fetches (~1s) and Ridge training (seconds) fit the
  60s budget easily. This is *only* possible because there's no GRIB fetch.
- **LightGBM bundle size:** the bias-correction still uses LightGBM; verify its wheel
  fits Vercel's function size limit, or switch it to sklearn `HistGradientBoostingRegressor`.
- **Connections:** always use the Supabase pooler (6543) from serverless; never the
  direct 5432 (connection exhaustion).
- **No raw grid:** point forecasts only. If you later need multi-point/spatial GFS,
  that requires the GRIB path on a long-running host (see the container stack).

---

**Summary:** with the Part-0 adaptations (mostly config), this stack runs entirely on
Supabase + Vercel free/Pro tiers, fetching GFS/AIFS/ICON from Open-Meteo, training the
correction/ensemble on schedule, and serving `/forecast` `/eval` `/best` + the
dashboard — no servers to keep alive.
