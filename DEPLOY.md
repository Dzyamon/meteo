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

## Part 0 — Serverless scaffolding (already in the repo)

This branch is serverless-ready — no code changes needed, just env vars (Part 3):

- **`api/index.py`** — Vercel Python entrypoint exposing the FastAPI ASGI `app`.
- **`vercel.json`** — rewrites every path to that function, plus the two Cron jobs.
- **`/cron/fetch` and `/cron/train`** endpoints in the API (lazy-imported so serving
  stays light), protected by `CRON_SECRET`. `fetch` pulls the Open-Meteo models and
  re-corrects; `train` retrains correction + ensemble and re-selects champions.
- **GFS via Open-Meteo** — set `OPENMETEO_MODELS=...,gfs_seamless:gfs`; `ai_pipeline`
  writes `nwp_forecasts(model='gfs')` + `predictions('gfs_raw')`, consumed by the
  existing correction/ensemble/champion code unchanged. (The GRIB path is simply not
  invoked.)
- **DB-backed model store** — `MODEL_STORE=db` persists trained models as bytes in a
  `model_artifacts` table, so they survive across stateless invocations (serverless
  disk is ephemeral). `MODEL_STORE=disk` (default) keeps the container behaviour.
- **Portable schema** — `scripts/init_db.sql` guards the `create_hypertable` calls, so
  it runs on plain Supabase Postgres and on TimescaleDB alike.
- **Tiny pools** — `DB_POOL_MIN_SIZE`/`DB_POOL_MAX_SIZE` env vars; set `0`/`1` for the
  serverless + Supabase-pooler pattern.
- **`requirements.txt`** — the lean runtime subset (no prefect/polars/kafka/GRIB).

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
MODEL_STORE=db                  # persist models in Postgres (serverless-safe)
DB_POOL_MIN_SIZE=0
DB_POOL_MAX_SIZE=1
CRON_SECRET=<a long random string>   # Vercel sends it; the /cron endpoints verify it
NWP_FORECAST_HOURS=48
USE_LOCAL_BRONZE=true            # simplest; or set false + MINIO_* for Supabase Storage
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
