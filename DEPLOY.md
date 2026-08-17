# Deploy — fully managed on Vercel + Supabase (Open-Meteo everywhere)

A **serverless, zero-VM** deployment split across three free services:
- **Supabase** — Postgres (+ optional Storage).
- **Vercel** — the API + dashboard only (a tiny, read-only bundle).
- **GitHub Actions** — the scheduled pipeline (fetch / correct / train). The ML stack
  (numpy/scipy/sklearn/lightgbm) is far too big for a Vercel function, and Actions
  runners have no size limit and free cron — so the heavy work lives there.

**Every model is pulled from the Open-Meteo API** (GFS via `gfs_seamless`, AIFS, ICON)
— no GRIB, no `eccodes`, no Prefect worker, no container that has to stay running.

> This trades the raw-grid GFS pipeline (Approach 3's GRIB extraction) for
> Open-Meteo's point forecasts. For point predictions the values are equivalent;
> you lose only multi-point / spatial-grid operations. In exchange the whole system
> becomes managed and reboot-proof — the recurring "laptop was off, everything's
> Exited" problem disappears.

```
 GitHub Actions (cron)                    Vercel
 ┌───────────────────────┐        ┌────────────────────────┐
 │ fetch+correct  (6h)   │        │ FastAPI /forecast /eval │
 │ train+champion (1/day)│──┐  ┌──│ /best   +  dashboard /  │
 └───────────────────────┘  ▼  ▼  └────────────────────────┘
 Open-Meteo API ─────────► Supabase Postgres ◄─── (read)
 (gfs/aifs/icon)           (obs, predictions, model_artifacts)
```

## What changes vs. the container stack

| Container stack | This deployment |
|---|---|
| TimescaleDB container | **Supabase Postgres** (plain tables — hypertables optional) |
| MinIO bronze | **Supabase Storage** (S3) — or disable bronze |
| GFS via GRIB (`meteo-model-worker`, eccodes) | **GFS via Open-Meteo** (`gfs_seamless`) — a JSON call |
| Prefect server + worker + cron | **GitHub Actions** cron running the pipeline modules |
| `meteo-serve` container | **Vercel Python serverless** (FastAPI) + static dashboard |

---

## Part 0 — Serverless scaffolding (already in the repo)

This branch is serverless-ready — no code changes needed, just env vars (Part 3):

- **`api/index.py`** — Vercel Python entrypoint exposing the FastAPI ASGI `app`.
- **`vercel.json`** — rewrites every path to that function (no crons: scheduling is on
  GitHub Actions).
- **`.github/workflows/pipeline.yml`** — the scheduler: `fetch + correct` every 6h and
  `train + champion` daily, running the pipeline modules against Supabase. Uses the
  `requirements-pipeline.txt` (full ML stack) — no Vercel size limit applies here.
- **`requirements.txt` vs `requirements-pipeline.txt`** — Vercel installs only the
  lean serving deps (fastapi/psycopg/pydantic/httpx); the heavy ML deps are pipeline-only.
- **`/cron/fetch` and `/cron/train`** endpoints also exist in the API (lazy-imported,
  `CRON_SECRET`-guarded) — usable if you later run on Vercel **Pro** with the full deps,
  but the free path uses GitHub Actions instead.
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
3. *(Optional)* Create a Storage bucket `meteo-bronze` for raw payloads; grab the S3
   credentials (Storage → Settings). Not required — `USE_LOCAL_BRONZE=true` skips it.

## Part 2 — Vercel (serving)

1. Import the Git repo. Framework preset: **Other**; root is the repo. `vercel.json` is
   already in the repo (rewrites all paths to the FastAPI app; no crons).
2. Set environment variables (Part 3).
3. Deploy — via the dashboard, or `vercel --prod` from a checkout of your branch. The
   dashboard is at `/`, the API at `/forecast`, `/eval`, `/best`, etc.

## Part 2b — GitHub Actions (the scheduler)

The pipeline runs from `.github/workflows/pipeline.yml` — no setup beyond one secret:

1. Repo → **Settings → Secrets and variables → Actions** → add secret **`DATABASE_URL`**
   = your Supabase pooler URI (same value as Vercel's).
2. Enable Actions if prompted. The workflow then runs **fetch+correct every 6h** and
   **train+champion daily**; you can also trigger it manually (**Actions → pipeline →
   Run workflow**) to bootstrap immediately.

> GitHub Actions cron is free and 6-hourly — it avoids Vercel Hobby's once-a-day cron
> limit *and* the function-size limit (the ML stack installs on the runner, not Vercel).

## Part 3 — Environment variables

Set these in **Vercel** (serving) — only `DATABASE_URL` is strictly required there,
the rest have defaults:
```
DATABASE_URL=postgresql://postgres.<ref>:<pw>@aws-0-<region>.pooler.supabase.com:6543/postgres
DB_POOL_MIN_SIZE=0
DB_POOL_MAX_SIZE=1
```
The **GitHub Actions** workflow already sets `OPENMETEO_MODELS` (with `gfs_seamless:gfs`),
`MODEL_STORE=db`, `USE_LOCAL_BRONZE=true`, `NWP_FORECAST_HOURS`, and
`BIAS_CORRECTION_MIN_SAMPLES` inline — only its `DATABASE_URL` comes from the repo secret.

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
- Letting the **GitHub Actions fetch job accumulate model forecasts forward** — the
  correction needs ~300 (forecast, obs) pairs, reached in a couple of weeks of
  6-hourly runs. (Correction/ensemble stay in cold-start passthrough until then.)

## Part 5 — Verify

```
curl https://<app>.vercel.app/health
curl https://<app>.vercel.app/eval/minsk?hours=336
curl https://<app>.vercel.app/best/minsk
# open https://<app>.vercel.app/  for the dashboard
```
Check **GitHub → Actions** that the `pipeline` runs are green (trigger one manually
first to bootstrap).

## Part 6 — Limits, caveats, cost

- **All free:** Supabase free (500 MB DB — the daily job keeps it from pausing),
  Vercel Hobby (serving only), and GitHub Actions free minutes cover a single location
  comfortably.
- **Why the split:** the ML stack (numpy/scipy/sklearn/lightgbm) is ~600 MB — it blows
  past Vercel's function-size limit. Keeping it on GitHub Actions (no size limit, free
  6-hourly cron) is what makes the whole thing fit the free tiers. Vercel's bundle is
  just fastapi + psycopg + a few small libs.
- **GitHub Actions caveats:** scheduled runs can start a few minutes late, and the
  workflow auto-disables after ~60 days of *zero repo activity* (a commit or a manual
  run re-arms it).
- **Connections:** always use the Supabase **pooler (6543)** from Vercel; never the
  direct 5432 (connection exhaustion).
- **No raw grid:** point forecasts only. If you later need multi-point/spatial GFS,
  that requires the GRIB path on a long-running host (see the container stack).

---

**Summary:** with the Part-0 adaptations (mostly config), this stack runs entirely on
Supabase + Vercel free/Pro tiers, fetching GFS/AIFS/ICON from Open-Meteo, training the
correction/ensemble on schedule, and serving `/forecast` `/eval` `/best` + the
dashboard — no servers to keep alive.
