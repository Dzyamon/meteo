local nowcasting (rain, wind, temperature), 15‑minute latency, using APIs + reanalysis + ML models like GraphCast, on free cloud tiers or local Docker—here are the most interesting data‑engineering approaches, with trade‑offs and concrete recommendations.
1) Architecture patterns to consider
A. Medallion (bronze/silver/gold) + batch + micro‑batches

Idea: Treat weather data like any other big‑data domain: raw → cleaned → feature‑ready, with frequent small batches to meet 15‑min latency.

    Bronze (raw):

        Store API responses and downloaded model files as‑is (JSON/Parquet/NetCDF).

    Silver (cleaned):

        Validate, de‑duplicate, align timestamps, handle missing data.

    Gold (features):

        Compute nowcasting features: rolling stats, lags, radar composites, station aggregates, etc.

Tech stack (local/Docker or free cloud):

    Ingestion: Python scripts or FastAPI workers.

    Orchestration: Prefect or Airflow in Docker (or managed free tier if available).

    Processing: Spark (local mode) or Polars/Dask if data volume is moderate.

    Storage:

        Local: MinIO (S3‑compatible) + PostgreSQL/TimescaleDB.

        Cloud free tier: e.g., Oracle Cloud free VM + local MinIO; or GCP/AWS free credits.

This pattern is used in student/prototype weather pipelines with Open‑Meteo and Spark, storing time‑series in InfluxDB and visualizing in Grafana.

Why it fits your case:

    You can run micro‑batch jobs every 5–15 minutes to keep latency low.

    Clean separation lets you experiment with different feature sets for nowcasting without touching raw data.

B. Streaming pipeline (Kafka / Redpanda + stream processing)

Idea: Treat observations and API outputs as event streams; process them in near‑real time.

    Producers:

        Poll APIs (Open‑Meteo, OpenWeather, etc.) every 5–15 minutes.

        Optionally ingest radar tiles or local station data.

    Stream:

        Kafka or Redpanda (lighter, Kafka‑compatible) in Docker.

    Consumers:

        Stream processors (Spark Structured Streaming, Flink, or custom Python) that:

            Validate, aggregate, and write to:

                Time‑series DB (TimescaleDB, InfluxDB).

                Or a feature store for ML.

Real‑time weather pipelines with Kafka + MongoDB/Cassandra show this pattern works well for sub‑hourly latencies.

Why it fits:

    Naturally supports ≤15‑minute latency.

    Easy to scale if you add more locations or data sources.

    Good base if you later want to serve live dashboards or alerts.

Trade‑off:

    More moving parts than batch; higher operational complexity.

    Better if you truly need streaming semantics; otherwise micro‑batch may be enough.

C. “Model‑centric” pipeline around global AI models (GraphCast, etc.)

Idea: Use global AI models as a backbone, then add local correction/nowcasting on top.

    Ingest:

        Global model outputs:

            GraphCast/WeatherNext, Pangu, FourCastNet, etc. (from open repos or your own runs).

            Traditional NWP (GFS, HRRR) via bulk downloads.

        Local observations via APIs.

    Transform:

        Downscale global fields to your region (interpolation, bias correction).

        Build local features (station history, radar, topography).

    Train:

        A local ML model (e.g., gradient boosting, small transformer, U‑Net for radar) that:

            Takes global forecasts + local recent observations.

            Predicts local rain/wind/temp for 0–6 hours.

Research shows ML can locally improve existing forecasts from meteorological providers.

Why it fits:

    You leverage powerful global models without having to train them from scratch.

    Local model focuses on what matters: site‑specific bias, micro‑climate, and nowcasting.

Data‑engineering angle:

    Store:

        Global model grids (NetCDF/Zarr) in object storage.

        Local features in a time‑series DB or feature store.

    Create datasets that join:

        “Global forecast at t” + “Local obs up to t” → “Local obs at t+Δt”.


2) Data sources aligned with your goals
For local nowcasting (rain, wind, temp)

    Open‑Meteo (highly recommended):

        No API key, generous free limits, good for prototyping.

        Provides historical + forecast data; you can poll frequently.

    OpenWeatherMap:

        Free tier: 60 calls/min, current + 5‑day forecast.

        Good for current conditions and short‑term forecasts.

    Radar / precipitation nowcasting:

        NEXRAD (US) via nexradaws or similar.

        EUMETSAT / national radar for other regions (often bulk files).

        Some nowcasting repos already integrate radar data for video‑style prediction.

For reanalysis & global models

    ERA5:

        Via ECMWF API or Copernicus Data Store.

        Use as training target / climatology.

    GFS / GEFS:

        NOAA sources (bulk GRIB/NetCDF).

    AI model weights:

        GraphCast/WeatherNext, Pangu, FourCastNet, Aurora, etc. from GitHub/Hugging Face.

        You can run them locally (GPU recommended) to generate global forecasts for your region, then downscale.

This matches your desire to combine APIs + reanalysis + model weights.
3) Meeting 15‑minute latency

To guarantee ≤15‑minute end‑to‑end latency:

    Ingestion frequency: Poll APIs every 5–10 minutes (respecting rate limits).

    Processing:

        Keep transformations lightweight for the “live” path (cleaning + a few key features).

        Run heavier feature engineering asynchronously (e.g., every hour).

    Serving:

        Store latest features in a low‑latency DB (TimescaleDB, Redis, or in‑memory cache).

        Your model service reads from there and returns predictions in <1s.

Two viable patterns:

    Micro‑batch every 5–15 minutes (simpler):

        Airflow/Prefect job:

            Fetch new data.

            Update silver/gold tables.

            Trigger model inference.

    Streaming with Kafka/Redpanda (more complex but more flexible):

        Continuous ingestion and processing; downstream consumers always see near‑current data.

For a first version, micro‑batch is often enough and easier to run on free tiers or a single Docker host.
4) Free cloud vs local Docker
Local Docker (very feasible)

You can run everything on one machine (or a cheap VM):

    Containers:

        Ingestion service (FastAPI or cron‑based script).

        Prefect/Airflow server.

        MinIO (S3‑compatible object storage).

        PostgreSQL + TimescaleDB (or InfluxDB).

        Kafka/Redpanda (if you go streaming).

        Model service (FastAPI + PyTorch/TF).

    Pros:

        Full control, no vendor lock‑in.

        Easy to develop/test locally.

    Cons:

        You manage backups, updates, scaling.

There are example repos showing end‑to‑end MLOps for weather with Dockerized components.
Free cloud tiers

Possible options:

    Oracle Cloud Free Tier:

        Always‑free VMs (ARM/x86), enough for a small pipeline.

    GCP / AWS free credits:

        Use for a few months while you prototype.

    Specialized free APIs:

        Open‑Meteo has very generous free usage and even open data on S3 that you can pull directly.

A realistic pattern:

    Run ingestion + processing on a free VM.

    Store data on:

        Local disk / MinIO on that VM, or

        Free object storage (if available).

    Use Docker Compose to define all services.