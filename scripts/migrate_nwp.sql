-- Migration for existing databases: add Approach 3 (model-centric) NWP forecast store.
-- Run: Get-Content scripts/migrate_nwp.sql | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo

CREATE TABLE IF NOT EXISTS nwp_forecasts (
    run_time        TIMESTAMPTZ NOT NULL,
    valid_time      TIMESTAMPTZ NOT NULL,
    location_id     TEXT        NOT NULL,
    model           TEXT        NOT NULL,
    horizon_hours   SMALLINT    NOT NULL,
    temperature_c   DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_ms   DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,
    humidity_pct    DOUBLE PRECISION,
    pressure_hpa    DOUBLE PRECISION,
    cloud_cover_pct DOUBLE PRECISION,
    PRIMARY KEY (run_time, valid_time, location_id, model)
);

SELECT create_hypertable('nwp_forecasts', 'run_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_nwp_location_valid
    ON nwp_forecasts (location_id, model, valid_time DESC);

-- Approach 3 stores several model_versions (gfs_raw, gfs_corrected) per cycle,
-- so model_version must be part of the predictions primary key.
ALTER TABLE predictions DROP CONSTRAINT IF EXISTS predictions_pkey;
ALTER TABLE predictions
    ADD PRIMARY KEY (created_at, location_id, horizon_hours, model_version);
