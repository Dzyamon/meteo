-- Portable schema: works on TimescaleDB (container) and plain Postgres (Supabase).
-- The hypertable calls are optional accelerations — guarded so a missing
-- timescaledb extension is a no-op, leaving standard tables that behave the same.
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'timescaledb unavailable — using plain tables';
END $$;

-- Silver: cleaned observations aligned to location + timestamp
CREATE TABLE IF NOT EXISTS observations (
    time        TIMESTAMPTZ NOT NULL,
    location_id TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    temperature_c     DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    wind_speed_ms     DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,
    humidity_pct      DOUBLE PRECISION,
    pressure_hpa      DOUBLE PRECISION,
    cloud_cover_pct   DOUBLE PRECISION,
    PRIMARY KEY (time, location_id, source)
);

DO $$ BEGIN PERFORM create_hypertable('observations', 'time', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_observations_location_time
    ON observations (location_id, time DESC);

-- Gold: engineered features for ML
CREATE TABLE IF NOT EXISTS features (
    time        TIMESTAMPTZ NOT NULL,
    location_id TEXT        NOT NULL,
    temperature_c_lag_1     DOUBLE PRECISION,
    temperature_c_lag_4     DOUBLE PRECISION,
    temperature_c_roll_3    DOUBLE PRECISION,
    temperature_c_roll_12   DOUBLE PRECISION,
    temperature_c_diff_1    DOUBLE PRECISION,
    precipitation_mm_lag_1  DOUBLE PRECISION,
    precipitation_mm_roll_3 DOUBLE PRECISION,
    precipitation_mm_roll_12 DOUBLE PRECISION,
    wind_speed_ms_lag_1     DOUBLE PRECISION,
    wind_speed_ms_roll_3    DOUBLE PRECISION,
    wind_speed_ms_roll_12   DOUBLE PRECISION,
    wind_speed_ms_diff_1    DOUBLE PRECISION,
    humidity_pct            DOUBLE PRECISION,
    pressure_hpa            DOUBLE PRECISION,
    PRIMARY KEY (time, location_id)
);

DO $$ BEGIN PERFORM create_hypertable('features', 'time', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_features_location_time
    ON features (location_id, time DESC);

-- Approach 3 (model-centric): raw NWP forecasts extracted at each location's grid cell
CREATE TABLE IF NOT EXISTS nwp_forecasts (
    run_time        TIMESTAMPTZ NOT NULL,   -- model cycle (e.g. GFS 00/06/12/18Z)
    valid_time      TIMESTAMPTZ NOT NULL,   -- time the forecast is valid for
    location_id     TEXT        NOT NULL,
    model           TEXT        NOT NULL,   -- 'gfs'
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

DO $$ BEGIN PERFORM create_hypertable('nwp_forecasts', 'run_time', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_nwp_location_valid
    ON nwp_forecasts (location_id, model, valid_time DESC);

-- Predictions served by the API
CREATE TABLE IF NOT EXISTS predictions (
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_time      TIMESTAMPTZ NOT NULL,
    location_id     TEXT        NOT NULL,
    horizon_hours   SMALLINT    NOT NULL,
    temperature_c   DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_ms   DOUBLE PRECISION,
    model_version   TEXT        NOT NULL DEFAULT 'baseline',
    PRIMARY KEY (created_at, location_id, horizon_hours, model_version)
);

DO $$ BEGIN PERFORM create_hypertable('predictions', 'created_at', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- Champion model per (location, variable): the best model_version chosen by
-- out-of-sample scoring, used to serve the "best-available" forecast.
CREATE TABLE IF NOT EXISTS model_champions (
    location_id   TEXT NOT NULL,
    variable      TEXT NOT NULL,
    model_version TEXT NOT NULL,
    mae           DOUBLE PRECISION,
    n_scored      INTEGER,
    window_hours  INTEGER,
    evaluated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (location_id, variable)
);

-- Trained model artifacts (joblib bytes), for the serverless model_store="db"
-- option where local disk is ephemeral.
CREATE TABLE IF NOT EXISTS model_artifacts (
    key        TEXT PRIMARY KEY,
    data       BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Alerts fired by the streaming alert service
CREATE TABLE IF NOT EXISTS alerts (
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    location_id     TEXT        NOT NULL,
    rule_id         TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    metric          TEXT        NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    threshold       DOUBLE PRECISION NOT NULL,
    message         TEXT        NOT NULL,
    observation_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (triggered_at, id)
);

DO $$ BEGIN PERFORM create_hypertable('alerts', 'triggered_at', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_alerts_location_time
    ON alerts (location_id, triggered_at DESC);
