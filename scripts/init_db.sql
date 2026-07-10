CREATE EXTENSION IF NOT EXISTS timescaledb;

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

SELECT create_hypertable('observations', 'time', if_not_exists => TRUE);

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

SELECT create_hypertable('features', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_features_location_time
    ON features (location_id, time DESC);

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
    PRIMARY KEY (created_at, location_id, horizon_hours)
);

SELECT create_hypertable('predictions', 'created_at', if_not_exists => TRUE);
