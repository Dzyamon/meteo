-- Run on existing databases that were created before alerts support:
-- docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo < scripts/migrate_alerts.sql

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

SELECT create_hypertable('alerts', 'triggered_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_alerts_location_time
    ON alerts (location_id, triggered_at DESC);
