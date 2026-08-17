-- Migration: champion-model table for the ensemble + champion-selection service.
-- Run: Get-Content scripts/migrate_ensemble.sql | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo

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
