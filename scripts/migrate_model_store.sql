-- Migration: DB-backed model artifact store (serverless model_store="db").
-- Run: Get-Content scripts/migrate_model_store.sql | docker exec -i meteo-timescaledb-1 psql -U meteo -d meteo

CREATE TABLE IF NOT EXISTS model_artifacts (
    key        TEXT PRIMARY KEY,
    data       BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
