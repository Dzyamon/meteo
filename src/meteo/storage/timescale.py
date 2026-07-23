from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from meteo.clients.weather import ObservationRow
from meteo.config import get_settings


class TimescaleStore:
    def __init__(self) -> None:
        settings = get_settings()
        self._pool = ConnectionPool(
            conninfo=settings.database_url,
            min_size=1,
            max_size=5,
            kwargs={"row_factory": dict_row},
        )

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with self._pool.connection() as conn:
            yield conn

    def upsert_observations(self, rows: list[ObservationRow]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO observations (
                time, location_id, source,
                temperature_c, precipitation_mm, wind_speed_ms, wind_direction_deg,
                humidity_pct, pressure_hpa, cloud_cover_pct
            ) VALUES (
                %(time)s, %(location_id)s, %(source)s,
                %(temperature_c)s, %(precipitation_mm)s, %(wind_speed_ms)s, %(wind_direction_deg)s,
                %(humidity_pct)s, %(pressure_hpa)s, %(cloud_cover_pct)s
            )
            ON CONFLICT (time, location_id, source) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                precipitation_mm = EXCLUDED.precipitation_mm,
                wind_speed_ms = EXCLUDED.wind_speed_ms,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                humidity_pct = EXCLUDED.humidity_pct,
                pressure_hpa = EXCLUDED.pressure_hpa,
                cloud_cover_pct = EXCLUDED.cloud_cover_pct
        """
        payload = [row.__dict__ for row in rows]
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, payload)
            conn.commit()
        return len(rows)

    def upsert_features(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO features (
                time, location_id,
                temperature_c_lag_1, temperature_c_lag_4,
                temperature_c_roll_3, temperature_c_roll_12, temperature_c_diff_1,
                precipitation_mm_lag_1, precipitation_mm_roll_3, precipitation_mm_roll_12,
                wind_speed_ms_lag_1, wind_speed_ms_roll_3, wind_speed_ms_roll_12, wind_speed_ms_diff_1,
                humidity_pct, pressure_hpa
            ) VALUES (
                %(time)s, %(location_id)s,
                %(temperature_c_lag_1)s, %(temperature_c_lag_4)s,
                %(temperature_c_roll_3)s, %(temperature_c_roll_12)s, %(temperature_c_diff_1)s,
                %(precipitation_mm_lag_1)s, %(precipitation_mm_roll_3)s, %(precipitation_mm_roll_12)s,
                %(wind_speed_ms_lag_1)s, %(wind_speed_ms_roll_3)s, %(wind_speed_ms_roll_12)s, %(wind_speed_ms_diff_1)s,
                %(humidity_pct)s, %(pressure_hpa)s
            )
            ON CONFLICT (time, location_id) DO UPDATE SET
                temperature_c_lag_1 = EXCLUDED.temperature_c_lag_1,
                temperature_c_lag_4 = EXCLUDED.temperature_c_lag_4,
                temperature_c_roll_3 = EXCLUDED.temperature_c_roll_3,
                temperature_c_roll_12 = EXCLUDED.temperature_c_roll_12,
                temperature_c_diff_1 = EXCLUDED.temperature_c_diff_1,
                precipitation_mm_lag_1 = EXCLUDED.precipitation_mm_lag_1,
                precipitation_mm_roll_3 = EXCLUDED.precipitation_mm_roll_3,
                precipitation_mm_roll_12 = EXCLUDED.precipitation_mm_roll_12,
                wind_speed_ms_lag_1 = EXCLUDED.wind_speed_ms_lag_1,
                wind_speed_ms_roll_3 = EXCLUDED.wind_speed_ms_roll_3,
                wind_speed_ms_roll_12 = EXCLUDED.wind_speed_ms_roll_12,
                wind_speed_ms_diff_1 = EXCLUDED.wind_speed_ms_diff_1,
                humidity_pct = EXCLUDED.humidity_pct,
                pressure_hpa = EXCLUDED.pressure_hpa
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def fetch_observations_for_features(
        self,
        location_id: str,
        source: str = "open_meteo",
        limit: int = 72,
    ) -> list[dict]:
        sql = """
            SELECT *
            FROM observations
            WHERE location_id = %s AND source = %s
            ORDER BY time DESC
            LIMIT %s
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id, source, limit))
                rows = cur.fetchall()
        return list(reversed(rows))

    def save_predictions(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO predictions (
                created_at, valid_time, location_id, horizon_hours,
                temperature_c, precipitation_mm, wind_speed_ms, model_version
            ) VALUES (
                %(created_at)s, %(valid_time)s, %(location_id)s, %(horizon_hours)s,
                %(temperature_c)s, %(precipitation_mm)s, %(wind_speed_ms)s, %(model_version)s
            )
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def latest_features(self, location_id: str) -> dict | None:
        sql = """
            SELECT *
            FROM features
            WHERE location_id = %s
            ORDER BY time DESC
            LIMIT 1
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id,))
                return cur.fetchone()

    def latest_predictions(self, location_id: str, model_version: str | None = None) -> list[dict]:
        if model_version:
            sql = """
                SELECT DISTINCT ON (horizon_hours)
                    valid_time, horizon_hours, temperature_c, precipitation_mm, wind_speed_ms, model_version, created_at
                FROM predictions
                WHERE location_id = %s AND model_version = %s
                ORDER BY horizon_hours, created_at DESC
            """
            params = (location_id, model_version)
        else:
            sql = """
                SELECT DISTINCT ON (horizon_hours)
                    valid_time, horizon_hours, temperature_c, precipitation_mm, wind_speed_ms, model_version, created_at
                FROM predictions
                WHERE location_id = %s
                ORDER BY horizon_hours, created_at DESC
            """
            params = (location_id,)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def save_alert(self, row: dict) -> None:
        sql = """
            INSERT INTO alerts (
                id, triggered_at, location_id, rule_id, severity,
                metric, value, threshold, message, observation_time
            ) VALUES (
                %(id)s, %(triggered_at)s, %(location_id)s, %(rule_id)s, %(severity)s,
                %(metric)s, %(value)s, %(threshold)s, %(message)s, %(observation_time)s
            )
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, row)
            conn.commit()

    def recent_alert_exists(
        self,
        location_id: str,
        rule_id: str,
        within_seconds: int,
    ) -> bool:
        sql = """
            SELECT 1
            FROM alerts
            WHERE location_id = %s
              AND rule_id = %s
              AND triggered_at > NOW() - (%s * INTERVAL '1 second')
            LIMIT 1
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id, rule_id, within_seconds))
                return cur.fetchone() is not None

    def list_alerts(self, location_id: str | None = None, limit: int = 50) -> list[dict]:
        if location_id:
            sql = """
                SELECT *
                FROM alerts
                WHERE location_id = %s
                ORDER BY triggered_at DESC
                LIMIT %s
            """
            params = (location_id, limit)
        else:
            sql = """
                SELECT *
                FROM alerts
                ORDER BY triggered_at DESC
                LIMIT %s
            """
            params = (limit,)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def upsert_nwp_forecasts(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO nwp_forecasts (
                run_time, valid_time, location_id, model, horizon_hours,
                temperature_c, precipitation_mm, wind_speed_ms, wind_direction_deg,
                humidity_pct, pressure_hpa, cloud_cover_pct
            ) VALUES (
                %(run_time)s, %(valid_time)s, %(location_id)s, %(model)s, %(horizon_hours)s,
                %(temperature_c)s, %(precipitation_mm)s, %(wind_speed_ms)s, %(wind_direction_deg)s,
                %(humidity_pct)s, %(pressure_hpa)s, %(cloud_cover_pct)s
            )
            ON CONFLICT (run_time, valid_time, location_id, model) DO UPDATE SET
                horizon_hours = EXCLUDED.horizon_hours,
                temperature_c = EXCLUDED.temperature_c,
                precipitation_mm = EXCLUDED.precipitation_mm,
                wind_speed_ms = EXCLUDED.wind_speed_ms,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                humidity_pct = EXCLUDED.humidity_pct,
                pressure_hpa = EXCLUDED.pressure_hpa,
                cloud_cover_pct = EXCLUDED.cloud_cover_pct
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def fetch_nwp_training_pairs(self, location_id: str, model: str) -> list[dict]:
        """
        Join each NWP forecast to the observation that actually occurred at its
        valid_time, so a bias-correction model can learn (forecast -> residual).
        """
        sql = """
            SELECT
                f.valid_time, f.horizon_hours,
                f.temperature_c   AS nwp_temperature_c,
                f.precipitation_mm AS nwp_precipitation_mm,
                f.wind_speed_ms   AS nwp_wind_speed_ms,
                f.humidity_pct    AS nwp_humidity_pct,
                f.pressure_hpa    AS nwp_pressure_hpa,
                o.temperature_c   AS obs_temperature_c,
                o.precipitation_mm AS obs_precipitation_mm,
                o.wind_speed_ms   AS obs_wind_speed_ms
            FROM nwp_forecasts f
            JOIN observations o
              ON o.location_id = f.location_id
             AND o.source = 'open_meteo'
             AND o.time = f.valid_time
            WHERE f.location_id = %s AND f.model = %s
            ORDER BY f.valid_time
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id, model))
                return cur.fetchall()

    def fetch_latest_nwp_forecast(self, location_id: str, model: str) -> list[dict]:
        """Return every horizon of the most recent model run for a location."""
        sql = """
            SELECT *
            FROM nwp_forecasts
            WHERE location_id = %s AND model = %s
              AND run_time = (
                  SELECT MAX(run_time) FROM nwp_forecasts
                  WHERE location_id = %s AND model = %s
              )
            ORDER BY horizon_hours
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id, model, location_id, model))
                return cur.fetchall()

    def close(self) -> None:
        self._pool.close()
