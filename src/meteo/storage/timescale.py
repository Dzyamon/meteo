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

    def latest_predictions(self, location_id: str) -> list[dict]:
        sql = """
            SELECT DISTINCT ON (horizon_hours)
                valid_time, horizon_hours, temperature_c, precipitation_mm, wind_speed_ms, model_version, created_at
            FROM predictions
            WHERE location_id = %s
            ORDER BY horizon_hours, created_at DESC
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id,))
                return cur.fetchall()

    def close(self) -> None:
        self._pool.close()
