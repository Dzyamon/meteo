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
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
            # Validate/reconnect a pooled connection before handing it out, so a
            # DB restart (or a long-idle connection) doesn't fail the next write.
            check=ConnectionPool.check_connection,
            # prepare_threshold=None disables server-side prepared statements —
            # required behind a transaction-mode pooler (Supabase Supavisor /
            # pgbouncer), which would otherwise raise DuplicatePreparedStatement.
            kwargs={"row_factory": dict_row, "prepare_threshold": None},
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

    def evaluate_models(
        self,
        location_id: str,
        since_hours: int = 168,
        source: str = "open_meteo",
    ) -> list[dict]:
        """Per-model forecast error vs observations, fairly scored.

        Uses the most recent forecast per (model_version, valid_time) so a model
        isn't rewarded for re-forecasting the same hour at shorter lead time.
        """
        sql = """
            WITH latest AS (
                SELECT DISTINCT ON (model_version, valid_time)
                    model_version, valid_time,
                    temperature_c, precipitation_mm, wind_speed_ms
                FROM predictions
                WHERE location_id = %(loc)s
                  AND valid_time >= NOW() - (%(hours)s * INTERVAL '1 hour')
                ORDER BY model_version, valid_time, created_at DESC
            )
            SELECT
                l.model_version,
                COUNT(*) AS scored,
                AVG(ABS(l.temperature_c - o.temperature_c)) AS temp_mae,
                SQRT(AVG(POWER(l.temperature_c - o.temperature_c, 2))) AS temp_rmse,
                AVG(l.temperature_c - o.temperature_c) AS temp_bias,
                COUNT(*) FILTER (
                    WHERE l.temperature_c IS NOT NULL AND o.temperature_c IS NOT NULL
                ) AS temp_n,
                AVG(ABS(l.wind_speed_ms - o.wind_speed_ms)) AS wind_mae,
                COUNT(*) FILTER (
                    WHERE l.wind_speed_ms IS NOT NULL AND o.wind_speed_ms IS NOT NULL
                ) AS wind_n,
                AVG(ABS(l.precipitation_mm - o.precipitation_mm)) AS precip_mae,
                COUNT(*) FILTER (
                    WHERE l.precipitation_mm IS NOT NULL AND o.precipitation_mm IS NOT NULL
                ) AS precip_n
            FROM latest l
            JOIN observations o
              ON o.location_id = %(loc)s AND o.source = %(source)s AND o.time = l.valid_time
            GROUP BY l.model_version
            ORDER BY temp_mae NULLS LAST
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"loc": location_id, "hours": since_hours, "source": source})
                return cur.fetchall()

    def fetch_ensemble_training_rows(self, location_id: str, members: list[str]) -> list[dict]:
        """Latest forecast per (member, valid_time) joined to the observation at that
        time — long format, one row per member per valid_time. The ensemble trainer
        pivots these into per-valid_time member columns."""
        sql = """
            WITH latest AS (
                SELECT DISTINCT ON (model_version, valid_time)
                    model_version, valid_time, horizon_hours,
                    temperature_c, precipitation_mm, wind_speed_ms
                FROM predictions
                WHERE location_id = %(loc)s AND model_version = ANY(%(members)s)
                ORDER BY model_version, valid_time, created_at DESC
            )
            SELECT l.valid_time, l.model_version, l.horizon_hours,
                   l.temperature_c, l.precipitation_mm, l.wind_speed_ms,
                   o.temperature_c   AS obs_temperature_c,
                   o.precipitation_mm AS obs_precipitation_mm,
                   o.wind_speed_ms   AS obs_wind_speed_ms
            FROM latest l
            JOIN observations o
              ON o.location_id = %(loc)s AND o.source = 'open_meteo' AND o.time = l.valid_time
            ORDER BY l.valid_time, l.model_version
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"loc": location_id, "members": members})
                return cur.fetchall()

    def fetch_latest_member_forecasts(self, location_id: str, members: list[str]) -> list[dict]:
        """Latest forecast per (member, valid_time), including future valid_times —
        the inputs the ensemble predictor blends."""
        sql = """
            SELECT DISTINCT ON (model_version, valid_time)
                model_version, valid_time, horizon_hours,
                temperature_c, precipitation_mm, wind_speed_ms
            FROM predictions
            WHERE location_id = %(loc)s AND model_version = ANY(%(members)s)
            ORDER BY model_version, valid_time, created_at DESC
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"loc": location_id, "members": members})
                return cur.fetchall()

    def upsert_champions(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO model_champions (
                location_id, variable, model_version, mae, n_scored, window_hours, evaluated_at
            ) VALUES (
                %(location_id)s, %(variable)s, %(model_version)s, %(mae)s, %(n_scored)s,
                %(window_hours)s, %(evaluated_at)s
            )
            ON CONFLICT (location_id, variable) DO UPDATE SET
                model_version = EXCLUDED.model_version,
                mae = EXCLUDED.mae,
                n_scored = EXCLUDED.n_scored,
                window_hours = EXCLUDED.window_hours,
                evaluated_at = EXCLUDED.evaluated_at
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def fetch_model_max_horizon(self, location_id: str, since_hours: int = 48) -> dict:
        """Max forecast horizon each model_version currently provides — used to keep
        short-range models (e.g. a 6h nowcaster) out of full-forecast champion picks."""
        sql = """
            SELECT model_version, MAX(horizon_hours) AS max_h
            FROM predictions
            WHERE location_id = %s AND created_at >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY model_version
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id, since_hours))
                return {r["model_version"]: r["max_h"] for r in cur.fetchall()}

    def fetch_champions(self, location_id: str) -> list[dict]:
        sql = "SELECT * FROM model_champions WHERE location_id = %s ORDER BY variable"
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (location_id,))
                return cur.fetchall()

    def save_model_artifact(self, key: str, data: bytes) -> None:
        sql = """
            INSERT INTO model_artifacts (key, data, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key, data))
            conn.commit()

    def load_model_artifact(self, key: str) -> bytes | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM model_artifacts WHERE key = %s", (key,))
                row = cur.fetchone()
        return bytes(row["data"]) if row else None

    def close(self) -> None:
        self._pool.close()
