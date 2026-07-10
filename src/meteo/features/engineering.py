from __future__ import annotations

import polars as pl


def build_feature_rows(observations: list[dict]) -> list[dict]:
    """Compute gold-layer features from silver observations (hourly, one source)."""
    if len(observations) < 2:
        return []

    df = (
        pl.DataFrame(observations)
        .sort("time")
        .with_columns(
            pl.col("temperature_c").cast(pl.Float64),
            pl.col("precipitation_mm").cast(pl.Float64),
            pl.col("wind_speed_ms").cast(pl.Float64),
            pl.col("humidity_pct").cast(pl.Float64),
            pl.col("pressure_hpa").cast(pl.Float64),
        )
    )

    featured = df.with_columns(
        pl.col("temperature_c").shift(1).alias("temperature_c_lag_1"),
        pl.col("temperature_c").shift(4).alias("temperature_c_lag_4"),
        pl.col("temperature_c").rolling_mean(window_size=3, min_samples=1).alias("temperature_c_roll_3"),
        pl.col("temperature_c").rolling_mean(window_size=12, min_samples=1).alias("temperature_c_roll_12"),
        (pl.col("temperature_c") - pl.col("temperature_c").shift(1)).alias("temperature_c_diff_1"),
        pl.col("precipitation_mm").shift(1).alias("precipitation_mm_lag_1"),
        pl.col("precipitation_mm").rolling_mean(window_size=3, min_samples=1).alias("precipitation_mm_roll_3"),
        pl.col("precipitation_mm").rolling_mean(window_size=12, min_samples=1).alias("precipitation_mm_roll_12"),
        pl.col("wind_speed_ms").shift(1).alias("wind_speed_ms_lag_1"),
        pl.col("wind_speed_ms").rolling_mean(window_size=3, min_samples=1).alias("wind_speed_ms_roll_3"),
        pl.col("wind_speed_ms").rolling_mean(window_size=12, min_samples=1).alias("wind_speed_ms_roll_12"),
        (pl.col("wind_speed_ms") - pl.col("wind_speed_ms").shift(1)).alias("wind_speed_ms_diff_1"),
    )

    latest = featured.tail(1)
    row = latest.to_dicts()[0]
    return [
        {
            "time": row["time"],
            "location_id": row["location_id"],
            "temperature_c_lag_1": row["temperature_c_lag_1"],
            "temperature_c_lag_4": row["temperature_c_lag_4"],
            "temperature_c_roll_3": row["temperature_c_roll_3"],
            "temperature_c_roll_12": row["temperature_c_roll_12"],
            "temperature_c_diff_1": row["temperature_c_diff_1"],
            "precipitation_mm_lag_1": row["precipitation_mm_lag_1"],
            "precipitation_mm_roll_3": row["precipitation_mm_roll_3"],
            "precipitation_mm_roll_12": row["precipitation_mm_roll_12"],
            "wind_speed_ms_lag_1": row["wind_speed_ms_lag_1"],
            "wind_speed_ms_roll_3": row["wind_speed_ms_roll_3"],
            "wind_speed_ms_roll_12": row["wind_speed_ms_roll_12"],
            "wind_speed_ms_diff_1": row["wind_speed_ms_diff_1"],
            "humidity_pct": row["humidity_pct"],
            "pressure_hpa": row["pressure_hpa"],
        }
    ]
