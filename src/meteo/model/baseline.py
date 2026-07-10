from __future__ import annotations

from datetime import datetime, timezone


class BaselineNowcaster:
    """
    MVP predictor: uses Open-Meteo hourly forecast as baseline predictions.
    Replace with trained LightGBM/XGBoost models once enough history is collected.
    """

    MODEL_VERSION = "open_meteo_baseline_v0"

    def predict_from_forecast(self, location_id: str, forecast_payload: dict) -> list[dict]:
        hourly = forecast_payload["hourly"]
        times = hourly["time"]
        created_at = datetime.now(timezone.utc)
        rows: list[dict] = []

        for horizon_idx, time_str in enumerate(times[:6]):
            rows.append(
                {
                    "created_at": created_at,
                    "valid_time": _parse_time(time_str),
                    "location_id": location_id,
                    "horizon_hours": horizon_idx + 1,
                    "temperature_c": _at(hourly, "temperature_2m", horizon_idx),
                    "precipitation_mm": _at(hourly, "precipitation", horizon_idx),
                    "wind_speed_ms": _at(hourly, "windspeed_10m", horizon_idx),
                    "model_version": self.MODEL_VERSION,
                }
            )

        return rows


def _at(data: dict, key: str, idx: int) -> float | None:
    values = data.get(key)
    if not values or idx >= len(values):
        return None
    value = values[idx]
    return None if value is None else float(value)


def _parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
