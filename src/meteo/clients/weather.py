from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from meteo.config import Location


@dataclass
class ObservationRow:
    time: datetime
    location_id: str
    source: str
    temperature_c: float | None
    precipitation_mm: float | None
    wind_speed_ms: float | None
    wind_direction_deg: float | None
    humidity_pct: float | None
    pressure_hpa: float | None
    cloud_cover_pct: float | None


class OpenMeteoClient:
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

    HOURLY_VARS = [
        "temperature_2m",
        "precipitation",
        "windspeed_10m",
        "winddirection_10m",
        "relative_humidity_2m",
        "surface_pressure",
        "cloud_cover",
    ]

    def __init__(self, timeout: float = 30.0) -> None:
        self._client = httpx.Client(timeout=timeout)

    def fetch_current_and_recent(
        self,
        location: Location,
        lookback_hours: int = 48,
    ) -> tuple[dict, list[ObservationRow]]:
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": "UTC",  # always store true UTC; Location.timezone is display metadata
            "hourly": ",".join(self.HOURLY_VARS),
            "windspeed_unit": "ms",  # Open-Meteo defaults to km/h; our column is wind_speed_ms
            "past_hours": lookback_hours,
            "forecast_hours": 1,
        }
        response = self._client.get(self.FORECAST_URL, params=params)
        response.raise_for_status()
        payload = response.json()
        rows = self._parse_hourly(payload, location.id, "open_meteo")
        return payload, rows

    def fetch_forecast(self, location: Location, forecast_hours: int = 6) -> dict:
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": "UTC",  # always store true UTC; Location.timezone is display metadata
            "hourly": ",".join(self.HOURLY_VARS),
            "windspeed_unit": "ms",  # Open-Meteo defaults to km/h; our column is wind_speed_ms
            "forecast_hours": forecast_hours,
        }
        response = self._client.get(self.FORECAST_URL, params=params)
        response.raise_for_status()
        return response.json()

    def fetch_archive(
        self,
        location: Location,
        start_date: str,
        end_date: str,
    ) -> tuple[dict, list[ObservationRow]]:
        """Historical hourly observations (ERA5 reanalysis) for a date range.

        Dates are ISO 'YYYY-MM-DD'. Used to backfill `observations` so Approach 3
        bias correction has enough nwp<->obs pairs to train.
        """
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": "UTC",  # always store true UTC; Location.timezone is display metadata
            "hourly": ",".join(self.HOURLY_VARS),
            "windspeed_unit": "ms",  # Open-Meteo defaults to km/h; our column is wind_speed_ms
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self._client.get(self.ARCHIVE_URL, params=params)
        response.raise_for_status()
        payload = response.json()
        rows = self._parse_hourly(payload, location.id, "open_meteo")
        return payload, rows

    def fetch_model_forecast(self, location: Location, model: str, forecast_hours: int = 48) -> dict:
        """Raw hourly forecast from a specific Open-Meteo model (e.g. 'ecmwf_aifs025_single').

        Returns the payload; times are UTC. Used to pull AI/physics model forecasts
        as additional model_versions for comparison.
        """
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": "UTC",
            "hourly": ",".join(self.HOURLY_VARS),
            "windspeed_unit": "ms",
            "models": model,
            "forecast_hours": forecast_hours,
        }
        response = self._client.get(self.FORECAST_URL, params=params)
        response.raise_for_status()
        return response.json()

    def _parse_hourly(self, payload: dict, location_id: str, source: str) -> list[ObservationRow]:
        hourly = payload["hourly"]
        times = hourly["time"]
        rows: list[ObservationRow] = []
        for idx, time_str in enumerate(times):
            rows.append(
                ObservationRow(
                    time=_parse_time(time_str),
                    location_id=location_id,
                    source=source,
                    temperature_c=_at(hourly, "temperature_2m", idx),
                    precipitation_mm=_at(hourly, "precipitation", idx),
                    wind_speed_ms=_at(hourly, "windspeed_10m", idx),
                    wind_direction_deg=_at(hourly, "winddirection_10m", idx),
                    humidity_pct=_at(hourly, "relative_humidity_2m", idx),
                    pressure_hpa=_at(hourly, "surface_pressure", idx),
                    cloud_cover_pct=_at(hourly, "cloud_cover", idx),
                )
            )
        return rows

    def close(self) -> None:
        self._client.close()


class OpenWeatherClient:
    BASE_URL = "https://api.openweathermap.org/data/2.5"

    def __init__(self, api_key: str, timeout: float = 30.0) -> None:
        if not api_key:
            raise ValueError("OpenWeather API key is required")
        self.api_key = api_key
        self._client = httpx.Client(timeout=timeout)

    def fetch_current(self, location: Location) -> tuple[dict, ObservationRow]:
        params = {
            "lat": location.latitude,
            "lon": location.longitude,
            "appid": self.api_key,
            "units": "metric",
        }
        response = self._client.get(f"{self.BASE_URL}/weather", params=params)
        response.raise_for_status()
        payload = response.json()
        row = ObservationRow(
            time=datetime.fromtimestamp(payload["dt"], tz=timezone.utc),
            location_id=location.id,
            source="openweather",
            temperature_c=payload["main"].get("temp"),
            precipitation_mm=_rain_mm(payload),
            wind_speed_ms=payload["wind"].get("speed"),
            wind_direction_deg=payload["wind"].get("deg"),
            humidity_pct=payload["main"].get("humidity"),
            pressure_hpa=payload["main"].get("pressure"),
            cloud_cover_pct=payload.get("clouds", {}).get("all"),
        )
        return payload, row

    def close(self) -> None:
        self._client.close()


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


def _rain_mm(payload: dict) -> float | None:
    rain = payload.get("rain") or {}
    if "1h" in rain:
        return float(rain["1h"])
    if "3h" in rain:
        return float(rain["3h"]) / 3.0
    return 0.0
