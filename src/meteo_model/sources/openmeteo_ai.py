from __future__ import annotations

import logging
from datetime import datetime, timezone

from meteo.clients.weather import OpenMeteoClient
from meteo.config import Location, get_settings
from meteo_model.schemas import NwpForecastRow

logger = logging.getLogger(__name__)


def _parse_utc(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def _at(hourly: dict, key: str, idx: int) -> float | None:
    values = hourly.get(key)
    if not values or idx >= len(values):
        return None
    value = values[idx]
    return None if value is None else float(value)


def fetch_forecast(
    location: Location,
    model_id: str,
    model_name: str,
    forecast_hours: int | None = None,
    client: OpenMeteoClient | None = None,
) -> tuple[datetime, list[NwpForecastRow]]:
    """Fetch an Open-Meteo-served model forecast as NwpForecastRows.

    Unlike GFS these are point forecasts (JSON), so no GRIB/grid extraction and no
    container needed. `model_id` is the Open-Meteo id (e.g. 'ecmwf_aifs025_single',
    'icon_seamless'); `model_name` is how we store it (e.g. 'aifs', 'icon').
    """
    settings = get_settings()
    forecast_hours = forecast_hours or settings.nwp_forecast_hours
    owns_client = client is None
    client = client or OpenMeteoClient()
    try:
        payload = client.fetch_model_forecast(location, model_id, forecast_hours)
    finally:
        if owns_client:
            client.close()

    hourly = payload["hourly"]
    times = hourly["time"]
    if not times:
        return datetime.now(timezone.utc), []

    run_time = _parse_utc(times[0])  # Open-Meteo anchors the series at the current hour
    rows: list[NwpForecastRow] = []
    for idx, t in enumerate(times):
        valid_time = _parse_utc(t)
        horizon = round((valid_time - run_time).total_seconds() / 3600)
        rows.append(
            NwpForecastRow(
                run_time=run_time,
                valid_time=valid_time,
                location_id=location.id,
                model=model_name,
                horizon_hours=horizon,
                temperature_c=_at(hourly, "temperature_2m", idx),
                precipitation_mm=_at(hourly, "precipitation", idx),
                wind_speed_ms=_at(hourly, "windspeed_10m", idx),
                wind_direction_deg=_at(hourly, "winddirection_10m", idx),
                humidity_pct=_at(hourly, "relative_humidity_2m", idx),
                pressure_hpa=_at(hourly, "surface_pressure", idx),
                cloud_cover_pct=_at(hourly, "cloud_cover", idx),
            )
        )
    logger.info("Fetched %s %s horizons for %s (run %s)", len(rows), model_name, location.id, run_time.isoformat())
    return run_time, rows
