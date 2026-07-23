from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from herbie import Herbie

from meteo.config import Location
from meteo_model.extract import (
    kelvin_to_celsius,
    pa_to_hpa,
    to_gfs_longitude,
    wind_direction,
    wind_speed,
)
from meteo_model.schemas import NwpForecastRow

logger = logging.getLogger(__name__)

PRODUCT = "pgrb2.0p25"  # GFS 0.25-degree global grid

# GRIB .idx search strings -> the single variable Herbie hands back per call.
# Wind is fetched as U/V components and combined into speed + direction.
_SCALAR_SEARCH = {
    "temperature_c": "TMP:2 m above ground",
    "humidity_pct": "RH:2 m above ground",
    "pressure_hpa": "PRES:surface",
    "precipitation_mm": "APCP:surface",
    "cloud_cover_pct": "TCDC:entire atmosphere",
}
_SCALAR_CONVERT = {
    "temperature_c": kelvin_to_celsius,
    "pressure_hpa": pa_to_hpa,
}


def latest_run_time(now: datetime, lag_hours: int = 5) -> datetime:
    """Most recent GFS cycle (00/06/12/18Z) expected to be published given a lag."""
    anchor = now - timedelta(hours=lag_hours)
    cycle_hour = (anchor.hour // 6) * 6
    return anchor.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)


def _point_value(dataset, latitude: float, longitude: float) -> float | None:
    """Nearest grid cell value of a Herbie variable subset.

    Herbie returns a *list* of datasets when cfgrib opens multiple hypercubes
    (e.g. TCDC matches several 'entire atmosphere' layers); take the first
    dataset that yields a finite value.
    """
    datasets = dataset if isinstance(dataset, list) else [dataset]
    for ds in datasets:
        data_vars = list(ds.data_vars)
        if not data_vars:
            continue
        try:
            selected = ds[data_vars[0]].sel(
                latitude=latitude,
                longitude=to_gfs_longitude(longitude),
                method="nearest",
            )
            value = float(selected.values)
        except Exception:  # noqa: BLE001 - try the next hypercube
            continue
        if value == value:  # not NaN
            return value
    return None


def _open(run_time: datetime, fxx: int, search: str):
    herbie = Herbie(
        run_time.strftime("%Y-%m-%d %H:%M"),
        model="gfs",
        product=PRODUCT,
        fxx=fxx,
    )
    return herbie.xarray(search)


def _resolve_run(now: datetime, max_steps_back: int = 2) -> datetime:
    """Find the newest cycle whose f001 file actually exists, stepping back 6h at a time."""
    run_time = latest_run_time(now)
    for _ in range(max_steps_back + 1):
        try:
            Herbie(run_time.strftime("%Y-%m-%d %H:%M"), model="gfs", product=PRODUCT, fxx=1).grib
            return run_time
        except Exception:
            logger.warning("GFS run %s not available yet, stepping back 6h", run_time.isoformat())
            run_time = run_time - timedelta(hours=6)
    return run_time


def fetch_forecast(
    location: Location,
    now: datetime,
    forecast_hours: int,
    model: str = "gfs",
) -> tuple[datetime, list[NwpForecastRow]]:
    """Fetch a GFS forecast series for one location, extracted at its grid cell."""
    run_time = _resolve_run(now)
    rows: list[NwpForecastRow] = []

    for fxx in range(1, forecast_hours + 1):
        valid_time = run_time + timedelta(hours=fxx)
        values: dict[str, float | None] = {}

        for field, search in _SCALAR_SEARCH.items():
            try:
                raw = _point_value(_open(run_time, fxx, search), location.latitude, location.longitude)
                convert = _SCALAR_CONVERT.get(field)
                values[field] = convert(raw) if convert else raw
            except Exception as exc:  # noqa: BLE001 - a missing field must not drop the horizon
                logger.debug("GFS %s f%03d %s unavailable: %s", location.id, fxx, field, exc)
                values[field] = None

        try:
            u = _point_value(_open(run_time, fxx, "UGRD:10 m above ground"), location.latitude, location.longitude)
            v = _point_value(_open(run_time, fxx, "VGRD:10 m above ground"), location.latitude, location.longitude)
        except Exception as exc:  # noqa: BLE001
            logger.debug("GFS %s f%03d wind unavailable: %s", location.id, fxx, exc)
            u = v = None

        rows.append(
            NwpForecastRow(
                run_time=run_time,
                valid_time=valid_time,
                location_id=location.id,
                model=model,
                horizon_hours=fxx,
                temperature_c=values["temperature_c"],
                precipitation_mm=values["precipitation_mm"],
                wind_speed_ms=wind_speed(u, v),
                wind_direction_deg=wind_direction(u, v),
                humidity_pct=values["humidity_pct"],
                pressure_hpa=values["pressure_hpa"],
                cloud_cover_pct=values["cloud_cover_pct"],
            )
        )

    logger.info("Fetched %s GFS horizons for %s (run %s)", len(rows), location.id, run_time.isoformat())
    return run_time, rows
