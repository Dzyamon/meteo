from __future__ import annotations

import logging
from datetime import datetime, timedelta

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

# One combined .idx search pulls every field we need per horizon in a single
# request (Herbie returns a list of datasets, one per level type).
_COMBINED_SEARCH = (
    ":(TMP:2 m above ground|RH:2 m above ground|PRES:surface|APCP:surface"
    "|TCDC:entire atmosphere|UGRD:10 m above ground|VGRD:10 m above ground):"
)

# cfgrib short name -> (our field, unit converter or None)
_VARMAP = {
    "t2m": ("temperature_c", kelvin_to_celsius),
    "r2": ("humidity_pct", None),
    "sp": ("pressure_hpa", pa_to_hpa),
    "tp": ("precipitation_mm", None),
    "tcc": ("cloud_cover_pct", None),
}


def latest_run_time(now: datetime, lag_hours: int = 5) -> datetime:
    """Most recent GFS cycle (00/06/12/18Z) expected to be published given a lag."""
    anchor = now - timedelta(hours=lag_hours)
    cycle_hour = (anchor.hour // 6) * 6
    return anchor.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)


def _resolve_run(now: datetime, max_steps_back: int = 2) -> datetime:
    """Find the newest cycle whose f001 file actually exists, stepping back 6h at a time."""
    run_time = latest_run_time(now)
    for _ in range(max_steps_back + 1):
        try:
            Herbie(run_time.strftime("%Y-%m-%d %H:%M"), model="gfs", product=PRODUCT, fxx=1).grib
            return run_time
        except Exception:  # noqa: BLE001
            logger.warning("GFS run %s not available yet, stepping back 6h", run_time.isoformat())
            run_time = run_time - timedelta(hours=6)
    return run_time


def _collect_vars(result) -> dict:
    """Flatten Herbie's list-of-datasets into {cfgrib_short_name: DataArray}."""
    datasets = result if isinstance(result, list) else [result]
    out: dict = {}
    for ds in datasets:
        for name in ds.data_vars:
            out.setdefault(name, ds[name])  # first wins (e.g. tcc has two layers)
    return out


def _point(da, latitude: float, longitude: float) -> float | None:
    if da is None:
        return None
    try:
        value = float(da.sel(latitude=latitude, longitude=to_gfs_longitude(longitude), method="nearest").values)
    except Exception:  # noqa: BLE001
        return None
    return None if value != value else value  # drop NaN


def fetch_forecasts(
    locations: list[Location],
    now: datetime,
    forecast_hours: int,
    model: str = "gfs",
) -> tuple[datetime, dict[str, list[NwpForecastRow]]]:
    """Fetch a GFS forecast series for every location at once.

    Each horizon is downloaded ONCE (all variables in one request) and every
    location's nearest grid cell is extracted from it — so wall time no longer
    scales with variables-per-horizon or with the number of locations.
    """
    run_time = _resolve_run(now)
    rows: dict[str, list[NwpForecastRow]] = {loc.id: [] for loc in locations}

    for fxx in range(1, forecast_hours + 1):
        valid_time = run_time + timedelta(hours=fxx)
        try:
            herbie = Herbie(run_time.strftime("%Y-%m-%d %H:%M"), model="gfs", product=PRODUCT, fxx=fxx)
            fields = _collect_vars(herbie.xarray(_COMBINED_SEARCH, remove_grib=True))
        except Exception as exc:  # noqa: BLE001 - a bad horizon must not drop the whole run
            logger.warning("GFS f%03d fetch failed: %s", fxx, exc)
            continue

        for loc in locations:
            scalars = {}
            for short, (field, convert) in _VARMAP.items():
                raw = _point(fields.get(short), loc.latitude, loc.longitude)
                scalars[field] = convert(raw) if convert else raw
            u = _point(fields.get("u10"), loc.latitude, loc.longitude)
            v = _point(fields.get("v10"), loc.latitude, loc.longitude)
            rows[loc.id].append(
                NwpForecastRow(
                    run_time=run_time,
                    valid_time=valid_time,
                    location_id=loc.id,
                    model=model,
                    horizon_hours=fxx,
                    temperature_c=scalars["temperature_c"],
                    precipitation_mm=scalars["precipitation_mm"],
                    wind_speed_ms=wind_speed(u, v),
                    wind_direction_deg=wind_direction(u, v),
                    humidity_pct=scalars["humidity_pct"],
                    pressure_hpa=scalars["pressure_hpa"],
                    cloud_cover_pct=scalars["cloud_cover_pct"],
                )
            )

    total = sum(len(v) for v in rows.values())
    logger.info("Fetched %s GFS rows across %s location(s) (run %s)", total, len(locations), run_time.isoformat())
    return run_time, rows


def fetch_forecast(
    location: Location,
    now: datetime,
    forecast_hours: int,
    model: str = "gfs",
) -> tuple[datetime, list[NwpForecastRow]]:
    """Single-location convenience wrapper around fetch_forecasts."""
    run_time, rows = fetch_forecasts([location], now, forecast_hours, model)
    return run_time, rows[location.id]
