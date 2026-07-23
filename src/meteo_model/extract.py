from __future__ import annotations

import math

"""Pure helpers for turning raw GFS grid values into our observation units.

Kept free of xarray/Herbie so they can be unit-tested without GRIB tooling.
"""


def to_gfs_longitude(longitude: float) -> float:
    """GFS grids use 0..360 longitudes; convert a signed longitude to that range."""
    return longitude % 360


def kelvin_to_celsius(value: float | None) -> float | None:
    return None if value is None else value - 273.15


def pa_to_hpa(value: float | None) -> float | None:
    return None if value is None else value / 100.0


def wind_speed(u: float | None, v: float | None) -> float | None:
    if u is None or v is None:
        return None
    return math.sqrt(u * u + v * v)


def wind_direction(u: float | None, v: float | None) -> float | None:
    """Meteorological direction (degrees the wind blows *from*), 0..360."""
    if u is None or v is None:
        return None
    if u == 0 and v == 0:
        return None
    return (270.0 - math.degrees(math.atan2(v, u))) % 360.0
