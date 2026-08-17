from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class NwpForecastRow:
    """One horizon of a numerical weather model forecast at a single location."""

    run_time: datetime
    valid_time: datetime
    location_id: str
    model: str
    horizon_hours: int
    temperature_c: float | None = None
    precipitation_mm: float | None = None
    wind_speed_ms: float | None = None
    wind_direction_deg: float | None = None
    humidity_pct: float | None = None
    pressure_hpa: float | None = None
    cloud_cover_pct: float | None = None

    def as_dict(self) -> dict:
        return asdict(self)


# Bias correction is trained per target; each maps the observed column to the
# raw-NWP column used as its main predictor.
CORRECTION_TARGETS: dict[str, str] = {
    "temperature_c": "nwp_temperature_c",
    "precipitation_mm": "nwp_precipitation_mm",
    "wind_speed_ms": "nwp_wind_speed_ms",
}
