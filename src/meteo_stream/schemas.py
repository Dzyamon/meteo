from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

from meteo.clients.weather import ObservationRow


class ObservationMessage(BaseModel):
    time: datetime
    location_id: str
    source: str
    temperature_c: float | None = None
    precipitation_mm: float | None = None
    wind_speed_ms: float | None = None
    wind_direction_deg: float | None = None
    humidity_pct: float | None = None
    pressure_hpa: float | None = None
    cloud_cover_pct: float | None = None

    @field_validator("time", mode="before")
    @classmethod
    def parse_time(cls, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        text = str(value)
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    @field_validator("temperature_c")
    @classmethod
    def validate_temperature(cls, value: float | None) -> float | None:
        if value is not None and not -80 <= value <= 60:
            raise ValueError(f"temperature out of range: {value}")
        return value

    @field_validator("wind_speed_ms")
    @classmethod
    def validate_wind(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError(f"wind speed cannot be negative: {value}")
        return value

    def to_row(self) -> ObservationRow:
        return ObservationRow(
            time=self.time,
            location_id=self.location_id,
            source=self.source,
            temperature_c=self.temperature_c,
            precipitation_mm=self.precipitation_mm,
            wind_speed_ms=self.wind_speed_ms,
            wind_direction_deg=self.wind_direction_deg,
            humidity_pct=self.humidity_pct,
            pressure_hpa=self.pressure_hpa,
            cloud_cover_pct=self.cloud_cover_pct,
        )


class ObservationBatchEvent(BaseModel):
    """One poll cycle for a single location."""

    event_id: str
    location_id: str
    source: str
    ingested_at: datetime
    observations: list[ObservationMessage]
    raw_payload: dict | None = None

    @field_validator("ingested_at", mode="before")
    @classmethod
    def parse_ingested_at(cls, value: str | datetime) -> datetime:
        return ObservationMessage.parse_time(value)


def row_to_message(row: ObservationRow) -> ObservationMessage:
    return ObservationMessage(
        time=row.time,
        location_id=row.location_id,
        source=row.source,
        temperature_c=row.temperature_c,
        precipitation_mm=row.precipitation_mm,
        wind_speed_ms=row.wind_speed_ms,
        wind_direction_deg=row.wind_direction_deg,
        humidity_pct=row.humidity_pct,
        pressure_hpa=row.pressure_hpa,
        cloud_cover_pct=row.cloud_cover_pct,
    )
