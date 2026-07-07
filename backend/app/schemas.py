"""Pydantic response models for the REST API.

Every field carries a description and example so the generated OpenAPI spec
(/docs, /redoc) is self-explanatory.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class StationOut(BaseModel):
    """A registered weather/soil monitoring station."""

    model_config = ConfigDict(from_attributes=True)

    station_id: int = Field(description="Unique station identifier.", examples=[17])
    name: str = Field(description="Human-readable station name.", examples=["Station 17"])
    latitude: float = Field(
        description="Station latitude in decimal degrees (WGS84).", examples=[45.88123]
    )
    longitude: float = Field(
        description="Station longitude in decimal degrees (WGS84).", examples=[-63.24816]
    )
    installed_at: datetime = Field(
        description=(
            "When the station was installed. For stations auto-registered from "
            "telemetry, this is the timestamp of their first reading."
        ),
        examples=["2026-01-01T00:00:00Z"],
    )


class ReadingOut(BaseModel):
    """One raw sensor reading from a station.

    Any sensor field may be null if that sensor was offline or is not fitted.
    """

    model_config = ConfigDict(from_attributes=True)

    station_id: int = Field(description="Station that produced the reading.", examples=[17])
    ts: datetime = Field(
        description="When the reading was taken (UTC).",
        examples=["2026-07-07T12:34:56.789Z"],
    )
    temperature_c: float | None = Field(
        None, description="Air temperature in °C.", examples=[18.42]
    )
    humidity_pct: float | None = Field(
        None, description="Relative humidity in percent.", examples=[72.5]
    )
    wind_speed_ms: float | None = Field(
        None, description="Wind speed in metres per second.", examples=[4.8]
    )
    wind_direction_deg: float | None = Field(
        None,
        description="Wind direction in degrees (meteorological convention, 0° = north).",
        examples=[231.0],
    )
    rainfall_mm: float | None = Field(
        None, description="Rainfall since the previous reading, in millimetres.", examples=[0.2]
    )
    pressure_hpa: float | None = Field(
        None, description="Barometric pressure in hectopascals.", examples=[1013.2]
    )
    soil_moisture_pct: float | None = Field(
        None, description="Volumetric soil moisture in percent.", examples=[34.7]
    )
    battery_pct: float | None = Field(
        None, description="Station battery charge remaining, in percent.", examples=[87.3]
    )


class SensorStats(BaseModel):
    """Min/max/avg of one sensor field over an aggregation bucket.

    All values are null when the sensor reported no data in the bucket.
    """

    min: float | None = Field(None, description="Minimum value in the bucket.", examples=[16.1])
    max: float | None = Field(None, description="Maximum value in the bucket.", examples=[21.7])
    avg: float | None = Field(None, description="Arithmetic mean over the bucket.", examples=[18.9])


class HourlyReadingOut(BaseModel):
    """Hourly rollup of one station's readings (from the continuous aggregate).

    Buckets are aligned to the start of the hour, UTC. The current (incomplete)
    hour is included via real-time aggregation, so its stats will still change
    as more readings arrive.
    """

    station_id: int = Field(description="Station the rollup belongs to.", examples=[17])
    bucket: datetime = Field(
        description="Start of the one-hour bucket (UTC).",
        examples=["2026-07-07T12:00:00Z"],
    )
    temperature_c: SensorStats = Field(description="Air temperature stats, °C.")
    humidity_pct: SensorStats = Field(description="Relative humidity stats, %.")
    wind_speed_ms: SensorStats = Field(description="Wind speed stats, m/s.")
    wind_direction_deg: SensorStats = Field(
        description=(
            "Wind direction stats, degrees. Note: plain averaging of a circular "
            "quantity is approximate (359° and 1° average to 180°)."
        )
    )
    rainfall_mm: SensorStats = Field(description="Rainfall stats per reading interval, mm.")
    pressure_hpa: SensorStats = Field(description="Barometric pressure stats, hPa.")
    soil_moisture_pct: SensorStats = Field(description="Soil moisture stats, %.")
    battery_pct: SensorStats = Field(description="Battery charge stats, %.")


class HealthOut(BaseModel):
    """Liveness probe response."""

    status: str = Field(description="Always 'ok' when the API is up.", examples=["ok"])


class ErrorOut(BaseModel):
    """Standard error envelope (FastAPI's HTTPException shape)."""

    detail: str = Field(
        description="Human-readable explanation of the error.",
        examples=["Station 999 not found"],
    )
