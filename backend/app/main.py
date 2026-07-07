"""AgIoT backend: REST API over the sensor platform database."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import Literal

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Path, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from app import schemas
from app.db import Reading, Station, engine, init_db
from app.kafka_consumer import consume_sensor_readings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agiot.backend")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db(engine)
    consumer_task = asyncio.create_task(consume_sensor_readings())
    yield
    consumer_task.cancel()
    with suppress(asyncio.CancelledError):
        await consumer_task
    await engine.dispose()


app = FastAPI(
    title="AgIoT Sensor Platform API",
    description=(
        "REST API for a fleet of weather/soil monitoring stations.\n\n"
        "Stations publish readings to Kafka; the backend ingests them into a "
        "TimescaleDB hypertable and rolls them up hourly via a continuous "
        "aggregate. Raw readings are retained for 90 days; hourly rollups are "
        "kept indefinitely."
    ),
    version="0.1.0",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "stations", "description": "Registered station metadata."},
        {"name": "readings", "description": "Raw and aggregated sensor readings."},
        {"name": "health", "description": "Service health probes."},
    ],
)

_NOT_FOUND = {404: {"model": schemas.ErrorOut, "description": "Unknown station."}}
_BAD_RANGE = {400: {"model": schemas.ErrorOut, "description": "Invalid time range."}}


async def _station_or_404(conn: AsyncConnection, station_id: int):
    row = (
        await conn.execute(select(Station).where(Station.station_id == station_id))
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Station {station_id} not found")
    return row


@app.get("/stations", response_model=list[schemas.StationOut], tags=["stations"])
async def list_stations(
    limit: int = Query(100, ge=1, le=1000, description="Page size."),
    offset: int = Query(0, ge=0, description="Rows to skip (pagination)."),
) -> list[schemas.StationOut]:
    """List all registered stations, ordered by station_id."""
    async with engine.connect() as conn:
        rows = await conn.execute(
            select(Station).order_by(Station.station_id).limit(limit).offset(offset)
        )
        return [schemas.StationOut.model_validate(r) for r in rows]


@app.get(
    "/stations/{station_id}",
    response_model=schemas.StationOut,
    responses=_NOT_FOUND,
    tags=["stations"],
)
async def get_station(
    station_id: int = Path(description="Station identifier.", examples=[17]),
) -> schemas.StationOut:
    """Fetch one station's metadata."""
    async with engine.connect() as conn:
        row = await _station_or_404(conn, station_id)
        return schemas.StationOut.model_validate(row)


@app.get("/readings/latest", response_model=list[schemas.ReadingOut], tags=["readings"])
async def latest_readings(
    limit: int = Query(100, ge=1, le=1000, description="Page size (stations per page)."),
    offset: int = Query(0, ge=0, description="Stations to skip (pagination)."),
) -> list[schemas.ReadingOut]:
    """Return each station's most recent reading (one row per station)."""
    async with engine.connect() as conn:
        # DISTINCT ON (station_id) ... ORDER BY station_id, ts DESC picks the
        # newest row per station in a single index-friendly scan.
        rows = await conn.execute(
            select(Reading)
            .distinct(Reading.station_id)
            .order_by(Reading.station_id, Reading.ts.desc())
            .limit(limit)
            .offset(offset)
        )
        return [schemas.ReadingOut.model_validate(r) for r in rows]


_HOURLY_COLUMNS = ", ".join(
    f"{f}_{agg}"
    for f in schemas.ReadingOut.model_fields
    if f not in ("station_id", "ts")
    for agg in ("min", "max", "avg")
)


def _hourly_row_to_schema(row) -> schemas.HourlyReadingOut:
    """Regroup the aggregate's flat *_min/_max/_avg columns into SensorStats."""
    payload: dict = {"station_id": row.station_id, "bucket": row.bucket}
    for field in schemas.HourlyReadingOut.model_fields:
        if field in ("station_id", "bucket"):
            continue
        payload[field] = {
            agg: getattr(row, f"{field}_{agg}") for agg in ("min", "max", "avg")
        }
    return schemas.HourlyReadingOut.model_validate(payload)


@app.get(
    "/readings/{station_id}",
    response_model=list[schemas.ReadingOut] | list[schemas.HourlyReadingOut],
    responses={**_NOT_FOUND, **_BAD_RANGE},
    tags=["readings"],
)
async def station_readings(
    station_id: int = Path(description="Station identifier.", examples=[17]),
    from_ts: datetime | None = Query(
        None, alias="from", description="Only readings at or after this time (UTC)."
    ),
    to_ts: datetime | None = Query(
        None, alias="to", description="Only readings at or before this time (UTC)."
    ),
    interval: Literal["raw", "hourly"] = Query(
        "raw",
        description=(
            "`raw` returns individual readings from the hypertable; `hourly` "
            "returns min/max/avg rollups from the continuous aggregate "
            "(cheaper for long time ranges)."
        ),
    ),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum rows returned."),
    offset: int = Query(0, ge=0, description="Rows to skip (pagination)."),
):
    """Historical readings for one station, newest first.

    Use `interval=hourly` for ranges longer than a day or two: the continuous
    aggregate serves those queries without scanning raw rows, and hourly data
    outlives the 90-day raw retention window.
    """
    if from_ts is not None and to_ts is not None and from_ts > to_ts:
        raise HTTPException(
            status_code=400,
            detail=f"'from' ({from_ts.isoformat()}) is after 'to' ({to_ts.isoformat()})",
        )

    async with engine.connect() as conn:
        await _station_or_404(conn, station_id)

        if interval == "raw":
            stmt = select(Reading).where(Reading.station_id == station_id)
            if from_ts is not None:
                stmt = stmt.where(Reading.ts >= from_ts)
            if to_ts is not None:
                stmt = stmt.where(Reading.ts <= to_ts)
            rows = await conn.execute(
                stmt.order_by(Reading.ts.desc()).limit(limit).offset(offset)
            )
            return [schemas.ReadingOut.model_validate(r) for r in rows]

        # interval == "hourly": the continuous aggregate is a view, not an ORM
        # table, so query it with raw SQL.
        clauses = ["station_id = :station_id"]
        params: dict = {"station_id": station_id, "limit": limit, "offset": offset}
        if from_ts is not None:
            clauses.append("bucket >= :from_ts")
            params["from_ts"] = from_ts
        if to_ts is not None:
            clauses.append("bucket <= :to_ts")
            params["to_ts"] = to_ts
        rows = await conn.execute(
            text(
                f"SELECT station_id, bucket, {_HOURLY_COLUMNS} FROM readings_hourly "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY bucket DESC LIMIT :limit OFFSET :offset"
            ),
            params,
        )
        return [_hourly_row_to_schema(r) for r in rows]


@app.get("/health", response_model=schemas.HealthOut, tags=["health"])
async def health() -> schemas.HealthOut:
    """Liveness probe: returns 200 whenever the API process is serving."""
    return schemas.HealthOut(status="ok")


@app.get("/health/deps", tags=["health"])
async def health_deps() -> dict:
    """Readiness probe: checks that TimescaleDB and Kafka are reachable."""
    deps = {}

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        deps["timescaledb"] = "ok"
    except Exception as exc:  # noqa: BLE001
        logger.exception("TimescaleDB check failed")
        deps["timescaledb"] = f"error: {exc}"

    kafka_probe = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        await kafka_probe.start()
        deps["kafka"] = "ok"
    except Exception as exc:  # noqa: BLE001
        logger.exception("Kafka check failed")
        deps["kafka"] = f"error: {exc}"
    finally:
        await kafka_probe.stop()

    return deps
