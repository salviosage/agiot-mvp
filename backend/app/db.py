"""Database schema and TimescaleDB setup for the sensor platform.

Two tables:
- ``stations``  — dimension table, one row per physical station.
- ``readings``  — fact table, one row per (station, timestamp) sample;
  turned into a TimescaleDB hypertable by :func:`init_db`.
"""

import logging
import os
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Text, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = logging.getLogger("agiot.db")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://agiot:agiot@timescaledb:5432/agiot"
)

engine: AsyncEngine = create_async_engine(DATABASE_URL)


class Base(DeclarativeBase):
    pass


class Station(Base):
    __tablename__ = "stations"

    station_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    latitude: Mapped[float]
    longitude: Mapped[float]
    installed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Reading(Base):
    __tablename__ = "readings"

    # Composite PK (station_id, ts): TimescaleDB requires the partitioning
    # column (ts) in every unique constraint, and this doubles as the natural
    # dedup key — one sample per station per timestamp. It also gives us the
    # (station_id, ts) index that per-station range queries need.
    station_id: Mapped[int] = mapped_column(
        ForeignKey("stations.station_id"), primary_key=True
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    # All sensor fields nullable: a station keeps reporting even when an
    # individual sensor fails or isn't fitted.
    temperature_c: Mapped[float | None]
    humidity_pct: Mapped[float | None]
    wind_speed_ms: Mapped[float | None]
    wind_direction_deg: Mapped[float | None]
    rainfall_mm: Mapped[float | None]
    pressure_hpa: Mapped[float | None]
    soil_moisture_pct: Mapped[float | None]
    battery_pct: Mapped[float | None]


# min/max/avg for every sensor field, e.g.
# "min(temperature_c) AS temperature_c_min, ..., avg(temperature_c) AS temperature_c_avg"
_SENSOR_FIELDS = [
    "temperature_c",
    "humidity_pct",
    "wind_speed_ms",
    "wind_direction_deg",  # NB: plain avg is wrong for circular data (359°+1°
    "rainfall_mm",         # averages to 180°); good enough for the MVP.
    "pressure_hpa",        # NB: rainfall would usually be summed, not averaged;
    "soil_moisture_pct",   # kept as min/max/avg for schema uniformity for now.
    "battery_pct",
]
_AGG_COLUMNS = ",\n        ".join(
    f"{fn}({f}) AS {f}_{fn}" for f in _SENSOR_FIELDS for fn in ("min", "max", "avg")
)

# All statements are idempotent (IF NOT EXISTS / if_not_exists => TRUE) so
# init_db() can safely run on every startup. Order matters: hypertable before
# compression/retention, continuous aggregate before its refresh policy.
_TIMESCALE_DDL = [
    # The timescaledb Docker image preloads the extension in the default DB,
    # but this keeps init_db() working against any Postgres that has the
    # extension installed.
    "CREATE EXTENSION IF NOT EXISTS timescaledb",
    #
    # Hypertable partitioned on ts. Default chunk interval (7 days) is fine
    # for MVP-scale ingest; revisit if chunks grow past ~25% of RAM.
    "SELECT create_hypertable('readings', 'ts', if_not_exists => TRUE)",
    #
    # Compression layout: segment by station_id so each compressed batch holds
    # one station's data (queries almost always filter by station), order by
    # ts DESC so recent-first range scans decompress sequentially.
    """
    ALTER TABLE readings SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'station_id',
        timescaledb.compress_orderby = 'ts DESC'
    )
    """,
    #
    # Compress chunks older than 7 days: recent data is hot (dashboards,
    # backfills, corrections) and compressed chunks are expensive to update,
    # so we wait until data has settled; after a week it's effectively
    # read-only and compression buys ~90%+ disk savings.
    "SELECT add_compression_policy('readings', INTERVAL '7 days', if_not_exists => TRUE)",
    #
    # Hourly rollup per station. WITH NO DATA so creation doesn't block
    # scanning existing rows; the refresh policy below fills it in.
    f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS readings_hourly
    WITH (timescaledb.continuous) AS
    SELECT
        station_id,
        time_bucket(INTERVAL '1 hour', ts) AS bucket,
        {_AGG_COLUMNS}
    FROM readings
    GROUP BY station_id, bucket
    WITH NO DATA
    """,
    #
    # Refresh the rollup every 30 min. end_offset 1h excludes the still-open
    # bucket (avoids churn re-materializing partial hours); start_offset 3d
    # re-covers a window where late/corrected data may still arrive.
    """
    SELECT add_continuous_aggregate_policy('readings_hourly',
        start_offset => INTERVAL '3 days',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '30 minutes',
        if_not_exists => TRUE)
    """,
    #
    # Drop raw chunks after 90 days: past that horizon consumers only need
    # trends, which readings_hourly preserves indefinitely (it is refreshed
    # long before day 90, so nothing is lost when raw chunks drop).
    "SELECT add_retention_policy('readings', INTERVAL '90 days', if_not_exists => TRUE)",
]


async def init_db(db_engine: AsyncEngine | None = None) -> None:
    """Create tables, then apply the TimescaleDB setup (idempotent)."""
    db_engine = db_engine or engine

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous) refuses to
    # run inside a transaction block, so run all Timescale DDL on an
    # autocommit connection.
    autocommit_engine = db_engine.execution_options(isolation_level="AUTOCOMMIT")
    async with autocommit_engine.connect() as conn:
        for stmt in _TIMESCALE_DDL:
            await conn.execute(text(stmt))

    logger.info("Database initialized (tables, hypertable, policies, rollup)")
