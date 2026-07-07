"""Kafka -> TimescaleDB ingestion task.

Consumes the ``sensor-readings`` topic and writes rows in batches. Offsets are
committed only after a batch is in the database, so delivery is at-least-once;
the ON CONFLICT DO NOTHING inserts make redelivered messages harmless.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import Reading, Station, engine

logger = logging.getLogger("agiot.consumer")

TOPIC = "sensor-readings"
GROUP_ID = "agiot-backend"
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
)

# Flush a batch every ~1s (the getmany poll timeout) or every N messages,
# whichever comes first. One multi-row INSERT per second beats one round-trip
# per message by orders of magnitude at fleet scale.
BATCH_MAX_MESSAGES = int(os.environ.get("BATCH_MAX_MESSAGES", "500"))
BATCH_WINDOW_MS = 1000

THROUGHPUT_LOG_INTERVAL_S = 10

_SENSOR_FIELDS = (
    "temperature_c",
    "humidity_pct",
    "wind_speed_ms",
    "wind_direction_deg",
    "rainfall_mm",
    "pressure_hpa",
    "soil_moisture_pct",
    "battery_pct",
)


def _parse(raw: bytes) -> dict | None:
    """Decode one Kafka message into insertable dicts, or None if malformed."""
    try:
        msg = json.loads(raw)
        return {
            "station_id": int(msg["station_id"]),
            "ts": datetime.fromisoformat(msg["ts"]),
            "latitude": float(msg.get("latitude", 0.0)),
            "longitude": float(msg.get("longitude", 0.0)),
            **{f: msg.get(f) for f in _SENSOR_FIELDS},
        }
    except (ValueError, KeyError, TypeError) as exc:
        logger.warning("Skipping malformed message: %s (%r)", exc, raw[:200])
        return None


async def _flush(rows: list[dict], known_stations: set[int]) -> None:
    """Write one batch: first-seen station registration, then readings."""
    new_ids = {r["station_id"] for r in rows} - known_stations
    if new_ids:
        first_seen = {}
        for r in rows:
            if r["station_id"] in new_ids and r["station_id"] not in first_seen:
                first_seen[r["station_id"]] = {
                    "station_id": r["station_id"],
                    "name": f"Station {r['station_id']}",
                    "latitude": r["latitude"],
                    "longitude": r["longitude"],
                    # Best guess for an auto-registered station: it existed at
                    # least as early as its first reading.
                    "installed_at": r["ts"],
                }
        station_rows = list(first_seen.values())

    reading_rows = [
        {k: v for k, v in r.items() if k not in ("latitude", "longitude")}
        for r in rows
    ]

    async with engine.begin() as conn:
        if new_ids:
            # DO NOTHING rather than update: the simulator's coordinates are
            # fixed, and a manually curated station row must win over telemetry.
            await conn.execute(
                pg_insert(Station).on_conflict_do_nothing(), station_rows
            )
        # Dedup on the (station_id, ts) PK absorbs Kafka redeliveries.
        await conn.execute(pg_insert(Reading).on_conflict_do_nothing(), reading_rows)

    known_stations.update(new_ids)


async def consume_sensor_readings() -> None:
    """Run forever; started as a background task and cancelled on shutdown."""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,  # commit manually, after the DB write
        auto_offset_reset="earliest",
    )

    delay = 1.0
    while True:
        try:
            await consumer.start()
            logger.info("Consuming %r from %s", TOPIC, KAFKA_BOOTSTRAP)
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("Kafka not ready (%s), retrying in %.0fs", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)

    known_stations: set[int] = set()
    ingested = 0
    window_start = time.monotonic()

    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=BATCH_WINDOW_MS, max_records=BATCH_MAX_MESSAGES
            )
            rows = [
                row
                for messages in batches.values()
                for m in messages
                if (row := _parse(m.value)) is not None
            ]
            if rows:
                await _flush(rows, known_stations)
                await consumer.commit()
                ingested += len(rows)

            elapsed = time.monotonic() - window_start
            if elapsed >= THROUGHPUT_LOG_INTERVAL_S:
                logger.info(
                    "Ingestion: %.1f msg/s (%d msgs / %.0fs), %d stations known",
                    ingested / elapsed, ingested, elapsed, len(known_stations),
                )
                ingested = 0
                window_start = time.monotonic()
    finally:
        await consumer.stop()
