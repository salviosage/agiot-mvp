"""AgIoT backend skeleton.

No business logic yet: exposes health endpoints that verify connectivity
to TimescaleDB and Kafka over the compose network.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from sqlalchemy import text

from app.db import engine, init_db
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


app = FastAPI(title="AgIoT MVP Backend", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/health/deps")
async def health_deps() -> dict:
    """Check that TimescaleDB and Kafka are reachable."""
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
