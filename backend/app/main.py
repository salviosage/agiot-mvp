"""AgIoT backend skeleton.

No business logic yet: exposes health endpoints that verify connectivity
to TimescaleDB and Kafka over the compose network.
"""

import logging
import os

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agiot.backend")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://agiot:agiot@timescaledb:5432/agiot"
)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

app = FastAPI(title="AgIoT MVP Backend")

engine = create_async_engine(DATABASE_URL)


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
