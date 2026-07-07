"""AgIoT producer skeleton.

No business logic yet: connects to Kafka over the compose network to prove
reachability, then idles until real telemetry production is implemented.
"""

import asyncio
import logging
import os

from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agiot.producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    while True:
        try:
            await producer.start()
            break
        except Exception:  # noqa: BLE001
            logger.warning("Kafka not reachable yet at %s, retrying in 3s", KAFKA_BOOTSTRAP_SERVERS)
            await asyncio.sleep(3)

    logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
    try:
        while True:
            await asyncio.sleep(60)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
