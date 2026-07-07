"""Simulated fleet of weather/soil stations publishing to Kafka.

Each station is one coroutine holding its own state; values evolve by a small
random walk between readings (so consecutive readings are correlated, unlike
pure noise), all sharing a single AIOKafkaProducer.
"""

import asyncio
import json
import logging
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agiot.simulator")

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
)
NUM_STATIONS = int(os.environ.get("NUM_STATIONS", "50"))
TOPIC = "sensor-readings"

# Atlantic Canada bounding box
LAT_RANGE = (44.0, 49.0)
LON_RANGE = (-68.0, -60.0)


def _drift(value: float, step: float, lo: float, hi: float) -> float:
    """One random-walk step, clamped to the field's physical range."""
    return min(hi, max(lo, value + random.gauss(0, step)))


@dataclass
class Station:
    station_id: int
    latitude: float
    longitude: float
    temperature_c: float
    humidity_pct: float
    wind_speed_ms: float
    wind_direction_deg: float
    pressure_hpa: float
    soil_moisture_pct: float
    battery_pct: float
    raining: bool = False
    rain_intensity_mm: float = 0.0  # mm per reading while raining

    @classmethod
    def spawn(cls, station_id: int) -> "Station":
        return cls(
            station_id=station_id,
            latitude=round(random.uniform(*LAT_RANGE), 5),
            longitude=round(random.uniform(*LON_RANGE), 5),
            temperature_c=random.uniform(5, 25),
            humidity_pct=random.uniform(40, 95),
            wind_speed_ms=random.uniform(0, 12),
            wind_direction_deg=random.uniform(0, 360),
            pressure_hpa=random.uniform(995, 1030),
            soil_moisture_pct=random.uniform(15, 60),
            battery_pct=random.uniform(70, 100),
        )

    def advance(self) -> None:
        """Evolve state by one reading interval."""
        self.temperature_c = _drift(self.temperature_c, 0.3, -30, 40)
        self.humidity_pct = _drift(self.humidity_pct, 1.5, 10, 100)
        self.wind_speed_ms = _drift(self.wind_speed_ms, 0.7, 0, 40)
        self.wind_direction_deg = math.fmod(
            self.wind_direction_deg + random.gauss(0, 12) + 360, 360
        )
        self.pressure_hpa = _drift(self.pressure_hpa, 0.4, 960, 1050)

        # Rain comes in episodes: rarely starts, tends to persist, then stops.
        if self.raining:
            self.rain_intensity_mm = _drift(self.rain_intensity_mm, 0.2, 0.05, 5.0)
            if random.random() < 0.05:
                self.raining = False
                self.rain_intensity_mm = 0.0
        elif random.random() < 0.01:
            self.raining = True
            self.rain_intensity_mm = random.uniform(0.1, 1.0)

        # Soil wets up while it rains, dries out slowly otherwise.
        if self.raining:
            self.soil_moisture_pct = _drift(self.soil_moisture_pct + 0.3, 0.2, 0, 100)
        else:
            self.soil_moisture_pct = _drift(self.soil_moisture_pct - 0.02, 0.1, 0, 100)

        # Battery only ever drains, slowly.
        self.battery_pct = max(0.0, self.battery_pct - random.uniform(0.001, 0.01))

    def reading(self) -> dict:
        return {
            "station_id": self.station_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "temperature_c": round(self.temperature_c, 2),
            "humidity_pct": round(self.humidity_pct, 2),
            "wind_speed_ms": round(self.wind_speed_ms, 2),
            "wind_direction_deg": round(self.wind_direction_deg, 1),
            "rainfall_mm": round(self.rain_intensity_mm, 2),
            "pressure_hpa": round(self.pressure_hpa, 2),
            "soil_moisture_pct": round(self.soil_moisture_pct, 2),
            "battery_pct": round(self.battery_pct, 2),
        }


async def start_with_retry(producer: AIOKafkaProducer) -> None:
    """Kafka may still be booting when we start; back off exponentially."""
    delay = 1.0
    while True:
        try:
            await producer.start()
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Kafka not ready (%s), retrying in %.0fs", exc, delay
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def run_station(producer: AIOKafkaProducer, station: Station) -> None:
    while True:
        await asyncio.sleep(random.uniform(3, 8))
        station.advance()
        # Key by station_id so a station's readings stay ordered per partition.
        await producer.send_and_wait(
            TOPIC,
            value=json.dumps(station.reading()).encode(),
            key=str(station.station_id).encode(),
        )


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await start_with_retry(producer)

    stations = [Station.spawn(sid) for sid in range(1, NUM_STATIONS + 1)]
    logger.info("Simulating %d stations -> topic %r", len(stations), TOPIC)
    try:
        await asyncio.gather(*(run_station(producer, s) for s in stations))
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
