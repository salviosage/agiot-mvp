# agiot-mvp

Skeleton for an agricultural IoT MVP. No business logic yet — this repo just wires
up the infrastructure so the services build, start, and can reach each other.

## Services

| Service     | Image / Build                      | Port (host) |
| ----------- | ---------------------------------- | ----------- |
| backend     | `./backend` (FastAPI + uvicorn)    | 8000        |
| kafka       | `bitnamilegacy/kafka` (KRaft, no ZooKeeper) | 9092 |
| timescaledb | `timescale/timescaledb:latest-pg16` | 5432       |
| producer    | `./producer` (aiokafka)            | —           |

## Run

```sh
docker compose up --build
```

TimescaleDB and Kafka have healthchecks; backend and producer wait for them to be
healthy before starting.

## Verify

- Backend liveness: `curl http://localhost:8000/health`
- Cross-service connectivity (backend → TimescaleDB and Kafka):
  `curl http://localhost:8000/health/deps` → `{"timescaledb": "ok", "kafka": "ok"}`
- Producer → Kafka: `docker compose logs producer` should show `Connected to Kafka`.

## Notes

- Postgres credentials (dev only): user `agiot`, password `agiot`, database `agiot`.
- Inside the compose network, Kafka is `kafka:9092` and TimescaleDB is `timescaledb:5432`.
- Kafka advertises `kafka:9092`, so host-side clients on `localhost:9092` will only
  work if they can resolve `kafka` (containers are unaffected).

## Stop

```sh
docker compose down        # keep data volumes
docker compose down -v     # also wipe TimescaleDB/Kafka data
```
