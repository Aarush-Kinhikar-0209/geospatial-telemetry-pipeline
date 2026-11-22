# 📋 Test Guide: Docker Compose Stack & Pipeline

This guide documents how to run the Real-Time Geospatial Fleet Analytics Engine with specific configuration parameters, and how to verify the output end-to-end.

## 📋 Test Configuration

Settings live in `.env` (copy from `.env.example`). Compose substitutes them into `docker-compose.yml`.

| Parameter | Default | Slow demo | Purpose |
|-----------|---------|-----------|---------|
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / *(required)* | | ClickHouse credentials shared by ClickHouse and the processor |
| `VEHICLE_COUNT` | `200` | `1` | Number of simulated vehicles |
| `MESSAGE_INTERVAL` | `0.2` | `2.0` | Seconds between messages per vehicle (200 x 0.2s ~= 1000 msg/s) |
| `CHAOS_PERCENTAGE` | `5` | `5` | Exact share of malformed payloads, for DLQ testing |
| `PROCESSOR_LOG_LEVEL` | `INFO` | `DEBUG` | `DEBUG` logs every DLQ record with its reason |

Processor settings fixed in `docker-compose.yml`: `WINDOW_SECONDS=10` (tumbling window), `ALLOWED_LATENESS_SECONDS=5`, `CHECKPOINT_INTERVAL=10` (ClickHouse flush + offset commit), `STATE_TTL_SECONDS=60` (Redis key expiry).

> **Windows:** keep `.env` with LF line endings. Compose does not strip `\r`, so a CRLF file turns the password into `secret\r`.

---

## 🚀 Quick Start

### 1. Start the Data Platform

```bash
cp .env.example .env    # set CLICKHOUSE_PASSWORD
docker compose up -d --build
```

This starts 5 services: Redpanda, ClickHouse, Redis, Generator, and Processor.

### 2. Verify All Services Are Healthy

```bash
docker compose ps
```

Redpanda, ClickHouse and Redis must show `(healthy)`; generator and processor show `Up`.

Kafka is reachable from containers at `redpanda:29092` and from the host at `localhost:9092`.

---

## 🔬 Running the Test

### 1. Watch Generator Output

```bash
docker compose logs -f generator
```

A metrics line every 10 seconds:
```text
generator: window=10s rate=1000.0 msg/sec total=10000 chaos=500 (5.0%)
```

### 2. Watch Processor Output

```bash
docker compose logs -f processor
```

Expected:
```text
processor: Connected to Kafka at redpanda:29092 (DLQ topic telemetry_dlq)
processor: Connected to Redis at redis:6379
processor: Connected to ClickHouse at clickhouse:9000
processor: Checkpoint: 9500 event(s), 193 window(s) -> ClickHouse; open windows=190
processor: rate=994.7 msg/sec processed=10008 dlq=500 redis_latency_p95=18.6ms
```

`redis_latency_p95` is the time from the Kafka record timestamp to the Redis write (target < 200 ms).

### 3. Verify Data in ClickHouse

`clickhouse-client` inside the container picks up the credentials from its environment.

```bash
# Tumbling-window averages (the view finalizes the AggregatingMergeTree partials)
docker exec clickhouse clickhouse-client -q "SELECT * FROM fleet_analytics.speed_rollups_avg ORDER BY window_start DESC LIMIT 5 FORMAT PrettyCompact"

# Raw validated events (FINAL collapses rows replayed after a crash)
docker exec clickhouse clickhouse-client -q "SELECT count() FROM fleet_analytics.telemetry_events FINAL"

# From the host over HTTP
curl -u "default:$CLICKHOUSE_PASSWORD" "http://localhost:8123/?query=SELECT%201"
```

### 4. Verify Data in Redis

```bash
docker exec redis redis-cli HGETALL vehicle:v-001
```

Every vehicle's `timestamp` should be under 15 seconds old. Keys expire after 60 seconds without updates.

### 5. Verify Data in Dead Letter Queue

```bash
docker exec redpanda rpk topic consume telemetry_dlq -n 10 -f '%v  <- %h\n'
```

Each record is the exact raw payload, with an `error` header giving the reason.

### 6. Consumer Lag

```bash
docker exec redpanda rpk group describe stream-processor
```

While running, committed lag stays around 15 seconds of data (window + lateness). The processor never commits past a record whose window is not yet in ClickHouse. After a clean `docker compose stop processor`, lag is 0.

---

## 🧪 Unit Tests

```bash
pip install -r requirements-dev.txt
pytest
```

---

## 🧹 Cleanup

```bash
docker compose down -v --remove-orphans
```

---

## 📁 Repository Structure

```
geospatial-telemetry-pipeline/
├── docker-compose.yml
├── .env.example
├── requirements-dev.txt
├── generator/          # Synthetic data producer
├── processor/          # Stream processor (DLQ, H3, windowing) + demo_local.py
├── infrastructure/
│   └── docker-entrypoint-initdb.d/init-clickhouse.sql
├── tests/              # pytest suites
├── wikis/
└── ui/
```
