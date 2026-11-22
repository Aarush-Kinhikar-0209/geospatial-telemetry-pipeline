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
| `CORS_ORIGINS` | `http://localhost:3000,http://localhost:5173` | | Comma-separated browser origins allowed by the API |
| `VITE_MAP_STYLE_URL` | *(empty = CARTO Dark Matter)* | | Base map style for the UI image (build time) |
| `VITE_DLQ_ALERT_RATIO` | `0.1` | | DLQ share over 3 s that turns the UI's DLQ monitor red (build time) |

Processor settings fixed in `docker-compose.yml`: `WINDOW_SECONDS=10` (tumbling window), `ALLOWED_LATENESS_SECONDS=5`, `CHECKPOINT_INTERVAL=10` (ClickHouse flush + offset commit), `STATE_TTL_SECONDS=60` (Redis key expiry).

> **Windows:** keep `.env` with LF line endings. Compose does not strip `\r`, so a CRLF file turns the password into `secret\r`.

---

## 🚀 Quick Start

### 1. Start the Data Platform

```bash
cp .env.example .env    # set CLICKHOUSE_PASSWORD
docker compose up -d --build
```

This starts 7 services: Redpanda, ClickHouse, Redis, Generator, Processor, the API, and the UI.

### 2. Verify All Services Are Healthy

```bash
docker compose ps
```

Redpanda, ClickHouse, Redis, the API and the UI must show `(healthy)`; generator and processor show `Up`.

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

### 7. Serving API

Swagger UI: http://localhost:8000/docs

```bash
# Health of both stores and the live feed (503 + "degraded" if a store is down)
curl -s http://localhost:8000/api/v1/health

# Snapshot of every live vehicle (from Redis)
curl -s http://localhost:8000/api/v1/vehicles | head -c 300

# Live stream: a `snapshot` event, then batched `update` events (~50 ms coalescing)
curl -N http://localhost:8000/api/v1/stream/vehicles
curl -N "http://localhost:8000/api/v1/stream/vehicles?format=ndjson"

# Top 5 busiest H3 cells over 15 minutes; query_ms is the ClickHouse time (target < 50 ms)
curl -s "http://localhost:8000/api/v1/analytics/zones?minutes=15&limit=5"

# DLQ rate over the last 10 s plus a 60 s per-second series
curl -s "http://localhost:8000/api/v1/metrics/dlq?window=10&history=60"
```

With the default 5% chaos, `dlq_ratio` sits near `0.05`.

**SSE load test** (reports push latency = API delivery time minus processor write time):

```bash
python api/loadtest_sse.py --clients 50 --seconds 20 --procs 4
python api/loadtest_sse.py --clients 100 --seconds 20 --procs 8
```

Reference run (WSL2, 10 cores): 50 clients p95 = 88 ms; 100 clients all connected, p95 = 130 ms (the load generator itself becomes the bottleneck when it runs on the same machine).

**Database drop:** `docker compose stop clickhouse` makes `/analytics/zones` return `503` with `{"detail": ..., "dependency": "clickhouse"}`; `docker compose stop redis` does the same for the Redis-backed routes. Open SSE streams stay connected through a Redis outage and resume when it returns. Start the service again and the endpoints recover without restarting the API.

### 8. Live Map UI

Open http://localhost:3000 (nginx serves the bundle and proxies `/api`, including the SSE stream).

| Check | Expected |
|-------|----------|
| Render | Dark base map of Mumbai, no "Base map" error banner, no console errors |
| Data binding | Fleet panel shows `Live` and ~`VEHICLE_COUNT` active vehicles; markers glide between updates |
| Zones | Purple H3 cells on the map; top 5 listed under "Busiest zones" |
| Time to live data | Footer "first data in … ms" < 1000 ms |
| FPS | Fleet panel FPS ~60 (at the display's refresh rate) |
| DLQ monitor | `Nominal` at ~5%; Redis / ClickHouse / Feed dots green |

**Chaos spike:**
```bash
CHAOS_PERCENTAGE=50 docker compose up -d generator
```
Within ~2 s of the generator reconnecting, the DLQ monitor turns red, flashes, and shows `DLQ spike`. Restore with `docker compose up -d generator`.

**1,000 markers:** `VEHICLE_COUNT=1000 MESSAGE_INTERVAL=1.0 docker compose up -d generator` (same ~1000 msg/s). Vehicles silent for 30 s are pruned client-side, so switching back to 200 vehicles shrinks the count after half a minute.

**Outages (no page reload needed):**
- `docker compose stop redis`: the stream stays connected but goes `Stalled` after 5 s; the Redis and Feed dots turn red. `start redis` and markers move again within a few seconds.
- `docker compose stop api`: the pill shows `Reconnecting…` and the API dot turns red. `start api` and it is `Live` again within ~2 s of the API being up.

Reference run (Edge headless on Intel UHD integrated GPU, 1,000 vehicles): 56 FPS, equal to that browser's empty-page frame rate; JS main thread ~16% busy; first data 290–460 ms on reload; JS heap flat over 30 minutes.

**UI dev server:** `cd ui && npm ci && npm run dev` serves http://localhost:5173 with `/api` proxied to `localhost:8000`.

---

## 🧪 Unit Tests

```bash
pip install -r requirements-dev.txt
pytest

cd ui && npm ci && npm run typecheck && npm test
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
├── api/                # FastAPI serving layer + loadtest_sse.py
├── infrastructure/
│   └── docker-entrypoint-initdb.d/init-clickhouse.sql
├── tests/              # pytest suites
├── wikis/
└── ui/                 # React + Vite + deck.gl dashboard (Dockerfile -> nginx)
```
