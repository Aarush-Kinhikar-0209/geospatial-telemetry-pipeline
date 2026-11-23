# 📋 Test Guide: Docker Compose Stack & Pipeline

This guide documents how to run the Real-Time Geospatial Fleet Analytics Engine with specific configuration parameters, and how to verify the output end-to-end.

## 📋 Test Configuration

Settings live in `.env` (copy from `.env.example`). Compose substitutes them into `docker-compose.yml`.

| Parameter | Default | Slow demo | Purpose |
|-----------|---------|-----------|---------|
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / *(required)* | | ClickHouse credentials shared by ClickHouse and the processor |
| `SIMULATION_DURATION_SECONDS` | `120` | `20` | How long one simulation runs before its data is cleared |
| `SIMULATION_HEARTBEAT_TIMEOUT_SECONDS` | `10` | | A running simulation is ended and cleared when the page sends no heartbeat for this long |
| `PROCESSOR_LOG_LEVEL` | `INFO` | `DEBUG` | `DEBUG` logs every DLQ record with its reason |
| `CORS_ORIGINS` | `http://localhost:3000,http://localhost:5173` | | Comma-separated browser origins allowed by the API |
| `VITE_MAP_STYLE_URL` | *(empty = CARTO Dark Matter)* | | Base map style for the UI image (build time) |
| `VITE_DLQ_ALERT_RATIO` | `0.1` | | DLQ share over 3 s that turns the UI's DLQ monitor red (build time) |

Vehicles (1-200), interval (0.1-2.0 s) and chaos (0-5%) are chosen per simulation, in the UI or through `POST /api/v1/simulation/start`; they are not environment settings.

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

The stack starts **idle**: the generator logs `Waiting for a simulation` and no data exists until one is started.

### 3. Start a Simulation

In the UI (http://localhost:3000) press **Start simulation**, or with the API (this one matches the old default load, 200 vehicles x 5 msg/s ~= 1000 msg/s):

```bash
curl -s -X POST http://localhost:8000/api/v1/simulation/start \
  -H 'Content-Type: application/json' \
  -d '{"vehicles": 200, "interval_seconds": 0.2, "chaos_percent": 5}'

# A run needs a heartbeat at least every SIMULATION_HEARTBEAT_TIMEOUT_SECONDS, which the UI sends
# for you. From the shell, keep one going (and stop it with Ctrl-C when you are done):
while true; do curl -s -o /dev/null -X POST http://localhost:8000/api/v1/simulation/heartbeat; sleep 2; done

curl -s http://localhost:8000/api/v1/simulation          # status, config, remaining_seconds
curl -s -X POST http://localhost:8000/api/v1/simulation/stop   # end it early
```

Without heartbeats the run is ended as "client lost" after 10 s and cleared, which is the behaviour for a closed browser tab. Invalid input is rejected with `422` (vehicles 1-200, interval 0.1-2.0, chaos 0-5 as a whole number); starting while a run is active returns `409`.

### 4. Verify Clearing

When the timer ends (or on stop, or when the page goes away) the status goes `running` -> `stopping` -> `cleaning` -> `idle`. Then:

```bash
docker exec redis redis-cli --scan --pattern 'vehicle:*'            # empty
docker exec redis redis-cli --scan --pattern 'metrics:pipeline:*'   # empty
docker exec clickhouse clickhouse-client -q "SELECT count() FROM fleet_analytics.telemetry_events"   # 0
docker exec clickhouse clickhouse-client -q "SELECT count() FROM fleet_analytics.speed_rollups"      # 0
docker exec redpanda rpk topic describe telemetry_raw -p            # LOG-START-OFFSET == HIGH-WATERMARK
docker exec redpanda rpk group describe stream-processor            # TOTAL-LAG 0
docker compose logs processor | grep Cleared
```

Kafka offsets keep counting up across runs (a purge moves the log start; it does not reset the numbering), so compare start with high watermark rather than expecting zero. The `simulation:*` keys in Redis are control state and are deliberately kept.

---

## 🔬 Running the Test

These steps need a simulation to be running (see above).

### 1. Watch Generator Output

```bash
docker compose logs -f generator
```

A line when a run starts and stops, and a metrics line every 10 seconds while it runs:
```text
generator: Run 0d779cc062f9 started: 200 vehicles, 0.20s interval, 5% chaos
generator: window=10s rate=1000.0 msg/sec total=10000 chaos=500 (5.0%)
generator: Run 0d779cc062f9 stopped and flushed
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

# DLQ rate over the last 10 s plus a 60 s per-second series
curl -s "http://localhost:8000/api/v1/metrics/dlq?window=10&history=60"
```

At 5% chaos, `dlq_ratio` sits near `0.05`.

**SSE load test** (reports push latency = API delivery time minus processor write time):

```bash
python api/loadtest_sse.py --clients 50 --seconds 20 --procs 4
python api/loadtest_sse.py --clients 100 --seconds 20 --procs 8
```

Reference run (WSL2, 10 cores): 50 clients p95 = 88 ms; 100 clients all connected, p95 = 130 ms (the load generator itself becomes the bottleneck when it runs on the same machine).

**Database drop:** `docker compose stop clickhouse` makes `/health` return `503` with `"clickhouse": false`. `docker compose stop redis` makes the Redis-backed routes return `503` with `"dependency": "redis"`. Open SSE streams stay connected through a Redis outage and resume when it returns. Start the service again and the endpoints recover without restarting the API.

### 8. Live Map UI

Open http://localhost:3000 (nginx serves the bundle and proxies `/api`, including the SSE stream).

| Check | Expected |
|-------|----------|
| Render | Dark base map of Mumbai, no "Base map" error banner, no console errors |
| Idle | Panel pills say `Idle`; the form shows 100 / 2.0 / 5; the map and Fleet stats are empty |
| Validation | Vehicles outside 1-200, interval outside 0.1-2.0, chaos outside 0-5 or not a whole number shows an inline error and disables **Start simulation** |
| Start | The form is replaced by a `M:SS` countdown, a progress bar, and a summary such as "20 vehicles · every 0.5 s · 2% chaos" |
| Data binding | Fleet panel shows `Live` and as many active vehicles as you chose; markers glide between updates |
| Time to live data | Footer "first data in … ms" < 1000 ms |
| FPS | Fleet panel FPS ~60 (at the display's refresh rate) |
| DLQ monitor | `Nominal` at ~5%; Redis / ClickHouse / Feed dots green |
| Reload mid-run | The run continues and the countdown shows the same time |
| Time up | The pill goes `Running` -> `Stopping…` -> `Clearing data…` -> `Idle`; the map, stats, DLQ history and speed chart are empty and a "cleared" notice appears |
| Page closed | Close the tab mid-run; ~10 s later (heartbeat timeout) the run is ended and cleared (`GET /api/v1/simulation` -> `last_run.reason: "client_lost"`) |

**Chaos spike** (above the form's 5% limit): run a standalone generator, configured from the environment:
```bash
docker compose run --rm -e CHAOS_PERCENTAGE=50 -e VEHICLE_COUNT=50 -e MESSAGE_INTERVAL=0.5 \
  generator python main.py --standalone
```
Within ~2 s the DLQ monitor turns red, flashes, and shows `DLQ spike`. Ctrl-C stops it. This traffic is not part of a simulation; the next simulation's cleanup removes it.

**1,000 markers:** `docker compose run --rm -e VEHICLE_COUNT=1000 -e MESSAGE_INTERVAL=1.0 generator python main.py --standalone` (~1000 msg/s). The map only draws vehicles while a simulation is running, so start one as well (its vehicles share the `v-001`... IDs); the standalone fleet then shows up on the map too. Vehicles silent for 30 s are pruned client-side.

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
