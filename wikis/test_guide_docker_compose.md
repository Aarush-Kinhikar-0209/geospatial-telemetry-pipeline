# 📋 Test Guide: Docker Compose Stack & Pipeline

This guide documents how to run the Real-Time Geospatial Fleet Analytics Engine with specific configuration parameters, and how to verify the output end-to-end.

## 📋 Test Configuration

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `VEHICLE_COUNT` | `1` | Simulate a single delivery vehicle |
| `CHAOS_PERCENTAGE` | `5.0` | Inject 5% malformed payloads for DLQ testing |
| `MESSAGE_INTERVAL` | `2.0` | Produce 1 telemetry message per vehicle every 2 seconds |
| `WINDOW_SECONDS` | `10` | Tumbling window size (ClickHouse aggregates) |
| `CHECKPOINT_INTERVAL` | `30` | Processor checkpoint flush interval |

---

## 🚀 Quick Start

### 1. Start the Data Platform

```powershell
cd d:\geospatial-telemetry-pipeline
docker compose up -d
```

This starts 5 services: Redpanda, ClickHouse, Redis, Generator, and Processor.

### 2. Verify All Services Are Healthy

```powershell
docker compose ps
```

Expected output:
```txt
NAME         STATUS                    PORTS
clickhouse   Up X min (health: healthy) 0.0.0.0:8123->8123/tcp, 0.0.0.0:9000->9000/tcp
generator    Up X min                                    (generator/main.py)
processor    Up X min                                    (python main.py)
redis        Up X min (healthy)  0.0.0.0:6379->6379/tcp
redpanda     Up X min (healthy)  0.0.0.0:9092->9092/tcp
```

All three data services (ClickHouse, Redis, Redpanda) must show `(health: healthy)` or `Up`.

---

## 🔬 Running the Test

### 1. Watch Generator Output (Terminal View)

In a separate terminal, stream the generator logs:

```powershell
docker compose logs -f generator
```

You should see 1 message every 2 seconds:
```text
2026-11-19 18:01:30,802 [INFO] generator: window=10s rate=0.5 msg/sec total=10 chaos=1 (10.0%)
2026-11-19 18:01:40,807 [INFO] generator: window=10s rate=0.5 msg/sec total=15 chaos=0 (0.0%)
2026-11-19 18:01:50,807 [INFO] generator: window=10s rate=0.5 msg/sec total=20 chaos=0 (0.0%)
2026-11-19 18:02:00,809 [INFO] generator: window=10s rate=0.5 msg/sec total=30 chaos=0 (0.0%)
```

**Note:** `chaos=1` means 1 out of 10 messages in that 10-second window was intentionally malformed. This varies per window due to the random 5% chaos injection.

### 2. Watch Processor Output (Terminal View)

In another terminal, stream the processor logs:

```powershell
docker compose logs -f processor
```

Expected behavior:
- **Connected to Kafka, Redis, ClickHouse** — all three connections successful
- **Schema violations routed to DLQ** — messages with missing fields or malformed JSON are caught and sent to `telemetry_dlq`
- **No crash** — the processor keeps running through malformed data

Example output:
```text
2026-11-19 18:02:45,764 [INFO] processor: Connected to ClickHouse at clickhouse:9000
2026-11-19 18:02:46,071 [WARNING] processor: Schema violation, routing to DLQ: ...
2026-11-19 18:04:08,075 [WARNING] processor: Schema violation, routing to DLQ: ...
2026-11-19 18:04:34,110 [WARNING] processor: Schema violation, routing to DLQ: ...
2026-11-19 18:04:44,120 [WARNING] processor: Schema violation, routing to DLQ: ...
```

### 3. Verify Data in ClickHouse

```powershell
docker exec clickhouse clickhouse-client --query "SELECT * FROM fleet_analytics.speed_rollups FORMAT Vertical"
```

### 4. Verify Data in Redis

```powershell
docker exec redis redis-cli HGETALL vehicle:v-001
```

### 5. Verify Data in Dead Letter Queue

```powershell
docker exec redpanda rpk topic consume telemetry_dlq -n 10
```

---

## 🧹 Cleanup

```powershell
docker compose down -v --remove-orphans
```

---

## 📁 Repository Structure After Test

```
d:\geospatial-telemetry-pipeline\
├── generator\          # Synthetic data producer
│   ├── main.py
│   ├── .venv\
│   └── requirements.txt
├── processor\          # Stream processor (DLQ, H3, windowing)
│   ├── main.py
│   ├── .venv\
│   ├── requirements.txt
│   ├── demo_local.py
│   └── Dockerfile
├── infrastructure\
│   └── docker-entrypoint-initdb.d\init-clickhouse.sql
├── wikis\
│   └── test_guide_docker_compose.md
└── ui\               # (empty - Next.js frontend)
```