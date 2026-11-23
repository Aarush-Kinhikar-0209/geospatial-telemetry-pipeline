# 🏎️ Real-Time Geospatial Fleet Analytics Engine

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=white)
![Redis](https://img.shields.io/badge/redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![React](https://img.shields.io/badge/react-%2320232a.svg?style=for-the-badge&logo=react&logoColor=%2361DAFB)
![Vite](https://img.shields.io/badge/vite-%23646CFF.svg?style=for-the-badge&logo=vite&logoColor=white)

An end-to-end, real-time data engineering pipeline demonstrating distributed stream processing, stateful windowing, and resilient architecture patterns (Dead Letter Queues). 

This project simulates a live fleet of delivery vehicles, processes their telemetry at high throughput, and serves live positions to a hardware-accelerated map.

## 🌟 Key Engineering Features

*   **Resilient Streaming (DLQ):** Intentionally injects malformed JSON and schema violations to demonstrate robust Dead Letter Queue routing without dropping the pipeline.
*   **Stateful Windowing:** Computes tumbling window aggregations (e.g., rolling speed averages) in real-time.
*   **Multi-Sink Architecture:** Routes "hot" state (current location) to Redis for $O(1)$ lookups, and "cold/analytical" state to ClickHouse for lightning-fast OLAP queries.
*   **Spatial Indexing:** Encodes each telemetry position into an H3 cell and stores it with the ClickHouse history.

## 🏗️ Architecture

```mermaid
graph TD
    A["Telemetry Generator<br/>(Simulates up to 200 Vehicles)"] -->|JSON over Kafka protocol| B["Kafka / Redpanda Broker"]
    
    B --> C["Stream Processor<br/>(Python asyncio + aiokafka)"]
    
    C -->|Malformed Pings| D["Dead Letter Queue (DLQ)"]
    C -->|Windowed speed history| E["ClickHouse<br/>(OLAP Storage)"]
    C -->|Latest Coordinates| F["Redis<br/>(In-Memory Cache)"]
    
    E --> G["FastAPI Backend"]
    F --> G
    
    G -->|Server-Sent Events + REST| H["React + deck.gl UI<br/>(Vite, MapLibre)"]
```

## 📂 Repository Structure

*   `docker-compose.yml`: Orchestrates every service (lives at the repository root).
*   `/infrastructure`: Initialization scripts for ClickHouse.
*   `/generator`: Python service that simulates fleet movement and injects chaotic/malformed data while a simulation runs.
*   `/processor`: Stream processing application handling H3 indexing, windowing, DLQ routing, and clearing a finished simulation.
*   `/api`: FastAPI application serving SSE streams, REST endpoints, and the simulation lifecycle.
*   `/ui`: React + Vite frontend with deck.gl (WebGL) layers over a MapLibre base map.
*   `/tests`: Unit and API contract tests for the generator, processor and API (`pytest`). UI unit tests live in `ui/src` (`vitest`).
*   `/docs`: Reserved for longer write-ups (currently empty).
*   `/wikis`: Architecture notes, phase briefs, and the Docker Compose test guide.

## 🧑‍💻 Local Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

ruff check . && ruff format --check .
pytest

# UI (Node 18.18+)
cd ui
npm ci
npm run typecheck && npm test
```

## 🚀 Quick Start

Ensure you have [Docker](https://docs.docker.com/get-docker/) and Docker Compose installed.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Aarush-Kinhikar-0209/geospatial-telemetry-pipeline.git
   cd geospatial-telemetry-pipeline
   ```

2. **Spin up the data platform:**
   ```bash
   cp .env.example .env   # set CLICKHOUSE_PASSWORD (and any tuning knobs)
   docker compose up -d --build
   ```
   *This builds and starts the broker, databases, stream processor, data generator, backend API, and the UI. Keep `.env` in LF line endings; Compose does not strip a trailing `\r` from values.*

   Step-by-step checks are in [`wikis/test_guide_docker_compose.md`](wikis/test_guide_docker_compose.md).

3. **View the Dashboard:**
   Navigate to `http://localhost:3000`. The stack starts idle; choose a fleet in the **Simulation** panel and press **Start simulation** to see the live map, telemetry metrics, and the DLQ monitor in action (see [Simulations](#-simulations)). The API's Swagger UI is at `http://localhost:8000/docs`.

4. **(Optional) Run the UI with hot reload:**
   ```bash
   cd ui
   npm ci
   npm run dev   # http://localhost:5173, proxies /api to http://localhost:8000
   ```

## ▶️ Simulations

Nothing is generated until you start a simulation. The **Simulation** panel takes:

| Setting | Range | Default |
|---|---|---|
| Vehicles | 1 – 200 (whole number) | 100 |
| Interval (s) | 0.1 – 2.0 | 2.0 |
| Chaos (%) | 0 – 5 (whole number) | 5 |

**Start simulation** runs the fleet for 2 minutes (`SIMULATION_DURATION_SECONDS`) with a countdown. Out-of-range input is flagged in the form and rejected by the API (`422`). Only one simulation runs at a time (`409` otherwise). **Stop & clear data** ends a run early.

When a run ends, **every trace of it is deleted** before a new one can start:

| Store | What is cleared |
|---|---|
| Kafka (Redpanda) | all records in `telemetry_raw` and `telemetry_dlq` (the consumer group moves to the end) |
| ClickHouse | `telemetry_events` and `speed_rollups` are truncated |
| Redis | `vehicle:*` latest states and the per-second `metrics:pipeline:*` counters |

A run ends, and is cleared, when:

* the timer reaches zero,
* you press **Stop & clear data**, or
* **the page goes away.** While a run is going the page sends a heartbeat every 2 s; if none arrives for `SIMULATION_HEARTBEAT_TIMEOUT_SECONDS` (10 s) because the tab was closed, the browser crashed, or the connection dropped, the API ends the run and clears the data. A page reload does not end it: the countdown picks up where it was.

How it fits together: the API owns the lifecycle (`running → stopping → cleaning → idle`) and records it in Redis (`simulation:state`). The generator idles until a run appears, produces for it, and acknowledges once it has stopped and flushed. The processor then discards its in-memory windows, clears the three stores, and acknowledges. Because the processor does the clearing on its own consumer loop, nothing it buffered can be written back after the wipe. Control state lives in Redis, so an API, generator, or processor restart resumes the lifecycle instead of losing it. Endpoints: `GET /api/v1/simulation`, `POST /api/v1/simulation/{start,stop,heartbeat}`.

## 🗺️ Base Map & API Keys

The map works out of the box with the keyless [CARTO Dark Matter](https://github.com/CartoDB/basemap-styles) style, so no key is required.

To use your own provider, point `VITE_MAP_STYLE_URL` at any **MapLibre-compatible** style JSON. For example, with a free [MapTiler](https://cloud.maptiler.com/account/keys/) key:

* **Dev server:** create `ui/.env.local` (gitignored; see `ui/.env.example`):
  ```bash
  VITE_MAP_STYLE_URL=https://api.maptiler.com/maps/dataviz-dark/style.json?key=YOUR_MAPTILER_KEY
  ```
  then restart `npm run dev`.
* **Docker:** put the same line in the root `.env` and rebuild the image (Vite bakes `VITE_*` values in at build time):
  ```bash
  docker compose up -d --build ui
  ```

**Mapbox:** the UI renders with MapLibre GL, which cannot resolve `mapbox://` style URLs. Either use a MapLibre-compatible provider as above, or switch `ui/src/components/FleetMap.tsx` from `react-map-gl/maplibre` to `react-map-gl/mapbox`, add the `mapbox-gl` package, and pass your token via `mapboxAccessToken` (read it from a `VITE_MAPBOX_TOKEN` variable in `ui/.env.local`).

If the style fails to load, the UI shows a red "Base map" banner; vehicle markers still render on a dark background.

## 🧪 Chaos Engineering & DLQ

The generator marks an exact share of payloads as invalid; you choose it (0-5%) when starting a simulation. At 5% the DLQ monitor reports about 5% and stays in the green **Nominal** state. Bad payloads are quarantined without stopping the consumer.

The monitor turns red and flashes **DLQ spike** when the dead-lettered share over the last 3 seconds reaches `VITE_DLQ_ALERT_RATIO` (default `0.1`, baked into the UI image at build time). The simulation form stops at 5%, so force a spike with a standalone generator that is configured from the environment instead:
```bash
docker compose run --rm -e CHAOS_PERCENTAGE=50 -e VEHICLE_COUNT=50 -e MESSAGE_INTERVAL=0.5 \
  generator python main.py --standalone   # monitor turns red within ~2 s; Ctrl-C to stop
```
That traffic is not part of a simulation, so it is cleared by the next simulation's cleanup (or `docker compose down -v`).

## 🛠️ Built With

*   **Ingestion:** Python, Redpanda (Kafka compatible)
*   **Processing:** Python asyncio, aiokafka, H3
*   **Storage:** ClickHouse, Redis
*   **Serving:** FastAPI (Python), Server-Sent Events (SSE)
*   **Frontend:** React 18, Vite, deck.gl, MapLibre GL (react-map-gl), Recharts