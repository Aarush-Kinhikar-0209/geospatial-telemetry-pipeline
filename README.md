# 🏎️ Real-Time Geospatial Fleet Analytics Engine

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=white)
![Redis](https://img.shields.io/badge/redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![React](https://img.shields.io/badge/react-%2320232a.svg?style=for-the-badge&logo=react&logoColor=%2361DAFB)
![Vite](https://img.shields.io/badge/vite-%23646CFF.svg?style=for-the-badge&logo=vite&logoColor=white)

An end-to-end, real-time data engineering pipeline demonstrating distributed stream processing, stateful windowing, and resilient architecture patterns (Dead Letter Queues). 

This project simulates a live fleet of delivery vehicles, processes their telemetry at high throughput, and serves sub-second spatial aggregations to a hardware-accelerated frontend.

## 🌟 Key Engineering Features

*   **Resilient Streaming (DLQ):** Intentionally injects malformed JSON and schema violations to demonstrate robust Dead Letter Queue routing without dropping the pipeline.
*   **Stateful Windowing:** Computes tumbling window aggregations (e.g., rolling speed averages) in real-time.
*   **Multi-Sink Architecture:** Routes "hot" state (current location) to Redis for $O(1)$ lookups, and "cold/analytical" state to ClickHouse for lightning-fast OLAP queries.
*   **Spatial Indexing:** Encodes raw latitude/longitude coordinates into H3 hexagonal grids on the fly for efficient geographic aggregations.

## 🏗️ Architecture

```mermaid
graph TD
    A["Telemetry Generator<br/>(Simulates 200+ Vehicles)"] -->|JSON over Kafka protocol| B["Kafka / Redpanda Broker"]
    
    B --> C["Stream Processor<br/>(Python asyncio + aiokafka)"]
    
    C -->|Malformed Pings| D["Dead Letter Queue (DLQ)"]
    C -->|Aggregates & Geohashes| E["ClickHouse<br/>(OLAP Storage)"]
    C -->|Latest Coordinates| F["Redis<br/>(In-Memory Cache)"]
    
    E --> G["FastAPI Backend"]
    F --> G
    
    G -->|Server-Sent Events + REST| H["React + deck.gl UI<br/>(Vite, MapLibre)"]
```

## 📂 Repository Structure

*   `docker-compose.yml`: Orchestrates every service (lives at the repository root).
*   `/infrastructure`: Initialization scripts for ClickHouse.
*   `/generator`: Python script to simulate fleet movement and inject chaotic/malformed data.
*   `/processor`: Stream processing application handling geohashing, windowing, and DLQ routing.
*   `/api`: FastAPI application serving SSE streams and aggregate REST endpoints.
*   `/ui`: React + Vite frontend with deck.gl (WebGL) layers over a MapLibre base map.
*   `/tests`: Unit and API contract tests for the generator, processor and API (`pytest`). UI unit tests live in `ui/src` (`vitest`).
*   `/docs`: Project documentation.
*   `/wikis`: Architecture notes and phase-by-phase acceptance criteria.

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
   git clone https://github.com/yourusername/realtime-fleet-analytics.git
   cd realtime-fleet-analytics
   ```

2. **Spin up the data platform:**
   ```bash
   cp .env.example .env   # then edit credentials / tuning knobs
   docker compose up -d
   ```
   *This single command boots the broker, databases, stream processor, data generator, backend API, and the UI.*

3. **View the Dashboard:**
   Navigate to `http://localhost:3000` to see the live map, telemetry metrics, and the DLQ monitor in action. The API's Swagger UI is at `http://localhost:8000/docs`.

4. **(Optional) Run the UI with hot reload:**
   ```bash
   cd ui
   npm ci
   npm run dev   # http://localhost:5173, proxies /api to http://localhost:8000
   ```

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

If the style fails to load, the UI shows a red "Base map" banner; vehicles and zones still render on a dark background.

## 🧪 Chaos Engineering & DLQ

To see the Dead Letter Queue in action, adjust the chaos parameter in your `.env` (consumed by the `generator` service in `docker-compose.yml`):
```bash
CHAOS_PERCENTAGE=5  # Forces 5% of records to violate schema
```
Watch the "DLQ Monitor" on the frontend light up as bad payloads are safely quarantined without crashing the consumer. To force a spike without editing `.env`:
```bash
CHAOS_PERCENTAGE=50 docker compose up -d generator   # monitor turns red within ~2 s
docker compose up -d generator                       # back to the .env value
```
The monitor alerts when the dead-lettered share over the last 3 seconds reaches `VITE_DLQ_ALERT_RATIO` (default `0.1`).

## 🛠️ Built With

*   **Ingestion:** Python, Redpanda (Kafka compatible)
*   **Processing:** Python asyncio, aiokafka, H3
*   **Storage:** ClickHouse, Redis
*   **Serving:** FastAPI (Python), Server-Sent Events (SSE)
*   **Frontend:** React 18, Vite, deck.gl, MapLibre GL (react-map-gl), Recharts