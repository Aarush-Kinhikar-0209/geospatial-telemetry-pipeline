# Phase 4: Fast Serving APIs (FastAPI)

## Overview
The frontend needs a low-latency, scalable way to read the data without directly querying databases. This Phase builds a FastAPI backend acting as the bridge, providing REST endpoints for historical aggregates and a Server-Sent Events (SSE) stream for live vehicle tracking.

## Implementation Details
* **Live SSE Endpoint (`/api/v1/stream/vehicles`):**
  * Subscribes to Redis Pub/Sub (if the stream processor publishes there) OR polls Redis keys `vehicle:*` at a 1-second interval.
  * Yields NDJSON (Newline Delimited JSON) to connected clients.
* **Aggregate REST Endpoint (`/api/v1/analytics/zones`):**
  * Queries ClickHouse for the top 5 busiest H3 hexagons over the last 15 minutes.
  * Returns standard JSON payloads.
* **DLQ Metrics Endpoint (`/api/v1/metrics/dlq`):**
  * Queries the stream processor metrics or ClickHouse DLQ sink to return the current rate of dropped/bad messages.

## Ways to Test
1. **SSE Testing:** Use `curl -N http://localhost:8000/api/v1/stream/vehicles` to verify the connection stays open and data is streamed continuously.
2. **Load Testing:** Use Locust or k6 to simulate 100 concurrent SSE connections.
3. **API Contracts:** Write `pytest` scripts utilizing FastAPI's `TestClient` to ensure HTTP 200 responses and correct schema returns for REST endpoints.

## Quality Benchmarks
* **SSE Latency:** Updates pushed to the client within $< 100\text{ms}$ of Redis state changing.
* **REST Performance:** ClickHouse aggregation queries must return in $< 50\text{ms}$.
* **Concurrency:** Must support at least 50 simultaneous SSE client connections with no noticeable degradation.

## Acceptance Criteria
- [ ] FastAPI application is fully typed with Pydantic response models.
- [ ] CORS (Cross-Origin Resource Sharing) is configured to allow requests from the local Next.js/React frontend.
- [ ] Endpoints handle database connection drops gracefully, returning 503 Service Unavailable instead of crashing.
- [ ] API is documented automatically via Swagger UI (available at `/docs`).