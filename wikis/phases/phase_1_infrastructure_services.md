# Phase 1: Core Services Containerization

## Overview
This phase establishes the foundational infrastructure required for the Real-Time Geospatial Fleet Analytics Engine. We use `docker-compose` to orchestrate our streaming broker (Redpanda/Kafka), fast OLAP database (ClickHouse), and in-memory cache (Redis) within an isolated, reproducible network.

## Implementation Details
* **Message Broker (Redpanda):** Chosen over traditional JVM Kafka for its lower memory footprint and C++ performance, making local development smoother. It will expose port `9092` for the Kafka API.
* **OLAP Storage (ClickHouse):** Configured with a default `AggregatingMergeTree` table schema initialized via a `/docker-entrypoint-initdb.d/` script. It will expose port `8123` for HTTP and `9000` for native clients.
* **State Store (Redis):** Runs a standard `redis:7-alpine` image to maintain the latest $O(1)$ state of every vehicle. Exposed on port `6379`.
* **Network & Volumes:** All containers share a custom bridge network (`data-mesh`) and use named volumes to persist data across container restarts.

## Ways to Test
1. **Startup Test:** Run `docker compose up -d`. Verify all containers reach the "healthy" state using `docker ps`.
2. **Connectivity Test:** 
   * Ping Redis: `redis-cli ping` should return `PONG`.
   * Check Redpanda: `rpk cluster info` should show 1 active broker.
   * Query ClickHouse: `curl "http://localhost:8123/?query=SELECT%201"` should return `1`.

## Quality Benchmarks
* **Startup Time:** The entire stack must be query-ready within 30 seconds from a cold start on a standard laptop.
* **Idle Resource Usage:** The idle stack should consume $< 2\text{GB}$ of RAM combined.

## Acceptance Criteria
- [ ] A single `docker-compose.yml` file is created.
- [ ] Explicit health checks are defined for all three services.
- [ ] Initialization scripts (SQL files) automatically create the necessary databases and tables in ClickHouse upon first boot.
- [ ] Services can communicate with each other using Docker DNS (e.g., `ping redpanda` from inside the ClickHouse container).