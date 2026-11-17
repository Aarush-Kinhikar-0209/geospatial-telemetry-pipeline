# Phase 2: Synthetic Telemetry Generator

## Overview
To demonstrate a robust pipeline, we need realistic, high-throughput, and occasionally flawed data. This Python service simulates a fleet of delivery vehicles moving around a city boundary, generating JSON telemetry and streaming it into Redpanda.

## Implementation Details
* **Logic:** Uses `asyncio` or multi-threading to simulate concurrent vehicle instances.
* **Geo-routing:** Employs basic bounding-box random walks or predefined GeoJSON routes to generate latitude/longitude updates.
* **Payload Structure:** 
  ```json
  {
    "vehicle_id": "v-104",
    "timestamp": "2026-11-17T18:45:00Z",
    "lat": 18.93,
    "lon": 72.82,
    "speed_kmh": 45.2,
    "battery_pct": 82
  }
  ```
* **Chaos Engineering:** The generator deliberately corrupts 2-5% of payloads (e.g., sending strings instead of floats for speed, omitting the timestamp, or sending malformed JSON) to prove the downstream Dead Letter Queue works.

## Ways to Test
1. **Dry Run:** Run the script with a `--stdout` flag to print generated JSON to the terminal without attempting to connect to the broker.
2. **Schema Validation:** Write a unit test against a Pydantic model to ensure the "clean" payloads strictly adhere to the expected schema.
3. **Throughput Test:** Run the script targeting the local Redpanda instance and use an external consumer tool (like `rpk` or Offset Explorer) to verify messages are landing in the `telemetry_raw` topic.

## Quality Benchmarks
* **Throughput:** Must sustain generating and publishing at least $500 \text{ messages/sec}$ on a single core.
* **Chaos Accuracy:** Exactly 2-5% of messages over a 1-minute window should be verifiably malformed based on the configured chaos parameter.

## Acceptance Criteria
- [ ] Script is containerized and included in the main `docker-compose.yml`.
- [ ] Graceful shutdown (SIGINT/SIGTERM handling) is implemented to close Kafka producer connections cleanly.
- [ ] Configurable via environment variables (e.g., `VEHICLE_COUNT=200`, `CHAOS_PERCENTAGE=5`).
- [ ] Metrics logging prints generation rate to standard output every 10 seconds.