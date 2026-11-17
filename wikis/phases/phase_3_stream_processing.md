# Phase 3: Stream Transformation & Routing

## Overview
This is the core data engineering logic. The stream processor consumes raw telemetry, applies real-time transformations, calculates aggregations, isolates bad data into a Dead Letter Queue (DLQ), and multiplexes outputs to Redis and ClickHouse.

## Implementation Details
* **Engine:** Apache Flink (via PyFlink) or a lightweight Python framework like Faust/QuixStreams.
* **Transformations:**
  * **Geospatial:** Maps `(lat, lon)` to an H3 resolution 9 hexagon ID.
  * **Windowing:** Calculates a 10-second tumbling window average for `speed_kmh` per vehicle.
* **DLQ Routing:** A `try/except` schema validation block intercepts bad JSON or schema violations. Valid records pass through; invalid records are written to a `telemetry_dlq` Kafka topic.
* **Sinks:**
  * Writes current state (H3 index, speed, battery) to Redis (Key: `vehicle:{id}`).
  * Writes the windowed aggregations to ClickHouse via JDBC or native clickhouse-driver batched inserts.

## Ways to Test
1. **Unit Testing:** Pass a mock array of 10 telemetry events (including 2 bad ones) to the transformation function. Assert that the output lists exactly 8 valid records and 2 DLQ records.
2. **Integration Testing:** Start the generator (Phase 2), let it run for 1 minute, and query the DLQ topic. Verify it contains the exact malformed payloads injected by the generator.
3. **State Verification:** Query Redis to ensure no vehicle has a timestamp older than 15 seconds (proving state is actively updating).

## Quality Benchmarks
* **End-to-End Latency:** Time from Redpanda ingestion to Redis update should be $< 200\text{ms}$ at the 95th percentile.
* **DLQ Reliability:** 100% of schema-violating events must be caught and routed to the DLQ without crashing the consumer application.

## Acceptance Criteria
- [ ] Stream processor connects to Kafka, Redis, and ClickHouse successfully on startup.
- [ ] Checkpointing is enabled (if using Flink) to ensure exactly-once or at-least-once processing guarantees.
- [ ] Tumbling window aggregations are successfully written to ClickHouse.
- [ ] Process continues to run uninterrupted when encountering malformed JSON.