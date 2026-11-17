-- ---------------------------------------------------------------------------
-- Phase 1: Core Services Containerization
-- ClickHouse initialization script.
--
-- This file is mounted into /docker-entrypoint-initdb.d/ and is executed
-- automatically by the clickhouse-server image on first boot. It creates the
-- fleet_analytics database and the tables used by the pipeline:
--   * telemetry_events   - raw per-vehicle telemetry (MergeTree)
--   * speed_rollups      - tumbling-window speed averages (AggregatingMergeTree)
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS fleet_analytics;

USE fleet_analytics;

-- Raw telemetry events produced by the generator service.
CREATE TABLE IF NOT EXISTS telemetry_events (
    vehicle_id   String,
    timestamp    DateTime64(3),
    latitude     Float64,
    longitude    Float64,
    speed_kmh    Float64,
    h3_index     String,
    event_type   String
) ENGINE = MergeTree()
ORDER BY (vehicle_id, timestamp);

-- Tumbling-window speed averages per H3 cell (AggregatingMergeTree).
-- Populated by the stream processor; queried by the API for OLAP dashboards.
CREATE TABLE IF NOT EXISTS speed_rollups (
    h3_index     String,
    window_start DateTime,
    avg_speed    AggregateFunction(avg, Float64),
    cnt          AggregateFunction(count, )
) ENGINE = AggregatingMergeTree()
ORDER BY (h3_index, window_start);
