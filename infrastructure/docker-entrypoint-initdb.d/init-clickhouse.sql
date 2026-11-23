-- ---------------------------------------------------------------------------
-- Phase 1: Core Services Containerization
-- ClickHouse initialization script.
--
-- This file is mounted into /docker-entrypoint-initdb.d/ and is executed
-- automatically by the clickhouse-server image on first boot (empty volume
-- only; run `docker compose down -v` to re-apply after editing). It creates
-- the fleet_analytics database and the tables used by the pipeline:
--   * telemetry_events   - raw validated telemetry (ReplacingMergeTree)
--   * speed_rollups      - tumbling-window speed partials (AggregatingMergeTree)
--   * speed_rollups_avg  - view that finalizes the per-window average
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS fleet_analytics;

USE fleet_analytics;

-- Raw validated telemetry written by the stream processor (historical paths).
-- The processor is at-least-once, so a restart can re-insert records; keying
-- on the source Kafka coordinates lets ReplacingMergeTree drop those replays.
-- Use FINAL (or GROUP BY the key) when exact counts matter before merges run.
CREATE TABLE IF NOT EXISTS telemetry_events (
    vehicle_id        String,
    timestamp         DateTime64(3, 'UTC'),
    latitude          Float64,
    longitude         Float64,
    speed_kmh         Float64,
    battery_pct       UInt8,
    h3_index          String,
    source_partition  UInt32,
    source_offset     UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (vehicle_id, timestamp, source_partition, source_offset);

-- Tumbling-window speed partials per vehicle.
-- The processor inserts (sum, count) per window; rows for the same window
-- (late events, restarts) are merged by summing, so averages stay correct.
-- Query speed_rollups_avg (or aggregate with sum/sum) instead of raw rows,
-- because merges happen asynchronously.
CREATE TABLE IF NOT EXISTS speed_rollups (
    vehicle_id     String,
    window_start   DateTime('UTC'),
    speed_sum      SimpleAggregateFunction(sum, Float64),
    cnt            SimpleAggregateFunction(sum, UInt64),
    h3_index       SimpleAggregateFunction(anyLast, String)
) ENGINE = AggregatingMergeTree()
ORDER BY (vehicle_id, window_start);

CREATE VIEW IF NOT EXISTS speed_rollups_avg AS
SELECT
    vehicle_id,
    window_start,
    sum(speed_sum) / sum(cnt) AS avg_speed_kmh,
    sum(cnt)                  AS sample_count,
    anyLast(h3_index)         AS last_h3_index
FROM speed_rollups
GROUP BY vehicle_id, window_start;
