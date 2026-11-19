#!/usr/bin/env python3
"""
Phase 3: Stream Transformation & Routing

Consumes raw telemetry from Redpanda (telemetry_raw), applies real-time
transformations, routes bad data to a Dead Letter Queue (telemetry_dlq),
and multiplexes outputs to Redis (latest state) and ClickHouse
(windowed aggregations).

Usage:
    python main.py
"""

import asyncio
import json
import logging
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime, timezone

import h3
from pydantic import BaseModel, ValidationError

try:  # python-dotenv is optional
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

# ---------------------------------------------------------------------------
# Configuration (via environment variables)
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS: str = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
INPUT_TOPIC: str = os.getenv('INPUT_TOPIC', 'telemetry_raw')
DLQ_TOPIC: str = os.getenv('DLQ_TOPIC', 'telemetry_dlq')
CONSUMER_GROUP: str = os.getenv('CONSUMER_GROUP', 'stream-processor')
REDIS_HOST: str = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_DB: str = os.getenv('CLICKHOUSE_DB', 'fleet_analytics')
WINDOW_SECONDS: int = int(os.getenv('WINDOW_SECONDS', '10'))
CHECKPOINT_INTERVAL: int = int(os.getenv('CHECKPOINT_INTERVAL', '30'))
H3_RESOLUTION: int = int(os.getenv('H3_RESOLUTION', '9'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] processor: %(message)s',
)
logger = logging.getLogger('processor')


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------
class TelemetryPayload(BaseModel):
    """Clean telemetry payload conforming to the pipeline schema."""

    vehicle_id: str
    timestamp: str
    lat: float
    lon: float
    speed_kmh: float
    battery_pct: int


class WindowedAggregation(BaseModel):
    """10-second tumbling window speed average per vehicle."""

    vehicle_id: str
    window_start: str
    avg_speed_kmh: float
    count: int
    h3_index: str


# ---------------------------------------------------------------------------
# Stream Processor
# ---------------------------------------------------------------------------
class StreamProcessor:
    """Consumes, transforms, and routes telemetry events."""

    def __init__(self) -> None:
        self.running = True
        self.consumer = None
        self.producer = None
        self.redis = None
        self.clickhouse = None
        self.processed: int = 0
        self.dlq_count: int = 0
        self.last_checkpoint: float | None = None

        # Tumbling window state: {vehicle_id: {window_start: [speeds]}}
        self.windows: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        # Latest H3 per vehicle (for window emission)
        self.latest_h3: dict[str, str] = {}

    # -- lifecycle ----------------------------------------------------------

    async def connect(self) -> None:
        """Establish connections to Kafka, Redis, and ClickHouse."""
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        self.consumer = AIOKafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,  # manual checkpointing for exactly-once
            auto_offset_reset='earliest',
            value_deserializer=lambda v: v.decode('utf-8'),
        )
        await self.consumer.start()
        logger.info('Connected to Kafka at %s', BOOTSTRAP_SERVERS)

        self.producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode('utf-8'),
        )
        await self.producer.start()
        logger.info('Kafka producer started for DLQ topic %s', DLQ_TOPIC)

        import redis as redis_lib

        self.redis = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.redis.ping()
        logger.info('Connected to Redis at %s:%d', REDIS_HOST, REDIS_PORT)

        from clickhouse_driver import connect as ch_connect

        self.clickhouse = ch_connect(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
        )
        logger.info('Connected to ClickHouse at %s:%d', CLICKHOUSE_HOST, CLICKHOUSE_PORT)

        self.last_checkpoint = asyncio.get_event_loop().time()

    async def close(self) -> None:
        """Gracefully close all connections."""
        # Flush any remaining windows before shutting down.
        await self.flush_windows(final=True)

        if self.consumer is not None:
            await self.consumer.stop()
        if self.producer is not None:
            await self.producer.stop()
        if self.redis is not None:
            self.redis.close()
        if self.clickhouse is not None:
            self.clickhouse.disconnect()

    # -- transformations -----------------------------------------------------

    def validate(self, raw: str) -> TelemetryPayload | None:
        """Parse and validate a raw JSON string. Returns None on failure."""
        try:
            obj = json.loads(raw)
            return TelemetryPayload(**obj)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning('Schema violation, routing to DLQ: %s', exc)
            return None

    def to_h3(self, lat: float, lon: float) -> str:
        """Convert (lat, lon) to an H3 resolution-9 hexagon ID."""
        return h3.latlng_to_cell(lat, lon, H3_RESOLUTION)

    def window_key(self, timestamp_str: str) -> str:
        """Derive the tumbling-window start key for a timestamp."""
        dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        epoch = int(dt.timestamp())
        window_epoch = (epoch // WINDOW_SECONDS) * WINDOW_SECONDS
        return datetime.fromtimestamp(window_epoch, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # -- sinks --------------------------------------------------------------

    async def write_redis(self, vehicle_id: str, payload: TelemetryPayload, h3_index: str) -> None:
        """Write latest vehicle state to Redis (Key: vehicle:{id})."""
        state = {
            'h3_index': h3_index,
            'speed_kmh': str(payload.speed_kmh),
            'battery_pct': str(payload.battery_pct),
            'timestamp': payload.timestamp,
            'lat': str(payload.lat),
            'lon': str(payload.lon),
        }
        self.redis.hset(f'vehicle:{vehicle_id}', mapping=state)

    async def write_clickhouse_window(
        self, vehicle_id: str, window_start: str, speeds: list[float], h3_index: str
    ) -> None:
        """Insert a completed tumbling-window aggregation into ClickHouse."""
        avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
        row = (
            vehicle_id,
            window_start,
            round(avg_speed, 2),
            len(speeds),
            h3_index,
        )
        self.clickhouse.execute(
            'INSERT INTO speed_rollups (vehicle_id, window_start, avg_speed_kmh, cnt, h3_index) VALUES',
            [row],
        )
        logger.debug(
            'ClickHouse insert: vehicle=%s window=%s avg=%.2f cnt=%d',
            vehicle_id,
            window_start,
            avg_speed,
            len(speeds),
        )

    async def flush_windows(self, final: bool = False) -> None:
        """Emit completed tumbling windows to ClickHouse and clear them."""
        now = asyncio.get_event_loop().time()
        if not final and self.last_checkpoint is not None:
            if now - self.last_checkpoint < CHECKPOINT_INTERVAL:
                return

        to_flush: list[tuple] = []
        for vehicle_id, windows in list(self.windows.items()):
            for window_start, speeds in list(windows.items()):
                if not speeds:
                    continue
                dt = datetime.strptime(window_start, '%Y-%m-%dT%H:%M:%SZ').replace(
                    tzinfo=timezone.utc
                )
                window_epoch = int(dt.timestamp())
                if final or (now - window_epoch) > WINDOW_SECONDS:
                    to_flush.append(
                        (vehicle_id, window_start, speeds, self.latest_h3.get(vehicle_id, ''))
                    )
                    del windows[window_start]

        for vehicle_id, window_start, speeds, h3_index in to_flush:
            await self.write_clickhouse_window(vehicle_id, window_start, speeds, h3_index)

        if to_flush:
            self.last_checkpoint = now
            logger.info('Checkpoint: flushed %d window(s) to ClickHouse', len(to_flush))

    # -- main loop ----------------------------------------------------------

    async def process_message(self, raw: str) -> None:
        """Validate, transform, and route a single telemetry event."""
        self.processed += 1

        payload = self.validate(raw)
        if payload is None:
            self.dlq_count += 1
            await self.producer.send(DLQ_TOPIC, raw)
            logger.debug('Routed invalid record to DLQ topic %s', DLQ_TOPIC)
            return

        # Geospatial transform: (lat, lon) -> H3 hexagon.
        h3_index = self.to_h3(payload.lat, payload.lon)
        self.latest_h3[payload.vehicle_id] = h3_index

        # Windowing: accumulate speed for the 10-second tumbling window.
        window_start = self.window_key(payload.timestamp)
        self.windows[payload.vehicle_id][window_start].append(payload.speed_kmh)

        # Sink: latest state to Redis.
        await self.write_redis(payload.vehicle_id, payload, h3_index)

    async def run(self) -> None:
        await self.connect()

        loop = asyncio.get_event_loop()
        stop_event = asyncio.Event()

        def signal_handler() -> None:
            logger.info('Shutdown signal received, stopping processor...')
            self.running = False
            stop_event.set()

        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)

        try:
            while self.running:
                # Periodically flush windows (checkpointing).
                await self.flush_windows(final=False)

                # Consume a batch of messages.
                records = await self.consumer.getmany(timeout_ms=1000, max_records=500)
                for _topic_partition, messages in records.items():
                    for message in messages:
                        if not self.running:
                            break
                        await self.process_message(message.value)

                # Commit offsets as a checkpoint (at-least-once).
                await self.consumer.commit()
        finally:
            await self.close()


def main() -> None:
    processor = StreamProcessor()
    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:  # pragma: no cover - handled by signal handler
        pass


if __name__ == '__main__':
    main()
