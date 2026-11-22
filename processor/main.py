#!/usr/bin/env python3
"""
Phase 3: Stream Transformation & Routing

Consumes raw telemetry from Redpanda (telemetry_raw), applies real-time
transformations, routes bad data to a Dead Letter Queue (telemetry_dlq),
and multiplexes outputs to Redis (latest state) and ClickHouse
(raw events + windowed aggregations).

Delivery guarantee: at-least-once. Offsets are committed only after every
record before them is durable (ClickHouse insert done, DLQ write acked) and
never past a record whose tumbling window is still open in memory.

Usage:
    python main.py
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone

import h3
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from clickhouse_driver import Client as ClickHouseClient
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError
from redis.asyncio import Redis

try:  # python-dotenv is optional
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

# ---------------------------------------------------------------------------
# Configuration (via environment variables)
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS: str = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
INPUT_TOPIC: str = os.getenv('INPUT_TOPIC', 'telemetry_raw')
DLQ_TOPIC: str = os.getenv('DLQ_TOPIC', 'telemetry_dlq')
CONSUMER_GROUP: str = os.getenv('CONSUMER_GROUP', 'stream-processor')
REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DB: str = os.getenv('CLICKHOUSE_DB', 'fleet_analytics')
WINDOW_SECONDS: int = int(os.getenv('WINDOW_SECONDS', '10'))
ALLOWED_LATENESS_SECONDS: int = int(os.getenv('ALLOWED_LATENESS_SECONDS', '5'))
CHECKPOINT_INTERVAL: float = float(os.getenv('CHECKPOINT_INTERVAL', '10'))
MAX_BUFFERED_EVENTS: int = int(os.getenv('MAX_BUFFERED_EVENTS', '50000'))
STATE_TTL_SECONDS: int = int(os.getenv('STATE_TTL_SECONDS', '60'))
UPDATES_CHANNEL: str = os.getenv('UPDATES_CHANNEL', 'vehicles:updates')
METRICS_KEY_PREFIX: str = os.getenv('METRICS_KEY_PREFIX', 'metrics:pipeline:')
METRICS_TTL_SECONDS: int = int(os.getenv('METRICS_TTL_SECONDS', '300'))
H3_RESOLUTION: int = int(os.getenv('H3_RESOLUTION', '9'))
METRICS_INTERVAL: float = float(os.getenv('METRICS_INTERVAL', '10'))
STARTUP_RETRIES: int = int(os.getenv('STARTUP_RETRIES', '15'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] processor: %(message)s',
)
logger = logging.getLogger('processor')
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO').upper())


# ---------------------------------------------------------------------------
# Schema & transformation (pure functions, unit tested without any broker)
# ---------------------------------------------------------------------------
class TelemetryPayload(BaseModel):
    """
    Telemetry contract. Anything that does not satisfy it goes to the DLQ.

    Kept in sync with generator/main.py, which emits this contract.
    """

    model_config = ConfigDict(strict=True)

    vehicle_id: str = Field(min_length=1)
    timestamp: AwareDatetime
    lat: float = Field(ge=-90, le=90, allow_inf_nan=False)
    lon: float = Field(ge=-180, le=180, allow_inf_nan=False)
    speed_kmh: float = Field(ge=0, le=300, allow_inf_nan=False)
    battery_pct: int = Field(ge=0, le=100)


@dataclass(frozen=True)
class EnrichedEvent:
    payload: TelemetryPayload
    h3_index: str
    window_start: datetime


def window_start_for(ts: datetime) -> datetime:
    """Start of the WINDOW_SECONDS tumbling window containing ts."""
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % WINDOW_SECONDS, tz=timezone.utc)


def describe_error(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        reason = '; '.join(
            f'{".".join(map(str, err["loc"])) or "payload"}: {err["msg"]}' for err in exc.errors()
        )
    else:
        reason = f'{type(exc).__name__}: {exc}'
    return reason[:500]


def classify(raw: bytes | str | None) -> tuple[EnrichedEvent | None, str | None]:
    """
    Validate and enrich one raw record.

    Returns (event, None) for valid records and (None, reason) for anything
    that must go to the DLQ. Never raises, so one poison pill cannot stop
    the consumer.
    """
    try:
        payload = TelemetryPayload.model_validate_json(raw)
        h3_index = h3.latlng_to_cell(payload.lat, payload.lon, H3_RESOLUTION)
        return EnrichedEvent(payload, h3_index, window_start_for(payload.timestamp)), None
    except Exception as exc:
        return None, describe_error(exc)


def route(raws: Iterable[bytes | str | None]) -> tuple[list[EnrichedEvent], list[tuple]]:
    """Split raw records into valid events and (raw, reason) DLQ records."""
    valid: list[EnrichedEvent] = []
    dlq: list[tuple] = []
    for raw in raws:
        event, reason = classify(raw)
        if event is None:
            dlq.append((raw, reason))
        else:
            valid.append(event)
    return valid, dlq


# ---------------------------------------------------------------------------
# Tumbling windows
# ---------------------------------------------------------------------------
@dataclass
class WindowState:
    speed_sum: float = 0.0
    count: int = 0
    h3_index: str = ''
    # Lowest source offset per partition that contributed to this window.
    min_offsets: dict = field(default_factory=dict)


class TumblingWindows:
    """
    Per-vehicle event-time tumbling windows.

    A window closes once wall-clock time passes its end plus the allowed
    lateness. Events arriving for an already-emitted window open a new
    partial for it; speed_rollups merges partials by summing.
    """

    def __init__(self, size: int = WINDOW_SECONDS, lateness: int = ALLOWED_LATENESS_SECONDS):
        self.size = size
        self.lateness = lateness
        self.open: dict[tuple[str, datetime], WindowState] = {}

    def add(self, event: EnrichedEvent, tp: TopicPartition | None = None, offset: int = -1):
        state = self.open.setdefault((event.payload.vehicle_id, event.window_start), WindowState())
        state.speed_sum += event.payload.speed_kmh
        state.count += 1
        state.h3_index = event.h3_index
        if tp is not None and offset < state.min_offsets.get(tp, offset + 1):
            state.min_offsets[tp] = offset

    def pop_closed(self, now: float, final: bool = False) -> list[tuple]:
        """Remove and return (vehicle_id, window_start, state) for closed windows."""
        closed = [
            key
            for key in self.open
            if final or key[1].timestamp() + self.size + self.lateness <= now
        ]
        return [(vid, start, self.open.pop((vid, start))) for vid, start in closed]

    def min_open_offsets(self) -> dict:
        """Per partition, the lowest offset still needed by an open window."""
        lowest: dict = {}
        for state in self.open.values():
            for tp, offset in state.min_offsets.items():
                lowest[tp] = min(offset, lowest.get(tp, offset))
        return lowest


def safe_commit_offsets(next_offsets: dict, open_min_offsets: dict) -> dict:
    """
    Offsets that are safe to commit: everything consumed, except records an
    open (not yet persisted) window still depends on.
    """
    return {
        tp: min(next_offset, open_min_offsets.get(tp, next_offset))
        for tp, next_offset in next_offsets.items()
    }


def p95(samples: list[float]) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]


def iso_z(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def vehicle_state(event: EnrichedEvent, processed_at: str) -> dict:
    """Latest-state record, stored in Redis and published to SSE subscribers."""
    return {
        'vehicle_id': event.payload.vehicle_id,
        'lat': event.payload.lat,
        'lon': event.payload.lon,
        'speed_kmh': event.payload.speed_kmh,
        'battery_pct': event.payload.battery_pct,
        'h3_index': event.h3_index,
        'timestamp': iso_z(event.payload.timestamp),
        'processed_at': processed_at,
    }


async def with_retries(what: str, attempt):
    """Retry a startup connection so the processor tolerates slow dependencies."""
    for i in range(1, STARTUP_RETRIES + 1):
        try:
            return await attempt()
        except Exception as exc:
            if i == STARTUP_RETRIES:
                raise
            logger.warning('%s not ready (attempt %d/%d): %s', what, i, STARTUP_RETRIES, exc)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Stream Processor
# ---------------------------------------------------------------------------
class StreamProcessor:
    """Consumes, transforms, and routes telemetry events."""

    def __init__(self) -> None:
        self.stopping = False
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self.redis: Redis | None = None
        self.clickhouse: ClickHouseClient | None = None

        self.windows = TumblingWindows()
        self.event_rows: list[tuple] = []
        self.next_offsets: dict[TopicPartition, int] = {}

        self.processed = 0
        self.dlq_count = 0
        self.latencies_ms: list[float] = []

    # -- lifecycle ----------------------------------------------------------

    async def connect(self) -> None:
        """Establish and verify connections to Kafka, Redis, and ClickHouse."""

        async def start_consumer():
            self.consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP,
                enable_auto_commit=False,
                auto_offset_reset='earliest',
            )
            try:
                await self.consumer.start()
            except Exception:
                await self.consumer.stop()
                raise

        async def start_producer():
            self.producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, acks='all')
            try:
                await self.producer.start()
            except Exception:
                await self.producer.stop()
                raise

        await with_retries('Kafka consumer', start_consumer)
        await with_retries('Kafka producer', start_producer)
        logger.info('Connected to Kafka at %s (DLQ topic %s)', BOOTSTRAP_SERVERS, DLQ_TOPIC)

        self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        await with_retries('Redis', self.redis.ping)
        logger.info('Connected to Redis at %s:%d', REDIS_HOST, REDIS_PORT)

        self.clickhouse = ClickHouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )
        await with_retries('ClickHouse', lambda: self.ch_execute('SELECT 1'))
        logger.info('Connected to ClickHouse at %s:%d', CLICKHOUSE_HOST, CLICKHOUSE_PORT)

    async def close(self) -> None:
        """Close every connection, even if one of them fails to close."""
        closers = [
            ('Kafka consumer', self.consumer.stop if self.consumer else None),
            ('Kafka producer', self.producer.stop if self.producer else None),
            ('Redis', self.redis.aclose if self.redis else None),
            (
                'ClickHouse',
                (lambda: asyncio.to_thread(self.clickhouse.disconnect))
                if self.clickhouse
                else None,
            ),
        ]
        for name, closer in closers:
            if closer is None:
                continue
            try:
                await closer()
            except Exception:
                logger.exception('Error while closing %s', name)

    async def ch_execute(self, query: str, rows: list | None = None):
        # clickhouse-driver is synchronous; keep it off the event loop.
        return await asyncio.to_thread(self.clickhouse.execute, query, rows)

    # -- per-batch processing -------------------------------------------------

    async def process_batch(self, records: dict) -> None:
        valid: list[tuple[EnrichedEvent, int]] = []
        batch_processed = batch_dlq = 0
        for tp, messages in records.items():
            for message in messages:
                self.processed += 1
                batch_processed += 1
                event, reason = classify(message.value)
                if event is None:
                    batch_dlq += 1
                    await self.send_to_dlq(message, reason)
                else:
                    self.windows.add(event, tp, message.offset)
                    self.event_rows.append(
                        (
                            event.payload.vehicle_id,
                            event.payload.timestamp,
                            event.payload.lat,
                            event.payload.lon,
                            event.payload.speed_kmh,
                            event.payload.battery_pct,
                            event.h3_index,
                            tp.partition,
                            message.offset,
                        )
                    )
                    valid.append((event, message.timestamp))
                self.next_offsets[tp] = message.offset + 1

        if batch_processed:
            await self.write_state(valid, batch_processed, batch_dlq)

    async def send_to_dlq(self, message, reason: str) -> None:
        self.dlq_count += 1
        logger.debug('DLQ offset=%d: %s', message.offset, reason)
        # Delivery is confirmed by producer.flush() before offsets are committed.
        await self.producer.send(
            DLQ_TOPIC,
            value=message.value,
            key=message.key,
            headers=[
                ('error', reason.encode()),
                ('source', f'{message.topic}/{message.partition}/{message.offset}'.encode()),
            ],
        )

    async def write_state(
        self, valid: list[tuple[EnrichedEvent, int]], processed: int, dlq: int
    ) -> None:
        """
        One Redis round trip per batch:
          * latest state per vehicle (Key: vehicle:{id}, with TTL)
          * per-second processed/DLQ counters (read by the API's DLQ metrics)
          * a pub/sub message with the changed vehicles (fanned out by the API's SSE)
        """
        latest: dict[str, EnrichedEvent] = {}
        for event, _ in valid:
            current = latest.get(event.payload.vehicle_id)
            if current is None or event.payload.timestamp >= current.payload.timestamp:
                latest[event.payload.vehicle_id] = event

        processed_at = iso_z(datetime.now(timezone.utc))
        states = [vehicle_state(event, processed_at) for event in latest.values()]
        metrics_key = f'{METRICS_KEY_PREFIX}{int(time.time())}'

        async with self.redis.pipeline(transaction=False) as pipe:
            for state in states:
                key = f'vehicle:{state["vehicle_id"]}'
                pipe.hset(key, mapping=state)
                pipe.expire(key, STATE_TTL_SECONDS)
            pipe.hincrby(metrics_key, 'processed', processed)
            pipe.hincrby(metrics_key, 'dlq', dlq)
            pipe.expire(metrics_key, METRICS_TTL_SECONDS)
            if states:
                pipe.publish(UPDATES_CHANNEL, json.dumps(states, separators=(',', ':')))
            await pipe.execute()

        # Kafka record timestamp (producer create time) -> Redis write acked.
        now_ms = time.time() * 1000
        self.latencies_ms.extend(now_ms - ts for _, ts in valid if ts and ts > 0)

    # -- checkpointing --------------------------------------------------------

    async def checkpoint(self, final: bool = False) -> None:
        """Persist closed windows and buffered events, then commit safe offsets."""
        closed = self.windows.pop_closed(time.time(), final=final)
        rollups = [
            (vid, start, state.speed_sum, state.count, state.h3_index)
            for vid, start, state in closed
        ]
        events, self.event_rows = self.event_rows, []

        if events:
            await self.ch_execute(
                'INSERT INTO telemetry_events '
                '(vehicle_id, timestamp, latitude, longitude, speed_kmh, battery_pct, h3_index, '
                'source_partition, source_offset) VALUES',
                events,
            )
        if rollups:
            await self.ch_execute(
                'INSERT INTO speed_rollups (vehicle_id, window_start, speed_sum, cnt, h3_index) '
                'VALUES',
                rollups,
            )
        await self.producer.flush()

        offsets = safe_commit_offsets(self.next_offsets, self.windows.min_open_offsets())
        if offsets:
            await self.consumer.commit(offsets)

        logger.info(
            'Checkpoint: %d event(s), %d window(s) -> ClickHouse; open windows=%d',
            len(events),
            len(rollups),
            len(self.windows.open),
        )

    # -- main loop ----------------------------------------------------------

    def log_metrics(self, elapsed: float) -> None:
        logger.info(
            'rate=%.1f msg/sec processed=%d dlq=%d redis_latency_p95=%.1fms',
            self.processed / elapsed if elapsed > 0 else 0,
            self.processed,
            self.dlq_count,
            p95(self.latencies_ms),
        )
        self.processed = 0
        self.dlq_count = 0
        self.latencies_ms = []

    async def run(self) -> None:
        loop = asyncio.get_running_loop()

        def signal_handler() -> None:
            logger.info('Shutdown signal received, finishing current batch...')
            self.stopping = True

        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)

        try:
            await self.connect()
            last_checkpoint = last_metrics = time.monotonic()
            while not self.stopping:
                records = await self.consumer.getmany(timeout_ms=500, max_records=1000)
                await self.process_batch(records)

                now = time.monotonic()
                if (
                    now - last_checkpoint >= CHECKPOINT_INTERVAL
                    or len(self.event_rows) >= MAX_BUFFERED_EVENTS
                ):
                    await self.checkpoint()
                    last_checkpoint = now
                if now - last_metrics >= METRICS_INTERVAL:
                    self.log_metrics(now - last_metrics)
                    last_metrics = now

            # Only reached on a clean stop: flush every open window and commit.
            await self.checkpoint(final=True)
        finally:
            await self.close()
            logger.info('Processor stopped.')


def main() -> None:
    processor = StreamProcessor()
    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:  # pragma: no cover - handled by signal handler
        pass


if __name__ == '__main__':
    main()
