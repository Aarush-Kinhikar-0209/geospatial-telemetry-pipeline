#!/usr/bin/env python3
"""
Phase 4: Fast Serving APIs (FastAPI)

Bridges the frontend to the data stores without exposing them directly:
  * GET /api/v1/stream/vehicles   live vehicle state (SSE, or NDJSON with ?format=ndjson)
  * GET /api/v1/vehicles          current state snapshot
  * GET /api/v1/metrics/dlq       processed / dead-lettered message rates (Redis)
  * GET /api/v1/health            dependency health

Live updates come from the processor's Redis pub/sub channel. A single
subscription is fanned out to every SSE client, so client count does not
multiply load on Redis.

Usage:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import contextlib
import inspect
import json
import logging
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

import clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError as ClickHouseOperationalError
from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

try:  # python-dotenv is optional
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

# ---------------------------------------------------------------------------
# Configuration (via environment variables)
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DB: str = os.getenv('CLICKHOUSE_DB', 'fleet_analytics')
UPDATES_CHANNEL: str = os.getenv('UPDATES_CHANNEL', 'vehicles:updates')
METRICS_KEY_PREFIX: str = os.getenv('METRICS_KEY_PREFIX', 'metrics:pipeline:')
CORS_ORIGINS: list[str] = [
    origin.strip()
    for origin in os.getenv('CORS_ORIGINS', 'http://localhost:3000,http://localhost:5173').split(
        ','
    )
    if origin.strip()
]
SSE_HEARTBEAT_SECONDS: float = float(os.getenv('SSE_HEARTBEAT_SECONDS', '15'))
SSE_QUEUE_SIZE: int = int(os.getenv('SSE_QUEUE_SIZE', '256'))
SSE_COALESCE_SECONDS: float = float(os.getenv('SSE_COALESCE_MS', '50')) / 1000

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] api: %(message)s')
logger = logging.getLogger('api')


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class VehicleState(BaseModel):
    vehicle_id: str
    lat: float
    lon: float
    speed_kmh: float
    battery_pct: int
    h3_index: str
    timestamp: str = Field(description='Event time reported by the vehicle (ISO-8601, UTC)')
    processed_at: str = Field(description='When the stream processor wrote this state')


class DlqPoint(BaseModel):
    ts: int = Field(description='Unix second')
    processed: int
    dlq: int


class DlqMetrics(BaseModel):
    window_seconds: int
    processed: int = Field(description='Messages processed within the window')
    dlq: int = Field(description='Messages routed to the DLQ within the window')
    processed_rate_per_sec: float
    dlq_rate_per_sec: float
    dlq_ratio: float = Field(description='dlq / processed within the window (0-1)')
    series: list[DlqPoint] = Field(description='Per-second history, oldest first')


class Health(BaseModel):
    status: Literal['ok', 'degraded']
    redis: bool
    clickhouse: bool
    sse_clients: int
    live_feed: bool = Field(description='Whether the Redis pub/sub subscription is active')


# ---------------------------------------------------------------------------
# Errors: a dropped database connection is a 503, not a crash or a 500
# ---------------------------------------------------------------------------
class ServiceUnavailable(Exception):
    def __init__(self, dependency: str, detail: str = '') -> None:
        super().__init__(detail)
        self.dependency = dependency
        self.detail = detail


def unavailable(dependency: str, exc: Exception) -> JSONResponse:
    logger.warning('%s unavailable: %s', dependency, exc)
    return JSONResponse(
        status_code=503,
        content={'detail': f'{dependency} is unavailable', 'dependency': dependency},
        headers={'Retry-After': '2'},
    )


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------
def parse_vehicle(key: str, fields: dict[str, str]) -> VehicleState | None:
    if not fields:
        return None  # expired between SCAN and HGETALL
    return VehicleState(
        vehicle_id=fields.get('vehicle_id') or key.split(':', 1)[1],
        lat=float(fields['lat']),
        lon=float(fields['lon']),
        speed_kmh=float(fields['speed_kmh']),
        battery_pct=int(fields['battery_pct']),
        h3_index=fields['h3_index'],
        timestamp=fields['timestamp'],
        processed_at=fields.get('processed_at', ''),
    )


async def load_snapshot(redis: Redis) -> list[VehicleState]:
    keys = [key async for key in redis.scan_iter(match='vehicle:*', count=1000)]
    if not keys:
        return []
    async with redis.pipeline(transaction=False) as pipe:
        for key in keys:
            pipe.hgetall(key)
        rows = await pipe.execute()
    vehicles = [parse_vehicle(key, fields) for key, fields in zip(keys, rows, strict=True)]
    return sorted((v for v in vehicles if v), key=lambda v: v.vehicle_id)


def summarize_dlq(buckets: list[DlqPoint], window: int) -> DlqMetrics:
    recent = buckets[-window:]
    processed = sum(point.processed for point in recent)
    dlq = sum(point.dlq for point in recent)
    return DlqMetrics(
        window_seconds=window,
        processed=processed,
        dlq=dlq,
        processed_rate_per_sec=processed / window,
        dlq_rate_per_sec=dlq / window,
        dlq_ratio=dlq / processed if processed else 0.0,
        series=buckets,
    )


# ---------------------------------------------------------------------------
# Live feed: one Redis subscription fanned out to all SSE clients
# ---------------------------------------------------------------------------
class VehicleBroadcaster:
    """
    Updates arriving within `coalesce_seconds` are merged (latest state per
    vehicle) into one event, so per-client sends stay bounded no matter how
    small the processor's batches are.
    """

    def __init__(
        self,
        redis: Redis,
        channel: str,
        queue_size: int = SSE_QUEUE_SIZE,
        coalesce_seconds: float = SSE_COALESCE_SECONDS,
    ) -> None:
        self.redis = redis
        self.channel = channel
        self.queue_size = queue_size
        self.coalesce_seconds = coalesce_seconds
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.connected = False
        self.pending: dict[str, dict] = {}
        self.flush_scheduled = False

    def ingest(self, message: str) -> None:
        """Merge one pub/sub message; the first one in a quiet period schedules a flush."""
        for vehicle in json.loads(message):
            self.pending[vehicle['vehicle_id']] = vehicle
        if not self.flush_scheduled:
            self.flush_scheduled = True
            asyncio.get_running_loop().call_later(self.coalesce_seconds, self.flush)

    def flush(self) -> None:
        self.flush_scheduled = False
        if self.pending:
            self.publish(json.dumps(list(self.pending.values()), separators=(',', ':')))
            self.pending.clear()

    def subscribe(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=self.queue_size)
        self.subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self.subscribers.discard(queue)

    def publish(self, message: str) -> None:
        for queue in self.subscribers:
            if queue.full():
                # A slow client loses its oldest update instead of stalling everyone;
                # each vehicle reports again within seconds.
                queue.get_nowait()
            queue.put_nowait(message)

    async def run(self) -> None:
        """Keep the subscription alive across Redis restarts."""
        while True:
            pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
            try:
                await pubsub.subscribe(self.channel)
                self.connected = True
                logger.info('Subscribed to Redis channel %s', self.channel)
                async for message in pubsub.listen():
                    if message['type'] == 'message':
                        try:
                            self.ingest(message['data'])
                        except (ValueError, KeyError, TypeError) as exc:
                            logger.warning('Ignoring malformed live update: %s', exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.connected:
                    logger.warning('Live feed lost (%s); reconnecting', exc)
                self.connected = False
                await asyncio.sleep(1)
            finally:
                self.connected = False
                with contextlib.suppress(Exception):
                    await pubsub.aclose()


def format_sse(event: str, data: str) -> str:
    return f'event: {event}\ndata: {data}\n\n'


def format_ndjson(data: str) -> str:
    return ''.join(
        json.dumps(vehicle, separators=(',', ':')) + '\n' for vehicle in json.loads(data)
    )


async def vehicle_events(
    snapshot: list[VehicleState],
    queue: asyncio.Queue[str],
    fmt: Literal['sse', 'ndjson'],
    heartbeat: float = SSE_HEARTBEAT_SECONDS,
) -> AsyncIterator[str]:
    """Snapshot first, then every live update; heartbeats keep idle proxies from closing."""
    snapshot_json = json.dumps([v.model_dump() for v in snapshot], separators=(',', ':'))
    if fmt == 'sse':
        yield 'retry: 2000\n\n'
        yield format_sse('snapshot', snapshot_json)
    else:
        yield format_ndjson(snapshot_json)

    while True:
        try:
            data = await asyncio.wait_for(queue.get(), timeout=heartbeat)
        except asyncio.TimeoutError:  # not the builtin TimeoutError on Python 3.10
            yield ': keepalive\n\n' if fmt == 'sse' else '\n'
            continue
        yield format_sse('update', data) if fmt == 'sse' else format_ndjson(data)


# ---------------------------------------------------------------------------
# ClickHouse (health check; the processor owns the analytical writes)
# ---------------------------------------------------------------------------
async def default_clickhouse_factory():
    return await clickhouse_connect.get_async_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        connect_timeout=2,
        send_receive_timeout=10,
    )


def default_redis_factory() -> Redis:
    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=5,
        health_check_interval=15,
        # redis-py's default retries with seconds of backoff; fail fast so callers
        # get a 503 promptly instead of a hung request.
        retry=Retry(ExponentialBackoff(cap=0.2, base=0.05), retries=1),
    )


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
def create_app(
    redis_factory: Callable[[], Redis] = default_redis_factory,
    clickhouse_factory: Callable[[], Awaitable[Any]] = default_clickhouse_factory,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = redis_factory()
        app.state.clickhouse = None
        app.state.clickhouse_lock = asyncio.Lock()
        app.state.broadcaster = VehicleBroadcaster(app.state.redis, UPDATES_CHANNEL)
        feed = asyncio.create_task(app.state.broadcaster.run())
        try:
            yield
        finally:
            feed.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await feed
            await app.state.redis.aclose()
            if app.state.clickhouse is not None:
                with contextlib.suppress(Exception):
                    closed = app.state.clickhouse.close()
                    if inspect.isawaitable(closed):  # async in newer clickhouse-connect
                        await closed

    app = FastAPI(
        title='Fleet Analytics API',
        version='1.0.0',
        description='Live vehicle state and pipeline health for the fleet UI.',
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_methods=['GET'],
        allow_headers=['*'],
    )

    @app.exception_handler(RedisConnectionError)
    @app.exception_handler(RedisTimeoutError)
    async def redis_down(_: Request, exc: Exception) -> JSONResponse:
        return unavailable('redis', exc)

    @app.exception_handler(ClickHouseOperationalError)
    async def clickhouse_down(_: Request, exc: Exception) -> JSONResponse:
        return unavailable('clickhouse', exc)

    @app.exception_handler(ServiceUnavailable)
    async def dependency_down(_: Request, exc: ServiceUnavailable) -> JSONResponse:
        return unavailable(exc.dependency, exc)

    def get_redis(request: Request) -> Redis:
        return request.app.state.redis

    async def get_clickhouse(request: Request):
        state = request.app.state
        if state.clickhouse is None:
            # Created lazily so the API starts (and serves Redis data) while ClickHouse is down.
            async with state.clickhouse_lock:
                if state.clickhouse is None:
                    try:
                        state.clickhouse = await clickhouse_factory()
                    except Exception as exc:
                        raise ServiceUnavailable('clickhouse', str(exc)) from exc
        return state.clickhouse

    RedisDep = Annotated[Redis, Depends(get_redis)]

    @app.get('/api/v1/vehicles', response_model=list[VehicleState], tags=['live'])
    async def vehicles(redis: RedisDep) -> list[VehicleState]:
        """Current state of every vehicle seen within the processor's state TTL."""
        return await load_snapshot(redis)

    @app.get(
        '/api/v1/stream/vehicles',
        tags=['live'],
        response_class=StreamingResponse,
        responses={
            200: {
                'description': (
                    'SSE: a `snapshot` event (all vehicles) followed by `update` events '
                    '(changed vehicles), each carrying a JSON array of VehicleState. '
                    'NDJSON: one VehicleState per line.'
                ),
                'content': {'text/event-stream': {}, 'application/x-ndjson': {}},
            },
            503: {'description': 'Redis unavailable'},
        },
    )
    async def stream_vehicles(
        request: Request,
        redis: RedisDep,
        format: Literal['sse', 'ndjson'] = 'sse',
    ) -> StreamingResponse:
        """Live vehicle positions, pushed as soon as the processor updates Redis."""
        broadcaster: VehicleBroadcaster = request.app.state.broadcaster
        # Subscribe before the snapshot so no update falls between the two.
        queue = broadcaster.subscribe()
        try:
            snapshot = await load_snapshot(redis)
        except Exception:
            broadcaster.unsubscribe(queue)
            raise

        async def body() -> AsyncIterator[str]:
            try:
                async for chunk in vehicle_events(snapshot, queue, format):
                    yield chunk
            finally:
                broadcaster.unsubscribe(queue)

        return StreamingResponse(
            body(),
            media_type='text/event-stream' if format == 'sse' else 'application/x-ndjson',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
        )

    @app.get('/api/v1/metrics/dlq', response_model=DlqMetrics, tags=['metrics'])
    async def dlq_metrics(
        redis: RedisDep,
        window: Annotated[int, Query(ge=1, le=300, description='Seconds to aggregate')] = 10,
        history: Annotated[int, Query(ge=1, le=300, description='Seconds of series')] = 60,
    ) -> DlqMetrics:
        """Dead-letter rate from the processor's per-second counters (current second excluded)."""
        span = max(window, history)
        now = int(time.time())
        seconds = list(range(now - span, now))
        async with redis.pipeline(transaction=False) as pipe:
            for second in seconds:
                pipe.hgetall(f'{METRICS_KEY_PREFIX}{second}')
            rows = await pipe.execute()
        buckets = [
            DlqPoint(ts=second, processed=int(row.get('processed', 0)), dlq=int(row.get('dlq', 0)))
            for second, row in zip(seconds, rows, strict=True)
        ]
        metrics = summarize_dlq(buckets, window)
        metrics.series = buckets[-history:]
        return metrics

    @app.get(
        '/api/v1/health',
        response_model=Health,
        tags=['ops'],
        responses={503: {'model': Health, 'description': 'A dependency is down'}},
    )
    async def health(request: Request) -> JSONResponse:
        state = request.app.state
        redis_ok = clickhouse_ok = False
        with contextlib.suppress(Exception):
            redis_ok = bool(await state.redis.ping())
        with contextlib.suppress(Exception):
            clickhouse_ok = bool(await (await get_clickhouse(request)).ping())
        body = Health(
            status='ok' if redis_ok and clickhouse_ok else 'degraded',
            redis=redis_ok,
            clickhouse=clickhouse_ok,
            sse_clients=len(state.broadcaster.subscribers),
            live_feed=state.broadcaster.connected,
        )
        return JSONResponse(body.model_dump(), status_code=200 if body.status == 'ok' else 503)

    return app


app = create_app()
