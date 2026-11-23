#!/usr/bin/env python3
"""
Phase 4: Fast Serving APIs (FastAPI)

Bridges the frontend to the data stores without exposing them directly:
  * GET /api/v1/stream/vehicles   live vehicle state (SSE, or NDJSON with ?format=ndjson)
  * GET /api/v1/vehicles          current state snapshot
  * GET /api/v1/metrics/dlq       processed / dead-lettered message rates (Redis)
  * GET /api/v1/health            dependency health
  * /api/v1/simulation/...        start, stop and observe the timed simulation

Live updates come from the processor's Redis pub/sub channel. A single
subscription is fanned out to every SSE client, so client count does not
multiply load on Redis.

A simulation is a timed run of the generator. The API owns its lifecycle
(running -> stopping -> cleaning -> idle) and records it in Redis; the
generator and processor follow that record. When the run ends, because time is
up, it is stopped, or the browser stopped sending heartbeats, the processor
clears Kafka, ClickHouse and Redis before the next run may start.

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
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

import clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError as ClickHouseOperationalError
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
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

# Simulation limits: the contract the UI form and the generator rely on.
MAX_VEHICLES = 200
MIN_INTERVAL_SECONDS = 0.1
MAX_INTERVAL_SECONDS = 2.0
MAX_CHAOS_PERCENT = 5
SIMULATION_DURATION_SECONDS: int = int(os.getenv('SIMULATION_DURATION_SECONDS', '120'))
# A running simulation with no heartbeat for this long is treated as an abandoned page.
SIMULATION_HEARTBEAT_TIMEOUT_SECONDS: float = float(
    os.getenv('SIMULATION_HEARTBEAT_TIMEOUT_SECONDS', '10')
)
# How long to wait for the generator to confirm it stopped before clearing anyway.
SIMULATION_ACK_TIMEOUT_SECONDS: float = float(os.getenv('SIMULATION_ACK_TIMEOUT_SECONDS', '10'))
SIMULATION_POLL_SECONDS: float = float(os.getenv('SIMULATION_POLL_SECONDS', '0.5'))

# Control plane keys, shared with generator/main.py and processor/main.py.
SIMULATION_STATE_KEY = 'simulation:state'
SIMULATION_HEARTBEAT_KEY = 'simulation:heartbeat'
SIMULATION_GENERATOR_ACK_KEY = 'simulation:generator_ack'
SIMULATION_PROCESSOR_ACK_KEY = 'simulation:processor_ack'
SIMULATION_LAST_KEY = 'simulation:last'

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


class SimulationConfig(BaseModel):
    """What the user asks a simulation to produce."""

    model_config = ConfigDict(strict=True, extra='forbid')

    vehicles: int = Field(ge=1, le=MAX_VEHICLES, description='Fleet size')
    interval_seconds: float = Field(
        ge=MIN_INTERVAL_SECONDS,
        le=MAX_INTERVAL_SECONDS,
        description='Seconds between messages from one vehicle',
    )
    chaos_percent: int = Field(
        ge=0, le=MAX_CHAOS_PERCENT, description='Share of messages deliberately corrupted'
    )


SimulationReason = Literal['completed', 'stopped', 'client_lost']


class SimulationLastRun(BaseModel):
    run_id: str
    reason: SimulationReason
    ended_at: float = Field(description='Unix time the run was fully cleared')


class SimulationState(BaseModel):
    status: Literal['idle', 'running', 'stopping', 'cleaning'] = Field(
        description='idle -> running -> stopping (generator) -> cleaning (data) -> idle'
    )
    run_id: str | None = None
    config: SimulationConfig | None = None
    duration_seconds: int
    remaining_seconds: float | None = Field(
        default=None, description='Seconds left while running; clients count down locally'
    )
    reason: SimulationReason | None = Field(
        default=None, description='Why a run is stopping or being cleared'
    )
    last_run: SimulationLastRun | None = Field(
        default=None, description='The most recent finished run (only while idle)'
    )


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
# Simulation lifecycle
# ---------------------------------------------------------------------------
class SimulationBusy(Exception):
    def __init__(self, status: str) -> None:
        super().__init__(status)
        self.status = status


class SimulationController:
    """
    Owns the simulation lifecycle; its state lives in Redis so it survives an
    API restart and is what the generator and processor follow.

        running   the generator produces; ends at the deadline, on stop, or when
                  the browser's heartbeat lapses
        stopping  the generator is told to stop; wait for its ack (or a timeout)
        cleaning  the processor clears Kafka, ClickHouse and Redis; wait for its ack
        (absent)  idle: a new run may start

    Every step is driven by `reconcile`, which reads the state and moves it one
    step, so a missed poll or a restart only delays the next transition.
    Assumes a single API instance (the SSE fan-out already does).
    """

    def __init__(
        self,
        redis: Redis,
        *,
        duration_seconds: int = SIMULATION_DURATION_SECONDS,
        heartbeat_timeout_seconds: float = SIMULATION_HEARTBEAT_TIMEOUT_SECONDS,
        ack_timeout_seconds: float = SIMULATION_ACK_TIMEOUT_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.redis = redis
        self.duration_seconds = duration_seconds
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.ack_timeout_seconds = ack_timeout_seconds
        self.clock = clock
        self.lock = asyncio.Lock()

    async def load(self) -> dict | None:
        raw = await self.redis.get(SIMULATION_STATE_KEY)
        return json.loads(raw) if raw else None

    async def save(self, state: dict) -> None:
        await self.redis.set(SIMULATION_STATE_KEY, json.dumps(state))

    async def beat(self) -> None:
        await self.redis.set(
            SIMULATION_HEARTBEAT_KEY, '1', px=max(1, int(self.heartbeat_timeout_seconds * 1000))
        )

    async def start(self, config: SimulationConfig) -> dict:
        async with self.lock:
            current = await self.load()
            if current is not None:
                raise SimulationBusy(current['status'])
            now = self.clock()
            state = {
                'run_id': uuid.uuid4().hex[:12],
                'status': 'running',
                'config': config.model_dump(),
                'started_at': now,
                'ends_at': now + self.duration_seconds,
                'duration_seconds': self.duration_seconds,
                'reason': None,
            }
            # The first heartbeat is part of starting, so the run is not abandoned at birth.
            await self.beat()
            await self.save(state)
            logger.info('Simulation %s started: %s', state['run_id'], state['config'])
            return state

    async def heartbeat(self) -> None:
        """The page is open: keep the running simulation alive."""
        state = await self.load()
        if state is not None and state['status'] == 'running':
            await self.beat()

    async def stop(self, reason: SimulationReason = 'stopped') -> dict | None:
        """Begin ending a running simulation; any other state is left as is."""
        async with self.lock:
            state = await self.load()
            if state is not None and state['status'] == 'running':
                state.update(status='stopping', reason=reason, stopped_at=self.clock())
                await self.save(state)
                logger.info('Simulation %s stopping (%s)', state['run_id'], reason)
            return state

    async def reconcile(self) -> None:
        """Move the simulation one step along its lifecycle if it is due."""
        state = await self.load()
        if state is None:
            return
        status = state['status']
        if status == 'running':
            if self.clock() >= state['ends_at']:
                await self.stop('completed')
            elif not await self.redis.exists(SIMULATION_HEARTBEAT_KEY):
                await self.stop('client_lost')
        elif status == 'stopping':
            await self.advance_after_generator(state)
        elif status == 'cleaning':
            await self.finish_after_processor(state)

    async def advance_after_generator(self, state: dict) -> None:
        async with self.lock:
            acked = await self.redis.get(SIMULATION_GENERATOR_ACK_KEY) == state['run_id']
            waited = self.clock() - state.get('stopped_at', 0)
            if acked or waited >= self.ack_timeout_seconds:
                if not acked:
                    logger.warning(
                        'Generator did not confirm stopping run %s; clearing anyway',
                        state['run_id'],
                    )
                current = await self.load()
                if current is not None and current['run_id'] == state['run_id']:
                    current.update(status='cleaning', cleaning_at=self.clock())
                    await self.save(current)
                    logger.info('Simulation %s cleaning', state['run_id'])

    async def finish_after_processor(self, state: dict) -> None:
        async with self.lock:
            if await self.redis.get(SIMULATION_PROCESSOR_ACK_KEY) != state['run_id']:
                return
            last = SimulationLastRun(
                run_id=state['run_id'],
                reason=state.get('reason') or 'stopped',
                ended_at=self.clock(),
            )
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.set(SIMULATION_LAST_KEY, last.model_dump_json())
                pipe.delete(SIMULATION_STATE_KEY)
                await pipe.execute()
            logger.info('Simulation %s cleared (%s)', state['run_id'], state['reason'])

    async def snapshot(self) -> SimulationState:
        state = await self.load()
        if state is None:
            raw = await self.redis.get(SIMULATION_LAST_KEY)
            return SimulationState(
                status='idle',
                duration_seconds=self.duration_seconds,
                last_run=SimulationLastRun.model_validate_json(raw) if raw else None,
            )
        remaining = None
        if state['status'] == 'running':
            remaining = max(0.0, state['ends_at'] - self.clock())
        return SimulationState(
            status=state['status'],
            run_id=state['run_id'],
            config=SimulationConfig.model_validate(state['config']),
            duration_seconds=state['duration_seconds'],
            remaining_seconds=remaining,
            reason=state.get('reason'),
        )

    async def run(self, poll_seconds: float) -> None:
        """Drive the lifecycle until cancelled; a Redis outage only delays it."""
        while True:
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning('Simulation reconcile failed: %s', exc)
            await asyncio.sleep(poll_seconds)


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
    simulation_clock: Callable[[], float] = time.time,
    simulation_poll_seconds: float = SIMULATION_POLL_SECONDS,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = redis_factory()
        app.state.clickhouse = None
        app.state.clickhouse_lock = asyncio.Lock()
        app.state.broadcaster = VehicleBroadcaster(app.state.redis, UPDATES_CHANNEL)
        app.state.simulation = SimulationController(app.state.redis, clock=simulation_clock)
        background = [
            asyncio.create_task(app.state.broadcaster.run()),
            asyncio.create_task(app.state.simulation.run(simulation_poll_seconds)),
        ]
        try:
            yield
        finally:
            for task in background:
                task.cancel()
            for task in background:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
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
        allow_methods=['GET', 'POST'],
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

    def get_simulation(request: Request) -> SimulationController:
        return request.app.state.simulation

    RedisDep = Annotated[Redis, Depends(get_redis)]
    SimulationDep = Annotated[SimulationController, Depends(get_simulation)]

    @app.get('/api/v1/simulation', response_model=SimulationState, tags=['simulation'])
    async def simulation_state(simulation: SimulationDep) -> SimulationState:
        """Where the simulation is in its lifecycle, and how long it has left."""
        return await simulation.snapshot()

    @app.post(
        '/api/v1/simulation/start',
        response_model=SimulationState,
        tags=['simulation'],
        responses={409: {'description': 'A simulation is already in progress'}},
    )
    async def simulation_start(
        config: SimulationConfig, simulation: SimulationDep
    ) -> SimulationState:
        """
        Start a timed simulation. When it ends (time up, stopped, or the page went
        away) every trace of it is deleted from Kafka, ClickHouse and Redis.
        """
        try:
            await simulation.start(config)
        except SimulationBusy as exc:
            raise HTTPException(
                status_code=409, detail=f'A simulation is already {exc.status}'
            ) from exc
        return await simulation.snapshot()

    @app.post('/api/v1/simulation/stop', response_model=SimulationState, tags=['simulation'])
    async def simulation_stop(simulation: SimulationDep) -> SimulationState:
        """End the running simulation now; its data is then cleared like any other run."""
        await simulation.stop('stopped')
        return await simulation.snapshot()

    @app.post('/api/v1/simulation/heartbeat', response_model=SimulationState, tags=['simulation'])
    async def simulation_heartbeat(simulation: SimulationDep) -> SimulationState:
        """
        Tell the API the page is still open. A running simulation whose heartbeat
        lapses (see SIMULATION_HEARTBEAT_TIMEOUT_SECONDS) is stopped and cleared.
        """
        await simulation.heartbeat()
        return await simulation.snapshot()

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
