"""Phase 4: API contracts (FastAPI TestClient with fake Redis / ClickHouse)."""

import asyncio
import json
import time

import fakeredis
import pytest
from clickhouse_connect.driver.exceptions import OperationalError
from fastapi.testclient import TestClient
from redis.asyncio import Redis

VEHICLE = {
    'vehicle_id': 'v-001',
    'lat': '18.93',
    'lon': '72.82',
    'speed_kmh': '45.2',
    'battery_pct': '82',
    'h3_index': '89608b0aabbffff',
    'timestamp': '2026-11-17T18:45:07Z',
    'processed_at': '2026-11-17T18:45:07.120000Z',
}


class FakeResult:
    def __init__(self, rows: list[dict]):
        self.rows = rows

    def named_results(self):
        return iter(self.rows)


class FakeClickHouse:
    def __init__(self, rows: list[dict] | None = None, fail: bool = False):
        self.rows = rows or []
        self.fail = fail
        self.queries: list[tuple[str, dict]] = []

    async def query(self, sql: str, parameters: dict | None = None):
        if self.fail:
            raise OperationalError('connection refused')
        self.queries.append((sql, parameters))
        return FakeResult(self.rows)

    async def ping(self) -> bool:
        return not self.fail

    async def close(self) -> None:
        pass


@pytest.fixture
def server():
    return fakeredis.FakeServer()


def make_client(api, server, clickhouse=None, redis_factory=None) -> TestClient:
    async def clickhouse_factory():
        if clickhouse is None:
            raise OperationalError('ClickHouse not reachable')
        return clickhouse

    app = api.create_app(
        redis_factory=redis_factory
        or (lambda: fakeredis.FakeAsyncRedis(server=server, decode_responses=True)),
        clickhouse_factory=clickhouse_factory,
    )
    return TestClient(app)


def seed(server, mapping: dict) -> None:
    sync = fakeredis.FakeRedis(server=server, decode_responses=True)
    for key, value in mapping.items():
        sync.hset(key, mapping=value)


def test_vehicles_snapshot(api, server):
    seed(server, {'vehicle:v-002': {**VEHICLE, 'vehicle_id': 'v-002'}, 'vehicle:v-001': VEHICLE})
    with make_client(api, server) as client:
        response = client.get('/api/v1/vehicles')
    assert response.status_code == 200
    body = response.json()
    assert [v['vehicle_id'] for v in body] == ['v-001', 'v-002']
    assert body[0] == {**VEHICLE, 'lat': 18.93, 'lon': 72.82, 'speed_kmh': 45.2, 'battery_pct': 82}


def test_dlq_metrics(api, server):
    now = int(time.time())
    seed(
        server,
        {
            f'metrics:pipeline:{now - 1}': {'processed': 1000, 'dlq': 50},
            f'metrics:pipeline:{now - 2}': {'processed': 1000, 'dlq': 150},
            f'metrics:pipeline:{now - 30}': {'processed': 1000, 'dlq': 0},
        },
    )
    with make_client(api, server) as client:
        response = client.get('/api/v1/metrics/dlq?window=10&history=60')
    assert response.status_code == 200
    body = response.json()
    assert (body['processed'], body['dlq']) == (2000, 200)
    assert body['dlq_rate_per_sec'] == 20
    assert body['dlq_ratio'] == 0.1
    assert len(body['series']) == 60
    assert body['series'][-1] == {'ts': now - 1, 'processed': 1000, 'dlq': 50}
    assert sum(p['processed'] for p in body['series']) == 3000


def unreachable_redis() -> Redis:
    return Redis(host='127.0.0.1', port=1, socket_connect_timeout=0.2, decode_responses=True)


@pytest.mark.parametrize(
    'path', ['/api/v1/vehicles', '/api/v1/metrics/dlq', '/api/v1/stream/vehicles']
)
def test_redis_down_returns_503(api, server, path):
    with make_client(api, server, redis_factory=unreachable_redis) as client:
        response = client.get(path)
    assert response.status_code == 503
    assert response.json()['dependency'] == 'redis'


def test_health(api, server):
    with make_client(api, server, FakeClickHouse()) as client:
        ok = client.get('/api/v1/health')
    with make_client(api, server, None) as client:
        degraded = client.get('/api/v1/health')
    assert ok.status_code == 200 and ok.json()['status'] == 'ok'
    assert degraded.status_code == 503 and degraded.json()['clickhouse'] is False


def test_cors_allows_frontend(api, server):
    with make_client(api, server) as client:
        response = client.get('/api/v1/vehicles', headers={'Origin': 'http://localhost:3000'})
    assert response.headers['access-control-allow-origin'] == 'http://localhost:3000'


def test_openapi_docs(api, server):
    with make_client(api, server) as client:
        assert client.get('/docs').status_code == 200
        paths = client.get('/openapi.json').json()['paths']
    assert {
        '/api/v1/stream/vehicles',
        '/api/v1/vehicles',
        '/api/v1/metrics/dlq',
    } <= set(paths)


def test_sse_stream_snapshot_then_updates(api):
    async def scenario():
        snapshot = [api.parse_vehicle('vehicle:v-001', VEHICLE)]
        queue: asyncio.Queue[str] = asyncio.Queue()
        events = api.vehicle_events(snapshot, queue, 'sse', heartbeat=0.05)

        assert await anext(events) == 'retry: 2000\n\n'
        first = await anext(events)
        assert first.startswith('event: snapshot\ndata: ')
        assert json.loads(first.split('data: ', 1)[1])[0]['vehicle_id'] == 'v-001'

        assert await anext(events) == ': keepalive\n\n'

        queue.put_nowait('[{"vehicle_id":"v-009","lat":1.0}]')
        assert await anext(events) == 'event: update\ndata: [{"vehicle_id":"v-009","lat":1.0}]\n\n'
        await events.aclose()

    asyncio.run(scenario())


def test_ndjson_stream(api):
    async def scenario():
        queue: asyncio.Queue[str] = asyncio.Queue()
        events = api.vehicle_events([], queue, 'ndjson', heartbeat=1)
        assert await anext(events) == ''
        queue.put_nowait('[{"vehicle_id":"a"},{"vehicle_id":"b"}]')
        assert await anext(events) == '{"vehicle_id":"a"}\n{"vehicle_id":"b"}\n'
        await events.aclose()

    asyncio.run(scenario())


def test_broadcaster_fans_out_and_drops_oldest_for_slow_clients(api):
    broadcaster = api.VehicleBroadcaster(redis=None, channel='c', queue_size=2)
    fast, slow = broadcaster.subscribe(), broadcaster.subscribe()
    for i in range(3):
        broadcaster.publish(str(i))
        if i < 2:
            assert fast.get_nowait() == str(i)
    assert fast.get_nowait() == '2'
    assert [slow.get_nowait(), slow.get_nowait()] == ['1', '2']
    broadcaster.unsubscribe(slow)
    assert broadcaster.subscribers == {fast}


def test_broadcaster_coalesces_updates_per_vehicle(api):
    async def scenario():
        broadcaster = api.VehicleBroadcaster(redis=None, channel='c', coalesce_seconds=0.02)
        queue = broadcaster.subscribe()
        broadcaster.ingest('[{"vehicle_id":"a","lat":1},{"vehicle_id":"b","lat":1}]')
        broadcaster.ingest('[{"vehicle_id":"a","lat":2}]')
        assert queue.empty()  # held for the coalescing window
        merged = json.loads(await asyncio.wait_for(queue.get(), timeout=1))
        assert queue.empty()
        return merged

    assert asyncio.run(scenario()) == [{'vehicle_id': 'a', 'lat': 2}, {'vehicle_id': 'b', 'lat': 1}]


def test_broadcaster_relays_redis_pubsub(api, server):
    async def scenario():
        redis = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
        broadcaster = api.VehicleBroadcaster(redis, 'vehicles:updates')
        queue = broadcaster.subscribe()
        task = asyncio.create_task(broadcaster.run())
        for _ in range(100):
            if broadcaster.connected:
                break
            await asyncio.sleep(0.01)
        await redis.publish('vehicles:updates', '[{"vehicle_id":"v-001"}]')
        message = await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()
        return message

    assert asyncio.run(scenario()) == '[{"vehicle_id":"v-001"}]'
