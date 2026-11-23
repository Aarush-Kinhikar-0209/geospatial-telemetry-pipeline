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


class Clock:
    """A settable clock, so simulation deadlines are tested without waiting."""

    def __init__(self, now: float = 1_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


def make_client(api, server, clickhouse=None, redis_factory=None, clock=None) -> TestClient:
    async def clickhouse_factory():
        if clickhouse is None:
            raise OperationalError('ClickHouse not reachable')
        return clickhouse

    app = api.create_app(
        redis_factory=redis_factory
        or (lambda: fakeredis.FakeAsyncRedis(server=server, decode_responses=True)),
        clickhouse_factory=clickhouse_factory,
        simulation_clock=clock or time.time,
        # Tests drive the lifecycle with reconcile(); keep the background loop out of the way.
        simulation_poll_seconds=3600,
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
        '/api/v1/simulation',
        '/api/v1/simulation/start',
        '/api/v1/simulation/stop',
        '/api/v1/simulation/heartbeat',
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


# ---------------------------------------------------------------------------
# Simulation lifecycle
# ---------------------------------------------------------------------------
START = '/api/v1/simulation/start'
STATE = '/api/v1/simulation'
VALID = {'vehicles': 100, 'interval_seconds': 2.0, 'chaos_percent': 5}


def sync_redis(server):
    return fakeredis.FakeRedis(server=server, decode_responses=True)


def reconcile(client) -> None:
    client.portal.call(client.app.state.simulation.reconcile)


def state(client) -> dict:
    return client.get(STATE).json()


def drive_to_idle(client, server) -> None:
    """Play the generator's and processor's parts of a finished run."""
    run_id = state(client)['run_id']
    reconcile(client)  # running -> stopping (if due)
    sync_redis(server).set('simulation:generator_ack', run_id)
    reconcile(client)  # stopping -> cleaning
    sync_redis(server).set('simulation:processor_ack', run_id)
    reconcile(client)  # cleaning -> idle


def test_idle_by_default(api, server):
    with make_client(api, server) as client:
        body = state(client)
    assert body['status'] == 'idle'
    assert body['duration_seconds'] == 120
    assert body['run_id'] is None and body['remaining_seconds'] is None


@pytest.mark.parametrize(
    'config',
    [
        {'vehicles': 1, 'interval_seconds': 0.1, 'chaos_percent': 0},
        {'vehicles': 200, 'interval_seconds': 2.0, 'chaos_percent': 5},
        {'vehicles': 37, 'interval_seconds': 0.75, 'chaos_percent': 3},
        {'vehicles': 10, 'interval_seconds': 1, 'chaos_percent': 2},  # JSON 1 is a valid float
    ],
)
def test_limits_are_inclusive(api, server, config):
    with make_client(api, server) as client:
        assert client.post(START, json=config).status_code == 200


@pytest.mark.parametrize(
    'patch',
    [
        {'vehicles': 0},
        {'vehicles': 201},
        {'vehicles': -5},
        {'vehicles': 10.5},
        {'vehicles': '10'},
        {'vehicles': True},
        {'interval_seconds': 0.09},
        {'interval_seconds': 2.01},
        {'interval_seconds': 0},
        {'interval_seconds': '1'},
        {'chaos_percent': -1},
        {'chaos_percent': 6},
        {'chaos_percent': 2.5},
        {'chaos_percent': '2'},
        {'unexpected': 1},
    ],
)
def test_out_of_range_input_is_rejected_and_nothing_starts(api, server, patch):
    with make_client(api, server) as client:
        response = client.post(START, json={**VALID, **patch})
        assert response.status_code == 422
        assert state(client)['status'] == 'idle'
    assert sync_redis(server).get('simulation:state') is None


@pytest.mark.parametrize('missing', ['vehicles', 'interval_seconds', 'chaos_percent'])
def test_every_setting_is_required(api, server, missing):
    body = {k: v for k, v in VALID.items() if k != missing}
    with make_client(api, server) as client:
        assert client.post(START, json=body).status_code == 422


def test_start_publishes_the_run_for_the_generator(api, server):
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        response = client.post(START, json=VALID)
    assert response.status_code == 200
    body = response.json()
    assert body['status'] == 'running'
    assert body['config'] == VALID
    assert body['remaining_seconds'] == 120

    stored = json.loads(sync_redis(server).get('simulation:state'))
    assert stored['run_id'] == body['run_id']
    assert stored['status'] == 'running'
    assert stored['config'] == VALID
    assert stored['ends_at'] == clock.now + 120
    assert sync_redis(server).exists('simulation:heartbeat')


def test_a_second_simulation_cannot_start_until_the_first_is_cleared(api, server):
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        first = client.post(START, json=VALID).json()
        assert client.post(START, json=VALID).status_code == 409
        assert state(client)['run_id'] == first['run_id']

        clock.now += 120
        reconcile(client)
        assert client.post(START, json=VALID).status_code == 409  # stopping
        sync_redis(server).set('simulation:generator_ack', first['run_id'])
        reconcile(client)
        assert client.post(START, json=VALID).status_code == 409  # cleaning

        sync_redis(server).set('simulation:processor_ack', first['run_id'])
        reconcile(client)
        second = client.post(START, json=VALID)
    assert second.status_code == 200
    assert second.json()['run_id'] != first['run_id']


def test_countdown_follows_the_clock(api, server):
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        client.post(START, json=VALID)
        clock.now += 45.5
        assert state(client)['remaining_seconds'] == 74.5
        clock.now += 500
        assert state(client)['remaining_seconds'] == 0  # never negative


def test_time_up_walks_through_stopping_and_cleaning_to_idle(api, server):
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        run_id = client.post(START, json=VALID).json()['run_id']

        clock.now += 119
        reconcile(client)
        assert state(client)['status'] == 'running'

        clock.now += 1
        reconcile(client)
        body = state(client)
        assert (body['status'], body['reason']) == ('stopping', 'completed')
        assert body['remaining_seconds'] is None

        reconcile(client)  # the generator has not confirmed yet
        assert state(client)['status'] == 'stopping'

        sync_redis(server).set('simulation:generator_ack', run_id)
        reconcile(client)
        assert state(client)['status'] == 'cleaning'

        reconcile(client)  # the processor has not finished clearing
        assert state(client)['status'] == 'cleaning'

        clock.now += 2
        sync_redis(server).set('simulation:processor_ack', run_id)
        reconcile(client)
        body = state(client)

    assert body['status'] == 'idle' and body['run_id'] is None
    assert body['last_run'] == {'run_id': run_id, 'reason': 'completed', 'ended_at': clock.now}
    assert sync_redis(server).get('simulation:state') is None


def test_an_old_acknowledgement_does_not_advance_a_new_run(api, server):
    clock = Clock()
    sync_redis(server).set('simulation:generator_ack', 'previous-run')
    sync_redis(server).set('simulation:processor_ack', 'previous-run')
    with make_client(api, server, clock=clock) as client:
        client.post(START, json=VALID)
        client.post('/api/v1/simulation/stop')
        reconcile(client)
        assert state(client)['status'] == 'stopping'


def test_clearing_proceeds_if_the_generator_never_confirms(api, server):
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        client.post(START, json=VALID)
        client.post('/api/v1/simulation/stop')
        clock.now += 9
        reconcile(client)
        assert state(client)['status'] == 'stopping'
        clock.now += 1
        reconcile(client)
        assert state(client)['status'] == 'cleaning'


def test_stop_ends_the_run_early_and_is_cleared_like_any_other(api, server):
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        client.post(START, json=VALID)
        clock.now += 30
        body = client.post('/api/v1/simulation/stop').json()
        assert (body['status'], body['reason']) == ('stopping', 'stopped')
        assert client.post('/api/v1/simulation/stop').json()['status'] == 'stopping'  # idempotent
        drive_to_idle(client, server)
        final = state(client)
    assert final['status'] == 'idle' and final['last_run']['reason'] == 'stopped'


def test_stop_while_idle_is_harmless(api, server):
    with make_client(api, server) as client:
        response = client.post('/api/v1/simulation/stop')
    assert response.status_code == 200 and response.json()['status'] == 'idle'


def test_losing_the_heartbeat_stops_and_clears_the_run(api, server):
    """The page was closed: nobody is left to watch, so the data must not linger."""
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        client.post(START, json=VALID)
        reconcile(client)
        assert state(client)['status'] == 'running'

        sync_redis(server).delete('simulation:heartbeat')  # what its TTL expiry does
        reconcile(client)
        body = state(client)
        assert (body['status'], body['reason']) == ('stopping', 'client_lost')

        drive_to_idle(client, server)
        assert state(client)['last_run']['reason'] == 'client_lost'


def test_heartbeats_keep_a_run_alive(api, server):
    with make_client(api, server) as client:
        client.post(START, json=VALID)
        sync_redis(server).delete('simulation:heartbeat')
        assert client.post('/api/v1/simulation/heartbeat').json()['status'] == 'running'
        assert sync_redis(server).exists('simulation:heartbeat')
        reconcile(client)
        assert state(client)['status'] == 'running'


def test_heartbeat_expires_by_itself(api, server):
    with make_client(api, server) as client:
        client.post(START, json=VALID)
    ttl_ms = sync_redis(server).pttl('simulation:heartbeat')
    assert 0 < ttl_ms <= 10_000


def test_heartbeat_while_idle_creates_nothing(api, server):
    with make_client(api, server) as client:
        body = client.post('/api/v1/simulation/heartbeat').json()
    assert body['status'] == 'idle'
    assert not sync_redis(server).exists('simulation:heartbeat')


def test_a_restarted_api_resumes_the_lifecycle(api, server):
    """State lives in Redis, so a new API process picks the run up where it was."""
    clock = Clock()
    with make_client(api, server, clock=clock) as client:
        run_id = client.post(START, json=VALID).json()['run_id']
    clock.now += 200
    with make_client(api, server, clock=clock) as client:
        assert state(client)['run_id'] == run_id
        reconcile(client)
        assert state(client)['status'] == 'stopping'


@pytest.mark.parametrize(
    ('method', 'path'),
    [
        ('get', STATE),
        ('post', START),
        ('post', '/api/v1/simulation/stop'),
        ('post', '/api/v1/simulation/heartbeat'),
    ],
)
def test_simulation_endpoints_return_503_when_redis_is_down(api, server, method, path):
    with make_client(api, server, redis_factory=unreachable_redis) as client:
        response = getattr(client, method)(path, **({'json': VALID} if method == 'post' else {}))
    assert response.status_code == 503
    assert response.json()['dependency'] == 'redis'


def test_cors_allows_posting_from_the_frontend(api, server):
    with make_client(api, server) as client:
        response = client.options(
            START,
            headers={
                'Origin': 'http://localhost:3000',
                'Access-Control-Request-Method': 'POST',
                'Access-Control-Request-Headers': 'content-type',
            },
        )
    assert response.status_code == 200
    assert 'POST' in response.headers['access-control-allow-methods']


def test_control_keys_agree_across_services(api, gen, proc):
    """The three services share these keys by convention; a typo would stall a run."""
    assert api.SIMULATION_STATE_KEY == gen.SIMULATION_STATE_KEY == proc.SIMULATION_STATE_KEY
    assert api.SIMULATION_GENERATOR_ACK_KEY == gen.GENERATOR_ACK_KEY
    assert api.SIMULATION_PROCESSOR_ACK_KEY == proc.PROCESSOR_ACK_KEY
    assert api.UPDATES_CHANNEL == proc.UPDATES_CHANNEL
    # The processor wipes only telemetry keys, never the control keys.
    for key in (
        api.SIMULATION_STATE_KEY,
        api.SIMULATION_HEARTBEAT_KEY,
        api.SIMULATION_GENERATOR_ACK_KEY,
        api.SIMULATION_PROCESSOR_ACK_KEY,
        api.SIMULATION_LAST_KEY,
    ):
        assert not key.startswith(('vehicle:', proc.METRICS_KEY_PREFIX))
