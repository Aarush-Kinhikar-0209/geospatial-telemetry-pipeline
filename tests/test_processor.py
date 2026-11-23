"""Phase 3: stream transformation & routing (no broker, Redis, or ClickHouse needed)."""

import json
from datetime import datetime, timezone

import h3
import pytest
from aiokafka import TopicPartition

TP = TopicPartition('telemetry_raw', 0)


def payload(**overrides) -> dict:
    base = {
        'vehicle_id': 'v-001',
        'timestamp': '2026-11-17T18:45:07Z',
        'lat': 18.93,
        'lon': 72.82,
        'speed_kmh': 45.2,
        'battery_pct': 82,
    }
    base.update(overrides)
    return base


def encode(obj: dict) -> bytes:
    return json.dumps(obj).encode()


def test_ten_events_two_bad_routes_eight_valid_two_dlq(proc):
    events = [encode(payload(vehicle_id=f'v-{i:03d}')) for i in range(8)]
    events.insert(3, b'{broken json')
    events.insert(7, encode({k: v for k, v in payload().items() if k != 'timestamp'}))

    valid, dlq = proc.route(events)

    assert len(valid) == 8
    assert len(dlq) == 2
    assert [raw for raw, _ in dlq] == [events[3], events[7]]


@pytest.mark.parametrize(
    'raw',
    [
        b'{broken json',
        b'',
        b'null',
        b'[1, 2]',
        b'"just a string"',
        b'\xff\xfe not utf-8',
        None,
        encode(payload(speed_kmh='fast')),
        encode(payload(speed_kmh='45.2')),
        encode(payload(speed_kmh=-12.5)),
        encode(payload(battery_pct=500)),
        encode(payload(battery_pct=82.5)),
        encode(payload(lat=999)),
        encode(payload(lon=-181)),
        encode(payload(vehicle_id='')),
        encode(payload(vehicle_id=104)),
        encode(payload(timestamp='not-a-time')),
        encode(payload(timestamp='2026-11-17T18:45:07')),  # no timezone
        json.dumps(payload()).replace('18.93', 'NaN').encode(),
        encode({k: v for k, v in payload().items() if k not in ('lat', 'lon')}),
    ],
)
def test_bad_records_go_to_dlq_without_raising(proc, raw):
    event, reason = proc.classify(raw)
    assert event is None
    assert reason


def test_valid_record_is_enriched(proc):
    event, reason = proc.classify(encode(payload()))
    assert reason is None
    assert event.h3_index == h3.latlng_to_cell(18.93, 72.82, 9)
    assert h3.get_resolution(event.h3_index) == 9
    assert event.window_start == datetime(2026, 11, 17, 18, 45, 0, tzinfo=timezone.utc)


def test_generator_contract(proc, gen):
    """Clean generator output is valid downstream; every chaos payload is DLQ'd."""
    for pct, expect_valid in ((0, True), (100, False)):
        g = gen.TelemetryGenerator(stdout=True)
        g.configure(gen.RunConfig(vehicles=200, interval=0.2, chaos_percent=pct))
        vehicles = list(g.vehicles.items())
        for i in range(1000):
            vid, state = vehicles[i % len(vehicles)]
            corrupted, is_chaos = g.inject_chaos(g.generate_payload(vid, state))
            event, _ = proc.classify(g.serialize(corrupted, is_chaos))
            assert (event is not None) is expect_valid


def make_event(proc, ts: str, speed: float, vehicle_id: str = 'v-001'):
    event, reason = proc.classify(
        encode(payload(timestamp=ts, speed_kmh=speed, vehicle_id=vehicle_id))
    )
    assert reason is None
    return event


def test_tumbling_window_average(proc):
    windows = proc.TumblingWindows(size=10, lateness=5)
    for ts, speed in [('18:45:00', 10), ('18:45:04', 20), ('18:45:09', 30), ('18:45:10', 99)]:
        windows.add(make_event(proc, f'2026-11-17T{ts}Z', speed))

    window_end = datetime(2026, 11, 17, 18, 45, 10, tzinfo=timezone.utc).timestamp()
    assert windows.pop_closed(now=window_end + 4.9) == []

    closed = windows.pop_closed(now=window_end + 5)
    assert len(closed) == 1
    vehicle_id, start, state = closed[0]
    assert vehicle_id == 'v-001'
    assert start == datetime(2026, 11, 17, 18, 45, 0, tzinfo=timezone.utc)
    assert (state.speed_sum, state.count) == (60, 3)
    assert state.speed_sum / state.count == 20

    assert len(windows.open) == 1  # the 18:45:10 window is still open
    assert len(windows.pop_closed(now=0, final=True)) == 1
    assert windows.open == {}


def test_windows_are_per_vehicle(proc):
    windows = proc.TumblingWindows(size=10, lateness=0)
    windows.add(make_event(proc, '2026-11-17T18:45:01Z', 10, 'v-001'))
    windows.add(make_event(proc, '2026-11-17T18:45:02Z', 50, 'v-002'))
    closed = {vid: state.count for vid, _, state in windows.pop_closed(now=0, final=True)}
    assert closed == {'v-001': 1, 'v-002': 1}


def test_commit_never_passes_an_open_window(proc):
    windows = proc.TumblingWindows(size=10, lateness=5)
    windows.add(make_event(proc, '2026-11-17T18:45:01Z', 10), TP, offset=100)
    windows.add(make_event(proc, '2026-11-17T18:45:11Z', 10), TP, offset=101)
    windows.add(make_event(proc, '2026-11-17T18:45:02Z', 10), TP, offset=102)
    next_offsets = {TP: 103}

    assert proc.safe_commit_offsets(next_offsets, windows.min_open_offsets()) == {TP: 100}

    first_window_end = datetime(2026, 11, 17, 18, 45, 10, tzinfo=timezone.utc).timestamp()
    windows.pop_closed(now=first_window_end + 5)
    assert proc.safe_commit_offsets(next_offsets, windows.min_open_offsets()) == {TP: 101}

    windows.pop_closed(now=0, final=True)
    assert proc.safe_commit_offsets(next_offsets, windows.min_open_offsets()) == {TP: 103}


def test_p95(proc):
    assert proc.p95([]) == 0
    assert proc.p95([float(i) for i in range(1, 101)]) == 96


# ---------------------------------------------------------------------------
# Clearing a finished simulation
# ---------------------------------------------------------------------------
def sim_state(status: str, run_id: str = 'r1') -> str:
    return json.dumps({'status': status, 'run_id': run_id})


def test_reset_is_requested_only_while_cleaning_an_unacknowledged_run(proc):
    assert proc.reset_requested(sim_state('cleaning'), None) == 'r1'
    assert proc.reset_requested(sim_state('cleaning'), 'older-run') == 'r1'
    assert proc.reset_requested(sim_state('cleaning'), 'r1') is None  # already done
    for status in ('running', 'stopping'):
        assert proc.reset_requested(sim_state(status), None) is None
    assert proc.reset_requested(None, None) is None


@pytest.mark.parametrize('raw', ['', '{broken', 'null', '[]', '{"status":"cleaning"}'])
def test_reset_ignores_unreadable_state(proc, raw):
    assert proc.reset_requested(raw, None) is None


def test_clearing_windows_discards_them_without_persisting(proc):
    windows = proc.TumblingWindows(size=10, lateness=5)
    windows.add(make_event(proc, '2026-11-17T18:45:01Z', 10), TP, offset=7)
    windows.clear()
    assert windows.open == {}
    assert windows.min_open_offsets() == {}


def test_reset_wipes_telemetry_from_redis_but_keeps_control_state(proc):
    import asyncio

    import fakeredis

    async def scenario():
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        for key in ('vehicle:v-001', 'vehicle:v-002', 'metrics:pipeline:1700000000'):
            await redis.hset(key, mapping={'a': '1'})
        for key in ('simulation:state', 'simulation:heartbeat', 'simulation:generator_ack'):
            await redis.set(key, 'keep')

        processor = proc.StreamProcessor()
        processor.redis = redis
        deleted = await processor.wipe_redis()

        remaining = sorted([key async for key in redis.scan_iter(match='*')])
        return deleted, remaining

    deleted, remaining = asyncio.run(scenario())
    assert deleted == 3
    assert remaining == ['simulation:generator_ack', 'simulation:heartbeat', 'simulation:state']
