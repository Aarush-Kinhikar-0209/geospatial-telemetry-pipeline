"""Phase 2: synthetic telemetry generator."""

import asyncio
import json
import time
from collections import Counter

import fakeredis
import pytest
from pydantic import ValidationError


def make_generator(gen, vehicles: int = 200, chaos: float = 0, interval: float = 0.2):
    g = gen.TelemetryGenerator(stdout=True)
    g.configure(gen.RunConfig(vehicles=vehicles, interval=interval, chaos_percent=chaos))
    return g


def produce_batch(g, n: int) -> list[tuple[str, str, bool]]:
    """Run the generator's payload pipeline n times (round-robin over vehicles), no broker."""
    vehicle_ids = list(g.vehicles)
    out = []
    for i in range(n):
        vid = vehicle_ids[i % len(vehicle_ids)]
        payload = g.generate_payload(vid, g.vehicles[vid])
        corrupted, is_chaos = g.inject_chaos(payload)
        out.append((vid, g.serialize(corrupted, is_chaos), is_chaos))
    return out


def test_clean_payloads_match_schema(gen):
    for _, message, is_chaos in produce_batch(make_generator(gen, chaos=0), 2000):
        assert not is_chaos
        payload = gen.TelemetryPayload.model_validate_json(message)
        assert gen.on_land(payload.lat, payload.lon)


def test_land_polygon_excludes_the_sea(gen):
    assert gen.on_land(18.94, 72.835)  # Fort, on the peninsula
    assert gen.on_land(19.12, 72.85)  # Andheri
    assert not gen.on_land(18.95, 72.75)  # Arabian Sea, west of the coast
    assert not gen.on_land(18.85, 72.83)  # open water south of Colaba
    assert not gen.on_land(18.95, 72.90)  # Mumbai Harbour


def test_vehicles_stay_on_land(gen):
    g = make_generator(gen, chaos=0)
    for _ in range(50):
        for vid, state in g.vehicles.items():
            payload = g.generate_payload(vid, state)
            assert gen.on_land(payload['lat'], payload['lon'])


def test_clean_payload_has_documented_shape(gen):
    _, message, _ = produce_batch(make_generator(gen, chaos=0), 1)[0]
    obj = json.loads(message)
    assert set(obj) == {'vehicle_id', 'timestamp', 'lat', 'lon', 'speed_kmh', 'battery_pct'}
    assert obj['timestamp'].endswith('Z')


def test_every_chaos_payload_violates_schema(gen):
    for _, message, is_chaos in produce_batch(make_generator(gen, chaos=100), 2000):
        assert is_chaos
        with pytest.raises(ValidationError):
            gen.TelemetryPayload.model_validate_json(message)


@pytest.mark.parametrize('pct', [0, 1, 2, 3.5, 5])
def test_chaos_rate_is_exact(gen, pct):
    n = 60_000  # one minute at 1000 msg/s
    chaos = sum(is_chaos for _, _, is_chaos in produce_batch(make_generator(gen, chaos=pct), n))
    assert chaos == round(n * pct / 100)


def test_chaos_is_spread_across_vehicles(gen):
    chaos_per_vehicle = Counter()
    for vid, _, is_chaos in produce_batch(make_generator(gen, vehicles=200, chaos=5), 60_000):
        chaos_per_vehicle[vid] += is_chaos
    # 300 messages per vehicle, ~15 expected chaos: none all-clean, none all-chaos.
    assert len(chaos_per_vehicle) == 200
    assert all(1 <= count <= 60 for count in chaos_per_vehicle.values())


@pytest.mark.parametrize('vehicles', [1, 37, 200])
def test_fleet_size_follows_the_config(gen, vehicles):
    g = make_generator(gen, vehicles=vehicles)
    assert list(g.vehicles) == [f'v-{i:03d}' for i in range(1, vehicles + 1)]


def test_configure_starts_a_fresh_run(gen):
    g = make_generator(gen, vehicles=5, chaos=100)
    produce_batch(g, 10)
    g.configure(gen.RunConfig(vehicles=3, interval=1.0, chaos_percent=0))
    assert len(g.vehicles) == 3
    assert g.chaos_schedule == []
    assert (g.messages_sent, g.total_messages) == (0, 0)
    assert not any(is_chaos for _, _, is_chaos in produce_batch(g, 500))


@pytest.mark.parametrize('chaos', [-1, 101])
def test_invalid_chaos_percentage_rejected(gen, chaos):
    with pytest.raises(ValueError):
        gen.RunConfig(vehicles=10, interval=1, chaos_percent=chaos).validate()


@pytest.mark.parametrize('kwargs', [{'vehicles': 0}, {'interval': 0}, {'interval': -1}])
def test_invalid_run_config_rejected(gen, kwargs):
    config = gen.RunConfig(**{'vehicles': 10, 'interval': 1, 'chaos_percent': 0, **kwargs})
    with pytest.raises(ValueError):
        config.validate()


def test_run_config_reads_the_api_state(gen):
    state = {'config': {'vehicles': 42, 'interval_seconds': 0.5, 'chaos_percent': 3}}
    assert gen.RunConfig.from_state(state) == gen.RunConfig(
        vehicles=42, interval=0.5, chaos_percent=3
    )


# ---------------------------------------------------------------------------
# Control plane: the generator follows the API's simulation state
# ---------------------------------------------------------------------------
NOW = 1_000.0


def run_state(status='running', run_id='r1', ends_at=NOW + 120):
    return {'status': status, 'run_id': run_id, 'ends_at': ends_at}


def test_idle_generator_does_nothing(gen):
    assert gen.next_action(None, None, None, None, NOW) is None


def test_a_running_simulation_is_started_once(gen):
    state = run_state()
    assert gen.next_action(state, None, None, None, NOW) == ('start', 'r1')
    assert gen.next_action(state, 'r1', 'r1', None, NOW) is None  # already producing


def test_generator_stops_when_the_state_leaves_running(gen):
    for state in (run_state('stopping'), run_state('cleaning'), None, run_state(run_id='r2')):
        assert gen.next_action(state, 'r1', 'r1', None, NOW) == ('stop', 'r1')


def test_generator_stops_at_the_deadline_without_the_api(gen):
    state = run_state(ends_at=NOW + 120)
    assert gen.next_action(state, 'r1', 'r1', None, NOW + 119) is None
    assert gen.next_action(state, 'r1', 'r1', None, NOW + 120) == ('stop', 'r1')


def test_an_expired_or_finished_run_is_not_restarted(gen):
    # A restarted generator never starts a run whose deadline has passed...
    assert gen.next_action(run_state(ends_at=NOW - 1), None, None, None, NOW) is None
    # ...and a run it already produced is not started again while the API catches up.
    assert gen.next_action(run_state(), None, 'r1', None, NOW) is None


def test_generator_acknowledges_once_it_has_stopped(gen):
    for status in ('stopping', 'cleaning'):
        state = run_state(status)
        assert gen.next_action(state, None, 'r1', None, NOW) == ('ack', 'r1')
        assert gen.next_action(state, None, 'r1', 'r1', NOW) is None
    # Never acknowledge while still producing: the stop comes first.
    assert gen.next_action(run_state('stopping'), 'r1', 'r1', None, NOW) == ('stop', 'r1')


def test_control_loop_produces_then_stops_and_acknowledges(gen, capsys):
    async def scenario():
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        g = gen.TelemetryGenerator(stdout=True)
        control = gen.ControlState()
        state = {
            'run_id': 'abc',
            'status': 'running',
            'config': {'vehicles': 4, 'interval_seconds': 0.05, 'chaos_percent': 0},
            'ends_at': time.time() + 60,
        }

        await redis.set(gen.SIMULATION_STATE_KEY, json.dumps(state))
        await g.control_step(redis, control)
        assert g.run_id == 'abc' and len(g.vehicles) == 4

        await asyncio.sleep(0.3)
        assert await redis.get(gen.GENERATOR_ACK_KEY) is None  # still producing

        await redis.set(gen.SIMULATION_STATE_KEY, json.dumps({**state, 'status': 'stopping'}))
        await g.control_step(redis, control)
        assert g.run_id is None
        assert await redis.get(gen.GENERATOR_ACK_KEY) == 'abc'

        produced = g.total_messages
        await asyncio.sleep(0.2)
        assert g.total_messages == produced  # nothing leaks out after the stop

    asyncio.run(scenario())
    lines = [line for line in capsys.readouterr().out.splitlines() if line]
    assert len(lines) > 4
    assert {json.loads(line)['vehicle_id'] for line in lines} <= {f'v-00{i}' for i in range(1, 5)}


def test_invalid_run_config_is_ignored_and_still_acknowledged(gen):
    async def scenario():
        redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        g = gen.TelemetryGenerator(stdout=True)
        control = gen.ControlState()
        bad = {
            'run_id': 'bad',
            'status': 'running',
            'config': {'vehicles': 0, 'interval_seconds': 1, 'chaos_percent': 0},
            'ends_at': time.time() + 60,
        }
        await redis.set(gen.SIMULATION_STATE_KEY, json.dumps(bad))
        await g.control_step(redis, control)
        assert g.run_id is None and g.tasks == []

        await redis.set(gen.SIMULATION_STATE_KEY, json.dumps({**bad, 'status': 'stopping'}))
        await g.control_step(redis, control)
        assert await redis.get(gen.GENERATOR_ACK_KEY) == 'bad'

    asyncio.run(scenario())
