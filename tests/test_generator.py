"""Phase 2: synthetic telemetry generator."""

import json
from collections import Counter

import pytest
from pydantic import ValidationError


def produce_batch(gen, n: int) -> list[tuple[str, str, bool]]:
    """Run the generator's payload pipeline n times (round-robin over vehicles), no broker."""
    g = gen.TelemetryGenerator(stdout=True)
    g.initialize_vehicles()
    vehicle_ids = list(g.vehicles)
    out = []
    for i in range(n):
        vid = vehicle_ids[i % len(vehicle_ids)]
        payload = g.generate_payload(vid, g.vehicles[vid])
        corrupted, is_chaos = g.inject_chaos(payload)
        out.append((vid, g.serialize(corrupted, is_chaos), is_chaos))
    return out


def test_clean_payloads_match_schema(gen):
    gen.CHAOS_PERCENTAGE = 0
    for _, message, is_chaos in produce_batch(gen, 2000):
        assert not is_chaos
        payload = gen.TelemetryPayload.model_validate_json(message)
        assert gen.LAT_MIN <= payload.lat <= gen.LAT_MAX
        assert gen.LON_MIN <= payload.lon <= gen.LON_MAX


def test_clean_payload_has_documented_shape(gen):
    gen.CHAOS_PERCENTAGE = 0
    _, message, _ = produce_batch(gen, 1)[0]
    obj = json.loads(message)
    assert set(obj) == {'vehicle_id', 'timestamp', 'lat', 'lon', 'speed_kmh', 'battery_pct'}
    assert obj['timestamp'].endswith('Z')


def test_every_chaos_payload_violates_schema(gen):
    gen.CHAOS_PERCENTAGE = 100
    for _, message, is_chaos in produce_batch(gen, 2000):
        assert is_chaos
        with pytest.raises(ValidationError):
            gen.TelemetryPayload.model_validate_json(message)


@pytest.mark.parametrize('pct', [2, 3.5, 5])
def test_chaos_rate_is_exact(gen, pct):
    gen.CHAOS_PERCENTAGE = pct
    n = 60_000  # one minute at 1000 msg/s
    chaos = sum(is_chaos for _, _, is_chaos in produce_batch(gen, n))
    assert chaos == round(n * pct / 100)


def test_chaos_is_spread_across_vehicles(gen):
    gen.CHAOS_PERCENTAGE = 5
    gen.VEHICLE_COUNT = 200
    chaos_per_vehicle = Counter()
    for vid, _, is_chaos in produce_batch(gen, 60_000):
        chaos_per_vehicle[vid] += is_chaos
    # 300 messages per vehicle, ~15 expected chaos: none all-clean, none all-chaos.
    assert len(chaos_per_vehicle) == 200
    assert all(1 <= count <= 60 for count in chaos_per_vehicle.values())


@pytest.mark.parametrize('pct', [-1, 101])
def test_invalid_chaos_percentage_rejected(gen, pct):
    gen.CHAOS_PERCENTAGE = pct
    with pytest.raises(ValueError):
        gen.validate_config()
