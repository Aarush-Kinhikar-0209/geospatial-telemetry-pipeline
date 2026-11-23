#!/usr/bin/env python3
"""
Phase 2: Synthetic Telemetry Generator

Simulates a fleet of delivery vehicles moving on land inside Mumbai,
generating JSON telemetry, and streaming it into Redpanda (Kafka-compatible).

Usage:
    python main.py                 # Stream to Redpanda (Kafka)
    python main.py --stdout        # Dry run: print JSON to stdout only
"""

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import datetime, timezone

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

try:  # python-dotenv is optional; env vars can be set directly in the shell
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - fallback when dotenv is missing
    pass

# ---------------------------------------------------------------------------
# Configuration (via environment variables)
# ---------------------------------------------------------------------------
VEHICLE_COUNT: int = int(os.getenv('VEHICLE_COUNT', '200'))
CHAOS_PERCENTAGE: float = float(os.getenv('CHAOS_PERCENTAGE', '5.0'))
BOOTSTRAP_SERVERS: str = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC: str = os.getenv('TOPIC', 'telemetry_raw')
MESSAGE_INTERVAL: float = float(os.getenv('MESSAGE_INTERVAL', '0.2'))
METRICS_INTERVAL: float = 10.0

# Mumbai city, traced inland of the Arabian Sea on the west and Mumbai Harbour /
# Thane Creek on the east. A rectangle around the city covers open water; this
# polygon does not. Vertices are (lat, lon), ordered around the boundary.
MUMBAI_LAND: list[tuple[float, float]] = [
    (18.912, 72.830),  # Colaba
    (18.948, 72.828),  # Marine Drive
    (19.005, 72.826),  # Worli
    (19.060, 72.832),  # Bandra
    (19.110, 72.836),  # Juhu
    (19.160, 72.836),  # Versova
    (19.210, 72.842),  # Malad
    (19.252, 72.858),  # Dahisar
    (19.248, 72.910),  # northern suburbs
    (19.175, 72.940),  # Mulund, west of the creek
    (19.085, 72.905),  # Ghatkopar
    (19.035, 72.872),  # Sion
    (18.975, 72.848),  # Parel
    (18.925, 72.838),  # Fort
]
LAT_MIN: float = min(lat for lat, _ in MUMBAI_LAND)
LAT_MAX: float = max(lat for lat, _ in MUMBAI_LAND)
LON_MIN: float = min(lon for _, lon in MUMBAI_LAND)
LON_MAX: float = max(lon for _, lon in MUMBAI_LAND)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] generator: %(message)s',
)
logger = logging.getLogger('generator')


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------
class TelemetryPayload(BaseModel):
    """
    Clean telemetry payload conforming to the pipeline schema.

    Kept in sync with processor/main.py, which enforces the same contract.
    """

    model_config = ConfigDict(strict=True)

    vehicle_id: str = Field(min_length=1)
    timestamp: AwareDatetime
    lat: float = Field(ge=-90, le=90, allow_inf_nan=False)
    lon: float = Field(ge=-180, le=180, allow_inf_nan=False)
    speed_kmh: float = Field(ge=0, le=300, allow_inf_nan=False)
    battery_pct: int = Field(ge=0, le=100)


def validate_config() -> None:
    """Fail fast on nonsensical settings."""
    if VEHICLE_COUNT < 1:
        raise ValueError(f'VEHICLE_COUNT must be >= 1, got {VEHICLE_COUNT}')
    if MESSAGE_INTERVAL <= 0:
        raise ValueError(f'MESSAGE_INTERVAL must be > 0, got {MESSAGE_INTERVAL}')
    if not 0 <= CHAOS_PERCENTAGE <= 100:
        raise ValueError(f'CHAOS_PERCENTAGE must be within 0-100, got {CHAOS_PERCENTAGE}')
    if not 2 <= CHAOS_PERCENTAGE <= 5:
        logger.warning(
            'CHAOS_PERCENTAGE=%.1f is outside the 2-5%% range the pipeline is specified for',
            CHAOS_PERCENTAGE,
        )


def on_land(lat: float, lon: float) -> bool:
    """True when the point is inside MUMBAI_LAND (ray cast along longitude)."""
    inside = False
    previous = MUMBAI_LAND[-1]
    for vertex in MUMBAI_LAND:
        lat_i, lon_i = vertex
        lat_j, lon_j = previous
        if (lat_i > lat) != (lat_j > lat):
            crossing = (lon_j - lon_i) * (lat - lat_i) / (lat_j - lat_i) + lon_i
            if lon < crossing:
                inside = not inside
        previous = vertex
    return inside


def random_land_point() -> tuple[float, float]:
    """A uniformly chosen point inside the land polygon."""
    for _ in range(10_000):
        lat = random.uniform(LAT_MIN, LAT_MAX)
        lon = random.uniform(LON_MIN, LON_MAX)
        if on_land(lat, lon):
            return lat, lon
    raise RuntimeError('failed to place a point inside the Mumbai land polygon')


def chaos_block() -> list[bool]:
    """
    A shuffled block of messages in which exactly CHAOS_PERCENTAGE are chaos.

    Exact per-block counts keep the observed rate verifiable over any window.
    Shuffling matters: vehicles are scheduled round-robin, so a fixed "every
    Nth message" pattern would keep corrupting the same vehicles.
    """
    for size in (100, 1_000, 10_000):
        chaos = CHAOS_PERCENTAGE * size / 100
        if chaos == int(chaos):
            break
    chaos = round(chaos)
    block = [True] * chaos + [False] * (size - chaos)
    random.shuffle(block)
    return block


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
class TelemetryGenerator:
    """Produces synthetic vehicle telemetry, optionally injecting chaos."""

    def __init__(self, stdout: bool = False) -> None:
        self.stdout = stdout
        self.running = True
        self.messages_sent: int = 0
        self.chaos_sent: int = 0
        self.total_messages: int = 0
        self.chaos_schedule: list[bool] = []
        self.vehicles: dict[str, dict] = {}
        self.producer = None

    # -- lifecycle ----------------------------------------------------------

    def initialize_vehicles(self) -> None:
        """Place each vehicle at a random position on land inside Mumbai."""
        for i in range(VEHICLE_COUNT):
            vid = f'v-{i + 1:03d}'
            lat, lon = random_land_point()
            self.vehicles[vid] = {
                'lat': lat,
                'lon': lon,
                'speed_kmh': random.uniform(0, 80),
                'battery_pct': random.randint(20, 100),
            }

    # -- payload generation -------------------------------------------------

    def generate_payload(self, vehicle_id: str, state: dict) -> dict:
        """Advance a vehicle's random walk and build a clean payload."""
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Random walk that stays inside the land polygon. A step into the sea
        # is discarded, so the vehicle holds its last on-land position.
        lat = state['lat'] + random.uniform(-0.005, 0.005)
        lon = state['lon'] + random.uniform(-0.005, 0.005)
        if on_land(lat, lon):
            state['lat'] = lat
            state['lon'] = lon
        state['speed_kmh'] = max(0, min(120, state['speed_kmh'] + random.uniform(-5, 5)))
        state['battery_pct'] = max(0, min(100, state['battery_pct'] + random.randint(-1, 1)))

        return {
            'vehicle_id': vehicle_id,
            'timestamp': now,
            'lat': round(state['lat'], 6),
            'lon': round(state['lon'], 6),
            'speed_kmh': round(state['speed_kmh'], 2),
            'battery_pct': state['battery_pct'],
        }

    def inject_chaos(self, payload: dict) -> tuple:
        """
        Corrupt exactly CHAOS_PERCENTAGE of payloads.

        Returns (possibly-corrupted payload, is_chaos).
        """
        if not self.chaos_schedule:
            self.chaos_schedule = chaos_block()
        if not self.chaos_schedule.pop():
            return payload, False

        corrupted = dict(payload)
        chaos_type = random.choice(
            [
                'speed_as_string',
                'missing_timestamp',
                'missing_coords',
                'negative_speed',
                'missing_battery',
            ]
        )

        if chaos_type == 'speed_as_string':
            corrupted['speed_kmh'] = 'fast'
        elif chaos_type == 'missing_timestamp':
            del corrupted['timestamp']
        elif chaos_type == 'missing_coords':
            del corrupted['lat']
            del corrupted['lon']
        elif chaos_type == 'negative_speed':
            # Strictly negative: a stationary vehicle would otherwise yield -0.0.
            corrupted['speed_kmh'] = -round(random.uniform(1, 120), 2)
        elif chaos_type == 'missing_battery':
            del corrupted['battery_pct']

        return corrupted, True

    def serialize(self, payload: dict, is_chaos: bool) -> str:
        """Serialize payload to JSON, occasionally emitting malformed JSON."""
        if is_chaos and random.random() < 0.3:
            return '{broken json'
        return json.dumps(payload)

    # -- production ---------------------------------------------------------

    async def produce(self, payload: dict, is_chaos: bool) -> None:
        message = self.serialize(payload, is_chaos)
        if self.stdout:
            print(message)
        else:
            assert self.producer is not None
            await self.producer.send(TOPIC, message.encode('utf-8'))

        self.messages_sent += 1
        self.total_messages += 1
        if is_chaos:
            self.chaos_sent += 1

    async def vehicle_loop(self, vehicle_id: str) -> None:
        state = self.vehicles[vehicle_id]
        while self.running:
            payload = self.generate_payload(vehicle_id, state)
            corrupted, is_chaos = self.inject_chaos(payload)
            try:
                await self.produce(corrupted, is_chaos)
            except Exception as exc:  # keep the vehicle alive across broker hiccups
                logger.warning('Failed to publish for %s: %s', vehicle_id, exc)
            await asyncio.sleep(MESSAGE_INTERVAL)

    async def metrics_loop(self) -> None:
        loop = asyncio.get_running_loop()
        last_metrics_time = loop.time()
        while self.running:
            await asyncio.sleep(METRICS_INTERVAL)
            if not self.running:
                break
            now = loop.time()
            elapsed = now - last_metrics_time
            rate = self.messages_sent / elapsed if elapsed > 0 else 0
            chaos_rate = (
                (self.chaos_sent / self.messages_sent * 100) if self.messages_sent > 0 else 0
            )
            logger.info(
                'window=%ds rate=%.1f msg/sec total=%d chaos=%d (%.1f%%)',
                int(elapsed),
                rate,
                self.total_messages,
                self.chaos_sent,
                chaos_rate,
            )
            self.messages_sent = 0
            self.chaos_sent = 0
            last_metrics_time = now

    # -- entry point --------------------------------------------------------

    async def run(self) -> None:
        validate_config()
        self.initialize_vehicles()

        if not self.stdout:
            # Import lazily so --stdout mode does not require aiokafka.
            from aiokafka import AIOKafkaProducer

            self.producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            await self.producer.start()
            logger.info('Connected to Redpanda at %s', BOOTSTRAP_SERVERS)

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def signal_handler() -> None:
            logger.info('Shutdown signal received, stopping generator...')
            self.running = False
            stop_event.set()

        # Signal handlers are not supported on Windows asyncio loops.
        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)

        tasks = [asyncio.create_task(self.vehicle_loop(vid)) for vid in self.vehicles]
        tasks.append(asyncio.create_task(self.metrics_loop()))

        await stop_event.wait()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        if self.producer is not None:
            await self.producer.stop()

        logger.info('Generator stopped gracefully. Total messages: %d', self.total_messages)


def main() -> None:
    parser = argparse.ArgumentParser(description='Synthetic Telemetry Generator')
    parser.add_argument(
        '--stdout',
        action='store_true',
        help='Print JSON to stdout instead of publishing to Kafka',
    )
    args = parser.parse_args()

    generator = TelemetryGenerator(stdout=args.stdout)

    try:
        asyncio.run(generator.run())
    except KeyboardInterrupt:  # pragma: no cover - handled by signal handler
        pass


if __name__ == '__main__':
    main()
