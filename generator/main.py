#!/usr/bin/env python3
"""
Phase 2: Synthetic Telemetry Generator

Simulates a fleet of delivery vehicles moving on land inside Mumbai,
generating JSON telemetry, and streaming it into Redpanda (Kafka-compatible).
Vehicles drive at city speeds. Each ping moves a vehicle by its speed times the
elapsed time, scaled by MAP_DISTANCE_SCALE so hops are visible on the map.

The generator is a controllable service. It stays idle until the API starts a
simulation by writing the run (vehicle count, interval, chaos rate, deadline)
to the Redis key `simulation:state`; it stops when that run ends, flushes what
it produced, and acknowledges in `simulation:generator_ack` so the pipeline
can be cleared safely.

Usage:
    python main.py                 # Service: wait for simulations started through the API
    python main.py --standalone    # Stream continuously, configured by VEHICLE_COUNT etc.
    python main.py --stdout        # Dry run: print JSON to stdout only (no Kafka, no Redis)
"""

import argparse
import asyncio
import contextlib
import json
import logging
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
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
BOOTSTRAP_SERVERS: str = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC: str = os.getenv('TOPIC', 'telemetry_raw')
REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
METRICS_INTERVAL: float = 10.0
CONTROL_POLL_SECONDS: float = float(os.getenv('CONTROL_POLL_SECONDS', '0.25'))

# Control plane keys, shared with api/main.py and processor/main.py.
SIMULATION_STATE_KEY = 'simulation:state'
GENERATOR_ACK_KEY = 'simulation:generator_ack'

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


# ---------------------------------------------------------------------------
# Run configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RunConfig:
    """What one simulation produces."""

    vehicles: int
    interval: float  # seconds between messages from one vehicle
    chaos_percent: float

    @classmethod
    def from_env(cls) -> 'RunConfig':
        """Standalone mode: settings come from the environment."""
        return cls(
            vehicles=int(os.getenv('VEHICLE_COUNT', '200')),
            interval=float(os.getenv('MESSAGE_INTERVAL', '0.2')),
            chaos_percent=float(os.getenv('CHAOS_PERCENTAGE', '5.0')),
        )

    @classmethod
    def from_state(cls, state: dict) -> 'RunConfig':
        """Service mode: settings come from the API's simulation state."""
        config = state['config']
        return cls(
            vehicles=int(config['vehicles']),
            interval=float(config['interval_seconds']),
            chaos_percent=float(config['chaos_percent']),
        )

    def validate(self) -> None:
        """Fail fast on nonsensical settings (the API enforces the user-facing limits)."""
        if self.vehicles < 1:
            raise ValueError(f'vehicles must be >= 1, got {self.vehicles}')
        if self.interval <= 0:
            raise ValueError(f'interval must be > 0, got {self.interval}')
        if not 0 <= self.chaos_percent <= 100:
            raise ValueError(f'chaos_percent must be within 0-100, got {self.chaos_percent}')
        if self.chaos_percent > 5:
            logger.warning(
                'chaos_percent=%.1f is outside the 0-5%% range the pipeline is specified for',
                self.chaos_percent,
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


# ---------------------------------------------------------------------------
# Vehicle motion
#
# A vehicle's speed and the distance between its pings agree: each step moves
# it `speed x elapsed time`, so a 0.2 s interval gives centimetre-to-metre hops
# and a 2 s interval gives several metres, from the same speed.
# ---------------------------------------------------------------------------
METERS_PER_DEG_LAT: float = 111_320.0
# Presentation only: each ping moves the vehicle this many times its true distance
# (speed x time), so hops are visible on a city-wide map. The speed a vehicle
# reports is not scaled, and battery drain uses the true distance.
MAP_DISTANCE_SCALE: float = 10.0
ACCEL_MS2: float = 1.5  # pulling away in traffic
BRAKE_MS2: float = 3.0  # braking is firmer than accelerating
TARGET_CHANGE_PER_S: float = 0.1  # a driver picks a new target speed every ~10 s
TURN_STD_DEG_PER_SQRT_S: float = 12.0  # heading wander; grows with sqrt(time), not per ping
BATTERY_PCT_PER_KM: float = 0.4
BATTERY_PCT_PER_HOUR: float = 1.0  # electronics draw while parked or crawling


def pick_target_speed() -> float:
    """Speed (km/h) a driver settles at next: mostly congested Mumbai streets."""
    roll = random.random()
    if roll < 0.15:
        return 0.0  # waiting at a signal or a pickup
    if roll < 0.85:
        return random.uniform(5, 25)  # congested streets
    return random.uniform(25, 50)  # open roads and flyovers


def advance_vehicle(state: dict, dt: float) -> None:
    """
    Move a vehicle forward `dt` seconds, in place.

    Speed eases toward a target (acceleration limited), the heading drifts, and
    the true distance covered is the mean speed over the step times `dt`; the map
    moves the vehicle MAP_DISTANCE_SCALE times that far. A step that
    would leave the land polygon is retried after turning around or sideways; if
    every try fails the vehicle brakes to a halt where it is.
    """
    if dt <= 0:
        return

    if random.random() < 1 - math.exp(-TARGET_CHANGE_PER_S * dt):
        state['target_kmh'] = pick_target_speed()

    old = state['speed_kmh'] / 3.6  # m/s
    target = state['target_kmh'] / 3.6
    if target > old:
        new = min(target, old + ACCEL_MS2 * dt)
    else:
        new = max(target, old - BRAKE_MS2 * dt)
    distance = (old + new) / 2 * dt

    if distance > 0:
        state['heading_deg'] = (
            state['heading_deg'] + random.gauss(0, TURN_STD_DEG_PER_SQRT_S * math.sqrt(dt))
        ) % 360
        ahead = state['heading_deg']
        turn_around = (ahead + 180 + random.uniform(-45, 45)) % 360
        step = distance * MAP_DISTANCE_SCALE
        for heading in (ahead, turn_around, (ahead + 90) % 360, (ahead - 90) % 360):
            radians = math.radians(heading)
            lat = state['lat'] + step * math.cos(radians) / METERS_PER_DEG_LAT
            lon = state['lon'] + step * math.sin(radians) / (
                METERS_PER_DEG_LAT * math.cos(math.radians(state['lat']))
            )
            if on_land(lat, lon):
                state['lat'], state['lon'], state['heading_deg'] = lat, lon, heading
                break
        else:
            new, distance = max(0.0, old - BRAKE_MS2 * dt), 0.0  # boxed in: brake, stay put

    state['speed_kmh'] = new * 3.6
    drain = BATTERY_PCT_PER_KM * distance / 1000 + BATTERY_PCT_PER_HOUR * dt / 3600
    state['battery_pct'] = max(0.0, state['battery_pct'] - drain)


def chaos_block(chaos_percent: float) -> list[bool]:
    """
    A shuffled block of messages in which exactly `chaos_percent` are chaos.

    Exact per-block counts keep the observed rate verifiable over any window.
    Shuffling matters: vehicles are scheduled round-robin, so a fixed "every
    Nth message" pattern would keep corrupting the same vehicles.
    """
    for size in (100, 1_000, 10_000):
        chaos = chaos_percent * size / 100
        if chaos == int(chaos):
            break
    chaos = round(chaos)
    block = [True] * chaos + [False] * (size - chaos)
    random.shuffle(block)
    return block


# ---------------------------------------------------------------------------
# Control plane decisions (pure, so they are unit tested without Redis)
# ---------------------------------------------------------------------------
@dataclass
class ControlState:
    """What the control loop remembers between polls."""

    state: dict | None = None  # last simulation state read from Redis
    last_started: str | None = None  # run this process already started (never restart it)
    acked: str | None = None  # run this process already acknowledged as stopped


def next_action(
    state: dict | None,
    active_run: str | None,
    last_started: str | None,
    acked: str | None,
    now: float,
) -> tuple[str, str] | None:
    """
    Decide what the generator must do next: ('start' | 'stop' | 'ack', run_id).

    A run produces only while its state is `running` and its deadline has not
    passed, so the generator also stops on its own if the API disappears. The
    ack tells the API production has stopped and been flushed.
    """
    status = state.get('status') if state else None
    run_id = state.get('run_id') if state else None
    live = status == 'running' and now < float(state.get('ends_at', 0))

    if active_run is not None:
        return None if live and run_id == active_run else ('stop', active_run)
    if live and run_id != last_started:
        return ('start', run_id)
    if status in ('stopping', 'cleaning') and run_id != acked:
        return ('ack', run_id)
    return None


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
class TelemetryGenerator:
    """Produces synthetic vehicle telemetry, optionally injecting chaos."""

    def __init__(self, config: RunConfig | None = None, stdout: bool = False) -> None:
        self.stdout = stdout
        self.config: RunConfig | None = None
        self.run_id: str | None = None  # simulation currently being produced (service mode)
        self.messages_sent: int = 0
        self.chaos_sent: int = 0
        self.total_messages: int = 0
        self.chaos_schedule: list[bool] = []
        self.vehicles: dict[str, dict] = {}
        self.tasks: list[asyncio.Task] = []
        self.producer = None
        if config is not None:
            self.configure(config)

    # -- lifecycle ----------------------------------------------------------

    def configure(self, config: RunConfig) -> None:
        """Prepare a fresh run: new fleet, new chaos schedule, zeroed counters."""
        config.validate()
        self.config = config
        self.messages_sent = self.chaos_sent = self.total_messages = 0
        self.chaos_schedule = []
        self.vehicles = {}
        self.initialize_vehicles()

    def initialize_vehicles(self) -> None:
        """Place each vehicle at a random position on land inside Mumbai."""
        assert self.config is not None
        for i in range(self.config.vehicles):
            vid = f'v-{i + 1:03d}'
            lat, lon = random_land_point()
            target = pick_target_speed()
            self.vehicles[vid] = {
                'lat': lat,
                'lon': lon,
                'heading_deg': random.uniform(0, 360),
                'speed_kmh': target,
                'target_kmh': target,
                'battery_pct': float(random.randint(10, 100)),
            }

    def start_tasks(self) -> None:
        self.tasks = [asyncio.create_task(self.vehicle_loop(vid)) for vid in self.vehicles]
        self.tasks.append(asyncio.create_task(self.metrics_loop()))

    async def stop_tasks(self) -> None:
        """Stop producing, then flush so every message sent is in the broker."""
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        if self.producer is not None:
            try:
                await self.producer.flush()
            except Exception as exc:
                logger.warning('Flush after stop failed: %s', exc)

    async def start_producer(self) -> None:
        if self.stdout:
            return
        # Import lazily so --stdout mode does not require aiokafka.
        from aiokafka import AIOKafkaProducer

        self.producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        await self.producer.start()
        logger.info('Connected to Redpanda at %s', BOOTSTRAP_SERVERS)

    async def stop_producer(self) -> None:
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None

    # -- payload generation -------------------------------------------------

    def generate_payload(self, vehicle_id: str, state: dict, dt: float | None = None) -> dict:
        """
        Advance a vehicle by `dt` seconds (default: the message interval) and
        build a clean payload for where it ended up.
        """
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        if dt is None:
            assert self.config is not None
            dt = self.config.interval
        advance_vehicle(state, dt)

        return {
            'vehicle_id': vehicle_id,
            'timestamp': now,
            'lat': round(state['lat'], 6),
            'lon': round(state['lon'], 6),
            'speed_kmh': round(state['speed_kmh'], 2),
            'battery_pct': round(state['battery_pct']),
        }

    def inject_chaos(self, payload: dict) -> tuple:
        """
        Corrupt exactly the configured percentage of payloads.

        Returns (possibly-corrupted payload, is_chaos).
        """
        assert self.config is not None
        if not self.chaos_schedule:
            self.chaos_schedule = chaos_block(self.config.chaos_percent)
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
        assert self.config is not None
        interval = self.config.interval
        state = self.vehicles[vehicle_id]
        # Spread the vehicles across one interval so messages arrive evenly
        # instead of in a burst from the whole fleet every `interval` seconds.
        loop = asyncio.get_running_loop()
        last = loop.time()
        await asyncio.sleep(random.uniform(0, interval))
        while True:
            # Move by the time that really passed, so a slow event loop shows up
            # as a longer hop, not as a vehicle that is slower than its speed.
            now = loop.time()
            payload = self.generate_payload(vehicle_id, state, dt=now - last)
            last = now
            corrupted, is_chaos = self.inject_chaos(payload)
            try:
                await self.produce(corrupted, is_chaos)
            except Exception as exc:  # keep the vehicle alive across broker hiccups
                logger.warning('Failed to publish for %s: %s', vehicle_id, exc)
            await asyncio.sleep(interval)

    async def metrics_loop(self) -> None:
        loop = asyncio.get_running_loop()
        last_metrics_time = loop.time()
        while True:
            await asyncio.sleep(METRICS_INTERVAL)
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

    # -- service mode: driven by the API through Redis ----------------------

    async def control_step(self, redis, control: ControlState) -> None:
        """Poll the simulation state once and start, stop, or acknowledge a run."""
        try:
            raw = await redis.get(SIMULATION_STATE_KEY)
            control.state = json.loads(raw) if raw else None
        except Exception as exc:
            # Redis unavailable: keep acting on the last known state. A run still
            # ends at its deadline, so a lost API cannot leave the generator running.
            logger.warning('Could not read simulation state: %s', exc)

        # Each action changes the inputs of the next decision, so this settles
        # in at most a stop followed by an ack.
        for _ in range(3):
            action = next_action(
                control.state, self.run_id, control.last_started, control.acked, time.time()
            )
            if action is None:
                return
            kind, run_id = action
            if kind == 'start':
                self.begin_run(run_id, control)
            elif kind == 'stop':
                await self.end_run()
            else:
                try:
                    await redis.set(GENERATOR_ACK_KEY, run_id)
                except Exception as exc:
                    logger.warning('Could not acknowledge run %s: %s', run_id, exc)
                    return
                control.acked = run_id
                logger.info('Run %s stopped and flushed', run_id)

    def begin_run(self, run_id: str, control: ControlState) -> None:
        control.last_started = run_id  # never retry a run, even one that fails to start
        assert control.state is not None
        try:
            self.configure(RunConfig.from_state(control.state))
        except (KeyError, TypeError, ValueError) as exc:
            logger.error('Ignoring run %s: invalid configuration (%s)', run_id, exc)
            return
        self.run_id = run_id
        self.start_tasks()
        assert self.config is not None
        logger.info(
            'Run %s started: %d vehicles, %.2fs interval, %s%% chaos',
            run_id,
            self.config.vehicles,
            self.config.interval,
            f'{self.config.chaos_percent:g}',
        )

    async def end_run(self) -> None:
        run_id = self.run_id
        await self.stop_tasks()
        self.run_id = None
        logger.info('Run %s ended after %d messages', run_id, self.total_messages)

    async def serve(self) -> None:
        """Idle until the API starts a simulation; produce until it ends."""
        from redis.asyncio import Redis

        redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        await self.start_producer()
        stop_event = self.install_signal_handlers()
        control = ControlState()
        logger.info('Waiting for a simulation (control key %s)', SIMULATION_STATE_KEY)
        try:
            while not stop_event.is_set():
                await self.control_step(redis, control)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=CONTROL_POLL_SECONDS)
        finally:
            await self.stop_tasks()
            await redis.aclose()
            await self.stop_producer()
            logger.info('Generator stopped gracefully.')

    # -- standalone mode: environment-configured, runs until interrupted ----

    def install_signal_handlers(self) -> asyncio.Event:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def signal_handler() -> None:
            logger.info('Shutdown signal received, stopping generator...')
            stop_event.set()

        # Signal handlers are not supported on Windows asyncio loops.
        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)
        return stop_event

    async def run(self) -> None:
        self.configure(self.config or RunConfig.from_env())
        await self.start_producer()
        stop_event = self.install_signal_handlers()

        self.start_tasks()
        await stop_event.wait()
        await self.stop_tasks()
        await self.stop_producer()

        logger.info('Generator stopped gracefully. Total messages: %d', self.total_messages)


def main() -> None:
    parser = argparse.ArgumentParser(description='Synthetic Telemetry Generator')
    parser.add_argument(
        '--stdout',
        action='store_true',
        help='Print JSON to stdout instead of publishing to Kafka (implies --standalone)',
    )
    parser.add_argument(
        '--standalone',
        action='store_true',
        help='Stream continuously using VEHICLE_COUNT, MESSAGE_INTERVAL and CHAOS_PERCENTAGE '
        'instead of waiting for the API to start simulations',
    )
    args = parser.parse_args()

    generator = TelemetryGenerator(stdout=args.stdout)
    entrypoint = generator.run if args.stdout or args.standalone else generator.serve

    try:
        asyncio.run(entrypoint())
    except KeyboardInterrupt:  # pragma: no cover - handled by signal handler
        pass


if __name__ == '__main__':
    main()
