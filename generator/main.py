#!/usr/bin/env python3
"""
Phase 2: Synthetic Telemetry Generator

Simulates a fleet of delivery vehicles moving within a bounding box,
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

from pydantic import BaseModel

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

# Bounding box around Mumbai (matches the example payload in the wiki).
LAT_MIN: float = 18.80
LAT_MAX: float = 19.10
LON_MIN: float = 72.70
LON_MAX: float = 73.00

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] generator: %(message)s',
)
logger = logging.getLogger('generator')


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------
class TelemetryPayload(BaseModel):
    """Clean telemetry payload conforming to the pipeline schema."""

    vehicle_id: str
    timestamp: str
    lat: float
    lon: float
    speed_kmh: float
    battery_pct: int


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
        self.last_metrics_time: float | None = None
        self.vehicles: dict[str, dict] = {}
        self.producer = None

    # -- lifecycle ----------------------------------------------------------

    def initialize_vehicles(self) -> None:
        """Place each vehicle at a random position inside the bounding box."""
        for i in range(VEHICLE_COUNT):
            vid = f'v-{i + 1:03d}'
            self.vehicles[vid] = {
                'lat': random.uniform(LAT_MIN, LAT_MAX),
                'lon': random.uniform(LON_MIN, LON_MAX),
                'speed_kmh': random.uniform(0, 80),
                'battery_pct': random.randint(20, 100),
            }

    # -- payload generation -------------------------------------------------

    def generate_payload(self, vehicle_id: str, state: dict) -> dict:
        """Advance a vehicle's random walk and build a clean payload."""
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Bounding-box random walk.
        state['lat'] = max(LAT_MIN, min(LAT_MAX, state['lat'] + random.uniform(-0.005, 0.005)))
        state['lon'] = max(LON_MIN, min(LON_MAX, state['lon'] + random.uniform(-0.005, 0.005)))
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
        Corrupt roughly CHAOS_PERCENTAGE of payloads.

        Returns (possibly-corrupted payload, is_chaos).
        """
        if random.random() * 100 >= CHAOS_PERCENTAGE:
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
            corrupted['speed_kmh'] = -abs(corrupted['speed_kmh'])  # type: ignore[assignment]
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
            await self.produce(corrupted, is_chaos)
            await asyncio.sleep(MESSAGE_INTERVAL)

    async def metrics_loop(self) -> None:
        while self.running:
            await asyncio.sleep(10)
            if not self.running:
                break
            now = asyncio.get_event_loop().time()
            if self.last_metrics_time is None:
                self.last_metrics_time = now
                continue
            elapsed = now - self.last_metrics_time
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
            self.last_metrics_time = now

    # -- entry point --------------------------------------------------------

    async def run(self) -> None:
        self.initialize_vehicles()

        if not self.stdout:
            # Import lazily so --stdout mode does not require aiokafka.
            from aiokafka import AIOKafkaProducer

            self.producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            await self.producer.start()
            logger.info('Connected to Redpanda at %s', BOOTSTRAP_SERVERS)

        loop = asyncio.get_event_loop()
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
