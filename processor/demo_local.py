#!/usr/bin/env python3
"""
Local end-to-end demo: Generator -> Processor (no Docker needed).

Runs 1 vehicle at a 2-second interval with 5% chaos, feeding events
directly into the StreamProcessor logic. Shows how the processor
handles each event: valid records go to Redis state + tumbling window;
invalid records go to the DLQ.

Usage:
    # Activate the processor virtual environment first:
    cd processor
    .venv\\Scripts\\activate      # PowerShell
    source .venv/bin/activate    # bash/zsh
    python demo_local.py

    # Or run directly with the venv python:
    .venv\\Scripts\\python.exe demo_local.py  # PowerShell
    .venv/bin/python demo_local.py           # bash/zsh

"""

import asyncio
import importlib.util
import sys

# Ensure we are running inside the processor virtual environment.
# The processor requires aiokafka, redis, clickhouse-driver, h3, pydantic.
if 'processor' not in sys.prefix.lower() and '.venv' not in sys.executable.lower():
    print(
        'WARNING: It is recommended to run this script from the processor '
        'virtual environment:\n'
        '  .venv\\Scripts\\python.exe demo_local.py   (PowerShell)\n'
        '  .venv/bin/python demo_local.py            (bash/zsh)\n',
        file=sys.stderr,
    )


def load_module(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Load generator module and override config
gen_mod = load_module('generator_main', '../generator/main.py')
gen_mod.VEHICLE_COUNT = 1
gen_mod.CHAOS_PERCENTAGE = 5.0
gen_mod.MESSAGE_INTERVAL = 2.0

# Load processor module
proc_mod = load_module('processor_main', 'main.py')

StreamProcessor = proc_mod.StreamProcessor
TelemetryGenerator = gen_mod.TelemetryGenerator


class DemoProcessor(StreamProcessor):
    """Processor subclass that prints handling decisions to the terminal."""

    async def process_message(self, raw: str) -> None:
        self.processed += 1
        print(f'\n{"=" * 60}')
        print(f'[#{self.processed}] RAW: {raw}')

        payload = self.validate(raw)
        if payload is None:
            self.dlq_count += 1
            print('  -> DLQ: schema violation, routing to telemetry_dlq topic')
            return

        h3_index = self.to_h3(payload.lat, payload.lon)
        self.latest_h3[payload.vehicle_id] = h3_index

        window_start = self.window_key(payload.timestamp)
        self.windows[payload.vehicle_id][window_start].append(payload.speed_kmh)

        await self.write_redis(payload.vehicle_id, payload, h3_index)

        print(
            f'  -> VALID: vehicle={payload.vehicle_id} '
            f'lat={payload.lat} lon={payload.lon} '
            f'speed={payload.speed_kmh}km/h batt={payload.battery_pct}%'
        )
        print(f'       H3={h3_index} window={window_start}')
        print(
            f'       REDIS vehicle:{payload.vehicle_id} '
            f'= {{h3_index:{h3_index}, speed_kmh:{payload.speed_kmh}, '
            f'battery_pct:{payload.battery_pct}, timestamp:{payload.timestamp}}}'
        )

        # Show current window state for this vehicle
        speeds = self.windows[payload.vehicle_id][window_start]
        avg = sum(speeds) / len(speeds)
        print(
            f'       WINDOW[{window_start}] speeds={speeds} '
            f'running_avg={avg:.2f}km/h (will flush to ClickHouse at checkpoint)'
        )


async def main() -> None:
    gen = TelemetryGenerator(stdout=False)
    gen.initialize_vehicles()

    proc = DemoProcessor()

    print('=' * 60)
    print('LOCAL DEMO: 1 vehicle, 2s interval, 5% chaos')
    print('=' * 60)
    print('Press Ctrl+C to stop\n')

    try:
        while True:
            vid = 'v-001'
            state = gen.vehicles[vid]
            payload = gen.generate_payload(vid, state)
            corrupted, is_chaos = gen.inject_chaos(payload)
            message = gen.serialize(corrupted, is_chaos)

            if is_chaos:
                print(f'\n[GENERATOR] CHAOS injected: {message}')
            await proc.process_message(message)

            await asyncio.sleep(2)
    except KeyboardInterrupt:
        print('\n\n' + '=' * 60)
        print('DEMO STOPPED')
        print(f'Total processed: {proc.processed}')
        print(f'DLQ count: {proc.dlq_count}')
        print(f'Windows pending flush: {sum(len(w) for w in proc.windows.values())}')
        print('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())
