#!/usr/bin/env python3
"""
Local demo: Generator -> Processor transformation logic (no Docker needed).

Runs 1 vehicle at a 2-second interval with 5% chaos and feeds each message
through the processor's real classify() function and tumbling windows,
printing how every record is handled: valid records update the would-be
Redis state and the window; invalid ones are routed to the DLQ.

Usage (from the repository root, with requirements-dev.txt installed):
    python processor/demo_local.py
"""

import importlib.util
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen_mod = load_module('generator_main', ROOT / 'generator' / 'main.py')
proc_mod = load_module('processor_main', ROOT / 'processor' / 'main.py')
gen_mod.VEHICLE_COUNT = 1
gen_mod.CHAOS_PERCENTAGE = 5.0


def print_closed(windows, final: bool = False) -> None:
    for vehicle_id, start, state in windows.pop_closed(time.time(), final=final):
        print(
            f'  >> WINDOW CLOSED vehicle={vehicle_id} start={start:%H:%M:%S} '
            f'avg={state.speed_sum / state.count:.2f}km/h n={state.count} '
            f'(-> ClickHouse speed_rollups)'
        )


def main() -> None:
    gen = gen_mod.TelemetryGenerator(stdout=True)
    gen.initialize_vehicles()
    windows = proc_mod.TumblingWindows()
    processed = dlq = 0

    print('=' * 60)
    print('LOCAL DEMO: 1 vehicle, 2s interval, 5% chaos. Ctrl+C to stop')
    print('=' * 60)

    try:
        while True:
            vid, state = next(iter(gen.vehicles.items()))
            corrupted, is_chaos = gen.inject_chaos(gen.generate_payload(vid, state))
            message = gen.serialize(corrupted, is_chaos)
            processed += 1

            print(f'\n[#{processed}] RAW: {message}')
            event, reason = proc_mod.classify(message)
            if event is None:
                dlq += 1
                print(f'  -> DLQ ({reason})')
            else:
                windows.add(event)
                p = event.payload
                print(
                    f'  -> VALID speed={p.speed_kmh}km/h batt={p.battery_pct}% '
                    f'H3={event.h3_index} window={event.window_start:%H:%M:%S}'
                )
                print(f'     REDIS vehicle:{p.vehicle_id} updated')
            print_closed(windows)
            time.sleep(2)
    except KeyboardInterrupt:
        print('\n' + '=' * 60)
        print_closed(windows, final=True)
        print(f'Total processed: {processed}  DLQ: {dlq}')


if __name__ == '__main__':
    main()
