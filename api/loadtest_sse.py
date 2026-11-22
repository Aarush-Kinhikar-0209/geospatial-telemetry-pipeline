#!/usr/bin/env python3
"""
SSE load test: open N concurrent /api/v1/stream/vehicles connections and
report connection success, delivered updates, and push latency.

Latency = client receive time - `processed_at` of the update, i.e. from the
processor writing Redis to this client receiving it. Clients are spread over
several processes so the load generator itself does not become the bottleneck.

Usage (requires httpx):
    python api/loadtest_sse.py --clients 100 --seconds 30 --procs 4
"""

import argparse
import asyncio
import multiprocessing
import time
from datetime import datetime

import httpx

PROCESSED_AT = '"processed_at":"'


def update_latency_ms(data: str, received: float) -> float:
    # Every vehicle in one update shares the processor batch's processed_at.
    start = data.index(PROCESSED_AT) + len(PROCESSED_AT)
    processed_at = data[start : data.index('"', start)].replace('Z', '+00:00')
    return (received - datetime.fromisoformat(processed_at).timestamp()) * 1000


async def client(url: str, seconds: float, stats: dict) -> None:
    deadline = time.monotonic() + seconds
    event = None
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10, read=30)) as http:
            async with http.stream('GET', url) as response:
                response.raise_for_status()
                stats['connected'] += 1
                async for line in response.aiter_lines():
                    if time.monotonic() > deadline:
                        break
                    if line.startswith('event: '):
                        event = line[7:]
                    elif line.startswith('data: ') and event == 'update':
                        stats['latencies'].append(update_latency_ms(line, time.time()))
                        stats['updates'] += line.count('"vehicle_id"')
    except Exception as exc:
        stats['errors'].append(f'{type(exc).__name__}: {exc}')


def run_worker(url: str, clients: int, seconds: float, results) -> None:
    stats = {'connected': 0, 'updates': 0, 'latencies': [], 'errors': []}

    async def go():
        await asyncio.gather(*(client(url, seconds, stats) for _ in range(clients)))

    asyncio.run(go())
    results.put(stats)


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * pct))] if ordered else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument('--url', default='http://localhost:8000/api/v1/stream/vehicles')
    parser.add_argument('--clients', type=int, default=100)
    parser.add_argument('--seconds', type=float, default=30)
    parser.add_argument('--procs', type=int, default=4)
    args = parser.parse_args()

    results = multiprocessing.Queue()
    shares = [
        args.clients // args.procs + (i < args.clients % args.procs) for i in range(args.procs)
    ]
    workers = [
        multiprocessing.Process(target=run_worker, args=(args.url, n, args.seconds, results))
        for n in shares
        if n
    ]
    for worker in workers:
        worker.start()
    merged = {'connected': 0, 'updates': 0, 'latencies': [], 'errors': []}
    for _ in workers:
        stats = results.get()
        for key in merged:
            merged[key] += stats[key]
    for worker in workers:
        worker.join()

    lat = merged['latencies']
    per_client = merged['updates'] / max(1, merged['connected']) / args.seconds
    print(f'clients connected : {merged["connected"]}/{args.clients}')
    print(f'errors            : {len(merged["errors"])} {merged["errors"][:3]}')
    print(f'vehicle updates   : {merged["updates"]} ({per_client:.0f}/s per client)')
    print(
        f'push latency (ms) : p50={percentile(lat, 0.5):.1f} '
        f'p95={percentile(lat, 0.95):.1f} p99={percentile(lat, 0.99):.1f}'
    )


if __name__ == '__main__':
    main()
