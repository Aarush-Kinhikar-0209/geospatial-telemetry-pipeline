import { useEffect, useState } from 'react';
import { endpoints, STALE_VEHICLE_MS, STREAM_FLUSH_MS } from '../config';
import { FleetStore, parseVehicles, type TrackedVehicle } from '../lib/fleet';

/** `stalled`: connected, but no vehicle updates for STALL_MS (e.g. Redis is down). */
export type StreamStatus = 'connecting' | 'live' | 'stalled' | 'reconnecting';

export interface VehicleStream {
  vehicles: TrackedVehicle[];
  status: StreamStatus;
  /** Page load to first snapshot, in ms. */
  connectMs: number | null;
  /** Vehicle updates received over the last second. */
  updatesPerSec: number;
}

const RECONNECT_MS = 2000;
const PRUNE_EVERY_MS = 5000;
const STALL_MS = 5000;

/**
 * Live vehicle positions from the API's SSE stream.
 *
 * Updates are buffered in a mutable store and pushed into React state at most
 * every STREAM_FLUSH_MS, so rendering cost does not scale with message rate.
 */
export function useVehicleStream(url: string = endpoints.stream): VehicleStream {
  const [vehicles, setVehicles] = useState<TrackedVehicle[]>([]);
  const [status, setStatus] = useState<StreamStatus>('connecting');
  const [connectMs, setConnectMs] = useState<number | null>(null);
  const [updatesPerSec, setUpdatesPerSec] = useState(0);

  useEffect(() => {
    const store = new FleetStore();
    let source: EventSource | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let dirty = false;
    let received = 0;
    let firstSnapshot = true;
    let connected = false;
    let lastDataAt = 0;

    const connect = () => {
      source = new EventSource(url);
      source.addEventListener('snapshot', (event) => {
        store.replace(parseVehicles((event as MessageEvent<string>).data), Date.now());
        dirty = true;
        connected = true;
        lastDataAt = Date.now();
        setStatus('live');
        if (firstSnapshot) {
          firstSnapshot = false;
          performance.mark('fleet:snapshot');
          setConnectMs(Math.round(performance.now()));
        }
      });
      source.addEventListener('update', (event) => {
        const states = parseVehicles((event as MessageEvent<string>).data);
        received += states.length;
        lastDataAt = Date.now();
        store.upsert(states, lastDataAt);
        dirty = true;
        setStatus('live');
      });
      source.onerror = () => {
        connected = false;
        setStatus('reconnecting');
        // EventSource retries dropped connections itself, but gives up for good
        // on an HTTP error (e.g. 503 while Redis is down).
        if (source?.readyState === EventSource.CLOSED) {
          source.close();
          reconnectTimer = setTimeout(connect, RECONNECT_MS);
        }
      };
    };
    connect();

    const flush = setInterval(() => {
      if (dirty) {
        dirty = false;
        setVehicles(store.list());
      }
    }, STREAM_FLUSH_MS);
    const prune = setInterval(() => {
      if (store.prune(Date.now(), STALE_VEHICLE_MS) > 0) dirty = true;
    }, PRUNE_EVERY_MS);
    const rate = setInterval(() => {
      setUpdatesPerSec(received);
      received = 0;
      if (connected && Date.now() - lastDataAt > STALL_MS) setStatus('stalled');
    }, 1000);

    return () => {
      source?.close();
      clearTimeout(reconnectTimer);
      clearInterval(flush);
      clearInterval(prune);
      clearInterval(rate);
    };
  }, [url]);

  return { vehicles, status, connectMs, updatesPerSec };
}
