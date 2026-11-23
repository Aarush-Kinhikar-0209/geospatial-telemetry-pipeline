import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { endpoints } from '../config';
import type { SimulationConfig, SimulationLastRun, SimulationState } from '../types';
import { parseOk } from './usePolling';

const POLL_MS = 1_000;
/** Well inside the API's heartbeat timeout (10 s), so a slow request or two is tolerated. */
const HEARTBEAT_MS = 2_000;

export interface Simulation {
  /** Latest server state; null until the first response. */
  state: SimulationState | null;
  /** `performance.now()` time at which the running simulation ends, or null. */
  deadline: number | null;
  /** How the simulation this page watched last ended; cleared when a new one starts. */
  finished: SimulationLastRun | null;
  /** Last request failure, cleared by the next success. */
  error: string | null;
  /** A start or stop request is in flight. */
  pending: boolean;
  start: (config: SimulationConfig) => Promise<void>;
  stop: () => Promise<void>;
}

interface Snapshot {
  state: SimulationState;
  /** Client clock when the state arrived, to turn remaining_seconds into a deadline. */
  receivedAt: number;
}

function post(url: string, body?: unknown): Promise<Response> {
  return fetch(url, {
    method: 'POST',
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

/**
 * The server owns the simulation; this hook mirrors it.
 *
 *  - polls the state every second, so every open page agrees on the timer
 *  - sends a heartbeat while a run is going, which is how the API knows the page is
 *    still open: if the heartbeats stop (tab closed, browser killed, network gone)
 *    the API stops the run and deletes its data
 *  - the countdown is derived from the server's remaining time, not a client timer,
 *    so a reload or a second tab shows the same clock
 */
export function useSimulation(): Simulation {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [finished, setFinished] = useState<SimulationLastRun | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  // A poll that began before an action's response must not overwrite that response.
  const generation = useRef(0);
  const previousStatus = useRef<SimulationState['status'] | null>(null);

  const apply = useCallback((state: SimulationState) => {
    const before = previousStatus.current;
    previousStatus.current = state.status;
    if (state.status === 'idle' && before !== null && before !== 'idle') {
      setFinished(state.last_run);
    } else if (state.status !== 'idle') {
      setFinished(null);
    }
    setSnapshot({ state, receivedAt: performance.now() });
    setError(null);
  }, []);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();

    const tick = async () => {
      const started = generation.current;
      try {
        const response = await fetch(endpoints.simulation, { signal: controller.signal });
        const state = await parseOk<SimulationState>(response);
        if (!cancelled && started === generation.current) apply(state);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
      if (!cancelled) timer = setTimeout(tick, POLL_MS);
    };
    tick();

    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, [apply]);

  const running = snapshot?.state.status === 'running';
  useEffect(() => {
    if (!running) return;
    const beat = () => {
      post(endpoints.simulationHeartbeat).catch(() => {
        // A missed beat is tolerated; the next one follows in HEARTBEAT_MS.
      });
    };
    beat();
    const timer = setInterval(beat, HEARTBEAT_MS);
    return () => clearInterval(timer);
  }, [running]);

  const act = useCallback(
    async (url: string, body?: unknown) => {
      setPending(true);
      try {
        const state = await parseOk<SimulationState>(await post(url, body));
        generation.current += 1;
        apply(state);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setPending(false);
      }
    },
    [apply],
  );

  const start = useCallback(
    (config: SimulationConfig) => act(endpoints.simulationStart, config),
    [act],
  );
  const stop = useCallback(() => act(endpoints.simulationStop), [act]);

  const deadline = useMemo(() => {
    const remaining = snapshot?.state.remaining_seconds;
    return snapshot && remaining != null ? snapshot.receivedAt + remaining * 1000 : null;
  }, [snapshot]);

  return { state: snapshot?.state ?? null, deadline, finished, error, pending, start, stop };
}
