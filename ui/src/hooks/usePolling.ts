import { useEffect, useState } from 'react';

export interface Polled<T> {
  data: T | null;
  /** Last failure; `data` keeps the last good value so panels can show it as stale. */
  error: string | null;
}

export type ResponseParser<T> = (response: Response) => Promise<T>;

export async function parseOk<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    // FastAPI sends a string for most errors, but a list of problems for a 422.
    const detail = typeof body?.detail === 'string' ? body.detail : null;
    throw new Error(detail ?? `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

/** Fetch `url` now and `intervalMs` after each completed request. */
export function usePolling<T>(
  url: string,
  intervalMs: number,
  parse: ResponseParser<T> = parseOk,
): Polled<T> {
  const [state, setState] = useState<Polled<T>>({ data: null, error: null });

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();

    const tick = async () => {
      try {
        const response = await fetch(url, { signal: controller.signal });
        const data = await parse(response);
        if (!cancelled) setState({ data, error: null });
      } catch (err) {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setState((prev) => ({ data: prev.data, error: message }));
      }
      if (!cancelled) timer = setTimeout(tick, intervalMs);
    };
    tick();

    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, [url, intervalMs, parse]);

  return state;
}
