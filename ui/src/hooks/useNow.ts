import { useEffect, useState } from 'react';

/** `performance.now()`, refreshed every `intervalMs` while `intervalMs` is set. */
export function useNow(intervalMs: number | null): number {
  const [now, setNow] = useState(() => performance.now());

  useEffect(() => {
    if (intervalMs === null) return;
    setNow(performance.now());
    const timer = setInterval(() => setNow(performance.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);

  return now;
}
