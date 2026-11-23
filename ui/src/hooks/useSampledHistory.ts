import { useEffect, useRef, useState } from 'react';

export interface Sample {
  t: number;
  value: number;
}

/**
 * Bounded history of `value`, sampled every `everyMs` (keeps the last `size`).
 * While `enabled` is false nothing is sampled and the history is emptied, so a
 * chart never carries one run's data (or an idle zero) into the next.
 */
export function useSampledHistory(
  value: number,
  enabled = true,
  everyMs = 1000,
  size = 60,
): Sample[] {
  const latest = useRef(value);
  latest.current = value;
  const [history, setHistory] = useState<Sample[]>([]);

  useEffect(() => {
    if (!enabled) {
      setHistory((previous) => (previous.length > 0 ? [] : previous));
      return;
    }
    const timer = setInterval(() => {
      setHistory((prev) => [...prev.slice(-(size - 1)), { t: Date.now(), value: latest.current }]);
    }, everyMs);
    return () => clearInterval(timer);
  }, [enabled, everyMs, size]);

  return history;
}
