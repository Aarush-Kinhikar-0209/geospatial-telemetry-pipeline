import { useEffect, useRef, useState } from 'react';

export interface Sample {
  t: number;
  value: number;
}

/** Bounded history of `value`, sampled every `everyMs` (keeps the last `size`). */
export function useSampledHistory(value: number, everyMs = 1000, size = 60): Sample[] {
  const latest = useRef(value);
  latest.current = value;
  const [history, setHistory] = useState<Sample[]>([]);

  useEffect(() => {
    const timer = setInterval(() => {
      setHistory((prev) => [...prev.slice(-(size - 1)), { t: Date.now(), value: latest.current }]);
    }, everyMs);
    return () => clearInterval(timer);
  }, [everyMs, size]);

  return history;
}
