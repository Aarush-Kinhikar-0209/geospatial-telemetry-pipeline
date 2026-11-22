import { useEffect, useState } from 'react';

/** Frames per second of the page's animation loop, sampled once a second. */
export function useFps(): number {
  const [fps, setFps] = useState(0);

  useEffect(() => {
    let frames = 0;
    let windowStart = performance.now();
    let handle = 0;

    const frame = (now: number) => {
      frames += 1;
      if (now - windowStart >= 1000) {
        setFps(Math.round((frames * 1000) / (now - windowStart)));
        frames = 0;
        windowStart = now;
      }
      handle = requestAnimationFrame(frame);
    };
    handle = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(handle);
  }, []);

  return fps;
}
