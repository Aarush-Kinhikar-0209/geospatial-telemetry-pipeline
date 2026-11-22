import { describe, expect, it } from 'vitest';
import type { DlqMetrics } from '../types';
import { dlqBars, dlqLevel, formatPercent } from './dlq';

function metrics(processed: number, dlq: number, series: DlqMetrics['series'] = []): DlqMetrics {
  return {
    window_seconds: 3,
    processed,
    dlq,
    processed_rate_per_sec: processed / 3,
    dlq_rate_per_sec: dlq / 3,
    dlq_ratio: processed ? dlq / processed : 0,
    series,
  };
}

describe('dlqLevel', () => {
  it('is nominal at the default 5% chaos rate', () => {
    expect(dlqLevel(metrics(3000, 150), 0.1)).toBe('ok');
  });

  it('alerts on a spike at or above the threshold', () => {
    expect(dlqLevel(metrics(3000, 300), 0.1)).toBe('alert');
    expect(dlqLevel(metrics(3000, 1500), 0.1)).toBe('alert');
  });

  it('alerts when only bad data is arriving', () => {
    expect(dlqLevel(metrics(0, 20), 0.1)).toBe('alert');
  });

  it('distinguishes no traffic and no data from healthy', () => {
    expect(dlqLevel(metrics(0, 0), 0.1)).toBe('idle');
    expect(dlqLevel(null, 0.1)).toBe('unknown');
  });
});

describe('dlqBars', () => {
  it('labels seconds relative to the newest and computes per-second ratios', () => {
    const bars = dlqBars(
      metrics(0, 0, [
        { ts: 100, processed: 1000, dlq: 50 },
        { ts: 101, processed: 0, dlq: 0 },
        { ts: 102, processed: 500, dlq: 250 },
      ]),
    );
    expect(bars).toEqual([
      { second: '-2s', dlq: 50, ratio: 0.05 },
      { second: '-1s', dlq: 0, ratio: 0 },
      { second: '0s', dlq: 250, ratio: 0.5 },
    ]);
  });

  it('is empty without data', () => {
    expect(dlqBars(null)).toEqual([]);
  });
});

it('formats ratios as percentages', () => {
  expect(formatPercent(0.0526)).toBe('5.3%');
});
