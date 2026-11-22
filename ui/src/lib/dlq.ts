import type { DlqMetrics } from '../types';

export type DlqLevel = 'unknown' | 'ok' | 'idle' | 'alert';

/**
 * `alert` when the recent DLQ share reaches `alertRatio`; `idle` when nothing
 * is flowing (so an empty pipeline does not look healthy by accident).
 */
export function dlqLevel(metrics: DlqMetrics | null, alertRatio: number): DlqLevel {
  if (!metrics) return 'unknown';
  if (metrics.processed === 0 && metrics.dlq === 0) return 'idle';
  if (metrics.dlq > 0 && metrics.processed === 0) return 'alert';
  return metrics.dlq_ratio >= alertRatio ? 'alert' : 'ok';
}

export interface DlqBar {
  second: string;
  dlq: number;
  ratio: number;
}

/** Per-second chart rows, labelled relative to the newest second. */
export function dlqBars(metrics: DlqMetrics | null): DlqBar[] {
  if (!metrics || metrics.series.length === 0) return [];
  const newest = metrics.series[metrics.series.length - 1].ts;
  return metrics.series.map((point) => ({
    second: `${point.ts - newest}s`,
    dlq: point.dlq,
    ratio: point.processed ? point.dlq / point.processed : 0,
  }));
}

export function formatPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(1)}%`;
}
