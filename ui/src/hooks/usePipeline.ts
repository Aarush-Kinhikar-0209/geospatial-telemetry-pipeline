import { endpoints } from '../config';
import type { DlqMetrics, Health } from '../types';
import { parseOk, usePolling } from './usePolling';

/** /health answers 503 with a body when a store is down; that body is the data. */
async function parseHealth(response: Response): Promise<Health> {
  if (response.status === 503) return (await response.json()) as Health;
  return parseOk<Health>(response);
}

export const useDlqMetrics = () => usePolling<DlqMetrics>(endpoints.dlq, 1_000);

export const useHealth = () => usePolling<Health>(endpoints.health, 5_000, parseHealth);
