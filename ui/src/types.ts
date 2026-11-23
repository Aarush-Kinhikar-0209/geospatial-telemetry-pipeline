// Mirrors the Pydantic response models in api/main.py.

export interface VehicleState {
  vehicle_id: string;
  lat: number;
  lon: number;
  speed_kmh: number;
  battery_pct: number;
  h3_index: string;
  timestamp: string;
  processed_at: string;
}

export interface DlqPoint {
  ts: number;
  processed: number;
  dlq: number;
}

export interface DlqMetrics {
  window_seconds: number;
  processed: number;
  dlq: number;
  processed_rate_per_sec: number;
  dlq_rate_per_sec: number;
  dlq_ratio: number;
  series: DlqPoint[];
}

export interface Health {
  status: 'ok' | 'degraded';
  redis: boolean;
  clickhouse: boolean;
  sse_clients: number;
  live_feed: boolean;
}

export interface SimulationConfig {
  vehicles: number;
  interval_seconds: number;
  chaos_percent: number;
}

export type SimulationStatus = 'idle' | 'running' | 'stopping' | 'cleaning';

export type SimulationReason = 'completed' | 'stopped' | 'client_lost';

export interface SimulationLastRun {
  run_id: string;
  reason: SimulationReason;
  ended_at: number;
}

export interface SimulationState {
  status: SimulationStatus;
  run_id: string | null;
  config: SimulationConfig | null;
  duration_seconds: number;
  /** Seconds left while running; the client counts down locally between polls. */
  remaining_seconds: number | null;
  reason: SimulationReason | null;
  last_run: SimulationLastRun | null;
}