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

export interface Zone {
  h3_index: string;
  vehicles: number;
  events: number;
  avg_speed_kmh: number;
}

export interface ZonesResponse {
  window_minutes: number;
  generated_at: string;
  query_ms: number;
  zones: Zone[];
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
