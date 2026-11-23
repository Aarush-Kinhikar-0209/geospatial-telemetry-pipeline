export const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '');

export const MAP_STYLE_URL =
  import.meta.env.VITE_MAP_STYLE_URL ||
  'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

export const DLQ_ALERT_RATIO = Number(import.meta.env.VITE_DLQ_ALERT_RATIO || 0.1);

/** Vehicles silent for this long are dropped from the map (and from memory). */
export const STALE_VEHICLE_MS = 30_000;

/** How often buffered SSE updates are pushed into React state. */
export const STREAM_FLUSH_MS = 100;

/** Marker glide between position updates; a bit longer than the flush interval. */
export const POSITION_TRANSITION_MS = 300;

export const INITIAL_VIEW_STATE = {
  longitude: 72.85,
  latitude: 18.95,
  zoom: 10.6,
  pitch: 0,
  bearing: 0,
};

export const endpoints = {
  stream: `${API_BASE}/api/v1/stream/vehicles`,
  dlq: `${API_BASE}/api/v1/metrics/dlq?window=3&history=60`,
  health: `${API_BASE}/api/v1/health`,
};
