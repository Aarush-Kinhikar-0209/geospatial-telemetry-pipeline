import type { VehicleState } from '../types';

export interface TrackedVehicle extends VehicleState {
  /** Client clock (ms) when the latest state arrived. */
  receivedAt: number;
}

export type Rgb = [number, number, number];

function isVehicleState(value: unknown): value is VehicleState {
  if (typeof value !== 'object' || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.vehicle_id === 'string' &&
    Number.isFinite(v.lat) &&
    Number.isFinite(v.lon) &&
    Number.isFinite(v.speed_kmh) &&
    Number.isFinite(v.battery_pct)
  );
}

/** Parse an SSE payload, skipping anything that is not a vehicle state. */
export function parseVehicles(data: string): VehicleState[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return [];
  }
  return Array.isArray(parsed) ? parsed.filter(isVehicleState) : [];
}

/**
 * Latest state per vehicle. Map iteration order is insertion order, so a
 * vehicle keeps its index across updates and deck.gl can animate it in place.
 */
export class FleetStore {
  private readonly vehicles = new Map<string, TrackedVehicle>();

  get size(): number {
    return this.vehicles.size;
  }

  replace(states: VehicleState[], now: number): void {
    this.vehicles.clear();
    this.upsert(states, now);
  }

  upsert(states: VehicleState[], now: number): void {
    for (const state of states) {
      this.vehicles.set(state.vehicle_id, { ...state, receivedAt: now });
    }
  }

  /** Drop vehicles not heard from in `maxAgeMs`; returns how many were removed. */
  prune(now: number, maxAgeMs: number): number {
    let removed = 0;
    for (const [id, vehicle] of this.vehicles) {
      if (now - vehicle.receivedAt > maxAgeMs) {
        this.vehicles.delete(id);
        removed += 1;
      }
    }
    return removed;
  }

  list(): TrackedVehicle[] {
    return Array.from(this.vehicles.values());
  }
}

export interface FleetStats {
  count: number;
  avgSpeedKmh: number;
  lowBattery: number;
}

export function fleetStats(vehicles: readonly VehicleState[]): FleetStats {
  let speed = 0;
  let lowBattery = 0;
  for (const vehicle of vehicles) {
    speed += vehicle.speed_kmh;
    if (vehicle.battery_pct < 20) lowBattery += 1;
  }
  return {
    count: vehicles.length,
    avgSpeedKmh: vehicles.length ? speed / vehicles.length : 0,
    lowBattery,
  };
}

export type SortKey = 'id' | 'speed';
export type SortDirection = 'asc' | 'desc';

const idCollator = new Intl.Collator('en', { numeric: true });

/**
 * A sorted copy of `vehicles`. IDs compare numerically ("v-2" before "v-10") and
 * break speed ties, so equal speeds never swap places between refreshes.
 */
export function sortVehicles<T extends VehicleState>(
  vehicles: readonly T[],
  key: SortKey,
  direction: SortDirection,
): T[] {
  const sign = direction === 'asc' ? 1 : -1;
  return [...vehicles].sort((a, b) => {
    if (key === 'speed' && a.speed_kmh !== b.speed_kmh) {
      return sign * (a.speed_kmh - b.speed_kmh);
    }
    const byId = idCollator.compare(a.vehicle_id, b.vehicle_id);
    return key === 'id' ? sign * byId : byId;
  });
}

function lerp(a: Rgb, b: Rgb, t: number): Rgb {
  return [0, 1, 2].map((i) => Math.round(a[i] + (b[i] - a[i]) * t)) as Rgb;
}

const SLOW: Rgb = [239, 68, 68];
const CRUISE: Rgb = [250, 204, 21];
const FAST: Rgb = [34, 197, 94];

/** Red when stopped, yellow around 40 km/h, green from 80 km/h. */
export function speedColor(speedKmh: number): Rgb {
  const t = Math.min(Math.max(speedKmh / 80, 0), 1);
  return t < 0.5 ? lerp(SLOW, CRUISE, t * 2) : lerp(CRUISE, FAST, (t - 0.5) * 2);
}
