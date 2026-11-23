import { describe, expect, it } from 'vitest';
import type { VehicleState } from '../types';
import { FleetStore, fleetStats, parseVehicles, sortVehicles, speedColor } from './fleet';

function vehicle(id: string, overrides: Partial<VehicleState> = {}): VehicleState {
  return {
    vehicle_id: id,
    lat: 18.95,
    lon: 72.85,
    speed_kmh: 40,
    battery_pct: 80,
    h3_index: '89608b0aa5bffff',
    timestamp: '2026-11-22T10:00:00.000Z',
    processed_at: '2026-11-22T10:00:00.050Z',
    ...overrides,
  };
}

describe('parseVehicles', () => {
  it('keeps well-formed states and drops the rest', () => {
    const payload = JSON.stringify([vehicle('v-1'), { vehicle_id: 'v-2', lat: 'x' }, null, 7]);
    expect(parseVehicles(payload).map((v) => v.vehicle_id)).toEqual(['v-1']);
  });

  it('returns nothing for invalid JSON or non-arrays', () => {
    expect(parseVehicles('{not json')).toEqual([]);
    expect(parseVehicles('{"vehicle_id": "v-1"}')).toEqual([]);
  });
});

describe('FleetStore', () => {
  it('keeps one entry per vehicle with its index stable across updates', () => {
    const store = new FleetStore();
    store.upsert([vehicle('v-1'), vehicle('v-2'), vehicle('v-3')], 0);
    store.upsert([vehicle('v-2', { lat: 19 })], 100);

    const list = store.list();
    expect(list.map((v) => v.vehicle_id)).toEqual(['v-1', 'v-2', 'v-3']);
    expect(list[1]).toMatchObject({ lat: 19, receivedAt: 100 });
  });

  it('replace() swaps in a fresh snapshot', () => {
    const store = new FleetStore();
    store.upsert([vehicle('v-1'), vehicle('v-2')], 0);
    store.replace([vehicle('v-3')], 10);
    expect(store.list().map((v) => v.vehicle_id)).toEqual(['v-3']);
  });

  it('prunes vehicles that went silent so memory stays bounded', () => {
    const store = new FleetStore();
    store.upsert([vehicle('old')], 0);
    store.upsert([vehicle('fresh')], 25_000);

    expect(store.prune(31_000, 30_000)).toBe(1);
    expect(store.list().map((v) => v.vehicle_id)).toEqual(['fresh']);
    expect(store.size).toBe(1);
  });

  it('does not grow with repeated updates for the same fleet', () => {
    const store = new FleetStore();
    for (let tick = 0; tick < 1000; tick += 1) {
      store.upsert([vehicle('v-1'), vehicle('v-2')], tick);
    }
    expect(store.size).toBe(2);
  });
});

describe('fleetStats', () => {
  it('averages speed and counts low batteries', () => {
    const stats = fleetStats([
      vehicle('a', { speed_kmh: 20, battery_pct: 10 }),
      vehicle('b', { speed_kmh: 60, battery_pct: 90 }),
    ]);
    expect(stats).toEqual({ count: 2, avgSpeedKmh: 40, lowBattery: 1 });
  });

  it('handles an empty fleet', () => {
    expect(fleetStats([])).toEqual({ count: 0, avgSpeedKmh: 0, lowBattery: 0 });
  });
});

describe('sortVehicles', () => {
  const fleet = [
    vehicle('v-10', { speed_kmh: 30 }),
    vehicle('v-2', { speed_kmh: 55.5 }),
    vehicle('v-1', { speed_kmh: 30 }),
    vehicle('v-3', { speed_kmh: 0 }),
  ];
  const ids = (list: VehicleState[]) => list.map((v) => v.vehicle_id);

  it('sorts by speed ascending and descending', () => {
    expect(ids(sortVehicles(fleet, 'speed', 'asc'))).toEqual(['v-3', 'v-1', 'v-10', 'v-2']);
    expect(ids(sortVehicles(fleet, 'speed', 'desc'))).toEqual(['v-2', 'v-1', 'v-10', 'v-3']);
  });

  it('sorts by vehicle id numerically in both directions', () => {
    expect(ids(sortVehicles(fleet, 'id', 'asc'))).toEqual(['v-1', 'v-2', 'v-3', 'v-10']);
    expect(ids(sortVehicles(fleet, 'id', 'desc'))).toEqual(['v-10', 'v-3', 'v-2', 'v-1']);
  });

  it('does not mutate its input', () => {
    const before = ids(fleet);
    sortVehicles(fleet, 'speed', 'desc');
    expect(ids(fleet)).toEqual(before);
  });

  it('handles an empty fleet', () => {
    expect(sortVehicles([], 'speed', 'asc')).toEqual([]);
  });
});

describe('colors', () => {
  it('maps speed from red through yellow to green and clamps', () => {
    expect(speedColor(0)).toEqual([239, 68, 68]);
    expect(speedColor(40)).toEqual([250, 204, 21]);
    expect(speedColor(80)).toEqual([34, 197, 94]);
    expect(speedColor(-5)).toEqual(speedColor(0));
    expect(speedColor(500)).toEqual(speedColor(80));
  });
});
