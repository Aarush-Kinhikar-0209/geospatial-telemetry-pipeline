import type { SimulationConfig, SimulationReason } from '../types';

/** Mirrors the limits enforced by the API (api/main.py); the API has the last word. */
export const LIMITS = {
  vehicles: { min: 1, max: 200 },
  interval: { min: 0.1, max: 2 },
  chaos: { min: 0, max: 5 },
} as const;

/** The form holds raw text so half-typed input is never rewritten under the user. */
export interface SimulationForm {
  vehicles: string;
  interval: string;
  chaos: string;
}

export type FormErrors = Partial<Record<keyof SimulationForm, string>>;

export type Validation =
  | { ok: true; config: SimulationConfig }
  | { ok: false; errors: FormErrors };

export const DEFAULT_FORM: SimulationForm = { vehicles: '100', interval: '2.0', chaos: '5' };

const INTEGER = /^\d+$/;
const DECIMAL = /^\d+(\.\d+)?$/;

function parseNumber(raw: string, pattern: RegExp, expected: string): number | string {
  const text = raw.trim();
  if (text === '') return 'Required';
  if (!pattern.test(text)) return expected;
  return Number(text);
}

export function validateForm(form: SimulationForm): Validation {
  const errors: FormErrors = {};

  const vehicles = parseNumber(form.vehicles, INTEGER, 'Enter a whole number');
  if (typeof vehicles === 'string') errors.vehicles = vehicles;
  else if (vehicles < LIMITS.vehicles.min || vehicles > LIMITS.vehicles.max) {
    errors.vehicles = `Between ${LIMITS.vehicles.min} and ${LIMITS.vehicles.max}`;
  }

  const interval = parseNumber(form.interval, DECIMAL, 'Enter a number, e.g. 0.5');
  if (typeof interval === 'string') errors.interval = interval;
  else if (interval < LIMITS.interval.min || interval > LIMITS.interval.max) {
    errors.interval = `Between ${LIMITS.interval.min.toFixed(1)} and ${LIMITS.interval.max.toFixed(1)} s`;
  }

  const chaos = parseNumber(form.chaos, INTEGER, 'Enter a whole number');
  if (typeof chaos === 'string') errors.chaos = chaos;
  else if (chaos < LIMITS.chaos.min || chaos > LIMITS.chaos.max) {
    errors.chaos = `Between ${LIMITS.chaos.min} and ${LIMITS.chaos.max}`;
  }

  if (Object.keys(errors).length > 0) return { ok: false, errors };
  return {
    ok: true,
    config: {
      vehicles: vehicles as number,
      interval_seconds: interval as number,
      chaos_percent: chaos as number,
    },
  };
}

/** Whole seconds left until `deadlineMs`; never negative. */
export function secondsLeft(deadlineMs: number, nowMs: number): number {
  return Math.max(0, Math.ceil((deadlineMs - nowMs) / 1000));
}

/** 125 -> "2:05" */
export function formatClock(totalSeconds: number): string {
  const seconds = Math.max(0, Math.floor(totalSeconds));
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}

/** Share of the run already elapsed, 0 to 1. */
export function elapsedFraction(durationSeconds: number, remainingSeconds: number): number {
  if (durationSeconds <= 0) return 1;
  return Math.min(Math.max(1 - remainingSeconds / durationSeconds, 0), 1);
}

export function describeFinish(reason: SimulationReason): string {
  switch (reason) {
    case 'completed':
      return 'Simulation finished. All telemetry has been cleared.';
    case 'stopped':
      return 'Simulation stopped. All telemetry has been cleared.';
    case 'client_lost':
      return 'Simulation ended when the page went away. All telemetry has been cleared.';
  }
}
