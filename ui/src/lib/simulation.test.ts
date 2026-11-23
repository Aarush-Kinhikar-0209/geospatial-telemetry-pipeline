import { describe, expect, it } from 'vitest';
import {
  DEFAULT_FORM,
  describeFinish,
  elapsedFraction,
  formatClock,
  secondsLeft,
  validateForm,
  type SimulationForm,
} from './simulation';

const form = (overrides: Partial<SimulationForm> = {}): SimulationForm => ({
  ...DEFAULT_FORM,
  ...overrides,
});

describe('validateForm', () => {
  it('accepts the defaults', () => {
    expect(validateForm(DEFAULT_FORM)).toEqual({
      ok: true,
      config: { vehicles: 100, interval_seconds: 2, chaos_percent: 5 },
    });
  });

  it('accepts both ends of every range', () => {
    expect(validateForm({ vehicles: '1', interval: '0.1', chaos: '0' }).ok).toBe(true);
    expect(validateForm({ vehicles: '200', interval: '2.0', chaos: '5' }).ok).toBe(true);
    expect(validateForm({ vehicles: '200', interval: '2', chaos: '5' }).ok).toBe(true);
  });

  it('trims whitespace and sends numbers, not text', () => {
    const result = validateForm({ vehicles: ' 42 ', interval: ' 0.75 ', chaos: ' 3 ' });
    expect(result).toEqual({
      ok: true,
      config: { vehicles: 42, interval_seconds: 0.75, chaos_percent: 3 },
    });
  });

  it.each(['0', '201', '1000'])('rejects %s vehicles', (vehicles) => {
    const result = validateForm(form({ vehicles }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.errors.vehicles).toBe('Between 1 and 200');
  });

  it.each(['0.09', '0.05', '2.01', '3'])('rejects a %s second interval', (interval) => {
    const result = validateForm(form({ interval }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.errors.interval).toBe('Between 0.1 and 2.0 s');
  });

  it.each(['6', '10', '100'])('rejects %s%% chaos', (chaos) => {
    const result = validateForm(form({ chaos }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.errors.chaos).toBe('Between 0 and 5');
  });

  it('requires whole numbers for vehicles and chaos', () => {
    for (const field of ['vehicles', 'chaos'] as const) {
      for (const value of ['2.5', '1e2', '-1', '+3', '0x10', 'ten']) {
        const result = validateForm(form({ [field]: value }));
        expect(result.ok, `${field}=${value}`).toBe(false);
        if (!result.ok) expect(result.errors[field], `${field}=${value}`).toBe('Enter a whole number');
      }
    }
  });

  it('rejects interval text that is not a plain decimal', () => {
    for (const interval of ['abc', '.5', '1.', '1e0', '-0.5', '1,5']) {
      expect(validateForm(form({ interval })).ok, interval).toBe(false);
    }
  });

  it('flags empty fields as required', () => {
    const result = validateForm({ vehicles: '', interval: '  ', chaos: '' });
    expect(result).toEqual({
      ok: false,
      errors: { vehicles: 'Required', interval: 'Required', chaos: 'Required' },
    });
  });

  it('reports every invalid field at once', () => {
    const result = validateForm({ vehicles: '500', interval: '9', chaos: '-2' });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(Object.keys(result.errors).sort()).toEqual(['chaos', 'interval', 'vehicles']);
  });
});

describe('countdown helpers', () => {
  it('counts whole seconds up, never below zero', () => {
    expect(secondsLeft(120_000, 0)).toBe(120);
    expect(secondsLeft(120_000, 100)).toBe(120);
    expect(secondsLeft(120_000, 119_001)).toBe(1);
    expect(secondsLeft(120_000, 120_000)).toBe(0);
    expect(secondsLeft(120_000, 999_999)).toBe(0);
  });

  it('formats minutes and seconds', () => {
    expect(formatClock(120)).toBe('2:00');
    expect(formatClock(119)).toBe('1:59');
    expect(formatClock(61)).toBe('1:01');
    expect(formatClock(9)).toBe('0:09');
    expect(formatClock(0)).toBe('0:00');
    expect(formatClock(-5)).toBe('0:00');
  });

  it('reports how much of the run has elapsed', () => {
    expect(elapsedFraction(120, 120)).toBe(0);
    expect(elapsedFraction(120, 30)).toBe(0.75);
    expect(elapsedFraction(120, 0)).toBe(1);
    expect(elapsedFraction(120, 500)).toBe(0);
    expect(elapsedFraction(0, 0)).toBe(1);
  });
});

describe('describeFinish', () => {
  it('says that the data is gone for every way a run can end', () => {
    for (const reason of ['completed', 'stopped', 'client_lost'] as const) {
      expect(describeFinish(reason)).toContain('cleared');
    }
  });
});
