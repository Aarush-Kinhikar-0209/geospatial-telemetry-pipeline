import { useState, type FormEvent } from 'react';
import { useNow } from '../hooks/useNow';
import type { Simulation } from '../hooks/useSimulation';
import {
  DEFAULT_FORM,
  describeFinish,
  elapsedFraction,
  formatClock,
  LIMITS,
  secondsLeft,
  validateForm,
  type SimulationForm,
} from '../lib/simulation';

const STATUS_LABEL = {
  idle: 'Idle',
  running: 'Running',
  stopping: 'Stopping…',
  cleaning: 'Clearing data…',
} as const;

interface FieldProps {
  id: keyof SimulationForm;
  label: string;
  hint: string;
  step: number;
  min: number;
  max: number;
  value: string;
  error?: string;
  onChange: (value: string) => void;
}

function Field({ id, label, hint, step, min, max, value, error, onChange }: FieldProps) {
  return (
    <div className="field">
      <label htmlFor={`sim-${id}`}>
        {label} <span className="hint">{hint}</span>
      </label>
      <input
        id={`sim-${id}`}
        data-testid={`sim-${id}`}
        type="number"
        inputMode={id === 'interval' ? 'decimal' : 'numeric'}
        step={step}
        min={min}
        max={max}
        value={value}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? `sim-${id}-error` : undefined}
        onChange={(event) => onChange(event.target.value)}
      />
      {error && (
        <span className="field-error" id={`sim-${id}-error`} data-testid={`sim-${id}-error`}>
          {error}
        </span>
      )}
    </div>
  );
}

/** Configure, start and watch the timed simulation. The server owns the clock. */
export function SimulationPanel({ simulation }: { simulation: Simulation }) {
  const { state, deadline, finished, error, pending, start, stop } = simulation;
  const status = state?.status ?? null;
  const running = status === 'running';

  const [form, setForm] = useState<SimulationForm>(DEFAULT_FORM);
  const validation = validateForm(form);
  const now = useNow(running ? 250 : null);

  const set = (key: keyof SimulationForm) => (value: string) =>
    setForm((previous) => ({ ...previous, [key]: value }));

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    if (validation.ok && status === 'idle' && !pending) void start(validation.config);
  };

  const errors = validation.ok ? {} : validation.errors;
  const duration = state?.duration_seconds ?? 120;
  const left = running && deadline !== null ? secondsLeft(deadline, now) : duration;
  const config = state?.config;

  return (
    <section className="panel simulation" aria-label="Simulation" data-testid="simulation-panel">
      <header className="panel-header">
        <h2>Simulation</h2>
        <span
          className={`pill pill-${status ?? 'connecting'}`}
          data-testid="sim-status"
          data-status={status ?? 'unknown'}
        >
          {status ? STATUS_LABEL[status] : 'Connecting…'}
        </span>
      </header>

      {(status === null || status === 'idle') && (
        <form onSubmit={onSubmit} noValidate>
          <Field
            id="vehicles"
            label="Vehicles"
            hint={`${LIMITS.vehicles.min}–${LIMITS.vehicles.max}`}
            step={1}
            min={LIMITS.vehicles.min}
            max={LIMITS.vehicles.max}
            value={form.vehicles}
            error={errors.vehicles}
            onChange={set('vehicles')}
          />
          <Field
            id="interval"
            label="Interval (s)"
            hint={`${LIMITS.interval.min.toFixed(1)}–${LIMITS.interval.max.toFixed(1)}`}
            step={0.1}
            min={LIMITS.interval.min}
            max={LIMITS.interval.max}
            value={form.interval}
            error={errors.interval}
            onChange={set('interval')}
          />
          <Field
            id="chaos"
            label="Chaos (%)"
            hint={`${LIMITS.chaos.min}–${LIMITS.chaos.max}`}
            step={1}
            min={LIMITS.chaos.min}
            max={LIMITS.chaos.max}
            value={form.chaos}
            error={errors.chaos}
            onChange={set('chaos')}
          />
          <button
            type="submit"
            className="primary"
            data-testid="sim-start"
            disabled={!validation.ok || status !== 'idle' || pending}
          >
            Start simulation
          </button>
          <p className="caption">
            Runs for {formatClock(duration)}, then every trace of it is deleted from Kafka,
            ClickHouse and Redis.
          </p>
          {finished && (
            <p className="notice" role="status" data-testid="sim-finished">
              {describeFinish(finished.reason)}
            </p>
          )}
        </form>
      )}

      {running && (
        <div data-testid="sim-running">
          <div className="countdown" role="timer" aria-label="Time remaining" data-testid="sim-timer">
            {formatClock(left)}
          </div>
          <div
            className="progress"
            role="progressbar"
            aria-valuemin={0}
            aria-valuemax={duration}
            aria-valuenow={duration - left}
          >
            <div
              className="progress-bar"
              style={{ width: `${elapsedFraction(duration, left) * 100}%` }}
            />
          </div>
          {config && (
            <p className="caption" data-testid="sim-config">
              {config.vehicles} vehicles · every {config.interval_seconds.toFixed(1)} s ·{' '}
              {config.chaos_percent}% chaos
            </p>
          )}
          <button
            type="button"
            className="secondary"
            data-testid="sim-stop"
            disabled={pending}
            onClick={() => void stop()}
          >
            Stop &amp; clear data
          </button>
          <p className="caption">Closing this page also ends the run and clears its data.</p>
        </div>
      )}

      {(status === 'stopping' || status === 'cleaning') && (
        <p className="clearing" role="status" data-testid="sim-clearing">
          {status === 'stopping'
            ? 'Stopping the generator…'
            : 'Clearing telemetry from Kafka, ClickHouse and Redis…'}
        </p>
      )}

      {error && (
        <p className="warning" role="alert" data-testid="sim-error">
          {error}
        </p>
      )}
    </section>
  );
}
