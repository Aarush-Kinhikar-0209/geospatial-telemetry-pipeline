import { memo } from 'react';
import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, YAxis } from 'recharts';
import { DLQ_ALERT_RATIO } from '../config';
import { dlqBars, dlqLevel, formatPercent } from '../lib/dlq';
import type { DlqMetrics, Health } from '../types';

interface DlqMonitorProps {
  metrics: DlqMetrics | null;
  metricsError: string | null;
  health: Health | null;
  healthError: string | null;
}

const LEVEL_LABEL = {
  unknown: 'No data',
  idle: 'Idle',
  ok: 'Nominal',
  alert: 'DLQ spike',
} as const;

function Dot({ label, ok }: { label: string; ok: boolean | null }) {
  const state = ok === null ? 'unknown' : ok ? 'up' : 'down';
  return (
    <li className={`dot dot-${state}`} data-testid={`health-${label.toLowerCase()}`}>
      {label}
    </li>
  );
}

/** Pipeline health, centred on how much traffic is being dead-lettered. */
export const DlqMonitor = memo(function DlqMonitor({
  metrics,
  metricsError,
  health,
  healthError,
}: DlqMonitorProps) {
  const level = dlqLevel(metrics, DLQ_ALERT_RATIO);
  const bars = dlqBars(metrics);
  const apiUp = healthError === null && health !== null;

  return (
    <section
      className={`panel dlq dlq-${level}`}
      aria-label="DLQ monitor"
      data-testid="dlq-monitor"
      data-level={level}
    >
      <header className="panel-header">
        <h2>DLQ Monitor</h2>
        <span className={`pill pill-dlq-${level}`} data-testid="dlq-status">
          {LEVEL_LABEL[level]}
        </span>
      </header>

      <div className="stats">
        <div className="stat">
          <span className="stat-value" data-testid="dlq-ratio">
            {metrics ? formatPercent(metrics.dlq_ratio) : '–'}
          </span>
          <span className="stat-label">dead-lettered (last {metrics?.window_seconds ?? 3}s)</span>
        </div>
        <div className="stat">
          <span className="stat-value" data-testid="dlq-rate">
            {metrics ? metrics.dlq_rate_per_sec.toFixed(1) : '–'}
          </span>
          <span className="stat-label">DLQ msg / s</span>
        </div>
        <div className="stat">
          <span className="stat-value">
            {metrics ? Math.round(metrics.processed_rate_per_sec) : '–'}
          </span>
          <span className="stat-label">processed / s</span>
        </div>
      </div>

      <div className="chart" aria-label="DLQ messages per second, last 60 seconds">
        <ResponsiveContainer width="100%" height={70}>
          <BarChart data={bars} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <YAxis hide />
            <Tooltip
              formatter={(value) => [Number(value), 'DLQ msgs']}
              labelFormatter={(label) => String(label)}
              contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            />
            <Bar dataKey="dlq" isAnimationActive={false}>
              {bars.map((bar) => (
                <Cell
                  key={bar.second}
                  fill={bar.ratio >= DLQ_ALERT_RATIO ? '#ef4444' : '#f59e0b'}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <span className="caption">
          DLQ msgs / s · last 60 s · alert at {formatPercent(DLQ_ALERT_RATIO)}
        </span>
      </div>

      {metricsError && <p className="warning">Metrics unavailable: {metricsError}</p>}

      <ul className="health" aria-label="Service health">
        <Dot label="API" ok={healthError === null && health === null ? null : apiUp} />
        <Dot label="Redis" ok={health ? health.redis : null} />
        <Dot label="ClickHouse" ok={health ? health.clickhouse : null} />
        <Dot label="Feed" ok={health ? health.live_feed : null} />
      </ul>
    </section>
  );
});
