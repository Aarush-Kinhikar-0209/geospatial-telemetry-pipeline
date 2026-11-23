import { memo } from 'react';
import { Line, LineChart, ResponsiveContainer, Tooltip, YAxis } from 'recharts';
import type { StreamStatus } from '../hooks/useVehicleStream';
import type { Sample } from '../hooks/useSampledHistory';
import type { FleetStats } from '../lib/fleet';

interface MetricsPanelProps {
  stats: FleetStats;
  speedHistory: Sample[];
  status: StreamStatus;
  connectMs: number | null;
  updatesPerSec: number;
  fps: number;
}

const STATUS_LABEL: Record<StreamStatus, string> = {
  connecting: 'Connecting…',
  live: 'Live',
  stalled: 'Stalled',
  reconnecting: 'Reconnecting…',
};

function Stat({ label, value, testId }: { label: string; value: string; testId?: string }) {
  return (
    <div className="stat">
      <span className="stat-value" data-testid={testId}>
        {value}
      </span>
      <span className="stat-label">{label}</span>
    </div>
  );
}

// The panel re-renders on every stream flush (~10/s); the chart only changes
// once a second, so it skips those renders.
const SpeedChart = memo(function SpeedChart({ history }: { history: Sample[] }) {
  return (
    <div className="chart" aria-label="Average fleet speed, last 60 seconds">
      <ResponsiveContainer width="100%" height={70}>
        <LineChart data={history} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
          <YAxis hide domain={['auto', 'auto']} />
          <Tooltip
            formatter={(value) => [`${Number(value).toFixed(1)} km/h`, 'avg speed']}
            labelFormatter={() => ''}
            contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
          />
          <Line
            type="monotone"
            dataKey="value"
            stroke="#38bdf8"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
      <span className="caption">avg speed · last 60 s</span>
    </div>
  );
});

export function MetricsPanel(props: MetricsPanelProps) {
  const { stats, speedHistory, status, connectMs, updatesPerSec, fps } = props;

  return (
    <section className="panel metrics" aria-label="Fleet metrics">
      <header className="panel-header">
        <h2>Fleet</h2>
        <span className={`pill pill-${status}`} data-testid="stream-status">
          {STATUS_LABEL[status]}
        </span>
      </header>

      <div className="stats">
        <Stat label="active vehicles" value={String(stats.count)} testId="vehicle-count" />
        <Stat label="avg speed km/h" value={stats.avgSpeedKmh.toFixed(1)} testId="avg-speed" />
        <Stat label="updates / s" value={String(updatesPerSec)} />
        <Stat label="FPS" value={String(fps)} testId="fps" />
      </div>

      <SpeedChart history={speedHistory} />

      <footer className="caption">
        {stats.lowBattery} low battery · first data in{' '}
        <span data-testid="connect-ms">{connectMs === null ? '…' : `${connectMs} ms`}</span>
      </footer>
    </section>
  );
}
