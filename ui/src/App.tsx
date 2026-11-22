import { lazy, Suspense, useMemo, useState } from 'react';
import { DlqMonitor } from './components/DlqMonitor';
import { MetricsPanel } from './components/MetricsPanel';
import { useFps } from './hooks/useFps';
import { useDlqMetrics, useHealth, useZones } from './hooks/usePipeline';
import { useSampledHistory } from './hooks/useSampledHistory';
import { useVehicleStream } from './hooks/useVehicleStream';
import { fleetStats } from './lib/fleet';
import type { Zone } from './types';

const NO_ZONES: Zone[] = [];

// deck.gl + MapLibre are most of the bundle; loading them lazily lets the SSE
// stream and panels start while the map code is still downloading.
const FleetMap = lazy(() =>
  import('./components/FleetMap').then((module) => ({ default: module.FleetMap })),
);

export default function App() {
  const stream = useVehicleStream();
  const zones = useZones();
  const dlq = useDlqMetrics();
  const health = useHealth();
  const fps = useFps();
  const [mapError, setMapError] = useState<string | null>(null);

  const stats = useMemo(() => fleetStats(stream.vehicles), [stream.vehicles]);
  const speedHistory = useSampledHistory(stats.avgSpeedKmh);

  return (
    <main className="app">
      <Suspense fallback={null}>
        <FleetMap
          vehicles={stream.vehicles}
          zones={zones.data?.zones ?? NO_ZONES}
          onMapError={setMapError}
        />
      </Suspense>

      <h1 className="title">Real-Time Fleet Analytics</h1>

      <MetricsPanel
        stats={stats}
        speedHistory={speedHistory}
        status={stream.status}
        connectMs={stream.connectMs}
        updatesPerSec={stream.updatesPerSec}
        fps={fps}
        zones={zones.data?.zones ?? NO_ZONES}
        zonesError={zones.error}
      />

      <DlqMonitor
        metrics={dlq.data}
        metricsError={dlq.error}
        health={health.data}
        healthError={health.error}
      />

      {mapError && (
        <div className="banner" role="alert" data-testid="map-error">
          Base map: {mapError}. Set VITE_MAP_STYLE_URL (see README).
        </div>
      )}
    </main>
  );
}
