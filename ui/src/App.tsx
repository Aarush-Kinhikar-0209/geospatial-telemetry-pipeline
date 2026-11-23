import { lazy, Suspense, useMemo, useState } from 'react';
import { DlqMonitor } from './components/DlqMonitor';
import { MetricsPanel, type FeedStatus } from './components/MetricsPanel';
import { SimulationPanel } from './components/SimulationPanel';
import { useFps } from './hooks/useFps';
import { useDlqMetrics, useHealth } from './hooks/usePipeline';
import { useSampledHistory } from './hooks/useSampledHistory';
import { useSimulation } from './hooks/useSimulation';
import { useVehicleStream } from './hooks/useVehicleStream';
import { fleetStats, type TrackedVehicle } from './lib/fleet';

// deck.gl + MapLibre are most of the bundle; loading them lazily lets the SSE
// stream and panels start while the map code is still downloading.
const FleetMap = lazy(() =>
  import('./components/FleetMap').then((module) => ({ default: module.FleetMap })),
);

const NO_VEHICLES: TrackedVehicle[] = [];

export default function App() {
  const simulation = useSimulation();
  const simStatus = simulation.state?.status ?? null;
  const running = simStatus === 'running';

  // A new run reconnects the stream with an empty store (see useVehicleStream).
  const stream = useVehicleStream(undefined, simulation.state?.run_id ?? null);
  const dlq = useDlqMetrics();
  const health = useHealth();
  const fps = useFps();
  const [mapError, setMapError] = useState<string | null>(null);

  // Outside a run the map and stats are empty: whatever the stream still holds is
  // from a run whose data is being (or has been) deleted.
  const vehicles = running ? stream.vehicles : NO_VEHICLES;
  const feedStatus: FeedStatus = simStatus === null ? 'connecting' : running ? stream.status : simStatus;

  const stats = useMemo(() => fleetStats(vehicles), [vehicles]);
  const speedHistory = useSampledHistory(stats.avgSpeedKmh, running);

  return (
    <main className="app">
      <Suspense fallback={null}>
        <FleetMap vehicles={vehicles} onMapError={setMapError} />
      </Suspense>

      <h1 className="title">Real-Time Fleet Analytics</h1>

      <div className="sidebar">
        <SimulationPanel simulation={simulation} />
        <MetricsPanel
          stats={stats}
          vehicles={vehicles}
          speedHistory={speedHistory}
          status={feedStatus}
          connectMs={stream.connectMs}
          updatesPerSec={stream.updatesPerSec}
          fps={fps}
        />
      </div>

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
