import type { PickingInfo } from '@deck.gl/core';
import { ScatterplotLayer } from '@deck.gl/layers';
import DeckGL from '@deck.gl/react';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Map } from 'react-map-gl/maplibre';
import { INITIAL_VIEW_STATE, MAP_STYLE_URL, POSITION_TRANSITION_MS } from '../config';
import { speedColor, type TrackedVehicle } from '../lib/fleet';

interface FleetMapProps {
  vehicles: TrackedVehicle[];
  onMapError?: (message: string) => void;
}

function tooltip({ object }: PickingInfo<TrackedVehicle>) {
  if (!object) return null;
  return `${object.vehicle_id}\n${object.speed_kmh.toFixed(1)} km/h · battery ${object.battery_pct}%`;
}

/** Presentation only: receives data from hooks, owns no fetching. */
export function FleetMap({ vehicles, onMapError }: FleetMapProps) {
  const layers = [
    new ScatterplotLayer<TrackedVehicle>({
      id: 'vehicles',
      data: vehicles,
      getPosition: (v) => [v.lon, v.lat],
      getFillColor: (v) => speedColor(v.speed_kmh),
      getLineColor: [15, 23, 42, 200],
      getRadius: 70,
      radiusMinPixels: 3,
      radiusMaxPixels: 10,
      lineWidthMinPixels: 1,
      stroked: true,
      pickable: true,
      transitions: {
        getPosition: { duration: POSITION_TRANSITION_MS, type: 'interpolation' },
        getFillColor: POSITION_TRANSITION_MS,
      },
    }),
  ];

  return (
    <DeckGL
      initialViewState={INITIAL_VIEW_STATE}
      controller
      layers={layers}
      getTooltip={tooltip}
    >
      <Map
        mapStyle={MAP_STYLE_URL}
        reuseMaps
        onError={(event) => onMapError?.(event.error?.message ?? 'Base map failed to load')}
      />
    </DeckGL>
  );
}
