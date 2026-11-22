import type { PickingInfo } from '@deck.gl/core';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
import { ScatterplotLayer } from '@deck.gl/layers';
import DeckGL from '@deck.gl/react';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { INITIAL_VIEW_STATE, MAP_STYLE_URL, POSITION_TRANSITION_MS } from '../config';
import { speedColor, zoneColor, type TrackedVehicle } from '../lib/fleet';
import type { Zone } from '../types';

interface FleetMapProps {
  vehicles: TrackedVehicle[];
  zones: Zone[];
  onMapError?: (message: string) => void;
}

function tooltip({ object, layer }: PickingInfo<TrackedVehicle | Zone>) {
  if (!object) return null;
  if (layer?.id === 'vehicles') {
    const v = object as TrackedVehicle;
    return `${v.vehicle_id}\n${v.speed_kmh.toFixed(1)} km/h · battery ${v.battery_pct}%`;
  }
  const z = object as Zone;
  return `Zone ${z.h3_index}\n${z.vehicles} vehicles · ${z.events} events (15 min)\navg ${z.avg_speed_kmh.toFixed(1)} km/h`;
}

/** Presentation only: receives data from hooks, owns no fetching. */
export function FleetMap({ vehicles, zones, onMapError }: FleetMapProps) {
  const busiest = useMemo(() => zones.reduce((max, z) => Math.max(max, z.vehicles), 1), [zones]);

  const layers = [
    new H3HexagonLayer<Zone>({
      id: 'zones',
      data: zones,
      getHexagon: (z) => z.h3_index,
      getFillColor: (z) => zoneColor(z.vehicles / busiest),
      getLineColor: [255, 255, 255, 80],
      lineWidthMinPixels: 1,
      extruded: false,
      pickable: true,
      updateTriggers: { getFillColor: busiest },
    }),
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
