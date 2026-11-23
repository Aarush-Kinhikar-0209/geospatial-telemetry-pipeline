import { useEffect, useMemo, useRef, useState } from 'react';
import { sortVehicles, type SortDirection, type SortKey, type TrackedVehicle } from '../lib/fleet';

/** The fleet streams ~10 updates/s; re-sorting that often makes rows impossible to read. */
const REFRESH_MS = 1000;

const FIRST_DIRECTION: Record<SortKey, SortDirection> = { speed: 'desc', id: 'asc' };

interface Sort {
  key: SortKey;
  direction: SortDirection;
}

interface SpeedDialogProps {
  vehicles: readonly TrackedVehicle[];
  onClose: () => void;
}

function ariaSort(sort: Sort, key: SortKey): 'ascending' | 'descending' | 'none' {
  if (sort.key !== key) return 'none';
  return sort.direction === 'asc' ? 'ascending' : 'descending';
}

function SortHeader(props: {
  label: string;
  sortKey: SortKey;
  sort: Sort;
  onSort: (key: SortKey) => void;
  align?: 'right';
}) {
  const { label, sortKey, sort, onSort, align } = props;
  const active = sort.key === sortKey;
  return (
    <th scope="col" aria-sort={ariaSort(sort, sortKey)} className={align === 'right' ? 'num' : undefined}>
      <button type="button" onClick={() => onSort(sortKey)} data-testid={`sort-${sortKey}`}>
        {label}
        <span className="sort-arrow" aria-hidden="true">
          {active ? (sort.direction === 'asc' ? '▲' : '▼') : '↕'}
        </span>
      </button>
    </th>
  );
}

/** Modal list of every vehicle's latest speed, sortable by speed or vehicle ID. */
export function SpeedDialog({ vehicles, onClose }: SpeedDialogProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const latest = useRef(vehicles);
  latest.current = vehicles;

  const [snapshot, setSnapshot] = useState(vehicles);
  const [sort, setSort] = useState<Sort>({ key: 'speed', direction: 'desc' });

  // showModal() gives focus trapping, Esc-to-close and focus restore for free.
  useEffect(() => {
    const dialog = dialogRef.current;
    if (dialog && !dialog.open) dialog.showModal();
  }, []);

  useEffect(() => {
    const timer = setInterval(() => setSnapshot(latest.current), REFRESH_MS);
    return () => clearInterval(timer);
  }, []);

  const rows = useMemo(
    () => sortVehicles(snapshot, sort.key, sort.direction),
    [snapshot, sort.key, sort.direction],
  );

  const toggleSort = (key: SortKey) =>
    setSort((current) =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: FIRST_DIRECTION[key] },
    );

  return (
    <dialog
      ref={dialogRef}
      className="speed-dialog"
      aria-labelledby="speed-dialog-title"
      onClose={onClose}
      onClick={(event) => {
        // Only the backdrop reports the dialog itself as the click target.
        if (event.target === event.currentTarget) dialogRef.current?.close();
      }}
      data-testid="speed-dialog"
    >
      <div className="speed-dialog-body">
        <header className="panel-header">
          <h2 id="speed-dialog-title">Vehicle speeds</h2>
          <button
            type="button"
            className="dialog-close"
            onClick={() => dialogRef.current?.close()}
            aria-label="Close"
            data-testid="speed-close"
          >
            ✕
          </button>
        </header>

        {rows.length === 0 ? (
          <p className="caption" data-testid="speed-empty">
            No vehicles reporting. Start a simulation to see live speeds.
          </p>
        ) : (
          <div className="speed-scroll">
            <table className="speed-table">
              <thead>
                <tr>
                  <SortHeader label="Vehicle ID" sortKey="id" sort={sort} onSort={toggleSort} />
                  <SortHeader label="Speed (km/h)" sortKey="speed" sort={sort} onSort={toggleSort} align="right" />
                </tr>
              </thead>
              <tbody>
                {rows.map((vehicle) => (
                  <tr key={vehicle.vehicle_id} data-testid="speed-row">
                    <th scope="row">{vehicle.vehicle_id}</th>
                    <td className="num">{vehicle.speed_kmh.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <p className="caption">
          {rows.length} {rows.length === 1 ? 'vehicle' : 'vehicles'} · latest reading each · refreshes every
          second
        </p>
      </div>
    </dialog>
  );
}
