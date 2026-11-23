# Phase 5: Interactive Geospatial UI

## Overview
The final step is translating the backend data engineering into a compelling visual narrative. We use a modern React framework (Next.js/Vite) paired with Deck.gl for 60 FPS hardware-accelerated rendering of moving fleets.

## Implementation Details
* **Framework:** React 18+ (Next.js or Vite).
* **Mapping Engine:** Deck.gl for data visualization layers overlaid on a Base Map (Mapbox GL JS or MapLibre).
* **Layers:**
  * `IconLayer` or `ScatterplotLayer`: Represents live vehicles (coordinates updated via the FastAPI SSE stream).
* **Metrics Dashboard:** Uses a lightweight charting library (like Tremor or Recharts) floating on the screen to show total active vehicles, average fleet speed, and a flashing "DLQ Monitor" for dropped data.

## Ways to Test
1. **Render Test:** Load the application in the browser. Verify the base map loads without API key errors.
2. **Data Binding:** Ensure the vehicle markers actually move across the screen rather than piling up in one spot (validating the SSE React hook).
3. **Chaos Visualization:** Manually trigger a "bad data" spike in the generator (Phase 2) and verify the UI's DLQ monitor flashes/updates within 2 seconds.

## Quality Benchmarks
* **Framerate:** The map must maintain a rendering speed of 60 FPS (Frames Per Second) with up to 1,000 simultaneous active vehicle markers.
* **Time to Interactive (TTI):** The initial UI load and websocket/SSE connection should complete in $< 1 \text{ second}$.
* **Memory Leak Prevention:** Leaving the browser tab open for 30 minutes must not crash the browser tab (ensuring old SSE state is properly garbage-collected).

## Acceptance Criteria
- [ ] React components cleanly separate mapping logic from data-fetching (custom hooks for SSE).
- [ ] Vehicle markers transition smoothly between coordinate updates (using Deck.gl's `transitions` prop).
- [ ] A dedicated UI element visualizes the health of the pipeline (specifically highlighting the DLQ routing).
- [ ] README includes instructions for adding a Mapbox/MapLibre API key locally.