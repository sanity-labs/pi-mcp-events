# @sanity-labs/pi-mcp-events

## 0.2.0

### Minor Changes

- 8c00b77: Start, list, change, and stop event watches in a Pi conversation. Saved sessions restore watches and resume progress. Time-bound watches stop at expiry and attempt bounded catch-up after a disconnect, with warnings when events may be missing.

### Patch Changes

- dc6731a: Keep conversational changes and stops for declared watches when a saved Pi session reloads or resumes. Update watch expiry without restarting its subscription, report conflicting durations on duplicate starts, and make room for new watches by removing old stopped history.

## 0.1.3

### Patch Changes

- 3457f71: Support HTTP routing headers for event arguments annotated with `x-mcp-header`, including catalog refresh and retry when headers change.
- 03ed536: Fix push subscriptions stopping before a large replay backlog has been delivered.
  
  Simplify event cards and clarify that agents can respond using existing instructions and permissions.
- de81c54: Add a stdio option to the weather demo, with poll and push delivery.

## 0.1.2

### Patch Changes

- 9fc87e4: Use the server’s heartbeat interval to detect disconnected event streams.
- 340f64b: Spread event stream reconnections over time after a disconnect.

## 0.1.1

### Patch Changes

- fb754df: Add a demo video preview for the Pi package gallery.
- ed11ae8: Link the README to the Sanity engineering blog post about building MCP Events streaming for Pi.
- 8d6c8da: Keep client-only npm installations from installing Pi and its model providers. The Pi extension continues to use the existing host installation.

## 0.1.0

### Minor Changes

- 883f5e7: Add MCP Events polling, streaming, and signed webhook subscriptions to Pi conversations, with per-subscription status and saved or in-memory sessions.
