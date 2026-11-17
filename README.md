# Pi MCP Events

An implementation of the **accepted MCP Events proposal, [SEP-3415 pinned at `60b15c1`](https://github.com/pja-ant/modelcontextprotocol/blob/60b15c16f2e7265ea34ac51099dc5be62f734692/seps/3415-events-extension.md)**, for [Pi](https://pi.dev). Targets MCP revision `2026-07-28`; the proposal is still open and may change before it is final. Receive events through polling, streaming, or signed webhooks. Events arrive while Pi works and are delivered in batches when it's ready for a follow-up turn.

Read [The dream of real-time MCP (and what stands in the way)](https://www.sanity.io/engineering/the-dream-of-real-time-mcp?utm_source=github&utm_medium=readme&utm_campaign=pi-mcp-events) on the Sanity engineering blog for the product drivers behind this extension, and what we learned along the way. [Watch the demo video (MP4)](https://cdn.sanity.io/media-libraries/al34RQcBfuD7/videos/7ef17ab43f3386c047e687ad54d3ba978df099c6.mp4).

## Requirements

- Node.js v24+
- An existing Pi installation
- To run integration tests: `openssl`

## Install

```sh
pi install npm:@sanity-labs/pi-mcp-events
```

To install from source and run the demo:

```sh
git clone git@github.com:sanity-labs/pi-mcp-events.git
cd pi-mcp-events
npm ci
pi install "$PWD"
```

`npm ci` builds the extension. Start a new Pi session or use `/reload` after installation. 

- For source installs, pull the reviewed changes and run `npm ci` again.
- To remove an npm installation, run `pi remove npm:@sanity-labs/pi-mcp-events`. For a source installation, use `pi remove /absolute/path/to/pi-mcp-events`.

## Try the demo

`pi-mcp-events` comes with a built-in MCP Events-enabled server that emits weather events. To try it out:

### Start the demo server

From this checkout:

```sh
npm run dev:weather
```

It serves `http://127.0.0.1:3015/mcp` and simulates weather on a fictional island. Harbor and Hilltop follow different timelines through clouds, rain, a storm, and recovery. Readings arrive in bursts of one to three, with random pauses of 10–20 seconds. The first storm warning arrives after 20 seconds; warnings and all-clears then alternate between the stations every 45 seconds. Each warning stays active for 90 seconds, and the weather cycle repeats every three minutes.

### Add the presenter prompt

Open another terminal and start Pi in a fresh scratch folder:

```sh
demo_dir=$(mktemp -d)
cd "$demo_dir"
pi
```

Without `.pi/mcp-events.json`, the extension stays idle. The `pi install` step above makes it available here.

Submit this prompt before turning on the feed:

```text
You are the island's radio presenter. Give brief updates when weather conditions change,
and announce storm warnings and all-clears. Skip routine readings.
I'll enable the weather feed next. Wait for its events.
```

### Turn on MCP Events

Paste this shell command into Pi to create the config:

```sh
!mkdir -p .pi && cat > .pi/mcp-events.json <<'JSON'
{
  "servers": {
    "weather": {"transport": "http", "url": "http://127.0.0.1:3015/mcp"}
  },
  "watches": [
    {"id": "readings", "server": "weather", "name": "weather.reading", "mode": "push"},
    {"id": "alerts", "server": "weather", "name": "weather.alert", "mode": "poll"}
  ]
}
JSON
```

Submit `/reload` on its own in the same Pi conversation:

```text
/reload
```

After reload completes, check the watches with a separate command:

```text
/mcp-events status
```

| Event | Payload |
| --- | --- |
| `weather.reading` | Station, condition, temperature (°C), wind (km/h), rain (mm/h), and daylight. |
| `weather.changed` | Current reading and previous condition. |
| `weather.alert` | Current reading, a storm warning message, and `raised` or `cleared` status. |

All three support `poll` and `push`, with optional `"arguments": {"stationId": "harbor"}` or `"arguments": {"stationId": "hilltop"}` to follow one station. Change the configuration and `/reload` to try another event or mode. Replay keeps the last 1,000 events in memory; stale or pre-restart cursors report a gap.

The HTTP demo uses the proposal’s `x-mcp-header` annotation for the station filter. The client reads it from the catalog and sends `Mcp-Param-Station` automatically.

### Run the demo over stdio

Pi can start the weather server as a local process. Replace the `weather` entry under `servers` with:

```json
{
  "transport": "stdio",
  "command": "node",
  "args": ["dev/weather-server.mjs", "--transport", "stdio"],
  "cwd": "/absolute/path/to/pi-mcp-events"
}
```

Set `cwd` to your checkout's absolute path. Keep the same watches and presenter prompt, then run `/reload`. Pi starts and stops its own weather process; no separate server terminal or HTTP port is needed. Poll and push use the same weather timeline, station filters, and in-memory replay as the HTTP demo. Restarting the process loses its history; retained cursors report a gap.

HTTP remains the default for `npm run dev:weather`. The webhook walkthrough below uses HTTP.

### Try local webhook delivery

The weather server can also send signed webhooks to Pi over local HTTPS. Stop the server from the earlier demo with Ctrl-C first: the webhook demo starts its own server on port 3015 with webhook delivery enabled.

From the checkout, run the following in one shell. It creates a temporary certificate and token, starts the server, and opens Pi with a webhook watch:

```sh
weather_dir=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$weather_dir/key.pem" -out "$weather_dir/cert.pem" \
  -subj '/CN=127.0.0.1' -addext 'subjectAltName=IP:127.0.0.1'
export WEATHER_WEBHOOK_TOKEN="$(openssl rand -hex 32)"
export WEATHER_WEBHOOK_CA="$weather_dir/cert.pem"
export WEATHER_WEBHOOK_LOCAL_ORIGIN='https://127.0.0.1:18081'
node dev/weather-server.mjs &
weather_pid=$!
mkdir -p "$weather_dir/.pi"
cat > "$weather_dir/.pi/mcp-events.json" <<JSON
{
  "servers": {"weather": {"transport": "http", "url": "http://127.0.0.1:3015/mcp", "headers": {"Authorization": {"env": "WEATHER_WEBHOOK_TOKEN", "prefix": "Bearer "}}}},
  "webhook": {"publicUrl": "https://127.0.0.1:18081/hooks", "host": "127.0.0.1", "port": 18081, "keyFile": "$weather_dir/key.pem", "certFile": "$weather_dir/cert.pem"},
  "watches": [{"id": "alerts", "server": "weather", "name": "weather.alert", "mode": "webhook"}]
}
JSON
(cd "$weather_dir" && pi)
kill "$weather_pid"
rm -rf "$weather_dir"
```

The server accepts webhook subscriptions only with the token. The loopback callback and its certificate are explicitly trusted for this demo. Subscriptions and weather history live in memory, and the server grants finite lifetimes; restarting can produce a replay gap.

To trigger events yourself, start with `npm run dev:weather -- --interval 0`, then advance one reading at a time, alternating Harbor and Hilltop:

```sh
curl -X POST http://127.0.0.1:3015/advance -H 'Content-Type: application/json' -d '{}'
```

## Getting started

Create `.pi/mcp-events.json` in the project where you run Pi:

```json
{
  "servers": {
    "example": {
      "transport": "http",
      "url": "https://events.example.com/mcp"
    }
  },
  "watches": [
    {
      "id": "changes",
      "server": "example",
      "name": "example.changed",
      "mode": "push",
      "arguments": {"channel": "demo"}
    }
  ]
}
```

Replace the example URL, event name, and arguments with values from your MCP server's Events catalog. The server must support era `2026-07-28`, advertise the Events extension, and support the chosen delivery mode. Start Pi and run `/mcp-events status`. When the server emits a matching event, Pi displays it in the conversation and schedules a follow-up turn. Event payloads are external data. Pi can respond using your existing instructions and permissions; instructions inside payloads do not change them.

### Configuring events subscriptions

Received event timestamps and webhook expiry/status timestamps accept `Z`, `±HH:MM`, and `±HHMM` timezone offsets, including `+0000` and `+0530`. The client validates the date, time, and offset and preserves the received string. Timestamps must include a timezone; local times and hour-only offsets are rejected. Payload fields follow the server's JSON Schema, including its `date-time` format rules.

The extension starts configured watches automatically. With no configuration file, it stays idle. Set `PI_MCP_EVENTS_CONFIG` to use a different file. After changing configuration, run `/reload`.

## Commands

| Command | What it does |
| --- | --- |
| `/mcp-events` or `/mcp-events status` | Show each watch's server, event, transport, and status, plus persistence mode, state location, and pending count. |
| `/mcp-events start <watchId>` | Start a stopped or failed configured watch without restarting other watches. Starting a running watch has no effect. |
| `/mcp-events stop <watchId>` | Stop the watch with the configured `id` (for example, `changes`). |
| `/mcp-events stop` | Stop all watches and close the extension's connections. |

Stopped and failed watches remain visible for the active Pi session, including after stopping all watches. Use `/mcp-events start <watchId>` to start one again with its retained cursor. Replay depends on the server and available history; missed events may be unavailable or reported as a gap. Webhook watches subscribe again and renew under the server's granted lifetime. Use `/reload` to reread configuration and restart all configured watches.

## Configuration

Each watch needs 

- a unique `id`
- a configured `server`
- an event `name`
- a `mode`: `poll`, `push`, or `webhook`

`arguments` defaults to `{}` and must satisfy the server's input schema. An event, its arguments, and a mode combine to form a unique configuration: duplicate watches can't be configured on one server.

Schemas can be JSON Schema objects or booleans. If this client cannot use an event's schemas, that event is unavailable; other events remain usable. `/mcp-events status` lists unavailable event types. After the server repairs a schema, use `/mcp-events start <watchId>` to retry the affected watch.

For a local MCP process, a server entry looks like this:

```json
{
  "transport": "stdio",
  "command": "node",
  "args": ["./server.mjs"],
  "cwd": ".."
}
```

Relative `cwd`, `stateDir`, and TLS file paths resolve from the configuration file's directory.

### Delivery modes

| Mode | Requirements and behavior |
| --- | --- |
| `poll` | Server advertises polling. Follows server polling hints and drains available pages; resumes from a cursor when supported. |
| `push` | Server advertises streaming. Receives correlated notifications, checks heartbeat liveness, and reconnects with its cursor. |
| `webhook` | Server advertises webhooks and can reach your HTTPS callback. Verifies signed requests, renews subscriptions, and attempts unsubscribe during shutdown. |

### General delivery guidance

- Each event type advertises its own replay capability and supported delivery modes.
- Set a watch's `maxEvents` to bound requested polling batches (default 50).
- `ttlMs` requests a lifetime where supported; the server decides the grant.

>With `pi --no-session`, webhook subscriptions always have an expiry, so the server can clean them up if Pi exits unexpectedly. Setting `ttlMs: null` does **not** disable expiry in this mode.

### Polling

Polling drains available pages immediately while the cursor advances or new events arrive. If a server repeatedly claims more events are available without making progress, or returns `ResourceExhausted`, the watch backs off with jitter. Retry delays start at the larger of `retryMs` and `pollFloorMs`, double up to 30 seconds (or a higher configured floor), and stop after `maxRetries`. Successful pages reset the retry budget. The watch retains its cursor; use `/mcp-events start <watchId>` to restart it after retries are exhausted.

### Push streams

If a connection fails, the client finishes delivering events it has already received before reconnecting. This applies to disk-backed and in-memory sessions, including event types with no replay cursor. Explicitly stopping a watch cancels further delivery.

Repeated short-lived streams reconnect with exponential backoff and jitter, capped at 30 seconds and bounded by `maxRetries`. The retry budget resets after accepted events or heartbeats span two configured heartbeat intervals, including on quiet streams. The client suggests a 30-second heartbeat interval by default (`heartbeatMs`) and uses the server’s granted interval when present. Any stream notification refreshes liveness; for older servers that omit the grant, it uses the suggested interval.

### Webhooks

Add a top-level `webhook` section and set the watch's `mode` to `webhook`:

```json
{
  "webhook": {
    "publicUrl": "https://your-callback.example.com/hooks",
    "host": "127.0.0.1",
    "port": 18081
  }
}
```

Replace `publicUrl` with your HTTPS origin and path. Forward it to your local receiver, preserving the path and raw request body. `pi-mcp-events` adds a unique route beneath `publicUrl` for each subscription, creates signing secrets, and verifies webhook signatures. Saved sessions restore their callback routes and signing secrets after restart to renew the same remote subscriptions. These secrets are stored in private state files. Lease renewal continues while event delivery or checkpointing is slow.

>The public URL must not contain credentials, a query, or a fragment.

Without TLS files, the receiver must bind to loopback behind your HTTPS proxy or tunnel. For direct HTTPS, provide both `keyFile` and `certFile` in the webhook section.

### Limits

Override `pi-mcp-events` preset limits in a top-level `client` object when needed:

| Setting | Default |
| --- | --- |
| `maxSubscriptions` | 32 per server |
| `maxCatalogEntries` / `maxCatalogPages` | 1,000 / 100 |
| `maxPollBytes` | 262,144 bytes |
| `maxTrackedEventIds` | 4,096 per watch |
| `maxRetries` / `retryMs` | 10 / 1,000 ms initial delay |
| `pollFloorMs` | 1,000 ms |

Webhook receiver limits:

| Setting | Default |
| --- | --- |
| `maxRegistrations` | 32 |
| `maxBodyBytes` | 262,144 |
| `maxPendingDeliveries` | 16 |

See [client options](src/options.ts) and [webhook options](src/webhook.ts) for the full configuration surface.

## Caveats

### Best-effort delivery

Accepted events remain in a bounded inbox, saved to disk or held in memory, until handed to Pi's conversation. Duplicate deliveries are suppressed within bounded tracking windows. Crashes, replay gaps, and exhausted limits can still produce duplicates or loss.

>Conversation handoff is not a guarantee that Pi has flushed the transcript to disk or completed an action.

### Session persistence

#### Agent-directory state file

Saved sessions keep event state under Pi's agent directory, normally `~/.pi/agent/mcp-events`. `PI_CODING_AGENT_DIR` changes that base directory. Set top-level `stateDir` for a separate location: `/mcp-events status` shows the resolved path. Use one process per saved session/state file.

#### In-memory via `--no-session`

With `--no-session`, event state stays in memory. Reconnects retain progress during the active session. Session replacement or process exit discards pending events and cursors. Subscriptions stop with the active session. After a crash, remote webhook subscriptions expire according to their finite server grant. Existing state files are left untouched.

#### State file issues and graceful degradation

If state files are corrupt, unreadable, or unwritable, the extension shows one conversation-visible warning and continues in memory where possible. Pending events and resume progress remain in memory for the duration of the problematic state file session: for example, if state file or `stateDir` permissions are at fault, fixing them does **not** take effect until the next session. Old disk state can replay after restart, and new state is not backfilled.

>`pi-mcp-events` fails open by default: ordinary Pi tools should remain usable even when extension dependencies fail.

### Event data and model providers

Event payloads become part of Pi's conversation and may be sent to the configured model provider. **Saved sessions also persist events locally, just as ordinary session turns preserve user/assistant messages.** Choose subscriptions and a model appropriate for that data.

## Compatibility and development

The package pins its own MCP client dependency and does not require the host to upgrade its MCP SDK. Stock Pi 1.5.2 is the tested host.

>Wildcard peer dependencies are not a claim that every Pi version is qualified: YMMV.

```sh
npm ci
npm run validate
```

Builds and type checks use TypeScript 7 through the `@typescript/native` npm alias. ESLint uses the TypeScript 6 API through the `typescript` compatibility alias, following [Microsoft's side-by-side setup](https://devblogs.microsoft.com/typescript/announcing-typescript-7-0/#running-side-by-side-with-typescript-6-0).

## Stability

`pi-mcp-events` is an early release. Expect public APIs to change frequently. The accepted MCP Events proposal may still change before it is final.

## Feedback

[Open an issue](https://github.com/sanity-labs/pi-mcp-events/issues/new) to report a bug, suggest an improvement, or share feedback.
