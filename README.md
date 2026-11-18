# Pi MCP Events

An implementation of the **accepted MCP Events proposal, [SEP-3415 pinned at `60b15c1`](https://github.com/pja-ant/modelcontextprotocol/blob/60b15c16f2e7265ea34ac51099dc5be62f734692/seps/3415-events-extension.md)**, for [Pi](https://pi.dev). Targets MCP revision `2026-07-28`; the proposal is still open and may change before it is final. Receive events through polling, streaming, or signed webhooks. Events arrive while Pi works and are delivered in batches when it's ready for a follow-up turn.

Read [The dream of real-time MCP (and what stands in the way)](https://www.sanity.io/engineering/the-dream-of-real-time-mcp?utm_source=github&utm_medium=readme&utm_campaign=pi-mcp-events) on the Sanity engineering blog for the product drivers behind this extension, and what we learned along the way. [Watch the demo video (MP4)](https://cdn.sanity.io/media-libraries/al34RQcBfuD7/videos/7ef17ab43f3386c047e687ad54d3ba978df099c6.mp4).

## Get started

Requires Node.js 24+, Pi, and an MCP server that supports Events.

```sh
pi install npm:@sanity-labs/pi-mcp-events
```

Add your server to `.pi/mcp-events.json` in the project where you run Pi:

```json
{
  "servers": {
    "events": {"transport": "http", "url": "https://events.example.com/mcp"}
  }
}
```

Replace the URL, then start Pi or run `/reload`. Ask Pi what events the server offers and tell it what to watch. Watches can start, change, and stop in conversation. The [weather demo](#try-the-demo) gives you a server to try this with.

## Try the demo

The included weather server simulates Harbor and Hilltop cycling through rain, storms, and recovery. Install from source and start it:

```sh
git clone https://github.com/sanity-labs/pi-mcp-events.git
cd pi-mcp-events
npm ci
pi install "$PWD"
npm run dev:weather
```

In another terminal, create a scratch project and connect Pi to the server:

```sh
demo_dir=$(mktemp -d)
mkdir -p "$demo_dir/.pi"
cat > "$demo_dir/.pi/mcp-events.json" <<'JSON'
{
  "servers": {
    "weather": {"transport": "http", "url": "http://127.0.0.1:3015/mcp"}
  }
}
JSON
cd "$demo_dir"
pi
```

Ask Pi:

> You are the island's radio presenter. Watch weather changes and storm alerts for both stations. Give brief updates when conditions change, and announce storm warnings and all-clears. Skip routine readings.

Then try these requests one at a time:

- “What am I watching?”
- “Change both watches to Harbor only.”
- “Watch storm alerts for another five minutes.”
- “Stop watching weather changes.”

Pi keeps receiving events while you work. It delivers them when it is ready for a follow-up turn.

## Using watches

### Behavior

Ask Pi to watch the events you care about, narrow them to the filters the server offers, or watch for a duration. Pi uses streaming when available and polling otherwise. Webhooks need an HTTPS receiver; see [configuration](#webhooks).

You can also work with watches through the `/mcp-events` command:

| Command | What it does |
| --- | --- |
| `/mcp-events status` | Show watches and their status. |
| `/mcp-events stop <watchId>` | Stop one watch. |
| `/mcp-events stop` | Stop all watches. |
| `/mcp-events start <watchId>` | Restart a stopped or failed watch. |

### Things to know

Saved sessions remember watches and changes, including stops, across reload and resume. New sessions start with any [declared defaults](#start-watches-automatically). With `--no-session`, conversational watches and changes end on reload or exit, and declared defaults start again on reload.

After a disconnect, Pi tries to catch up where the server supports replay. Delivery is best effort: history can be missing, and Pi warns about gaps or incomplete catch-up. Stopping a watch leaves events already received queued for delivery.

> Note: event data becomes part of the conversation and may be sent to your model provider. Saved sessions also store it locally.

## Configuration

Edit `.pi/mcp-events.json`, then run `/reload`. Set `PI_MCP_EVENTS_CONFIG` to use another file. Relative paths resolve from the config file's directory.

### Connect servers

An HTTP server with a token, alongside a local weather process that Pi starts for you:

```json
{
  "servers": {
    "events": {
      "transport": "http",
      "url": "https://events.example.com/mcp",
      "headers": {"Authorization": {"env": "EVENTS_TOKEN", "prefix": "Bearer "}}
    },
    "weather": {
      "transport": "stdio",
      "command": "node",
      "args": ["dev/weather-server.mjs", "--transport", "stdio"],
      "cwd": "/absolute/path/to/pi-mcp-events"
    }
  }
}
```

Set `EVENTS_TOKEN` in the shell that runs Pi, and replace the URL and checkout path. The local weather process needs no separate server terminal.

### Start watches automatically

To watch Harbor alerts in every new session, add this alongside `servers`:

```json
{
  "watches": [
    {
      "id": "harbor-alerts",
      "server": "weather",
      "name": "weather.alert",
      "mode": "push",
      "arguments": {"stationId": "harbor"}
    }
  ]
}
```

Pi can manage declared watches through conversation too. Changes apply to the session; the JSON file sets the defaults for new sessions. Saved changes survive `/reload`, so start a new session to use the declared defaults again.

### Webhooks

To receive events at a public HTTPS address, add a receiver alongside `servers`:

```json
{
  "webhook": {
    "publicUrl": "https://your-callback.example.com/hooks",
    "host": "127.0.0.1",
    "port": 18081
  }
}
```

Forward that URL and its subpaths to `127.0.0.1:18081`, preserving the path and request body. Then ask Pi to use webhook delivery for an event that supports it. For direct HTTPS, add `keyFile` and `certFile` to `webhook`.

Saved event state normally lives in `~/.pi/agent/mcp-events`; top-level `stateDir` changes that location. See [client options](src/options.ts) and [receiver options](src/webhook.ts) for additional settings.

## Development

Tested with Pi 1.5.2. From the checkout, run the checks (requires `openssl`):

```sh
npm run validate
```

This is an early release. [Report an issue](https://github.com/sanity-labs/pi-mcp-events/issues/new) if something goes wrong.
