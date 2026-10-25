# Pi MCP Events

An implementation of the **MCP Events specification draft, [SEP-3415 pinned at `b0efd116`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/b0efd116faca84c9caad109ccf0778da4dc49579/seps/0000-events-extension.md)**, for [Pi](https://pi.dev). Targets MCP revision `2026-07-28`; compatibility can change as the draft evolves.

Receive events through polling, streaming, or signed webhooks. Events arrive while Pi works and are delivered in batches when it can take a follow-up turn.

## Install

Use Node 24 or newer and an existing Pi installation. The tested baseline is Node 24.21.0 and Pi 1.4.0.

```sh
git clone git@github.com:sanity-labs/pi-mcp-events.git
cd pi-mcp-events
npm ci
pi install "$PWD"
```

`npm ci` builds the extension. Start a new Pi session or use `/reload` after installation. To update, pull the reviewed changes and run `npm ci` again. To remove it, run `pi remove /absolute/path/to/pi-mcp-events`.

The extension uses Pi's public APIs and owns its MCP connections. It does not reuse Pi's MCP configuration or require a Pi fork. Application tools belong in separate extensions.

## Try this first

Create `.pi/mcp-events.json` in the project where you run Pi:

```json
{
  "servers": {
    "example": {
      "transport": "http",
      "url": "https://events.example.com/mcp",
      "headers": {
        "Authorization": {"env": "EVENTS_TOKEN", "prefix": "Bearer "}
      }
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

Replace the example URL, event name, and arguments with values from your server's Events catalog. The server must support MCP `2026-07-28`, advertise the Events extension, and support the chosen delivery mode. Supply `EVENTS_TOKEN` through your environment, then start Pi and run `/mcp-events status`.

When the server emits a matching event, Pi displays it in the conversation and schedules a follow-up turn. Event content is presented as untrusted data. It does not grant permission to take actions.

The extension starts configured watches automatically. With no configuration file, it stays idle. Set `PI_MCP_EVENTS_CONFIG` to use a different file. After changing configuration, use `/reload`.

## Commands

| Command | What it does |
| --- | --- |
| `/mcp-events` or `/mcp-events status` | Show each watch's server, event, transport, and status, plus persistence mode, state location, and pending count. |
| `/mcp-events stop changes` | Stop the watch whose configured ID is `changes`. |
| `/mcp-events stop` | Stop all watches and close the extension's connections. |

Individually stopped and failed watches remain visible while the extension session is active. Stopping all watches clears that session's status. Use `/reload` to restart configured watches. An active status reflects the client's latest observation; it does not prove uninterrupted end-to-end delivery.

## Configuration

Each watch needs a unique `id`, a configured `server`, an event `name`, and a `mode`: `poll`, `push`, or `webhook`. `arguments` defaults to `{}` and must satisfy the server's input schema. The same event, arguments, and mode cannot be configured twice on one server.

HTTP servers support headers supplied directly or through `{ "env": "VARIABLE", "prefix": "optional prefix" }`. Keep credentials in environment variables. There is no interactive OAuth sign-in flow in this package.

For a local MCP process, use a server entry such as:

```json
{
  "transport": "stdio",
  "command": "node",
  "args": ["./server.mjs"],
  "cwd": "..",
  "env": {"API_TOKEN": {"env": "EVENTS_TOKEN"}}
}
```

Relative `cwd`, `stateDir`, and TLS file paths resolve from the configuration file's directory.

### Delivery modes

| Mode | Requirements and behavior |
| --- | --- |
| `poll` | Server advertises polling. Follows server polling hints and drains available pages; resumes from a cursor when supported. |
| `push` | Server advertises streaming. Receives correlated notifications, checks heartbeat liveness, and reconnects with its cursor. |
| `webhook` | Server advertises webhooks and can reach your HTTPS callback. Verifies signed requests, renews subscriptions, and attempts unsubscribe during shutdown. |

Set a watch's `maxEvents` to bound requested polling batches (default 50). `ttlMs` requests a lifetime where supported; the server decides the grant. Memory-only sessions require webhook subscriptions to expire, even if configuration asks for no expiry. Available modes and replay behavior depend on the server's event source.

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

Forward that HTTPS origin and path to the local receiver, preserving the path and raw request body. The extension adds a unique route beneath `publicUrl` for each subscription. It creates signing secrets and verifies Standard Webhooks signatures. The public URL must not contain credentials, a query, or a fragment.

Without TLS files, the receiver must bind to loopback behind your HTTPS proxy or tunnel. For direct HTTPS, provide both `keyFile` and `certFile` in the webhook section. Tunnel hosting and certificate management are your responsibility; no relay is bundled.

### Limits

Limits are implementation defaults, not requirements of the Events draft. Override them in a top-level `client` object when needed:

| Setting | Default |
| --- | --- |
| `maxSubscriptions` | 32 per server |
| `maxCatalogEntries` / `maxCatalogPages` | 1,000 / 100 |
| `maxPollBytes` | 262,144 bytes |
| `maxTrackedEventIds` | 4,096 per watch |
| `maxRetries` / `retryMs` | 10 / 1,000 ms initial delay |
| `pollFloorMs` | 1,000 ms |

Webhook receiver limits are separate: `maxRegistrations` defaults to 32, `maxBodyBytes` to 262,144, and `maxPendingDeliveries` to 16. See [client options](src/options.ts) and [webhook options](src/webhook.ts) for the full configuration surface.

## Sessions and delivery guarantees

Delivery is **best effort**. Accepted events remain in a bounded inbox until handed to Pi's conversation. Duplicate deliveries are suppressed within bounded tracking windows; crashes, replay gaps, and exhausted limits can still produce duplicates or loss. Conversation handoff is not a guarantee that Pi has flushed the transcript to disk or completed an action.

Saved sessions keep event state under Pi's agent directory, normally `~/.pi/agent/mcp-events`. `PI_CODING_AGENT_DIR` changes that base directory. Set top-level `stateDir` for a separate location; `/mcp-events status` shows the resolved path. Use one process per saved session/state file.

With **`--no-session`**, event state stays in memory. Reconnects retain progress during the active session. Session replacement or process exit discards pending events and cursors; subscriptions stop with the active session. After a crash, remote webhook subscriptions expire according to their finite server grant. Existing state files are left untouched.

If state files are corrupt, unreadable, or unwritable, the extension shows one conversation-visible warning and continues in memory where possible. Ordinary Pi tools remain usable. It stays in memory for that session; fixing permissions takes effect in a subsequent session. It does not backfill disk state automatically, and old disk state can replay after restart. In a sandbox, allow the displayed state path or accept this memory-only fallback.

Event payloads become part of Pi's conversation and may be sent to the configured model provider. Saved sessions also persist payloads locally. Choose subscriptions and a model appropriate for that data.

## Compatibility and development

The package pins its own MCP client dependency and does not require the host to upgrade its MCP SDK. Stock Pi 1.4.0 is the tested host; the wildcard peer dependency is not a claim that every Pi version is qualified.

```sh
npm ci
npm run validate
```

Validation runs type checks, unit/protocol tests, a build, and stock-Pi integration scenarios. Tests use local synthetic events, a deterministic model, and temporary certificates; install `openssl` for the integration suite. No Mellon checkout, model API credentials, or hosted tunnel is required.

The package exports the experimental client API from `@sanity-labs/pi-mcp-events`, the observation contract from `/pi`, and the Pi entrypoint from `/extension`. API changes are possible during the pilot. See [the exports](src/index.ts) and [observation contract](src/pi-contract.ts).

## Feedback

[Open an issue](https://github.com/sanity-labs/pi-mcp-events/issues/new) to report a bug, suggest an improvement, or share feedback.
