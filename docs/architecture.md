# Components and extension API

This is one package with four layers. The initial commits extract the previously tested prototype into reviewable components; they do not claim a new protocol implementation was written from scratch in that sequence.

| Component | Owns | Isolated checks |
| --- | --- | --- |
| `protocol.ts`, `validation.ts`, `wire.ts` | Pinned schemas, capability negotiation, routing headers, stream correlation and cancellation | `protocol.test.ts`, `wire.test.ts` |
| `client.ts`, `webhook.ts` | Discovery, watch lifecycle, poll/push/webhook delivery, retries, signature verification | `client.test.ts`, `webhook.test.ts`, `recovery.test.ts`, `review-regressions.test.ts` |
| `config.ts`, `inbox.ts`, `handoff.ts` | Configuration, bounded persistence, cursors, confirmed Pi handoff | `config.test.ts`, `session.test.ts` |
| `extension.ts`, `pi-contract.ts` | Pi lifecycle, commands, batching, observation bus | `extension.test.ts`, packed stock-Pi integration |

The protocol and delivery client have no Pi runtime dependency. Session handoff uses Pi types and public session accessors. The adapter uses Pi's public lifecycle, message, command, and event-bus APIs. No Pi fork, monkeypatch, or application-specific tools are required.

## Client API

```ts
import {EventsClient, EventsWire, createHttpTransport} from '@sanity-labs/pi-mcp-events';

const client = new EventsClient(new EventsWire(() =>
  createHttpTransport(new URL('https://example.org/mcp'), {
    requestInit: {headers: {Authorization: `Bearer ${process.env.MCP_TOKEN}`}},
  }),
));
try {
  await client.connect();
  console.log(client.list());
  const watch = client.watch({name: 'example.changed', arguments: {channel: 'demo'}, mode: 'poll'},
    observation => { console.log(observation); },
  );
  await watch.ready;
  // Keep the client alive for the consumer's lifetime, then call watch.stop().
  await watch.closed;
} finally {
  await client.close();
}
```

`watch.ready` resolves when delivery is active. `watch.closed` rejects on terminal failure. An optional third callback persists cursor progress after accepted observations. Rejecting an observation applies backpressure; delivery retries are bounded. Applications own their storage and client lifetime. Webhook users supply a `WebhookReceiver` in `ClientOptions` and close it after closing their clients.

Exports and Pi bus types are experimental during private iteration. Import documented package paths rather than internal files. HTTP and stdio use the official MCP client; the wire adapter handles draft extension streams that the ordinary request API does not expose as event iterators.

## Separate Pi extensions

```ts
import type {ExtensionAPI} from '@earendil-works/pi-coding-agent';
import {OBSERVATION_EVENT, type PiEventObservation} from '@sanity-labs/pi-mcp-events/pi';

export default function applicationExtension(pi: ExtensionAPI) {
  let unsubscribe: (() => void) | undefined;
  pi.on('session_start', () => {
    unsubscribe?.();
    unsubscribe = pi.events.on(OBSERVATION_EVENT, raw => {
      const value = raw as PiEventObservation;
      if (value.observation.type !== 'event') return;
      // Handle the observation according to this extension's own user authorization.
    });
  });
  pi.on('session_shutdown', () => { unsubscribe?.(); });
}
```

The bus payload contains `server`, `watch`, and `observation`. Observations are events, gaps, or status changes. Bus delivery is immediate after inbox acceptance, can occur while Pi is busy, and can repeat. It is not a durable subscriber API; restored inbox batches go to the conversation, not back through this bus. Application extensions should deduplicate when needed and must not treat server payloads as instructions.

Aperture/presence integrations belong in their own packages. Mellon History-backed polling is unrelated server work. Neither adds responsibilities to this package.
