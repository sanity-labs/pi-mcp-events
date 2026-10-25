import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, type Observation} from '../src/client.ts';
import {WebhookReceiver} from '../src/webhook.ts';
import {createHttpTransport, EventsWire} from '../src/wire.ts';

const connectionPath = process.env.LOCAL_EVENTS_CONNECTION_FILE;
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

describe.skipIf(!connectionPath)('local Mellon protocol integration', () => {
  it('delivers all three modes from the same generic event and cleans up server subscriptions', async () => {
    const connection = JSON.parse(readFileSync(connectionPath!, 'utf8')) as {url: string; baseUrl: string; token: string; callbackOrigin: string};
    if (new URL(connection.url).hostname !== '127.0.0.1') throw new Error('Integration test requires local Mellon');
    const receiver = new WebhookReceiver({
      publicUrl: `${connection.callbackOrigin}/hooks`, host: '127.0.0.1', port: Number(new URL(connection.callbackOrigin).port),
      tls: {key: readFileSync(resolve('.local/tls/localhost.key'), 'utf8'), cert: readFileSync(resolve('.local/tls/localhost.crt'), 'utf8')},
    });
    cleanups.push(() => receiver.close());
    const client = new EventsClient(new EventsWire(() => createHttpTransport(new URL(connection.url), {
      requestInit: {headers: {authorization: `Bearer ${connection.token}`}},
    })), {receiver, pollFloorMs: 25, retryMs: 10, maxRetries: 2});
    cleanups.push(() => client.close());
    await client.connect();
    expect(client.list()[0]?.delivery).toEqual(['poll', 'push', 'webhook']);
    const observations = new Map<string, Observation[]>();
    const watches = ['poll', 'push', 'webhook'].map(mode => {
      const values: Observation[] = [];
      observations.set(mode, values);
      return client.watch({name: 'example.changed', arguments: {channel: mode}, mode: mode as 'poll' | 'push' | 'webhook', ttlMs: 2000}, observation => {values.push(observation);});
    });
    await Promise.all(watches.map(watch => watch.ready));
    for (const mode of observations.keys()) {
      const response = await fetch(`${connection.baseUrl}/emit`, {method: 'POST', headers: {'content-type': 'application/json', authorization: `Bearer ${connection.token}`}, body: JSON.stringify({channel: mode, value: 42})});
      expect(response.ok).toBe(true);
    }
    await vi.waitFor(() => {
      for (const values of observations.values()) expect(values.some(item => item.type === 'event' && item.event.data.value === 42)).toBe(true);
    }, {timeout: 5000});
    for (const watch of watches) expect(watch.cursor).toBeTypeOf('string');
    const pushCursor = watches[1]!.cursor;
    await watches[1]!.stop();
    for (let value = 0; value < 110; value++) {
      const response = await fetch(`${connection.baseUrl}/emit`, {method: 'POST', headers: {'content-type': 'application/json', authorization: `Bearer ${connection.token}`}, body: JSON.stringify({channel: 'push', value})});
      expect(response.ok).toBe(true);
    }
    const replayed: Observation[] = [];
    const replay = client.watch({name: 'example.changed', arguments: {channel: 'push'}, mode: 'push', cursor: pushCursor}, observation => {replayed.push(observation);});
    await replay.ready;
    await vi.waitFor(() => expect(replayed.filter(item => item.type === 'event')).toHaveLength(110), {timeout: 5000});
    await client.close();
    const response = await fetch(`${connection.baseUrl}/status`, {headers: {authorization: `Bearer ${connection.token}`}});
    expect(await response.json()).toEqual({streams: 0, webhooks: 0});
  }, 15_000);
});
