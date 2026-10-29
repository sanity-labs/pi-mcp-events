import {createHmac, randomUUID} from 'node:crypto';
import {request} from 'node:http';
import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, EventsWire, WebhookReceiver, type DeliveryMode, type Observation} from '../src/index.ts';
import {TestTransport} from './transport.ts';

const original = {
  name: 'example.changed', delivery: ['poll', 'push', 'webhook'], inputSchema: {type: 'object'},
  payloadSchema: {type: 'object', properties: {value: {type: 'number'}}, additionalProperties: false},
};
const updated = {...original, payloadSchema: {
  ...original.payloadSchema, properties: {...original.payloadSchema.properties, extra: {type: 'string'}},
}};
const event = {
  eventId: 'new-field', name: original.name, timestamp: '2026-10-29T12:00:00Z',
  data: {value: 1, extra: 'new optional field'}, cursor: 'next',
};
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });

async function setup(mode: DeliveryMode) {
  const receiver = new WebhookReceiver({publicUrl: 'https://events.example/hooks', port: 0});
  cleanups.push(() => receiver.close());
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {webhookReceiver: receiver, retryMs: 1, maxRetries: 1});
  cleanups.push(() => client.close());
  const reads: JSONRPCRequest[] = [];
  const deliveries: JSONRPCRequest[] = [];
  let connected = false;
  transport.handle = req => {
    if (req.method === 'events/list') {
      if (connected) reads.push(req);
      else transport.respond(req, {events: [original]});
    } else if (req.method === 'events/unsubscribe') transport.respond(req, {});
    else {
      deliveries.push(req);
      if (mode === 'push') transport.notify(req, 'active', {cursor: 'previous', truncated: false});
      if (mode === 'webhook') transport.respond(req, {
        id: 'sub_1', refreshBefore: new Date(Date.now() + 60_000).toISOString(), cursor: 'previous', truncated: false,
      });
    }
  };
  await client.connect();
  connected = true;
  const observations: Observation[] = [];
  const checkpoint = vi.fn();
  const watch = client.watch({name: original.name, arguments: {}, mode, cursor: 'previous'}, item => {
    observations.push(item);
  }, checkpoint);
  await vi.waitFor(() => expect(deliveries).toHaveLength(1));
  if (mode !== 'poll') await watch.ready;
  checkpoint.mockClear();
  const deliver = async (value = event) => {
    const req = deliveries[0]!;
    if (mode === 'poll') {
      transport.respond(req, {events: [value], cursor: value.cursor, truncated: false, hasMore: false, nextPollMs: 60_000});
      return;
    }
    if (mode === 'push') { transport.notify(req, 'event', value); return; }
    const delivery = req.params!.delivery as {url: string; secret: string};
    const address = receiver.address();
    if (!address || typeof address === 'string') throw new Error('Receiver is not listening');
    const raw = JSON.stringify(value);
    const id = randomUUID();
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = createHmac('sha256', Buffer.from(delivery.secret.slice(6), 'base64')).update(`${id}.${timestamp}.${raw}`).digest('base64');
    return new Promise<number>((resolve, reject) => {
      const outgoing = request(`http://127.0.0.1:${address.port}${new URL(delivery.url).pathname}`, {method: 'POST', headers: {
        'content-type': 'application/json', 'webhook-id': id, 'webhook-timestamp': timestamp,
        'webhook-signature': `v1,${signature}`, 'x-mcp-subscription-id': 'sub_1',
      }}, response => { response.resume(); response.on('end', () => resolve(response.statusCode!)); });
      outgoing.on('error', reject);
      outgoing.end(raw);
    });
  };
  return {client, transport, reads, watch, observations, checkpoint, deliver};
}

describe.each(['poll', 'push', 'webhook'] as const)('%s payload schema refresh', mode => {
  it('keeps the event and cursor until a read newer than the in-flight catalog succeeds', async () => {
    const h = await setup(mode);
    const older = h.client.refresh();
    await vi.waitFor(() => expect(h.reads).toHaveLength(1));
    const delivered = h.deliver();
    await new Promise(resolve => setImmediate(resolve));
    expect(h.checkpoint).not.toHaveBeenCalled();
    expect(h.watch.cursor).toBe('previous');
    expect(h.observations).toEqual([]);
    h.transport.respond(h.reads[0]!, {events: [original]});
    await older;
    await vi.waitFor(() => expect(h.reads).toHaveLength(2));
    h.transport.respond(h.reads[1]!, {events: [updated]});
    await delivered;
    await vi.waitFor(() => expect(h.watch.cursor).toBe('next'));
    expect(h.observations).toMatchObject([{type: 'event', event}]);
    expect(h.checkpoint.mock.calls).toEqual([['next']]);
    expect(h.watch.status).toBe('active');
  });

  it('retries a transient catalog failure while retaining the event', async () => {
    const h = await setup(mode);
    const delivered = h.deliver();
    await vi.waitFor(() => expect(h.reads).toHaveLength(1));
    h.transport.fail(h.reads[0]!, -32603);
    await vi.waitFor(() => expect(h.reads).toHaveLength(2));
    expect(h.watch.cursor).toBe('previous');
    expect(h.checkpoint).not.toHaveBeenCalled();
    h.transport.respond(h.reads[1]!, {events: [updated]});
    await delivered;
    await vi.waitFor(() => expect(h.watch.cursor).toBe('next'));
    expect(h.observations.filter(item => item.type === 'event')).toMatchObject([{type: 'event', event}]);
  });

  it('stops after one refresh if the payload is still invalid, without checkpointing it', async () => {
    const h = await setup(mode);
    const delivered = h.deliver();
    await vi.waitFor(() => expect(h.reads).toHaveLength(1));
    h.transport.respond(h.reads[0]!, {events: [original]});
    await expect(h.watch.closed).rejects.toMatchObject({code: 'INVALID_EVENT'});
    await delivered;
    expect(h.reads).toHaveLength(1);
    expect(h.watch.cursor).toBe('previous');
    expect(h.checkpoint).not.toHaveBeenCalled();
    expect(h.observations.filter(item => item.type === 'event')).toEqual([]);
  });

  it('rejects an event for a different name without refreshing the catalog', async () => {
    const h = await setup(mode);
    const delivered = h.deliver({...event, name: 'other.changed'});
    await expect(h.watch.closed).rejects.toMatchObject({code: 'INVALID_EVENT'});
    await delivered;
    expect(h.reads).toHaveLength(0);
    expect(h.checkpoint).not.toHaveBeenCalled();
  });

  it.each(['older', 'recovery'])('stops promptly during the %s catalog read and does not deliver afterward', async read => {
    const h = await setup(mode);
    const older = read === 'older' ? h.client.refresh() : undefined;
    const delivered = h.deliver();
    await vi.waitFor(() => expect(h.reads).toHaveLength(1));
    await h.watch.stop();
    expect(h.watch.status).toBe('stopped');
    h.transport.respond(h.reads[0]!, {events: [updated]});
    await older;
    await delivered;
    await new Promise(resolve => setImmediate(resolve));
    expect(h.reads).toHaveLength(1);
    expect(h.observations).toEqual([]);
    expect(h.checkpoint).not.toHaveBeenCalled();
    expect(h.watch.cursor).toBe('previous');
  });
});
