import {createHmac, randomUUID} from 'node:crypto';
import {request} from 'node:http';
import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, EventsWire, WebhookReceiver, EventsClientErrorCode as ErrorCode, type ClientOptions, type Observation} from '../src/index.ts';
import {TestTransport} from './transport.ts';

const descriptor = {
  name: 'example.changed', delivery: ['webhook'], inputSchema: {type: 'object'},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'number'}}},
};
const options = {name: descriptor.name, arguments: {}, mode: 'webhook' as const, cursor: 'start'};
const event = (eventId: string, cursor = eventId) => ({eventId, cursor, name: descriptor.name, timestamp: '2026-10-25T12:00:00Z', data: {value: 1}});
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});
async function setup(settings: ClientOptions = {}) {
  const receiver = new WebhookReceiver({publicUrl: 'https://events.example/hooks', port: 0});
  cleanups.push(() => receiver.close());
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {
    webhookReceiver: receiver, retryMs: 1, maxRetries: 2, webhookUnsubscribeTimeoutMs: 30, ...settings,
  });
  cleanups.push(() => client.close());
  const subscriptions: JSONRPCRequest[] = [];
  const unsubscribes: JSONRPCRequest[] = [];
  let lists = 0;
  const grant = (req: JSONRPCRequest, result: Record<string, unknown> = {}) => transport.respond(req, {
    id: 'sub_1', refreshBefore: new Date(Date.now() + 60_000).toISOString(), cursor: 'grant', truncated: false, ...result,
  });
  let subscribe: (req: JSONRPCRequest) => void | Promise<void> = req => grant(req);
  let unsubscribe = (req: JSONRPCRequest) => transport.respond(req, {});
  transport.handle = req => {
    if (req.method === 'events/list') { lists++; transport.respond(req, {events: [descriptor]}); }
    if (req.method === 'events/subscribe') { subscriptions.push(req); return subscribe(req); }
    if (req.method === 'events/unsubscribe') { unsubscribes.push(req); unsubscribe(req); }
  };
  await client.connect();
  const post = (req: JSONRPCRequest, body: unknown, id = randomUUID()) => {
    const delivery = req.params!.delivery as {url: string; secret: string};
    const address = receiver.address();
    if (!address || typeof address === 'string') throw new Error('Receiver is not listening');
    const raw = JSON.stringify(body);
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
  return {client, receiver, transport, subscriptions, unsubscribes, grant, post, get lists() { return lists; },
    subscribe: (handler: typeof subscribe) => { subscribe = handler; },
    unsubscribe: (handler: typeof unsubscribe) => { unsubscribe = handler; },
  };
}

describe('webhook watches', () => {
  it('reports suspended delivery and recovers on renewal without an event', async () => {
    const s = await setup({webhookRefreshMs: 20});
    s.subscribe(() => {});
    const watch = s.client.watch(options, () => {});
    expect(watch.status).toBe('starting');
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(1));
    s.grant(s.subscriptions[0]!, {deliveryStatus: {active: false, lastError: 'timeout'}});
    await watch.ready;
    expect(watch.status).toBe('suspended');
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(2));
    s.transport.fail(s.subscriptions[1]!, -32603);
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(3));
    expect(watch.status).toBe('retrying');
    s.grant(s.subscriptions[2]!);
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(4));
    expect(watch.status).toBe('suspended');
    s.grant(s.subscriptions[3]!, {deliveryStatus: {active: true}});
    await vi.waitFor(() => expect(watch.status).toBe('active'));
    await watch.stop();
    expect(watch.status).toBe('stopped');
  });
  it('verifies before the first grant, retries early delivery, and checkpoints accepted events', async () => {
    const s = await setup();
    s.subscribe(async req => {
      expect(await s.post(req, {type: 'verification', challenge: 'nonce'})).toBe(200);
      expect(await s.post(req, event('early'))).toBe(503);
      s.grant(req, {truncated: true});
    });
    const observations: Observation[] = [];
    const checkpoint = vi.fn();
    const watch = s.client.watch({...options, ttlMs: 1000, maxAgeMs: 500}, item => { observations.push(item); }, checkpoint);
    await watch.ready;
    expect(s.subscriptions[0]!.params).toMatchObject({name: descriptor.name, cursor: 'start', ttlMs: 1000, maxAgeMs: 500, delivery: {mode: 'webhook'}});
    expect(s.subscriptions[0]!.params).not.toHaveProperty('maxEvents');
    expect(await s.post(s.subscriptions[0]!, event('early'))).toBe(204);
    expect(await s.post(s.subscriptions[0]!, event('early', 'new-watermark'))).toBe(204);
    expect(observations).toEqual([{type: 'gap', cursor: 'grant'}, {type: 'event', event: event('early')}]);
    expect(checkpoint.mock.calls.map(call => call[0])).toEqual(['grant', 'early', 'new-watermark']);
    expect(watch.cursor).toBe('new-watermark');
    await watch.stop();
    expect(s.unsubscribes[0]!.params).toMatchObject({name: descriptor.name, arguments: {}, delivery: {url: (s.subscriptions[0]!.params!.delivery as {url: string}).url}});
    expect(s.unsubscribes[0]!.params!.delivery).not.toHaveProperty('secret');
    expect(await s.post(s.subscriptions[0]!, event('late'))).toBe(404);
  });

  it('renews the same key and secret from the latest persisted cursor using the granted expiry', async () => {
    const s = await setup();
    const renewed = Promise.withResolvers<JSONRPCRequest>();
    s.subscribe(req => {
      if (s.subscriptions.length === 1) s.grant(req, {refreshBefore: new Date(Date.now() + 160).toISOString()});
      else { s.grant(req, {cursor: 'quiet'}); renewed.resolve(req); }
    });
    const watch = s.client.watch({...options, ttlMs: 86_400_000}, () => {});
    await watch.ready;
    expect(await s.post(s.subscriptions[0]!, event('accepted'))).toBe(204);
    const next = await renewed.promise;
    await vi.waitFor(() => expect(watch.cursor).toBe('quiet'));
    expect(next.params).toMatchObject({cursor: 'accepted', ttlMs: 86_400_000, delivery: s.subscriptions[0]!.params!.delivery});
  });

  it('refreshes no-expiry subscriptions and catalogs on the configured health interval, exposing delivery status', async () => {
    const s = await setup({webhookRefreshMs: 20});
    const statuses: Observation[] = [];
    s.subscribe(req => s.grant(req, {refreshBefore: null, deliveryStatus: {active: false, lastError: 'timeout', throttled: true, retryAfterMs: 100}}));
    const watch = s.client.watch({...options, ttlMs: null}, item => { statuses.push(item); });
    await watch.ready;
    await vi.waitFor(() => expect(s.subscriptions.length).toBeGreaterThan(1));
    expect(s.lists).toBeGreaterThan(1);
    expect(statuses).toContainEqual({type: 'status', status: 'webhook-delivery', deliveryStatus: {active: false, lastError: 'timeout', throttled: true, retryAfterMs: 100}});
  });

  it.each([330, -270])('renews grants with a basic numeric offset of %i minutes and preserves status timestamps', async offsetMinutes => {
    const s = await setup();
    const offset = offsetMinutes > 0 ? '+0530' : '-0430';
    const withOffset = (time: number) => new Date(time + offsetMinutes * 60_000).toISOString().replace(/Z$/, offset);
    const deliveryStatus = {active: true, lastDeliveryAt: withOffset(Date.now() - 1000), failedSince: withOffset(Date.now() - 2000)};
    s.subscribe(req => s.grant(req, {
      refreshBefore: withOffset(Date.now() + (s.subscriptions.length === 1 ? 400 : 60_000)),
      deliveryStatus,
    }));
    const observations: Observation[] = [];
    const watch = s.client.watch(options, item => { observations.push(item); });
    await watch.ready;
    await vi.waitFor(() => expect(observations).toContainEqual({type: 'status', status: 'webhook-delivery', deliveryStatus}));
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(2), {timeout: 2000});
    expect(watch.status).toBe('active');
    await watch.stop();
  });

  it.each([undefined, 1000])('rejects unsolicited no-expiry grants for ttlMs=%s', async ttlMs => {
    const s = await setup();
    s.subscribe(req => s.grant(req, {refreshBefore: null}));
    const watch = s.client.watch({...options, ttlMs}, () => {});
    await expect(watch.ready).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
    expect(s.unsubscribes).toHaveLength(1);
  });

  it('accepts a finite grant for an explicit no-expiry request', async () => {
    const s = await setup();
    const watch = s.client.watch({...options, ttlMs: null}, () => {});
    await watch.ready;
    expect(watch.cursor).toBe('grant');
  });

  it.each([{refreshBefore: '2020-01-01T00:00:00Z'}, {refreshBefore: 'not-a-date'}, {id: ''}])('rejects invalid grants: %j', async result => {
    const s = await setup(); s.subscribe(req => s.grant(req, result));
    const watch = s.client.watch(options, () => {});
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
  });

  it('rejects a changed subscription ID on renewal', async () => {
    const s = await setup({webhookRefreshMs: 10});
    s.subscribe(req => s.grant(req, {id: s.subscriptions.length === 1 ? 'sub_1' : 'sub_changed'}));
    const watch = s.client.watch(options, () => {});
    await watch.ready;
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
    expect(s.subscriptions).toHaveLength(2);
  });

  it('serializes a renewal checkpoint behind an in-flight delivery', async () => {
    const s = await setup({webhookRefreshMs: 30});
    const renewal = Promise.withResolvers<JSONRPCRequest>();
    s.subscribe(req => { if (s.subscriptions.length === 1) s.grant(req); else renewal.resolve(req); });
    const accepted = Promise.withResolvers<void>();
    const began = Promise.withResolvers<void>();
    const checkpoints: Array<string | null> = [];
    const watch = s.client.watch(options, item => {
      if (item.type === 'event') { began.resolve(); return accepted.promise; }
    }, cursor => { checkpoints.push(cursor); });
    await watch.ready;
    const pending = s.post(s.subscriptions[0]!, event('slow'));
    await began.promise;
    s.grant(await renewal.promise, {cursor: 'renewal'});
    expect(watch.cursor).toBe('grant');
    accepted.resolve();
    expect(await pending).toBe(204);
    await vi.waitFor(() => expect(watch.cursor).toBe('renewal'));
    expect(checkpoints).toEqual(['grant', 'slow', 'renewal']);
  });

  it('keeps renewing while delivery is blocked and coalesces grants without losing a gap', async () => {
    const s = await setup({webhookRefreshMs: 10});
    const began = Promise.withResolvers<void>();
    s.subscribe(async req => {
      const count = s.subscriptions.length;
      if (count > 1) await began.promise;
      if (count <= 5) s.grant(req, {cursor: count === 5 ? null : `grant-${count}`, truncated: count === 2,
        refreshBefore: new Date(Date.now() + 100).toISOString()});
    });
    const accepted = Promise.withResolvers<void>();
    const checkpoints: Array<string | null> = [];
    const observations: Observation[] = [];
    const watch = s.client.watch(options, item => {
      observations.push(item);
      if (item.type === 'event') { began.resolve(); return accepted.promise; }
    }, cursor => { checkpoints.push(cursor); });
    try {
      await watch.ready;
      const pending = s.post(s.subscriptions[0]!, event('slow'));
      await began.promise;
      await vi.waitFor(() => expect(s.subscriptions).toHaveLength(6));
      expect(s.subscriptions.slice(1).every(req => req.params!.cursor === 'grant-1')).toBe(true);
      expect(checkpoints).toEqual(['grant-1']);
      accepted.resolve();
      expect(await pending).toBe(204);
      await vi.waitFor(() => expect(watch.cursor).toBeNull());
      expect(checkpoints).toEqual(['grant-1', 'slow', 'grant-2', null]);
      expect(observations.filter(item => item.type === 'gap')).toEqual([{type: 'gap', cursor: 'grant-2'}]);
    } finally { accepted.resolve(); }
  });

  it('renews while an initial checkpoint is blocked and preserves the next delivery boundary', async () => {
    const s = await setup({webhookRefreshMs: 10});
    const arrived = Promise.withResolvers<void>();
    s.subscribe(async req => {
      const count = s.subscriptions.length;
      if (count > 1) await arrived.promise;
      if (count <= 4) s.grant(req, {cursor: `grant-${count}`});
    });
    const checkpointGate = Promise.withResolvers<void>();
    const began = Promise.withResolvers<void>();
    const register = s.receiver.register.bind(s.receiver);
    vi.spyOn(s.receiver, 'register').mockImplementation((accept, identity) => register(body => {
      const pending = accept(body);
      arrived.resolve();
      return pending;
    }, identity));
    const checkpoints: Array<string | null> = [];
    const watch = s.client.watch(options, () => {}, async cursor => {
      checkpoints.push(cursor);
      if (cursor === 'grant-1') { began.resolve(); await checkpointGate.promise; }
    });
    try {
      await began.promise;
      const pending = s.post(s.subscriptions[0]!, event('between'));
      await arrived.promise;
      await vi.waitFor(() => expect(s.subscriptions).toHaveLength(5));
      checkpointGate.resolve();
      await watch.ready;
      expect(await pending).toBe(204);
      await vi.waitFor(() => expect(watch.cursor).toBe('grant-4'));
      expect(checkpoints.indexOf('between')).toBeGreaterThan(0);
      expect(checkpoints.at(-1)).toBe('grant-4');
    } finally { checkpointGate.resolve(); }
  });

  it('continues lease retries while the retry status consumer is blocked', async () => {
    const s = await setup({webhookRefreshMs: 10});
    s.subscribe(req => {
      const count = s.subscriptions.length;
      if (count === 2) s.transport.fail(req, -32603);
      else if (count <= 4) s.grant(req, {cursor: `grant-${count}`});
    });
    const accepted = Promise.withResolvers<void>();
    const began = Promise.withResolvers<void>();
    const watch = s.client.watch(options, item => {
      if (item.type === 'status' && item.status === 'retrying') { began.resolve(); return accepted.promise; }
    });
    try {
      await watch.ready;
      await began.promise;
      await vi.waitFor(() => expect(s.subscriptions).toHaveLength(5));
      expect(watch.cursor).toBe('grant-1');
      accepted.resolve();
      await vi.waitFor(() => expect(watch.cursor).toBe('grant-4'));
    } finally { accepted.resolve(); }
  });

  it('retries consumer and checkpoint failures independently without another subscribe', async () => {
    const s = await setup();
    let deliveries = 0; let saves = 0;
    const watch = s.client.watch(options, item => {
      if (item.type === 'event' && ++deliveries === 1) throw new Error('busy');
    }, cursor => { if (cursor === 'e' && ++saves === 1) throw new Error('disk busy'); });
    await watch.ready;
    expect(await s.post(s.subscriptions[0]!, event('e'))).toBe(204);
    expect(deliveries).toBe(2); expect(saves).toBe(2);
    expect(s.subscriptions).toHaveLength(1);
    expect(watch.cursor).toBe('e');
  });

  it.each(['consumer', 'checkpoint', 'payload'])('terminates on permanent %s failure without acknowledging the event', async failure => {
    const s = await setup({maxRetries: 0});
    const watch = s.client.watch(options, item => {
      if (item.type === 'event' && failure === 'consumer') throw new Error('full');
    }, cursor => { if (cursor === 'e' && failure === 'checkpoint') throw new Error('disk full'); });
    await watch.ready;
    const body = failure === 'payload' ? {...event('e'), data: {value: 'invalid'}} : event('e');
    expect(await s.post(s.subscriptions[0]!, body)).toBe(503);
    await expect(watch.closed).rejects.toMatchObject({code: failure === 'payload' ? ErrorCode.InvalidEvent : ErrorCode.RetryExhausted});
    expect(watch.cursor).toBe('grant');
  });

  it('forwards signed gap controls, including null cursors', async () => {
    const s = await setup(); const accept = vi.fn();
    const watch = s.client.watch(options, accept); await watch.ready;
    for (const cursor of ['fresh', null]) {
      expect(await s.post(s.subscriptions[0]!, {type: 'gap', cursor})).toBe(204);
      expect(accept).toHaveBeenLastCalledWith({type: 'gap', cursor});
      expect(watch.cursor).toBe(cursor);
    }
  });

  it.each([-32024, -32001])('cancels an in-flight renewal on signed termination, even for code %s', async code => {
    const s = await setup({webhookRefreshMs: 10});
    const renewing = Promise.withResolvers<void>();
    s.subscribe(req => { if (s.subscriptions.length === 1) s.grant(req); else renewing.resolve(); });
    const watch = s.client.watch(options, () => {}); await watch.ready; await renewing.promise;
    expect(await s.post(s.subscriptions[0]!, {type: 'terminated', error: {code, message: 'ended'}})).toBe(204);
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.SubscriptionTerminated, cause: {code}});
    expect(s.subscriptions).toHaveLength(2); expect(s.unsubscribes).toHaveLength(1);
    s.grant(s.subscriptions[1]!, {cursor: 'late'});
    expect(watch.cursor).toBe('grant');
  });

  it('rediscovers once on schema termination and stops on persistent incompatibility', async () => {
    const s = await setup(); const accept = vi.fn();
    const watch = s.client.watch(options, accept); await watch.ready;
    const ended = {type: 'terminated', error: {code: -32026, message: 'changed', data: {reason: 'schema_changed'}}};
    expect(await s.post(s.subscriptions[0]!, ended)).toBe(204);
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(2));
    await vi.waitFor(() => expect(watch.cursor).toBe('grant'));
    expect(s.lists).toBe(2);
    expect(s.subscriptions[1]!.params!.cursor).toBe(null);
    expect(accept).toHaveBeenCalledWith({type: 'gap', cursor: null});
    expect(await s.post(s.subscriptions[1]!, ended)).toBe(204);
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.SubscriptionTerminated});
    expect(s.subscriptions).toHaveLength(2);
  });

  it('retries transient subscribe errors with the same callback and a bounded budget', async () => {
    const s = await setup();
    s.subscribe(req => s.transport.fail(req, -32603));
    const watch = s.client.watch(options, () => {});
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
    expect(s.subscriptions).toHaveLength(3);
    expect(new Set(s.subscriptions.map(req => JSON.stringify(req.params!.delivery))).size).toBe(1);
    expect(s.unsubscribes).toHaveLength(1);
  });

  it('stops during subscribe and bounds an unresponsive unsubscribe', async () => {
    const s = await setup(); s.subscribe(() => {}); s.unsubscribe(() => {});
    const watch = s.client.watch(options, () => {});
    await vi.waitFor(() => expect(s.subscriptions).toHaveLength(1));
    await watch.stop();
    await expect(watch.ready).rejects.toMatchObject({code: ErrorCode.Stopped});
    expect(s.unsubscribes).toHaveLength(1);
    expect(await s.post(s.subscriptions[0]!, event('late'))).toBe(404);
  });

  it('isolates concurrent watches and leaves the shared receiver available after client close', async () => {
    const s = await setup(); const left = vi.fn(); const right = vi.fn();
    const a = s.client.watch(options, left);
    const b = s.client.watch({...options, arguments: {group: 'b'}}, right);
    await Promise.all([a.ready, b.ready]);
    expect(await s.post(s.subscriptions[0]!, event('same'))).toBe(204);
    expect(await s.post(s.subscriptions[1]!, event('same'))).toBe(204);
    await a.stop();
    expect(await s.post(s.subscriptions[1]!, event('next'))).toBe(204);
    expect(left.mock.calls.filter(([item]) => item.type === 'event')).toHaveLength(1);
    expect(right.mock.calls.filter(([item]) => item.type === 'event')).toHaveLength(2);
    await s.client.close();
    expect(s.unsubscribes).toHaveLength(2);
    expect(s.receiver.address()).not.toBe(null);
    const unused = s.receiver.register(() => {}); await unused.close();
  });

  it('drains an active consumer before stop completes and skips its pending checkpoint', async () => {
    const s = await setup();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const watch = s.client.watch(options, item => {
      if (item.type === 'event') { entered.resolve(); return release.promise; }
    });
    await watch.ready;
    const posted = s.post(s.subscriptions[0]!, event('busy'));
    await entered.promise;
    let closed = false;
    const stopping = watch.stop().then(() => { closed = true; });
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(closed).toBe(false);
    release.resolve();
    expect(await posted).toBe(503);
    await stopping;
    expect(watch.cursor).toBe('grant');
  });

  it('bounds cleanup while a disconnected wire is still reconnecting', async () => {
    const receiver = new WebhookReceiver({publicUrl: 'https://events.example/hooks', port: 0});
    cleanups.push(() => receiver.close());
    const first = new TestTransport();
    const reconnecting = new TestTransport();
    reconnecting.send = async () => {};
    let connections = 0;
    const wire = new EventsWire(() => ++connections === 1 ? first : reconnecting);
    const client = new EventsClient(wire, {webhookReceiver: receiver, webhookUnsubscribeTimeoutMs: 20});
    cleanups.push(() => client.close());
    first.handle = req => {
      if (req.method === 'events/list') first.respond(req, {events: [descriptor]});
      if (req.method === 'events/subscribe') first.respond(req, {id: 'sub_1', refreshBefore: new Date(Date.now() + 60_000).toISOString(), cursor: null, truncated: false});
    };
    await client.connect();
    const watch = client.watch(options, () => {}); await watch.ready;
    first.onclose?.();
    await watch.stop();
    expect(connections).toBe(2);
  });

  it('rediscovers an unknown event only once and preserves the protocol error', async () => {
    const s = await setup(); s.subscribe(req => s.transport.fail(req, -32023));
    const watch = s.client.watch(options, () => {});
    await expect(watch.closed).rejects.toMatchObject({code: -32023});
    expect(s.lists).toBe(2); expect(s.subscriptions).toHaveLength(2);
  });

  it('uses a fresh retry budget after a successful renewal and omits unspecified TTL', async () => {
    const s = await setup({webhookRefreshMs: 20, maxRetries: 1});
    s.subscribe(req => {
      if (s.subscriptions.length % 2 === 1) s.transport.fail(req, -32603);
      else s.grant(req, {cursor: String(s.subscriptions.length)});
    });
    const watch = s.client.watch(options, () => {}); await watch.ready;
    await vi.waitFor(() => expect(Number(watch.cursor)).toBeGreaterThanOrEqual(4));
    expect(s.subscriptions.every(req => !('ttlMs' in req.params!))).toBe(true);
  });

  it('requires a receiver and validates TTL and timing options', async () => {
    const s = await setup({webhookReceiver: undefined});
    expect(() => s.client.watch(options, () => {})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    const valid = await setup();
    for (const ttlMs of [0, -1, 0.5, Infinity]) {
      expect(() => valid.client.watch({...options, ttlMs}, () => {})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    }
    for (const settings of [{webhookRefreshMs: 0}, {webhookUnsubscribeTimeoutMs: 0}]) {
      expect(() => new EventsClient(new EventsWire(new TestTransport()), settings)).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    }
  });
});
