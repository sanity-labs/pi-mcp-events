import {createHmac} from 'node:crypto';
import {request} from 'node:http';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {createWebhookIdentity} from '../src/webhook-identities.ts';
import {EventsClientErrorCode as ErrorCode, WebhookReceiver, type WebhookOptions, type WebhookRegistration} from '../src/index.ts';

const options = {publicUrl: 'https://events.example/hooks', port: 0};
const event = (eventId = 'e') => ({eventId, name: 'example.changed', timestamp: '2026-10-25T12:00:00Z', cursor: 'watermark', data: {value: 'café'}});
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(cleanups.splice(0).map(close => close()));
  vi.restoreAllMocks();
});
async function setup(settings: Partial<WebhookOptions> = {}) {
  const receiver = new WebhookReceiver({...options, ...settings});
  cleanups.push(() => receiver.close());
  await receiver.start();
  const address = receiver.address();
  if (!address || typeof address === 'string') throw new Error('Missing receiver address');
  const post = (route: WebhookRegistration, body: unknown, settings: {
    id?: string; timestamp?: number; signature?: string; raw?: Buffer; headers?: Record<string, string | string[] | undefined>; method?: string;
  } = {}) => {
    const raw = settings.raw ?? Buffer.from(JSON.stringify(body));
    const id = settings.id ?? 'msg_unique';
    const timestamp = String(settings.timestamp ?? Math.floor(Date.now() / 1000));
    const signature = `v1,${createHmac('sha256', Buffer.from(route.secret.slice(6), 'base64')).update(`${id}.${timestamp}.`).update(raw).digest('base64')}`;
    const url = new URL(route.url);
    return new Promise<{status: number; body: string}>((resolve, reject) => {
      const req = request(`http://127.0.0.1:${address.port}${url.pathname}${url.search}`, {
        method: settings.method ?? 'POST', headers: {
          'content-type': 'application/json', 'webhook-id': id, 'webhook-timestamp': timestamp,
          'webhook-signature': settings.signature ?? signature, 'x-mcp-subscription-id': 'sub_1', ...settings.headers,
        },
      }, res => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({status: res.statusCode!, body}));
        res.on('error', reject);
      });
      req.on('error', reject);
      req.end(raw);
    });
  };
  return {receiver, post};
}

describe('signed webhook receiver', () => {
  it('generates independent secrets and answers verification before binding without forwarding it', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept);
    const other = receiver.register(accept);
    expect(route.url).not.toBe(other.url);
    expect(route.secret).not.toBe(other.secret);
    expect(Buffer.from(route.secret.slice(6), 'base64')).toHaveLength(32);
    expect(new URL(route.url).pathname).toMatch(/^\/hooks\/[a-f0-9]{48}$/);
    const response = await post(route, {type: 'verification', challenge: 'one-time-nonce'});
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({challenge: 'one-time-nonce'});
    expect(accept).not.toHaveBeenCalled();
  });

  it('restores the generated route and signing secret and rejects simultaneous reuse', async () => {
    const {receiver, post} = await setup();
    const identity = createWebhookIdentity();
    const first = receiver.register(() => {}, identity);
    first.bind('sub_1');
    expect((await post(first, event())).status).toBe(204);
    expect(() => receiver.register(() => {}, identity)).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    await first.close();
    const accept = vi.fn();
    const restored = receiver.register(accept, identity);
    expect(restored.url).toBe(first.url); expect(restored.secret).toBe(first.secret);
    restored.bind('sub_1');
    expect((await post(first, event())).status).toBe(204);
    expect(accept).toHaveBeenCalledOnce();
    expect(() => receiver.register(() => {}, {...identity, routeId: '../unsafe'})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect(() => receiver.register(() => {}, {...identity, secret: 'whsec_weak'})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
  });

  it.each([event(), {type: 'gap', cursor: 'gap'}, {type: 'terminated', error: {code: -32024, message: 'ended'}}])('retries delivery racing the subscribe response, then forwards it after binding: %j', async body => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept);
    expect((await post(route, body)).status).toBe(503);
    expect(accept).not.toHaveBeenCalled();
    route.bind('sub_1');
    expect((await post(route, body)).status).toBe(204);
    expect(accept).toHaveBeenCalledWith(body);
    expect((await post(route, body, {headers: {'x-mcp-subscription-id': 'other'}})).status).toBe(403);
  });

  it('verifies the original UTF-8 JSON bytes, including whitespace and Unicode', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    expect((await post(route, event(), {raw: Buffer.from(JSON.stringify(event(), null, 2))})).status).toBe(204);
    expect(accept).toHaveBeenCalledWith(event());
  });

  it('preserves a basic-offset timestamp after verifying the original signed bytes', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    const body = {...event(), timestamp: '2026-10-25T17:30:00+0530'};
    expect((await post(route, body, {raw: Buffer.from(JSON.stringify(body, null, 2))})).status).toBe(204);
    expect(accept).toHaveBeenCalledWith(body);
    const invalid = {...body, timestamp: '2026-10-25T17:30:00+0560'};
    expect((await post(route, invalid, {id: 'invalid'})).status).toBe(400);
    expect(accept).toHaveBeenCalledOnce();
  });

  it('accepts a valid rotation signature among other signature versions', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    const timestamp = Math.floor(Date.now() / 1000);
    const raw = JSON.stringify(event());
    const sig = createHmac('sha256', Buffer.from(route.secret.slice(6), 'base64')).update(`msg_unique.${timestamp}.${raw}`).digest('base64');
    expect((await post(route, event(), {timestamp, signature: `v1,wrong v1a,ignored v1,${sig}`})).status).toBe(204);
    expect(accept).toHaveBeenCalledOnce();
  });

  it.each([-301, 301])('rejects timestamps outside the freshness window: %i seconds', async offset => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    expect((await post(route, event(), {timestamp: Math.floor(Date.now() / 1000) + offset})).status).toBe(401);
    expect(accept).not.toHaveBeenCalled();
  });

  it.each([
    {headers: {'webhook-id': 'bad.id'}, status: 400},
    {headers: {'webhook-timestamp': '123junk'}, status: 400},
    {headers: {'webhook-signature': ''}, status: 400},
    {headers: {'x-mcp-subscription-id': ''}, status: 400},
    {headers: {'webhook-id': ['one', 'two']}, status: 400},
    {headers: {'content-type': 'text/plain'}, status: 415},
    {headers: {'content-encoding': 'gzip'}, status: 415},
    {headers: {'webhook-signature': 'v1,invalid'}, status: 401},
  ])('rejects invalid headers before forwarding: %j', async ({headers, status}) => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    expect((await post(route, event(), {headers})).status).toBe(status);
    expect(accept).not.toHaveBeenCalled();
  });

  it.each([Buffer.from('{'), Buffer.from([0xff]), Buffer.from('{"type":"unknown"}'), Buffer.from('{}')])('rejects malformed signed payloads', async raw => {
    const {receiver, post} = await setup();
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    expect((await post(route, null, {raw})).status).toBe(400);
    expect(accept).not.toHaveBeenCalled();
  });

  it('does not parse or echo a challenge with an invalid signature', async () => {
    const {receiver, post} = await setup();
    const route = receiver.register(() => {});
    const response = await post(route, {type: 'verification', challenge: 'nonce'}, {signature: 'v1,invalid'});
    expect(response).toEqual({status: 401, body: ''});
    expect((await post(route, null, {raw: Buffer.from('{'), signature: 'v1,invalid'})).status).toBe(401);
  });

  it('acknowledges only accepted deliveries and deduplicates simultaneous retries within each registration', async () => {
    const {receiver, post} = await setup();
    const gate = Promise.withResolvers<void>();
    const accept = vi.fn(() => gate.promise);
    const route = receiver.register(accept); route.bind('sub_1');
    const responded = vi.fn();
    const first = post(route, event()).then(response => { responded(); return response; });
    const retry = post(route, event());
    await vi.waitFor(() => expect(accept).toHaveBeenCalledOnce());
    expect(responded).not.toHaveBeenCalled();
    gate.resolve();
    expect((await first).status).toBe(204);
    expect((await retry).status).toBe(204);
    expect(accept).toHaveBeenCalledOnce();
    const other = receiver.register(accept); other.bind('sub_1');
    expect((await post(other, event())).status).toBe(204);
    expect((await post(route, event(), {id: 'another_message'})).status).toBe(204);
    expect(accept).toHaveBeenCalledTimes(3);
  });

  it('allows retry after forwarding fails', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn().mockRejectedValueOnce(new Error('busy')).mockResolvedValue(undefined);
    const route = receiver.register(accept); route.bind('sub_1');
    expect((await post(route, event())).status).toBe(503);
    expect((await post(route, event())).status).toBe(204);
    expect(accept).toHaveBeenCalledTimes(2);
  });

  it('bounds pending deliveries and body size using configurable limits', async () => {
    const {receiver, post} = await setup({maxPendingDeliveries: 1, maxBodyBytes: 512});
    const gate = Promise.withResolvers<void>();
    const accept = vi.fn(() => gate.promise);
    const route = receiver.register(accept); route.bind('sub_1');
    expect((await post(route, {...event(), data: {value: 'x'.repeat(1024)}})).status).toBe(413);
    const pending = post(route, event());
    await vi.waitFor(() => expect(accept).toHaveBeenCalledOnce());
    expect((await post(route, event(), {id: 'full'})).status).toBe(503);
    gate.resolve();
    expect((await pending).status).toBe(204);
    expect((await post(route, event(), {id: 'available'})).status).toBe(204);
  });

  it('applies backpressure to verification and accepts a retry when capacity returns', async () => {
    const {receiver, post} = await setup({maxPendingDeliveries: 1});
    const gate = Promise.withResolvers<void>();
    const accept = vi.fn(() => gate.promise);
    const route = receiver.register(accept); route.bind('sub_1');
    const delivery = post(route, event());
    await vi.waitFor(() => expect(accept).toHaveBeenCalledOnce());
    const challenge = {type: 'verification', challenge: 'retry-nonce'};
    expect((await post(route, challenge, {id: 'msg_verification'})).status).toBe(503);
    gate.resolve();
    expect((await delivery).status).toBe(204);
    const response = await post(route, challenge, {id: 'msg_verification'});
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({challenge: 'retry-nonce'});
    expect(accept).toHaveBeenCalledOnce();
  });

  it('expires and bounds delivery deduplication history', async () => {
    const {receiver, post} = await setup({maxTrackedDeliveries: 1, deduplicationTtlMs: 1000});
    const accept = vi.fn();
    const route = receiver.register(accept); route.bind('sub_1');
    for (const id of ['one', 'two', 'one']) expect((await post(route, event(), {id})).status).toBe(204);
    expect(accept).toHaveBeenCalledTimes(3);
    const now = Date.now();
    vi.spyOn(Date, 'now').mockReturnValue(now + 2000);
    expect((await post(route, event(), {id: 'one'})).status).toBe(204);
    expect(accept).toHaveBeenCalledTimes(4);
  });

  it('shares concurrent starts and rejects lifecycle operations after close', async () => {
    const receiver = new WebhookReceiver(options);
    cleanups.push(() => receiver.close());
    const start = receiver.start();
    expect(receiver.start()).toBe(start);
    await start;
    const address = receiver.address();
    await receiver.start();
    expect(receiver.address()).toEqual(address);
    const closing = receiver.close();
    expect(receiver.close()).toBe(closing);
    await closing;
    expect(() => receiver.start()).toThrow(expect.objectContaining({code: ErrorCode.Closed}));
    expect(() => receiver.register(() => {})).toThrow(expect.objectContaining({code: ErrorCode.Closed}));
  });

  it('retries a failed bind and handles close during startup', async () => {
    const {receiver: occupied} = await setup();
    const address = occupied.address();
    if (!address || typeof address === 'string') throw new Error('Missing address');
    const receiver = new WebhookReceiver({...options, port: address.port});
    cleanups.push(() => receiver.close());
    await expect(receiver.start()).rejects.toMatchObject({code: ErrorCode.ConnectionFailed});
    await occupied.close();
    await receiver.start();
    await receiver.close();
    const concurrent = new WebhookReceiver(options);
    const starting = concurrent.start();
    const rejected = expect(starting).rejects.toMatchObject({code: ErrorCode.Closed});
    await concurrent.close();
    await rejected;
    expect(concurrent.address()).toBeUndefined();
  });

  it('drains a closing registration, skips its queued deliveries, and frees its slot', async () => {
    const {receiver, post} = await setup({maxRegistrations: 1});
    const gate = Promise.withResolvers<void>();
    const accept = vi.fn(() => gate.promise);
    const route = receiver.register(accept); route.bind('sub_1');
    expect(() => receiver.register(() => {})).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    const first = post(route, event());
    const queued = post(route, event(), {id: 'queued'});
    await vi.waitFor(() => expect(accept).toHaveBeenCalledOnce());
    const drained = vi.fn();
    const closing = route.close().then(drained);
    expect(drained).not.toHaveBeenCalled();
    expect((await post(route, event())).status).toBe(404);
    gate.resolve();
    expect((await first).status).toBe(204);
    expect([404, 503]).toContain((await queued).status);
    await closing;
    expect(accept).toHaveBeenCalledOnce();
    expect(() => receiver.register(() => {})).not.toThrow();
    expect(() => route.bind('sub_1')).toThrow(expect.objectContaining({code: ErrorCode.Closed}));
  });

  it('drains an active callback even when its registration is already closing', async () => {
    const {receiver, post} = await setup();
    const gate = Promise.withResolvers<void>();
    const accept = vi.fn(() => gate.promise);
    const route = receiver.register(accept); route.bind('sub_1');
    const delivery = post(route, event()).catch(() => undefined);
    await vi.waitFor(() => expect(accept).toHaveBeenCalledOnce());
    const registrationClosed = route.close();
    const drained = vi.fn();
    const closing = receiver.close().then(drained);
    await new Promise(resolve => setImmediate(resolve));
    expect(drained).not.toHaveBeenCalled();
    gate.resolve();
    await Promise.all([closing, registrationClosed, delivery]);
    expect(drained).toHaveBeenCalledOnce();
    expect(receiver.address()).toBeUndefined();
  });

  it('keeps subscription IDs stable and enforces POST routing after binding', async () => {
    const {receiver, post} = await setup();
    const route = receiver.register(() => {}); route.bind('sub_1');
    expect(() => route.bind('sub_1')).not.toThrow();
    expect(() => route.bind('other')).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect(() => route.bind('')).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect((await post(route, event(), {method: 'GET'})).status).toBe(405);
    expect((await post({...route, url: `${route.url}?extra=1`}, event())).status).toBe(404);
    expect((await post(route, {type: 'verification', challenge: 'nonce'}, {headers: {'x-mcp-subscription-id': 'other'}})).status).toBe(403);
    await route.close();
    expect((await post(route, event())).status).toBe(404);
  });

  it.each([
    {publicUrl: 'http://events.example'}, {publicUrl: 'https://user:pass@events.example'},
    {publicUrl: 'https://events.example?key=value'}, {host: '0.0.0.0'},
    {port: -1}, {maxBodyBytes: 0}, {maxPendingDeliveries: Infinity}, {maxTrackedDeliveries: 0},
  ])('validates receiver configuration: %j', input => {
    expect(() => new WebhookReceiver({...options, ...input})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
  });
});
