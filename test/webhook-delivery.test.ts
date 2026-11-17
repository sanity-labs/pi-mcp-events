import {createHmac, randomBytes} from 'node:crypto';
import {afterEach, describe, expect, it} from 'vitest';
// @ts-expect-error The development server is plain JavaScript.
import {createWebhookDelivery} from '../dev/webhook-delivery.mjs';

const secret = `whsec_${randomBytes(32).toString('base64')}`;
const input = (overrides = {}) => ({
  principal: 'user-1', name: 'weather.reading', arguments: {stationId: 'north'},
  url: 'https://hooks.example.test/events', secret, ttlMs: 60_000, ...overrides,
});
const event = (eventId: string, cursor: string) => ({
  eventId, name: 'weather.reading', timestamp: new Date(0).toISOString(),
  data: {stationId: 'north', temperatureC: 20}, cursor,
});
const publicDns = async () => [{address: '8.8.8.8', family: 4}];

describe('webhook delivery core', () => {
  const managers: Array<ReturnType<typeof createWebhookDelivery>> = [];
  afterEach(async () => {
    for (const manager of managers.splice(0)) {
      await manager.waitForIdle();
      manager.close();
    }
  });

  it('requires authentication and a valid secret, and grants finite TTLs', async () => {
    const manager = createWebhookDelivery({allowUrl: () => true});
    managers.push(manager);
    await expect(manager.subscribe(input({principal: ''}))).rejects.toThrow('Authenticated principal required');
    await expect(manager.subscribe(input({secret: 'weak'}))).rejects.toThrow('Invalid webhook secret');
    expect((await manager.subscribe(input({ttlMs: null}))).refreshBefore).toEqual(expect.any(String));
    expect((await manager.subscribe(input({ttlMs: 86_400_001}))).refreshBefore).toEqual(expect.any(String));
  });

  it('derives stable identities from principal, URL, event name, and canonical arguments', async () => {
    const manager = createWebhookDelivery({allowUrl: () => true});
    managers.push(manager);
    const first = await manager.subscribe(input({arguments: {stationId: 'north', level: 2}}));
    const sameKey = await manager.subscribe(input({arguments: {level: 2, stationId: 'north'}}));
    const otherPrincipal = await manager.subscribe(input({principal: 'user-2', arguments: {stationId: 'north', level: 2}}));
    expect(sameKey.id).toBe(first.id);
    expect(otherPrincipal.id).not.toBe(first.id);
    expect(first.refreshBefore).toBeTruthy();
  });

  it('verifies a signed challenge before creating a subscription', async () => {
    const calls: Array<{body: Buffer; headers: Record<string, string>}> = [];
    const manager = createWebhookDelivery({
      allowLocalAddresses: true,
      post: async ({body, headers}: {body: Buffer; headers: Record<string, string>}) => {
        calls.push({body, headers});
        const payload = JSON.parse(body.toString()) as {challenge: string};
        const key = Buffer.from(secret.slice('whsec_'.length), 'base64');
        const expected = `v1,${createHmac('sha256', key).update(`${headers['webhook-id']}.${headers['webhook-timestamp']}.`).update(body).digest('base64')}`;
        expect(headers['webhook-signature']).toBe(expected);
        return {status: 200, body: Buffer.from(JSON.stringify({challenge: payload.challenge}))};
      },
    });
    managers.push(manager);
    const result = await manager.subscribe(input({url: 'https://localhost:8443/hooks'}));
    expect(result.id).toMatch(/^sub_[a-f0-9]{24}$/);
    expect(calls).toHaveLength(1);
    expect(JSON.parse(calls[0].body.toString())).toMatchObject({type: 'verification'});
  });

  it('serializes concurrent subscriptions to the same endpoint', async () => {
    let challenges = 0;
    const manager = createWebhookDelivery({allowLocalAddresses: true,
      post: async ({body}: {body: Buffer}) => {
        challenges++;
        await new Promise(resolve => setTimeout(resolve, 5));
        const {challenge} = JSON.parse(body.toString());
        return {status: 200, body: Buffer.from(JSON.stringify({challenge}))};
      },
    });
    managers.push(manager);
    const options = input({url: 'https://localhost:8443/hooks'});
    const [first, second] = await Promise.all([manager.subscribe(options), manager.subscribe(options)]);
    expect(first.id).toBe(second.id);
    expect(manager.size).toBe(1);
    expect(challenges).toBe(1);
  });

  it('rejects non-HTTPS and private callback destinations unless explicitly allowed', async () => {
    const manager = createWebhookDelivery({allowUrl: () => true});
    managers.push(manager);
    await expect(manager.subscribe(input({url: 'http://hooks.example.test/'}))).rejects.toThrow('HTTPS');
    let posts = 0;
    const privateAddress = createWebhookDelivery({allowUrl: () => true, maxAttempts: 1,
      resolveHost: async () => [{address: '10.2.3.4', family: 4}],
      post: async () => { posts++; return {status: 204}; },
    });
    managers.push(privateAddress);
    await privateAddress.subscribe(input());
    expect(privateAddress.emit(event('evt-1', 'cursor-1'))).toBe(1);
    await privateAddress.waitForIdle();
    expect(posts).toBe(0);
  });

  it('retries throttled deliveries with the same message ID', async () => {
    const ids: string[] = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns, retryBaseMs: 1,
      post: async ({headers}: {headers: Record<string, string>}) => {
        ids.push(headers['webhook-id']);
        return {status: ids.length === 1 ? 429 : 204};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    expect(ids).toHaveLength(2);
    expect(ids[1]).toBe(ids[0]);
  });

  it('checks the destination again on retry to prevent DNS rebinding', async () => {
    let resolutions = 0;
    let posts = 0;
    const manager = createWebhookDelivery({allowUrl: () => true, retryBaseMs: 1, maxAttempts: 2,
      resolveHost: async () => [{address: ++resolutions === 1 ? '8.8.8.8' : '127.0.0.1', family: 4}],
      post: async () => { posts++; return {status: 503}; },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    expect(resolutions).toBeGreaterThanOrEqual(2);
    expect(posts).toBe(1);
  });

  it('uses the replacement secret for deliveries after a refresh', async () => {
    const nextSecret = `whsec_${randomBytes(32).toString('base64')}`;
    let received: {body: Buffer; headers: Record<string, string>} | undefined;
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns,
      post: async (request: {body: Buffer; headers: Record<string, string>}) => {
        received = request;
        return {status: 204};
      },
    });
    managers.push(manager);
    const original = await manager.subscribe(input());
    const refreshed = await manager.subscribe(input({secret: nextSecret}));
    expect(refreshed.id).toBe(original.id);
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    const {body, headers} = received!;
    const signature = createHmac('sha256', Buffer.from(nextSecret.slice(6), 'base64'))
      .update(`${headers['webhook-id']}.${headers['webhook-timestamp']}.`).update(body).digest('base64');
    expect(headers['webhook-signature']).toBe(`v1,${signature}`);
  });

  it('does not retry a 410 response or remove the subscription', async () => {
    const ids: string[] = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns,
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString());
        ids.push(payload.eventId);
        return {status: payload.eventId === 'evt-1' ? 410 : 204};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    expect((await manager.subscribe(input())).cursor).toBe('c1');
    manager.emit(event('evt-2', 'c2'));
    await manager.waitForIdle();
    expect(ids).toEqual(['evt-1', 'evt-2']);
    expect(manager.size).toBe(1);
  });

  it('renews the timestamp and signature while preserving the retry message ID', async () => {
    let time = 1000;
    const headers: Array<Record<string, string>> = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns, retryBaseMs: 1,
      now: () => time,
      post: async (request: {headers: Record<string, string>}) => {
        headers.push(request.headers);
        if (headers.length === 1) { time += 1000; return {status: 503}; }
        return {status: 204};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    expect(headers).toHaveLength(2);
    expect(headers[1]['webhook-id']).toBe(headers[0]['webhook-id']);
    expect(headers.map(value => value['webhook-timestamp'])).toEqual(['1', '2']);
    expect(headers[1]['webhook-signature']).not.toBe(headers[0]['webhook-signature']);
  });

  it('reports a gap when the bounded queue overflows', async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const deliveries: Array<{type?: string; eventId?: string; cursor: string | null}> = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns, maxPendingPerSubscription: 1,
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString());
        deliveries.push(payload);
        if (payload.eventId === 'evt-1') await gate;
        return {status: 204};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    manager.emit(event('evt-2', 'c2'));
    manager.emit(event('evt-3', 'c3'));
    release?.();
    await manager.waitForIdle();
    expect(deliveries).toEqual([{...event('evt-1', 'c1'), cursor: null}, {type: 'gap', cursor: 'c3'}]);
  });

  it('reports a gap when a receiver rejects an oversized event', async () => {
    const deliveries: Array<{type?: string; eventId?: string}> = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns,
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString());
        deliveries.push(payload);
        return {status: payload.type === 'gap' ? 204 : 413};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    expect(deliveries).toMatchObject([{eventId: 'evt-1'}, {type: 'gap', cursor: 'c1'}]);
  });

  it.each([413, 503])('keeps the full overflow gap when an earlier delivery fails with %i', async status => {
    const gate = Promise.withResolvers<void>();
    const deliveries: Array<{type?: string; eventId?: string; cursor: string | null}> = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns,
      maxPendingPerSubscription: 1, maxAttempts: 1,
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString());
        deliveries.push(payload);
        if (payload.eventId) { await gate.promise; return {status}; }
        return {status: 204};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    for (let index = 1; index <= 3; index++) manager.emit(event(`evt-${index}`, `c${index}`));
    gate.resolve();
    await manager.waitForIdle();
    expect(deliveries).toMatchObject([{eventId: 'evt-1'}, {type: 'gap', cursor: 'c3'}]);
    expect((await manager.subscribe(input())).cursor).toBe('c3');
  });

  it('reports a gap after an event exhausts its retries', async () => {
    const deliveries: Array<{type?: string; eventId?: string}> = [];
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns, maxAttempts: 2, retryBaseMs: 1,
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString());
        deliveries.push(payload);
        return {status: payload.type === 'gap' ? 204 : 503};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await manager.waitForIdle();
    expect(deliveries).toMatchObject([{eventId: 'evt-1'}, {eventId: 'evt-1'}, {type: 'gap', cursor: 'c1'}]);
  });

  it('holds the saved cursor until the receiver accepts a gap', async () => {
    let releaseGap: (() => void) | undefined;
    let gapStarted: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { releaseGap = resolve; });
    const started = new Promise<void>(resolve => { gapStarted = resolve; });
    const manager = createWebhookDelivery({allowUrl: () => true, resolveHost: publicDns, maxAttempts: 1,
      getCurrentCursor: () => 'start',
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString());
        if (payload.type === 'gap') {
          gapStarted?.();
          await gate;
          return {status: 204};
        }
        return {status: 503};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    await started;
    expect((await manager.subscribe(input())).cursor).toBe('start');
    releaseGap?.();
    await manager.waitForIdle();
    expect((await manager.subscribe(input())).cursor).toBe('c1');
  });

  it('cancels an in-flight verification when the server closes', async () => {
    let started: (() => void) | undefined;
    const ready = new Promise<void>(resolve => { started = resolve; });
    const manager = createWebhookDelivery({allowLocalAddresses: true,
      post: ({signal}: {signal: AbortSignal}) => new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => reject(new Error('aborted')), {once: true});
        started?.();
      }),
    });
    managers.push(manager);
    const pending = manager.subscribe(input({url: 'https://localhost:8443/hooks'}));
    await ready;
    manager.close();
    await expect(pending).rejects.toThrow('Webhook endpoint verification failed');
    expect(manager.size).toBe(0);
  });

  it('pins a contiguous safe watermark through overlapping delivery retries', async () => {
    const calls: Array<{payload: {eventId: string; cursor: string | null}; status: number}> = [];
    let releaseFirst: (() => void) | undefined;
    const firstBlocked = new Promise<void>(resolve => { releaseFirst = resolve; });
    let attempts = 0;
    const manager = createWebhookDelivery({
      allowUrl: () => true,
      resolveHost: publicDns,
      maxAttempts: 2,
      retryBaseMs: 1,
      maxPendingPerSubscription: 10,
      post: async ({body}: {body: Buffer}) => {
        const payload = JSON.parse(body.toString()) as {eventId: string; cursor: string | null};
        calls.push({payload, status: 0});
        if (payload.eventId === 'evt-1' && attempts++ === 0) {
          await firstBlocked;
          return {status: 503, body: Buffer.alloc(0)};
        }
        return {status: 204, body: Buffer.alloc(0)};
      },
    });
    managers.push(manager);
    await manager.subscribe(input());
    manager.emit(event('evt-1', 'c1'));
    manager.emit(event('evt-2', 'c2'));
    await new Promise(resolve => setTimeout(resolve, 10));
    releaseFirst?.();
    await manager.waitForIdle();
    const secondPayload = calls.find(call => call.payload.eventId === 'evt-2')?.payload;
    expect(secondPayload?.cursor).toBeNull();
    expect(calls.filter(call => call.payload.eventId === 'evt-1')).toHaveLength(2);
    expect(manager.emit(event('evt-3', 'c3'))).toBe(1);
    await manager.waitForIdle();
  });

  it('keeps tenant unsubscribe scoped and applies expiry cleanup', async () => {
    let time = 1_000;
    const manager = createWebhookDelivery({now: () => time, allowUrl: () => true});
    managers.push(manager);
    await manager.subscribe(input());
    expect(await manager.unsubscribe(input({principal: 'user-2'}))).toBe(false);
    expect(manager.size).toBe(1);
    time += 60_001;
    expect(manager.size).toBe(0);
  });
});
