import {createHmac} from 'node:crypto';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {WebhookReceiver, type WebhookRegistration} from '../src/webhook.ts';
import {EventsWire} from '../src/wire.ts';
import {EventsClient, type Observation} from '../src/client.ts';
import {TestTransport, event, testDescriptor} from './transport.ts';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });
async function setup() {
  const receiver = new WebhookReceiver({publicUrl: 'https://events.example/hooks', port: 0});
  await receiver.start();
  cleanups.push(() => receiver.close());
  const address = receiver.address();
  if (typeof address !== 'object' || !address) throw new Error('Missing receiver address');
  const post = async (registration: Pick<WebhookRegistration, 'url' | 'secret'>, body: unknown, options: {id?: string; timestamp?: number; signature?: string; subscription?: string} = {}) => {
    const raw = JSON.stringify(body);
    const id = options.id ?? 'msg_unique';
    const timestamp = String(options.timestamp ?? Math.floor(Date.now() / 1000));
    const signature = options.signature ?? `v1,${createHmac('sha256', Buffer.from(registration.secret.slice(6), 'base64')).update(`${id}.${timestamp}.${raw}`).digest('base64')}`;
    return fetch(`http://127.0.0.1:${address.port}${new URL(registration.url).pathname}`, {
      method: 'POST', headers: {
        'content-type': 'application/json', 'webhook-id': id, 'webhook-timestamp': timestamp,
        'webhook-signature': signature, 'x-mcp-subscription-id': options.subscription ?? 'sub_1',
      }, body: raw,
    });
  };
  return {receiver, post};
}
describe('webhook receiver', () => {
  it('answers a signed verification before binding, but retries an event that races subscribe', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn(async () => {});
    const route = receiver.register(accept);
    const response = await post(route, {type: 'verification', challenge: 'nonce'});
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({challenge: 'nonce'});
    expect((await post(route, event('e'))).status).toBe(503);
    expect(accept).not.toHaveBeenCalled();
    route.bind('sub_1');
    expect((await post(route, event('e'))).status).toBe(204);
    expect(accept).toHaveBeenCalledOnce();
  });
  it('rejects invalid signatures, stale timestamps, and incorrect subscription routing', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn(async () => {});
    const route = receiver.register(accept);
    route.bind('sub_1');
    expect((await post(route, {type: 'verification', challenge: 'nonce'}, {signature: 'v1,wrong'})).status).toBe(401);
    expect((await post(route, event('e'), {timestamp: Math.floor(Date.now() / 1000) - 301})).status).toBe(401);
    expect((await post(route, event('e'), {subscription: 'other'})).status).toBe(403);
    expect(accept).not.toHaveBeenCalled();
  });
  it('acknowledges only after forwarding and deduplicates simultaneous retries', async () => {
    const {receiver, post} = await setup();
    const forwarded = Promise.withResolvers<void>();
    const accept = vi.fn(() => forwarded.promise);
    const route = receiver.register(accept);
    route.bind('sub_1');
    let responded = false;
    const first = post(route, event('e')).then(result => {responded = true; return result;});
    const retry = post(route, event('e'));
    await vi.waitFor(() => expect(accept).toHaveBeenCalledOnce());
    expect(responded).toBe(false);
    forwarded.resolve();
    expect((await first).status).toBe(204);
    expect((await retry).status).toBe(204);
    expect(accept).toHaveBeenCalledOnce();
    expect((await post(route, {type: 'gap', cursor: 'watermark'}, {id: 'msg_gap_1'})).status).toBe(204);
    expect(accept).toHaveBeenLastCalledWith({type: 'gap', cursor: 'watermark'});
  });
  it('allows retry after forwarding fails', async () => {
    const {receiver, post} = await setup();
    const accept = vi.fn().mockRejectedValueOnce(new Error('queue full')).mockResolvedValue(undefined);
    const route = receiver.register(accept);
    route.bind('sub_1');
    expect((await post(route, event('e'))).status).toBe(503);
    expect((await post(route, event('e'))).status).toBe(204);
    expect(accept).toHaveBeenCalledTimes(2);
  });
  it.each([-32024, -32001])('preserves server termination %i during an in-flight renewal', async code => {
    const {receiver, post} = await setup();
    const transport = new TestTransport();
    const client = new EventsClient(new EventsWire(transport), {receiver, retryMs: 1});
    cleanups.push(() => client.close());
    let subscriptions = 0;
    let delivery: Pick<WebhookRegistration, 'url' | 'secret'> | undefined;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/subscribe') {
        delivery = request.params!.delivery as {url: string; secret: string};
        subscriptions++;
        if (subscriptions === 1) transport.respond(request, {id: 'sub_1', refreshBefore: new Date(Date.now() + 50).toISOString(), truncated: false});
      }
      if (request.method === 'events/unsubscribe') transport.respond(request, {});
    };
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'webhook'}, () => {});
    await watch.ready;
    await vi.waitFor(() => expect(subscriptions).toBe(2));
    const result = await post(delivery!, {type: 'terminated', error: {code, message: 'Subscription ended'}}, {id: 'msg_terminated_1'});
    expect(result.status).toBe(204);
    await expect(watch.closed).rejects.toMatchObject({code});
    expect(subscriptions).toBe(2);
  });
  it('renews the granted TTL, forwards signed events and controls, and unsubscribes on stop', async () => {
    const {receiver, post} = await setup();
    const transport = new TestTransport();
    const client = new EventsClient(new EventsWire(transport), {receiver, retryMs: 1});
    cleanups.push(() => client.close());
    let delivery: Pick<WebhookRegistration, 'url' | 'secret'> | undefined;
    let subscriptions = 0;
    let unsubscribed = false;
    transport.handle = async request => {
      switch (request.method) {
        case 'events/list': transport.respond(request, {events: [testDescriptor]}); break;
        case 'events/subscribe': {
          delivery = request.params!.delivery as {url: string; secret: string};
          if (subscriptions++ === 0) {
            const verification = await post(delivery, {type: 'verification', challenge: 'local-nonce'});
            expect(await verification.json()).toEqual({challenge: 'local-nonce'});
          }
          transport.respond(request, {id: 'sub_1', refreshBefore: new Date(Date.now() + 100).toISOString(), cursor: 'safe-watermark', truncated: false});
          break;
        }
        case 'events/unsubscribe':
          unsubscribed = true;
          expect(request.params?.delivery).toEqual({url: delivery!.url});
          transport.respond(request, {});
          break;
      }
    };
    await client.connect();
    const observations: Observation[] = [];
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'webhook', ttlMs: 1000}, value => {observations.push(value);});
    await watch.ready;
    expect(delivery!.secret).toMatch(/^whsec_/);
    expect((await post(delivery!, event('one', 'watermark-1'))).status).toBe(204);
    expect((await post(delivery!, event('one', 'watermark-1'), {id: 'msg_same_event_again'})).status).toBe(204);
    expect(observations.filter(value => value.type === 'event')).toHaveLength(1);
    expect((await post(delivery!, {type: 'gap', cursor: 'gap-watermark'}, {id: 'msg_gap_2'})).status).toBe(204);
    expect(watch.cursor).toBe('gap-watermark');
    await vi.waitFor(() => expect(subscriptions).toBeGreaterThan(1));
    expect(watch.cursor).toBe('safe-watermark');
    await watch.stop();
    expect(unsubscribed).toBe(true);
  });
});
