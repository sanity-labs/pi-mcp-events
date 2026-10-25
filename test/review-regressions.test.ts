import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient} from '../src/client.ts';
import {EventsWire} from '../src/wire.ts';
import {SUBSCRIPTION_ID} from '../src/protocol.ts';
import {TestTransport, testDescriptor, event} from './transport.ts';

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {for (const cleanup of cleanups.splice(0).reverse()) await cleanup();});
function setup(settings = {}) {
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {retryMs: 1, maxRetries: 2, ...settings});
  cleanups.push(() => client.close());
  return {transport, client};
}
describe('review regressions', () => {
  it('bounds reconnects when every active stream delivers an invalid payload', async () => {
    const {transport, client} = setup({maxRetries: 1});
    let streams = 0;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/stream') {
        streams++;
        transport.notify(request, 'active', {cursor: 'a', truncated: false});
        transport.notify(request, 'event', {...event('invalid'), data: {value: 'not a number'}});
      }
    };
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {});
    await expect(watch.closed).rejects.toThrow('retry limit');
    expect(streams).toBe(2);
  });
  it('reconnects healthy completed streams without exhausting the failure budget', async () => {
    const {transport, client} = setup({maxRetries: 1});
    let streams = 0;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/stream') {
        streams++;
        transport.notify(request, 'active', {cursor: String(streams), truncated: false});
        if (streams < 4) transport.respond(request, {});
      }
    };
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {});
    await watch.ready;
    await vi.waitFor(() => expect(streams).toBe(4));
    expect(watch.cursor).toBe('4');
    await watch.stop();
    await expect(watch.closed).resolves.toBeUndefined();
  });
  it('bounds reconnects when streams complete without becoming active', async () => {
    const {transport, client} = setup({maxRetries: 1});
    let streams = 0;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/stream') {
        streams++;
        transport.respond(request, {});
      }
    };
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {});
    await expect(watch.closed).rejects.toThrow('retry limit');
    expect(streams).toBe(2);
  });
  it('retains partial poll progress while draining more than the deduplication window', async () => {
    const {transport, client} = setup();
    let polls = 0;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/poll') {
        polls++;
        const offset = Number(request.params?.cursor ?? 0);
        const count = Math.min(Number(request.params?.maxEvents), 4200 - offset);
        expect(count).toBeLessThanOrEqual(50);
        transport.respond(request, {events: Array.from({length: count}, (_, i) => event(String(offset + i))), cursor: String(offset + count), truncated: false, hasMore: offset + count < 4200, nextPollMs: 100000});
      }
    };
    await client.connect();
    const received: string[] = [];
    let failedAt = -1;
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'poll', cursor: '0'}, observation => {
      if (observation.type !== 'event') return;
      if (received.length % 37 === 0 && failedAt !== received.length) {
        failedAt = received.length;
        throw new Error('Consumer temporarily full');
      }
      received.push(observation.event.eventId);
    });
    await vi.waitFor(() => expect(watch.cursor).toBe('4200'), {timeout: 5000});
    expect(received).toHaveLength(4200);
    expect(new Set(received).size).toBe(4200);
    expect(polls).toBe(84);
  });
  it('retries a transient catalog failure during schema rediscovery', async () => {
    const {transport, client} = setup({maxRetries: 3});
    let lists = 0;
    let streams = 0;
    transport.handle = request => {
      if (request.method === 'events/list') {
        if (++lists === 2) transport.fail(request, -32603);
        else transport.respond(request, {events: [testDescriptor]});
      }
      if (request.method === 'events/stream') {
        streams++;
        transport.notify(request, 'active', {cursor: 'a', truncated: false});
        if (streams === 1) transport.notify(request, 'terminated', {error: {code: -32026, message: 'changed', data: {reason: 'schema_changed'}}});
      }
    };
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {});
    await watch.ready;
    await vi.waitFor(() => expect(streams).toBe(2));
    expect(lists).toBe(3);
  });
  it('retains the old checkpoint until a schema-change gap has been accepted', async () => {
    const {transport, client} = setup();
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/stream') {
        transport.notify(request, 'active', {cursor: 'old', truncated: false});
        transport.notify(request, 'terminated', {error: {code: -32026, message: 'changed', data: {reason: 'schema_changed'}}});
      }
    };
    await client.connect();
    let persisted: string | null = 'old';
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push', cursor: persisted}, observation => {
      if (observation.type === 'gap') throw new Error('Consumer full');
    }, cursor => {persisted = cursor;});
    await expect(watch.closed).rejects.toThrow('retry limit');
    expect(persisted).toBe('old');
  });
  it('resynchronizes the catalog across more healthy reconnects than its failure budget', async () => {
    const {transport, client} = setup({maxRetries: 1});
    transport.listChanged = true;
    let streams = 0;
    let lists = 0;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [{...testDescriptor, description: String(++lists)}]});
      if (request.method === 'subscriptions/listen') {
        streams++;
        transport.onmessage?.({jsonrpc: '2.0', method: 'notifications/subscriptions/acknowledged', params: {notifications: {eventsListChanged: true}, _meta: {[SUBSCRIPTION_ID]: request.id}}});
        if (streams < 4) transport.respond(request, {});
      }
    };
    await client.connect();
    await vi.waitFor(() => expect(streams).toBe(4));
    expect(lists).toBe(5);
    expect(client.list()[0]!.description).toBe('5');
  });
});
