import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, type ClientOptions} from '../src/client.ts';
import {EventsClientErrorCode as ErrorCode} from '../src/errors.ts';
import {SUBSCRIPTION_ID} from '../src/protocol.ts';
import {EventsWire} from '../src/wire.ts';
import {TestTransport} from './transport.ts';

const descriptor = {name: 'example.changed', delivery: ['poll'], inputSchema: {}, payloadSchema: {}};
const cleanups: Array<() => Promise<void>> = [];
beforeEach(() => vi.useFakeTimers());
afterEach(async () => {
  await Promise.all(cleanups.splice(0).map(close => close()));
  vi.useRealTimers();
});
const flush = () => vi.advanceTimersByTimeAsync(0);
function setup(settings: ClientOptions = {}) {
  const transport = new TestTransport();
  transport.listChanged = true;
  const client = new EventsClient(new EventsWire(transport), {retryMs: 10, maxRetries: 2, catalogAckTimeoutMs: 100, ...settings});
  const listeners: JSONRPCRequest[] = [];
  const lists: JSONRPCRequest[] = [];
  transport.handle = request => {
    if (request.method === 'events/list') { lists.push(request); transport.respond(request, {events: [descriptor]}); }
    else listeners.push(request);
  };
  cleanups.push(() => client.close());
  const acknowledge = (request = listeners.at(-1)!, notifications: Record<string, unknown> = {eventsListChanged: true}) => {
    transport.onmessage?.({jsonrpc: '2.0', method: 'notifications/subscriptions/acknowledged', params: {
      notifications, _meta: {[SUBSCRIPTION_ID]: request.id},
    }});
  };
  return {client, transport, listeners, lists, acknowledge};
}

describe('automatic catalog updates', () => {
  it('keeps catalog monitoring and supported watches working through schema failures and repairs', async () => {
    const h = setup();
    await h.client.connect(); await flush(); h.acknowledge(); await h.client.catalogUpdates!.ready;
    let events = [descriptor, {...descriptor, name: 'unavailable', payloadSchema: {$schema: 'https://example.com/schema'}}];
    const polls: JSONRPCRequest[] = [];
    h.transport.handle = request => {
      if (request.method === 'events/list') h.transport.respond(request, {events});
      else polls.push(request);
    };
    const watch = h.client.watch({name: descriptor.name, arguments: {}, mode: 'poll'}, () => {});
    await flush();
    h.transport.respond(polls[0]!, {events: [], cursor: 'quiet', truncated: false, hasMore: false, nextPollMs: 100_000});
    await watch.ready;
    h.transport.notify(h.listeners[0]!, 'list_changed', {});
    await flush();
    expect(h.client.catalogIssues.map(issue => issue.name)).toEqual(['unavailable']);
    expect(watch.status).toBe('active');
    events = [descriptor];
    h.transport.notify(h.listeners[0]!, 'list_changed', {});
    await flush();
    expect(h.client.catalogIssues).toEqual([]);
    expect(h.client.list()).toEqual([descriptor]);
    expect(h.listeners).toHaveLength(1);
    expect(watch.status).toBe('active');
  });
  it('opens one subscription for concurrent connects only when listChanged is advertised', async () => {
    const {client, transport, listeners} = setup();
    transport.listChanged = false;
    await client.connect();
    await flush();
    expect(client.catalogUpdates).toBeUndefined();
    expect(listeners).toHaveLength(0);
    const enabled = setup();
    await Promise.all([enabled.client.connect(), enabled.client.connect()]);
    await flush();
    expect(enabled.listeners).toHaveLength(1);
    expect(enabled.listeners[0]!.params).toMatchObject({notifications: {eventsListChanged: true}});
    enabled.acknowledge();
    await enabled.client.catalogUpdates!.ready;
    await enabled.client.connect();
    expect(enabled.listeners).toHaveLength(1);
  });

  it('waits for acknowledgement and refresh before ready, then remains open without heartbeats', async () => {
    const {client, transport, listeners, lists, acknowledge} = setup();
    await client.connect();
    await flush();
    const ready = vi.fn();
    void client.catalogUpdates!.ready.then(ready);
    const pending: JSONRPCRequest[] = [];
    transport.handle = request => { pending.push(request); };
    acknowledge();
    await flush();
    expect(ready).not.toHaveBeenCalled();
    expect(pending).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(listeners).toHaveLength(1);
    expect(lists).toHaveLength(1);
    transport.respond(pending[0]!, {events: [{...descriptor, description: 'new'}]});
    await client.catalogUpdates!.ready;
    expect(client.list()[0]!.description).toBe('new');
    await vi.advanceTimersByTimeAsync(120_000);
    expect(pending).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('refreshes only for notifications on the correlated, acknowledged stream', async () => {
    const {client, transport, listeners, lists, acknowledge} = setup();
    await client.connect();
    await flush(); acknowledge(); await client.catalogUpdates!.ready;
    transport.onmessage?.({jsonrpc: '2.0', method: 'notifications/events/list_changed', params: {}});
    transport.notify({...listeners[0]!, id: 'pi-events:unrelated'}, 'list_changed', {});
    await flush();
    expect(lists).toHaveLength(2);
    transport.notify(listeners[0]!, 'list_changed', {});
    await flush();
    expect(lists).toHaveLength(3);
  });

  it.each(['missing', 'false', 'before-ack', 'duplicate', 'unexpected'])('surfaces invalid subscription behavior: %s', async kind => {
    const {client, transport, listeners, acknowledge} = setup();
    await client.connect(); await flush();
    const updates = client.catalogUpdates!;
    if (kind === 'missing') acknowledge(listeners[0]!, {});
    if (kind === 'false') acknowledge(listeners[0]!, {eventsListChanged: false});
    if (kind === 'before-ack') transport.notify(listeners[0]!, 'list_changed', {});
    if (kind === 'duplicate' || kind === 'unexpected') {
      acknowledge(); await updates.ready;
      if (kind === 'duplicate') acknowledge();
      else transport.notify(listeners[0]!, 'event', {});
    }
    await expect(updates.closed).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
    if (kind !== 'duplicate' && kind !== 'unexpected') await expect(updates.ready).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
    expect(listeners).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
    expect(client.list()).toEqual([descriptor]);
  });

  it('bounds retries when acknowledgement never arrives', async () => {
    const {client, listeners} = setup();
    await client.connect(); await flush();
    await vi.advanceTimersByTimeAsync(330);
    await expect(client.catalogUpdates!.closed).rejects.toMatchObject({
      code: ErrorCode.RetryExhausted, cause: {code: ErrorCode.RequestFailed},
    });
    expect(listeners).toHaveLength(3);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('supports acknowledgement deadlines beyond the native timer range', async () => {
    const {client, transport, listeners} = setup({catalogAckTimeoutMs: 2 ** 31, maxRetries: 0});
    await client.connect(); await flush();
    await vi.advanceTimersByTimeAsync(2 ** 31 - 1);
    expect(listeners).toHaveLength(1);
    expect(transport.messages.some(({message}) => 'method' in message && message.method === 'notifications/cancelled')).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await expect(client.catalogUpdates!.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
  });

  it.each(['disconnect', 'complete'])('refreshes after reconnecting following %s', async ending => {
    const {client, transport, listeners, lists, acknowledge} = setup({maxRetries: 1});
    await client.connect(); await flush();
    for (let i = 0; i < 3; i++) {
      acknowledge(); await flush();
      expect(lists).toHaveLength(i + 2);
      const request = listeners[i]!;
      if (ending === 'complete') transport.respond(request, {});
      else transport.messages.find(({message}) => 'id' in message && message.id === request.id)!.options!.onRequestStreamEnd!();
      await vi.advanceTimersByTimeAsync(10);
      expect(listeners).toHaveLength(i + 2);
    }
    await client.close();
    await expect(client.catalogUpdates!.closed).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('retries a failed catalog read on the existing stream', async () => {
    const {client, transport, listeners, acknowledge} = setup();
    await client.connect(); await flush(); acknowledge(); await client.catalogUpdates!.ready;
    let reads = 0;
    transport.handle = request => {
      if (++reads === 1) transport.fail(request, -32603);
      else transport.respond(request, {events: [{...descriptor, description: 'updated'}]});
    };
    transport.notify(listeners[0]!, 'list_changed', {});
    await vi.advanceTimersByTimeAsync(10);
    expect(reads).toBe(2);
    expect(listeners).toHaveLength(1);
    expect(client.list()[0]!.description).toBe('updated');
  });

  it('starts a fresh read when a notification arrives during an older manual refresh', async () => {
    const {client, transport, listeners, acknowledge} = setup();
    await client.connect(); await flush(); acknowledge(); await client.catalogUpdates!.ready;
    const pending: JSONRPCRequest[] = [];
    transport.handle = request => { pending.push(request); };
    const manual = client.refresh();
    await flush();
    transport.notify(listeners[0]!, 'list_changed', {});
    await flush();
    expect(pending).toHaveLength(1);
    transport.respond(pending[0]!, {events: [{...descriptor, description: 'old snapshot'}]});
    await manual; await flush();
    expect(pending).toHaveLength(2);
    transport.respond(pending[1]!, {events: [{...descriptor, description: 'after notification'}]});
    await flush();
    expect(client.list()[0]!.description).toBe('after notification');
  });

  it('retains the previous catalog on invalid refresh and permits an explicit listener restart', async () => {
    const {client, transport, listeners, acknowledge} = setup();
    await client.connect(); await flush(); acknowledge(); await client.catalogUpdates!.ready;
    const previous = client.catalogUpdates!;
    const original = transport.handle;
    transport.handle = request => transport.respond(request, {events: [descriptor, descriptor]});
    transport.notify(listeners[0]!, 'list_changed', {});
    await expect(previous.closed).rejects.toMatchObject({code: ErrorCode.InvalidCatalog});
    expect(client.list()).toEqual([descriptor]);
    transport.handle = original;
    await Promise.all([client.connect(), client.connect()]); await flush();
    expect(listeners).toHaveLength(2);
    expect(client.catalogUpdates).not.toBe(previous);
    acknowledge(); await client.catalogUpdates!.ready;
  });

  it('keeps event watches running if the optional catalog listener fails', async () => {
    const {client, transport, listeners, acknowledge} = setup();
    await client.connect(); await flush(); acknowledge(); await client.catalogUpdates!.ready;
    transport.handle = request => transport.respond(request, {events: [], cursor: 'poll', truncated: false, hasMore: false, nextPollMs: 100_000});
    const watch = client.watch({name: descriptor.name, arguments: {}, mode: 'poll'}, () => {});
    await watch.ready;
    transport.fail(listeners[0]!, -32024);
    await expect(client.catalogUpdates!.closed).rejects.toMatchObject({code: -32024});
    const closed = vi.fn();
    void watch.closed.then(closed);
    await flush();
    expect(closed).not.toHaveBeenCalled();
    expect(watch.cursor).toBe('poll');
  });

  it.each([false, true])('cancels the catalog stream and its pending refresh on close; HTTP=%s', async http => {
    const {client, transport, listeners, acknowledge} = setup();
    transport.hasPerRequestStream = http;
    await client.connect(); await flush(); acknowledge(); await client.catalogUpdates!.ready;
    const pending: JSONRPCRequest[] = [];
    transport.handle = request => { pending.push(request); };
    transport.notify(listeners[0]!, 'list_changed', {});
    await flush();
    expect(pending).toHaveLength(1);
    await client.close();
    await expect(client.catalogUpdates!.closed).resolves.toBeUndefined();
    expect(client.list()).toEqual([descriptor]);
    const stream = transport.messages.find(({message}) => 'method' in message && message.method === 'subscriptions/listen')!;
    expect(stream.options?.requestSignal?.aborted).toBe(true);
    const cancels = transport.messages.filter(({message}) => 'method' in message && message.method === 'notifications/cancelled');
    expect(cancels.some(({message}) => 'params' in message && message.params?.requestId === listeners[0]!.id)).toBe(!http);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('rejects readiness and closes cleanly when stopped before acknowledgement', async () => {
    const {client} = setup();
    await client.connect(); await flush();
    await client.close();
    await expect(client.catalogUpdates!.ready).rejects.toMatchObject({code: ErrorCode.Closed});
    await expect(client.catalogUpdates!.closed).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });
});
