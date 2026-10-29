import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, beforeEach, expect, it, vi} from 'vitest';
import {EventsClient, EventsWire, SessionState, EventsClientErrorCode as ErrorCode, type ClientOptions} from '../src/index.ts';
import {TestTransport} from './transport.ts';

const descriptor = {name: 'example.changed', delivery: ['push'], inputSchema: true, payloadSchema: true};
const options = {name: descriptor.name, arguments: {}, mode: 'push' as const, cursor: 'saved'};
const event = (eventId: string, cursor: string | null = eventId) => ({eventId, cursor, name: descriptor.name, timestamp: '2026-10-29T12:00:00Z', data: {value: 1}});
type Stream = {transport: TestTransport; request: JSONRPCRequest};
const notify = ({transport, request}: Stream, kind: string, value: Record<string, unknown>) => transport.notify(request, kind, value);
const flush = () => vi.advanceTimersByTimeAsync(0);
const cleanups: Array<() => void | Promise<void>> = [];
beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, 'random').mockReturnValue(0.5);
});
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

async function setup(settings: ClientOptions = {}) {
  const transports: TestTransport[] = [];
  const streams: Stream[] = [];
  let onStream = (_stream: Stream) => {};
  const client = new EventsClient(new EventsWire(() => {
    const transport = new TestTransport();
    transports.push(transport);
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
      if (request.method === 'events/stream') {
        const stream = {transport, request};
        streams.push(stream);
        onStream(stream);
      }
    };
    return transport;
  }), {retryMs: 10, heartbeatMs: 100, maxRetries: 2, ...settings});
  cleanups.push(() => client.close());
  await client.connect();
  return {client, transports, streams, onStream(handler: typeof onStream) { onStream = handler; },
    get catalogs() { return transports.flatMap(transport => transport.messages).filter(({message}) => 'method' in message && message.method === 'events/list').length; },
  };
}

it.each([
  {persistent: false, replay: false}, {persistent: true, replay: false},
  {persistent: false, replay: true}, {persistent: true, replay: true},
])('drains queued events before reconnecting: %j', async ({persistent, replay}) => {
  let path;
  if (persistent) {
    const directory = mkdtempSync(join(tmpdir(), 'pi-events-push-'));
    cleanups.push(() => rmSync(directory, {recursive: true, force: true}));
    path = join(directory, 'state.json');
  }
  const state = new SessionState(path);
  state.setCursor('watch', 'saved');
  const s = await setup();
  const gate = Promise.withResolvers<void>();
  const watch = s.client.watch(options, async observation => {
    if (observation.type !== 'event') return;
    if (observation.event.eventId === 'first') await gate.promise;
    state.accept({server: 'server', watch: 'watch', observation});
  }, cursor => { state.setCursor('watch', cursor); });
  await flush();
  const stream = s.streams[0]!;
  notify(stream, 'active', {cursor: replay ? 'active' : null, truncated: false});
  await watch.ready;
  notify(stream, 'event', event('first', replay ? 'first' : null));
  notify(stream, 'event', event('second', replay ? 'second' : null));
  await flush();
  await stream.transport.close();
  await vi.advanceTimersByTimeAsync(50);
  expect(s.streams).toHaveLength(1);
  expect(state.size).toBe(0);
  gate.resolve();
  await flush();
  expect(state.snapshot().map(item => item.observation)).toMatchObject([
    {type: 'event', event: event('first', replay ? 'first' : null)},
    {type: 'event', event: event('second', replay ? 'second' : null)},
  ]);
  expect(state.getCursor('watch')).toBe(replay ? 'second' : null);
  if (path) {
    const restored = new SessionState(path);
    expect(restored.snapshot()).toEqual(state.snapshot());
    expect(restored.getCursor('watch')).toBe(replay ? 'second' : null);
  }
  await vi.advanceTimersByTimeAsync(15);
  expect(s.streams).toHaveLength(2);
  expect(s.streams[1]!.request.params?.cursor).toBe(replay ? 'second' : null);
});

it('keeps draining streams tied to the old transport while another watch reconnects', async () => {
  const s = await setup();
  const gate = Promise.withResolvers<void>();
  const left = s.client.watch(options, item => item.type === 'event' ? gate.promise : undefined);
  const right = s.client.watch({...options, arguments: {other: true}}, () => {});
  await flush();
  for (const stream of s.streams) notify(stream, 'active', {cursor: 'active', truncated: false});
  await Promise.all([left.ready, right.ready]);
  notify(s.streams[0]!, 'event', event('first'));
  notify(s.streams[0]!, 'event', event('second'));
  await flush();
  await s.transports[0]!.close();
  await vi.advanceTimersByTimeAsync(15);
  expect(s.transports).toHaveLength(2);
  expect(s.streams).toHaveLength(3);
  const reconnected = s.streams[2]!;
  notify(reconnected, 'active', {cursor: 'right', truncated: false});
  await flush();
  await s.transports[0]!.close();
  await s.client.refresh();
  gate.resolve();
  await vi.advanceTimersByTimeAsync(15);
  expect(s.transports).toHaveLength(2);
  expect(s.streams).toHaveLength(4);
  expect(s.streams[3]!.request.params?.cursor).toBe('second');
  notify(reconnected, 'heartbeat', {cursor: 'still-right'});
  await flush();
  expect(right.cursor).toBe('still-right');
});

it.each(['heartbeat', 'duplicate-event'])('bounds short-lived streams with %s traffic using exponential jitter', async traffic => {
  const s = await setup();
  s.onStream(stream => {
    notify(stream, 'active', {cursor: 'same', truncated: false});
    if (traffic === 'heartbeat') notify(stream, 'heartbeat', {cursor: 'same'});
    else notify(stream, 'event', event('same'));
    stream.transport.respond(stream.request, {});
  });
  const accepted: string[] = [];
  const watch = s.client.watch(options, item => { if (item.type === 'event') accepted.push(item.event.eventId); });
  const failed = expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
  await flush();
  expect(s.streams).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(14);
  expect(s.streams).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(s.streams).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(29);
  expect(s.streams).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(1);
  await failed;
  expect(s.streams).toHaveLength(3);
  expect(accepted).toEqual(traffic === 'heartbeat' ? [] : ['same']);
  expect(watch.cursor).toBe('same');
});

it.each(['same', null])('resets recovery after sustained quiet health with cursor %s', async cursor => {
  const s = await setup({maxRetries: 1});
  s.onStream(stream => {
    notify(stream, 'active', {cursor, truncated: false});
    notify(stream, 'heartbeat', {cursor});
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  for (let i = 0; i < 3; i++) {
    const stream = s.streams[i]!;
    for (let beat = 0; beat < 2; beat++) {
      await vi.advanceTimersByTimeAsync(100);
      notify(stream, 'heartbeat', {cursor});
      await flush();
    }
    stream.transport.respond(stream.request, {});
    await flush();
    expect(s.streams).toHaveLength(i + 2);
  }
  const failed = expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
  s.streams[3]!.transport.respond(s.streams[3]!.request, {});
  await vi.advanceTimersByTimeAsync(15);
  expect(s.streams).toHaveLength(5);
  s.streams[4]!.transport.respond(s.streams[4]!.request, {});
  await flush();
  await failed;
});

it('updates repeated active cursors without extending heartbeat liveness', async () => {
  const s = await setup({heartbeatMs: 20});
  s.onStream(stream => notify(stream, 'active', {cursor: 'initial', truncated: false}));
  const watch = s.client.watch(options, () => {});
  await flush();
  for (let i = 1; i <= 3; i++) {
    await vi.advanceTimersByTimeAsync(10);
    notify(s.streams[0]!, 'active', {cursor: `active-${i}`, truncated: i === 3});
    await flush();
    expect(watch.cursor).toBe(`active-${i}`);
  }
  await vi.advanceTimersByTimeAsync(10);
  expect(watch.status).toBe('retrying');
  await vi.advanceTimersByTimeAsync(15);
  expect(s.streams).toHaveLength(2);
  expect(s.streams[1]!.request.params?.cursor).toBe('active-3');
});

it('keeps the default 30-second heartbeat assumption', async () => {
  const s = await setup({heartbeatMs: undefined});
  s.onStream(stream => notify(stream, 'active', {cursor: 'active', truncated: false}));
  const watch = s.client.watch(options, () => {});
  await flush();
  await vi.advanceTimersByTimeAsync(59_999);
  expect(s.streams).toHaveLength(1);
  expect(watch.status).toBe('active');
  await vi.advanceTimersByTimeAsync(16);
  expect(s.streams).toHaveLength(2);
});

const terminateSchema = (stream: Stream) => notify(stream, 'terminated', {
  error: {code: -32026, message: 'Schema changed', data: {feature: 'payloadSchema', reason: 'schema_changed'}},
});

it.each(['heartbeat', 'duplicate-event', 'new-event'])('bounds schema recovery with short-lived %s traffic', async traffic => {
  const s = await setup();
  s.onStream(stream => {
    notify(stream, 'active', {cursor: 'same', truncated: false});
    if (traffic === 'heartbeat') notify(stream, 'heartbeat', {cursor: 'same'});
    else notify(stream, 'event', event(traffic === 'new-event' ? `event-${s.streams.length}` : 'same', 'same'));
    if (s.streams.length <= 4) terminateSchema(stream);
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  expect(s.streams).toHaveLength(2);
  expect(s.catalogs).toBe(2);
  await vi.advanceTimersByTimeAsync(14);
  expect(s.streams).toHaveLength(2);
  expect(s.catalogs).toBe(2);
  await vi.advanceTimersByTimeAsync(1);
  expect(s.streams).toHaveLength(3);
  expect(s.catalogs).toBe(3);
  await vi.advanceTimersByTimeAsync(29);
  expect(s.streams).toHaveLength(3);
  expect(s.catalogs).toBe(3);
  await vi.advanceTimersByTimeAsync(1);
  await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted,
    cause: {code: ErrorCode.SubscriptionTerminated, cause: {code: -32026}}});
  expect(s.streams).toHaveLength(4);
  expect(s.catalogs).toBe(4);
  expect(s.streams.map(stream => stream.request.params?.cursor)).toEqual(['saved', null, null, null]);
});

it.each([
  {code: -32026, message: 'Schema changed', data: {reason: 'schema_changed'}},
  {code: -32023, message: 'Event removed', data: {kind: 'event'}},
])('allows one immediate rediscovery with retries disabled for $message, then bounds catalog reads', async error => {
  const s = await setup({maxRetries: 0});
  s.onStream(stream => {
    notify(stream, 'active', {cursor: 'same', truncated: false});
    notify(stream, 'heartbeat', {cursor: 'same'});
    if (s.streams.length <= 2) notify(stream, 'terminated', {error});
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  expect(s.streams).toHaveLength(2);
  await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
  expect(s.catalogs).toBe(2);
});

it.each(['same', null])('resets schema recovery only after sustained quiet health with cursor %s', async cursor => {
  const s = await setup({maxRetries: 1});
  s.onStream(stream => {
    notify(stream, 'active', {cursor, truncated: false});
    notify(stream, 'heartbeat', {cursor});
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  terminateSchema(s.streams[0]!);
  await flush();
  expect(s.streams).toHaveLength(2);
  terminateSchema(s.streams[1]!);
  await flush();
  expect(s.streams).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(15);
  expect(s.streams).toHaveLength(3);
  for (let beat = 0; beat < 2; beat++) {
    await vi.advanceTimersByTimeAsync(100);
    notify(s.streams[2]!, 'heartbeat', {cursor});
    await flush();
  }
  terminateSchema(s.streams[2]!);
  await flush();
  expect(s.streams).toHaveLength(4);
  terminateSchema(s.streams[3]!);
  await flush();
  expect(s.streams).toHaveLength(4);
  await vi.advanceTimersByTimeAsync(15);
  expect(s.streams).toHaveLength(5);
  terminateSchema(s.streams[4]!);
  await flush();
  await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
  expect(s.catalogs).toBe(5);
});

it('shares the stream retry budget across schema terminations and connection failures', async () => {
  const s = await setup();
  s.onStream(stream => {
    notify(stream, 'active', {cursor: 'same', truncated: false});
    notify(stream, 'heartbeat', {cursor: 'same'});
    if ([1, 3].includes(s.streams.length)) terminateSchema(stream);
    else stream.transport.respond(stream.request, {});
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  expect(s.streams).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(15);
  expect(s.streams).toHaveLength(3);
  await vi.advanceTimersByTimeAsync(29);
  expect(s.streams).toHaveLength(3);
  await vi.advanceTimersByTimeAsync(1);
  await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
  expect(s.streams).toHaveLength(4);
  expect(s.catalogs).toBe(3);
});

it('cancels schema-recovery backoff without another stream or catalog read', async () => {
  const s = await setup();
  s.onStream(stream => {
    notify(stream, 'active', {cursor: 'same', truncated: false});
    notify(stream, 'heartbeat', {cursor: 'same'});
    if (s.streams.length <= 2) terminateSchema(stream);
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  expect(s.streams).toHaveLength(2);
  await watch.stop();
  await vi.advanceTimersByTimeAsync(1000);
  expect(s.streams).toHaveLength(2);
  expect(s.catalogs).toBe(2);
  expect(vi.getTimerCount()).toBe(0);
});

it('keeps authorization termination fatal after schema recovery', async () => {
  const s = await setup();
  s.onStream(stream => {
    notify(stream, 'active', {cursor: 'same', truncated: false});
    notify(stream, 'heartbeat', {cursor: 'same'});
    if (s.streams.length === 1) terminateSchema(stream);
    else notify(stream, 'terminated', {error: {code: -32024, message: 'Access revoked'}});
  });
  const watch = s.client.watch(options, () => {});
  await flush();
  await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.SubscriptionTerminated, cause: {code: -32024}});
  expect(s.streams).toHaveLength(2);
  expect(s.catalogs).toBe(2);
});
