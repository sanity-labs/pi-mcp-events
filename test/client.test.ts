import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, type ClientOptions, type Observation} from '../src/client.ts';
import {EventsWire} from '../src/wire.ts';
import {TestTransport} from './transport.ts';
import {EventsClientError, EventsClientErrorCode as ErrorCode, EventProtocolError, EventProtocolErrorCode} from '../src/index.ts';

const descriptor = {
  name: 'example.changed', delivery: ['poll'],
  inputSchema: {type: 'object', properties: {group: {type: 'string'}}},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'number'}}},
};
const event = (eventId: string) => ({eventId, name: descriptor.name, timestamp: '2026-10-25T12:00:00Z', data: {value: 1}});
const options = {name: descriptor.name, arguments: {}, mode: 'poll' as const, cursor: 'start'};
const batch = {events: [], cursor: 'next', truncated: false, hasMore: false, nextPollMs: 100_000};
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(cleanups.splice(0).map(close => close()));
  vi.useRealTimers();
});
function setup(settings: ClientOptions = {}) {
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {retryMs: 1, pollFloorMs: 10, maxRetries: 2, ...settings});
  const polls: JSONRPCRequest[] = [];
  transport.handle = request => {
    if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
    else polls.push(request);
  };
  cleanups.push(() => client.close());
  return {client, transport, polls};
}

describe('event discovery', () => {
  it('publishes a complete paginated catalog and shares concurrent refreshes', async () => {
    const {client, transport} = setup();
    await client.connect();
    const pages: JSONRPCRequest[] = [];
    transport.handle = request => { pages.push(request); };
    const first = client.refresh();
    expect(client.refresh()).toBe(first);
    await vi.waitFor(() => expect(pages).toHaveLength(1));
    transport.respond(pages[0]!, {events: [{...descriptor, name: 'a'}], nextCursor: 'page2'});
    await vi.waitFor(() => expect(pages).toHaveLength(2));
    expect(client.list()).toEqual([descriptor]);
    expect(pages[1]!.params?.cursor).toBe('page2');
    transport.respond(pages[1]!, {events: [{...descriptor, name: 'b'}]});
    expect((await first).map(item => item.name)).toEqual(['a', 'b']);
  });
  it.each(['duplicate', 'cursor-loop', 'invalid-schema'])('keeps the previous catalog after %s discovery fails', async kind => {
    const {client, transport} = setup();
    await client.connect();
    transport.handle = request => {
      if (kind === 'duplicate') transport.respond(request, {events: [descriptor, descriptor]});
      if (kind === 'cursor-loop') transport.respond(request, {events: [], nextCursor: 'same'});
      if (kind === 'invalid-schema') transport.respond(request, {events: [{...descriptor, payloadSchema: {type: 'invalid'}}]});
    };
    await expect(client.refresh()).rejects.toThrow();
    expect(client.list()).toEqual([descriptor]);
  });
  it('rejects unknown events, invalid arguments, unsupported modes, and duplicate watches before polling', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    expect(() => client.watch({...options, name: 'missing'}, () => {})).toThrow('current catalog');
    expect(() => client.watch({...options, arguments: {group: 1}}, () => {})).toThrow('inputSchema');
    expect(() => client.watch({...options, mode: 'push' as 'poll'}, () => {})).toThrow();
    const watch = client.watch({...options, arguments: {a: 1, b: 2}}, () => {});
    expect(() => client.watch({...options, arguments: {b: 2, a: 1}}, () => {})).toThrow('already running');
    await watch.stop();
    expect(polls).toHaveLength(0);
    transport.handle = request => transport.respond(request, {events: [{...descriptor, delivery: ['push']}]});
    await client.refresh();
    expect(() => client.watch(options, () => {})).toThrow('delivery mode');
  });
});

describe('poll delivery', () => {
  it('waits for consumer and checkpoint acceptance before advancing or draining hasMore', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const accepted = Promise.withResolvers<void>();
    const saved = Promise.withResolvers<void>();
    const checkpoint = vi.fn(() => saved.promise);
    const watch = client.watch({...options, maxAgeMs: 500, maxEvents: 2}, () => accepted.promise, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    expect(polls[0]!.params).toMatchObject({name: descriptor.name, arguments: {}, cursor: 'start', maxAgeMs: 500, maxEvents: 2});
    transport.respond(polls[0]!, {...batch, events: [event('e')], hasMore: true});
    await new Promise(resolve => setImmediate(resolve));
    expect(watch.cursor).toBe('start');
    expect(checkpoint).not.toHaveBeenCalled();
    accepted.resolve();
    await vi.waitFor(() => expect(checkpoint).toHaveBeenCalledWith('next'));
    expect(watch.cursor).toBe('start');
    expect(polls).toHaveLength(1);
    saved.resolve();
    await watch.ready;
    await vi.waitFor(() => expect(polls).toHaveLength(2));
    expect(polls[1]!.params?.cursor).toBe('next');
  });
  it.each([0, 100, 2 ** 31])('honors polling delays of %i ms and the configured floor', async nextPollMs => {
    vi.useFakeTimers();
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.advanceTimersByTimeAsync(0);
    transport.respond(polls[0]!, {...batch, nextPollMs});
    await watch.ready;
    await vi.advanceTimersByTimeAsync(Math.max(10, nextPollMs) - 1);
    expect(polls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(polls).toHaveLength(2);
  });
  it('reports a replay gap before events and clears the cursor when replay is unavailable', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const observed: Observation[] = [];
    const watch = client.watch(options, value => { observed.push(value); });
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, events: [event('e')], truncated: true, hasMore: true});
    await vi.waitFor(() => expect(polls).toHaveLength(2));
    expect(observed.map(item => item.type)).toEqual(['gap', 'event']);
    expect(observed[0]).toEqual({type: 'gap', cursor: 'next'});
    transport.respond(polls[1]!, {...batch, cursor: null, truncated: true});
    await vi.waitFor(() => expect(watch.cursor).toBe(null));
    expect(observed).toHaveLength(2);
  });
  it('retains partial batches through consumer and checkpoint failures without refetching', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const accepted: string[] = [];
    let failed = false;
    const checkpoint = vi.fn().mockRejectedValueOnce(new Error('disk full')).mockResolvedValue(undefined);
    const watch = client.watch(options, value => {
      if (value.type === 'status') throw new Error('inbox full');
      if (value.type !== 'event') return;
      if (value.event.eventId === 'b' && !failed) { failed = true; throw new Error('inbox full'); }
      accepted.push(value.event.eventId);
    }, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, events: [event('a'), event('b'), event('a')]});
    await watch.ready;
    expect(accepted).toEqual(['a', 'b']);
    expect(polls).toHaveLength(1);
    expect(checkpoint).toHaveBeenCalledTimes(2);
    expect(watch.cursor).toBe('next');
  });
  it('retries a rejected replay gap but does not repeat it after acceptance', async () => {
    const {client, transport, polls} = setup({maxRetries: 3});
    await client.connect();
    let attempts = 0;
    const accepted: Observation[] = [];
    const checkpoint = vi.fn().mockRejectedValueOnce(new Error('disk full')).mockResolvedValue(undefined);
    const watch = client.watch(options, value => {
      if (value.type === 'status') return;
      if (value.type === 'gap' && ++attempts === 1) throw new Error('inbox full');
      accepted.push(value);
    }, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, truncated: true, events: [event('e')]});
    await watch.ready;
    expect(attempts).toBe(2);
    expect(accepted.map(value => value.type)).toEqual(['gap', 'event']);
    expect(checkpoint).toHaveBeenCalledTimes(2);
    expect(polls).toHaveLength(1);
    expect(watch.cursor).toBe('next');
  });
  it.each(['invalid-payload', 'wrong-name', 'too-many', 'too-large'])('rejects %s batches without advancing the checkpoint', async kind => {
    const {client, transport, polls} = setup({maxRetries: 0});
    await client.connect();
    const checkpoint = vi.fn();
    const accept = vi.fn();
    const watch = client.watch({...options, maxEvents: 1}, accept, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    const invalid = {
      'invalid-payload': [{...event('e'), data: {value: 'bad'}}],
      'wrong-name': [{...event('e'), name: 'other'}],
      'too-many': [event('a'), event('b')],
      'too-large': [{...event('e'), data: {value: 1, text: 'x'.repeat(256 * 1024)}}],
    }[kind]!;
    transport.respond(polls[0]!, {...batch, events: invalid});
    const code = kind === 'too-large' ? ErrorCode.LimitExceeded : ErrorCode.InvalidEvent;
    await expect(watch.closed).rejects.toMatchObject({code: kind === 'too-many' ? ErrorCode.InvalidResponse : code});
    expect(watch.cursor).toBe('start');
    expect(checkpoint).not.toHaveBeenCalled();
    expect(accept.mock.calls.flat().filter(item => item.type === 'event')).toHaveLength(0);
  });
  it.each([-32601, -32602, -32024, -32025, -32026])('stops on non-retryable protocol error %i', async code => {
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.fail(polls[0]!, code);
    await expect(watch.closed).rejects.toMatchObject({code});
    await expect(watch.ready).rejects.toMatchObject({code});
    expect(polls).toHaveLength(1);
  });
  it('bounds retries and releases a failed watch for replacement', async () => {
    const {client, transport} = setup();
    await client.connect();
    let requests = 0;
    transport.handle = request => { requests++; transport.fail(request, -32603); };
    const watch = client.watch(options, () => {});
    await expect(watch.closed).rejects.toThrow('retry limit');
    expect(requests).toBe(3);
    await client.watch(options, () => {}).stop();
  });
  it('retains partial progress across more events than the deduplication window', async () => {
    const {client, transport} = setup();
    await client.connect();
    let polls = 0;
    transport.handle = request => {
      polls++;
      const offset = Number(request.params?.cursor);
      const count = Math.min(Number(request.params?.maxEvents), 4200 - offset);
      expect(count).toBeLessThanOrEqual(50);
      transport.respond(request, {...batch, events: Array.from({length: count}, (_, i) => event(String(offset + i))), cursor: String(offset + count), hasMore: offset + count < 4200});
    };
    const received: string[] = [];
    let failedAt = -1;
    const watch = client.watch({...options, cursor: '0', maxEvents: 50}, value => {
      if (value.type !== 'event') return;
      if (received.length % 37 === 0 && failedAt !== received.length) {
        failedAt = received.length;
        throw new Error('Consumer temporarily full');
      }
      received.push(value.event.eventId);
    });
    await vi.waitFor(() => expect(watch.cursor).toBe('4200'), {timeout: 5000});
    expect(received).toHaveLength(4200);
    expect(new Set(received).size).toBe(4200);
    expect(polls).toBe(84);
  });
  it('reconnects a disconnected transport and resumes from the accepted cursor', async () => {
    const transports: TestTransport[] = [];
    const polls: JSONRPCRequest[] = [];
    const client = new EventsClient(new EventsWire(() => {
      const transport = new TestTransport();
      transport.handle = request => {
        if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
        else polls.push(request);
      };
      transports.push(transport);
      return transport;
    }), {pollFloorMs: 1, retryMs: 1});
    cleanups.push(() => client.close());
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transports[0]!.respond(polls[0]!, {...batch, cursor: 'saved', hasMore: true});
    await vi.waitFor(() => expect(polls).toHaveLength(2));
    await transports[0]!.close();
    await vi.waitFor(() => expect(polls).toHaveLength(3));
    expect(transports).toHaveLength(2);
    expect(transports[1]!.messages[0]!.message).toMatchObject({method: 'server/discover'});
    expect(polls[2]!.params?.cursor).toBe('saved');
    expect(watch.cursor).toBe('saved');
  });
  it('rediscovers schema changes, retries catalog failures, and clears the cursor only after accepting the gap', async () => {
    const {client, transport, polls} = setup({maxRetries: 3});
    await client.connect();
    let lists = 0;
    transport.handle = request => {
      if (request.method === 'events/list') {
        if (++lists === 1) transport.fail(request, -32603);
        else transport.respond(request, {events: [descriptor]});
      } else polls.push(request);
    };
    const gap = Promise.withResolvers<void>();
    const checkpoint = vi.fn();
    const accept = vi.fn((value: Observation) => { if (value.type === 'gap') return gap.promise; });
    const watch = client.watch(options, accept, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.fail(polls[0]!, -32026, {reason: 'schema_changed'});
    await vi.waitFor(() => expect(accept).toHaveBeenCalledWith({type: 'gap', cursor: null}));
    expect(watch.cursor).toBe('start');
    expect(checkpoint).not.toHaveBeenCalled();
    gap.resolve();
    await vi.waitFor(() => expect(polls).toHaveLength(2));
    expect(polls[1]!.params?.cursor).toBe(null);
    expect(checkpoint).toHaveBeenCalledWith(null);
    expect(lists).toBe(2);
  });
  it('stops on removal after rediscovery', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.handle = request => transport.respond(request, {events: []});
    transport.fail(polls[0]!, -32023);
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.EventNotFound});
    expect(polls).toHaveLength(1);
  });
  it('stops with the validation error when a changed schema rejects existing arguments', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    let lists = 0;
    transport.handle = request => {
      lists++;
      transport.respond(request, {events: [{...descriptor, inputSchema: {...descriptor.inputSchema, required: ['group']}}]});
    };
    transport.fail(polls[0]!, -32026, {reason: 'schema_changed'});
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.InvalidArguments, message: 'Subscription arguments do not match inputSchema'});
    expect(lists).toBe(1);
    expect(polls).toHaveLength(1);
    expect(watch.cursor).toBe('start');
  });
  it('does not repeat an accepted schema-change gap when saving its checkpoint fails', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const checkpoint = vi.fn().mockRejectedValueOnce(new Error('disk full')).mockResolvedValue(undefined);
    const observed: Observation[] = [];
    const watch = client.watch(options, value => { observed.push(value); }, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.fail(polls[0]!, -32026, {reason: 'schema_changed'});
    await vi.waitFor(() => expect(polls).toHaveLength(2));
    expect(observed.filter(value => value.type === 'gap')).toEqual([{type: 'gap', cursor: null}]);
    expect(checkpoint.mock.calls).toEqual([[null], [null]]);
    expect(watch.cursor).toBe(null);
    expect(polls[1]!.params?.cursor).toBe(null);
  });
  it('finishes the active callback on stop without delivering the rest of the batch', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const accepted = Promise.withResolvers<void>();
    const accept = vi.fn(() => accepted.promise);
    const checkpoint = vi.fn();
    const watch = client.watch(options, accept, checkpoint);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, events: [event('a'), event('b')]});
    await vi.waitFor(() => expect(accept).toHaveBeenCalledTimes(1));
    const stopped = watch.stop();
    accepted.resolve();
    await stopped;
    expect(accept).toHaveBeenCalledTimes(1);
    expect(checkpoint).not.toHaveBeenCalled();
    expect(watch.cursor).toBe('start');
    await client.close();
    expect(() => client.watch(options, () => {})).toThrow('closed');
  });
});

describe('client limits and exported errors', () => {
  it('keeps 32 subscriptions as a default and allows a higher configured limit', async () => {
    const normal = setup();
    await normal.client.connect();
    for (let i = 0; i < 32; i++) normal.client.watch({...options, arguments: {group: String(i)}}, () => {});
    expect(() => normal.client.watch({...options, arguments: {group: 'extra'}}, () => {}))
      .toThrowError(expect.objectContaining({code: ErrorCode.LimitExceeded, details: {limit: 'maxSubscriptions', max: 32}}));
    await normal.client.close();
    const raised = setup({maxSubscriptions: 33});
    await raised.client.connect();
    for (let i = 0; i < 33; i++) raised.client.watch({...options, arguments: {group: String(i)}}, () => {});
    await raised.client.close();
  });
  it.each(['maxCatalogEntries', 'maxCatalogPages'] as const)('reports %s structurally and allows raising it', async limit => {
    for (const max of [1, 2]) {
      const {client, transport} = setup({[limit]: max});
      transport.handle = request => {
        if (limit === 'maxCatalogEntries') transport.respond(request, {events: [descriptor, {...descriptor, name: 'second'}]});
        else transport.respond(request, {events: [], ...(request.params?.cursor ? {} : {nextCursor: 'second'})});
      };
      if (max === 1) await expect(client.connect()).rejects.toMatchObject({code: ErrorCode.LimitExceeded, details: {limit, max}});
      else await expect(client.connect()).resolves.toBeUndefined();
    }
  });
  it('honors an explicit batch size above the default of 50', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const accept = vi.fn();
    const watch = client.watch({...options, maxEvents: 75}, accept);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    expect(polls[0]!.params?.maxEvents).toBe(75);
    transport.respond(polls[0]!, {...batch, events: Array.from({length: 75}, (_, i) => event(String(i)))});
    await watch.ready;
    expect(accept).toHaveBeenCalledTimes(75);
  });
  it('allows a larger poll response when the configured byte limit permits it', async () => {
    const {client, transport, polls} = setup({maxPollBytes: 512 * 1024});
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, events: [{...event('large'), data: {value: 1, text: 'x'.repeat(300 * 1024)}}]});
    await watch.ready;
    expect(watch.cursor).toBe('next');
  });
  it('bounds deduplication by the configured number of tracked IDs', async () => {
    const {client, transport, polls} = setup({maxTrackedEventIds: 1});
    await client.connect();
    const accept = vi.fn();
    const watch = client.watch(options, accept);
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, events: [event('a'), event('a'), event('b'), event('a')]});
    await watch.ready;
    expect(accept.mock.calls.map(([value]) => value.event.eventId)).toEqual(['a', 'b', 'a']);
  });
  it('exports SDK-independent local errors with stable codes, details, and causes', async () => {
    expect(() => setup({maxSubscriptions: 0})).toThrowError(expect.objectContaining({code: ErrorCode.InvalidOptions, cause: expect.any(Error)}));
    const {client} = setup();
    await client.connect();
    expect(() => client.watch({...options, arguments: {group: 123}}, () => {}))
      .toThrowError(expect.objectContaining({code: ErrorCode.InvalidArguments, cause: expect.any(Error)}));
    const error = new EventsClientError(ErrorCode.LimitExceeded, 'custom limit', {details: {limit: 'example', max: 1}});
    expect(error).toBeInstanceOf(Error);
    expect(error).toMatchObject({code: ErrorCode.LimitExceeded, details: {limit: 'example', max: 1}});
    await client.close();
    await expect(client.refresh()).rejects.toMatchObject({code: ErrorCode.Closed});
  });
  it('normalizes remote SDK errors into the exported protocol error while preserving data', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.fail(polls[0]!, EventProtocolErrorCode.Forbidden, {reason: 'access revoked'});
    await expect(watch.closed).rejects.toBeInstanceOf(EventProtocolError);
    await expect(watch.closed).rejects.toMatchObject({code: EventProtocolErrorCode.Forbidden, data: {reason: 'access revoked'}, cause: expect.any(Error)});
  });
  it('does not retry a malformed protocol response as a transient delivery failure', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, nextPollMs: 'invalid'});
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.InvalidResponse, cause: expect.any(Error)});
    expect(polls).toHaveLength(1);
  });
  it('retries a consumer rejection even when the underlying cause is a TypeError', async () => {
    const {client, transport, polls} = setup();
    await client.connect();
    let attempts = 0;
    const watch = client.watch(options, value => {
      if (value.type === 'event' && ++attempts === 1) throw new TypeError('consumer unavailable');
    });
    await vi.waitFor(() => expect(polls).toHaveLength(1));
    transport.respond(polls[0]!, {...batch, events: [event('e')]});
    await watch.ready;
    expect(attempts).toBe(2);
    expect(polls).toHaveLength(1);
  });
  it('stops if the server still requires rediscovery immediately after a successful refresh', async () => {
    const {client, transport} = setup();
    let polls = 0;
    let lists = 0;
    transport.handle = request => {
      if (request.method === 'events/list') { lists++; transport.respond(request, {events: [descriptor]}); }
      else { polls++; transport.fail(request, EventProtocolErrorCode.Unsupported, {reason: 'schema_changed'}); }
    };
    await client.connect();
    const watch = client.watch(options, () => {});
    await expect(watch.closed).rejects.toMatchObject({code: EventProtocolErrorCode.Unsupported});
    expect(polls).toBe(2);
    expect(lists).toBe(2);
  });
  it('cancels a long polling wait on stop without issuing another poll', async () => {
    vi.useFakeTimers();
    const {client, transport, polls} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await vi.advanceTimersByTimeAsync(0);
    transport.respond(polls[0]!, {...batch, nextPollMs: 2 ** 31});
    await watch.ready;
    await watch.stop();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(2 ** 31);
    expect(polls).toHaveLength(1);
  });
});

it('retries catalog discovery on the established connection after connect fails', async () => {
  const {client, transport} = setup();
  let catalogs = 0;
  transport.handle = request => {
    if (++catalogs === 1) transport.fail(request, EventProtocolErrorCode.InternalError);
    else transport.respond(request, {events: [descriptor]});
  };
  await expect(client.connect()).rejects.toMatchObject({code: EventProtocolErrorCode.InternalError});
  expect(client.list()).toEqual([]);
  expect(() => client.watch(options, () => {})).toThrowError(expect.objectContaining({code: ErrorCode.EventNotFound}));
  await client.connect();
  expect(client.list()).toEqual([descriptor]);
  expect(catalogs).toBe(2);
  expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'server/discover')).toHaveLength(1);
});

it('reports failures after readiness through closed and the stopped observation', async () => {
  const {client, transport, polls} = setup();
  await client.connect();
  const observations: Observation[] = [];
  const watch = client.watch(options, value => { observations.push(value); });
  await vi.waitFor(() => expect(polls).toHaveLength(1));
  transport.respond(polls[0]!, {...batch, hasMore: true});
  await watch.ready;
  await vi.waitFor(() => expect(polls).toHaveLength(2));
  transport.fail(polls[1]!, EventProtocolErrorCode.Forbidden);
  await expect(watch.closed).rejects.toMatchObject({code: EventProtocolErrorCode.Forbidden});
  await expect(watch.ready).resolves.toBeUndefined();
  expect(observations).toContainEqual({type: 'status', status: 'stopped'});
});

it('deduplicates stable event IDs across schema rediscovery and cursor resets', async () => {
  const {client, transport, polls} = setup();
  await client.connect();
  const observed: Observation[] = [];
  const watch = client.watch(options, value => { observed.push(value); });
  await vi.waitFor(() => expect(polls).toHaveLength(1));
  transport.respond(polls[0]!, {...batch, events: [event('known')], hasMore: true});
  await vi.waitFor(() => expect(polls).toHaveLength(2));
  transport.fail(polls[1]!, EventProtocolErrorCode.Unsupported, {reason: 'schema_changed'});
  await vi.waitFor(() => expect(polls).toHaveLength(3));
  expect(polls[2]!.params?.cursor).toBe(null);
  transport.respond(polls[2]!, {...batch, cursor: 'reset', hasMore: true});
  await vi.waitFor(() => expect(polls).toHaveLength(4));
  transport.respond(polls[3]!, {...batch, cursor: 'after', events: [event('known'), event('new')]});
  await vi.waitFor(() => expect(watch.cursor).toBe('after'));
  expect(observed.filter(value => value.type === 'event').map(value => value.event.eventId)).toEqual(['known', 'new']);
  expect(observed.filter(value => value.type === 'gap')).toEqual([{type: 'gap', cursor: null}]);
});
