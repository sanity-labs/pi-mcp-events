import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, EventsWire, EventsClientErrorCode as ErrorCode, type ClientOptions, type Observation} from '../src/index.ts';
import {TestTransport} from './transport.ts';

const available = {name: 'example.changed', delivery: ['poll'], inputSchema: true, payloadSchema: true};
const unavailable = {...available, name: 'other.changed', payloadSchema: {$schema: 'https://example.com/private-schema'}};
const options = {name: available.name, arguments: {}, mode: 'poll' as const, cursor: 'before'};
const event = {eventId: 'one', name: available.name, timestamp: '2026-10-29T12:00:00Z', data: {nested: [1, 'two']}};
const batch = {events: [event], cursor: 'after', truncated: false, hasMore: false, nextPollMs: 100_000};
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { await Promise.all(cleanups.splice(0).map(close => close())); });

function setup(initial: unknown[] = [available, unavailable], settings: ClientOptions = {}) {
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {retryMs: 1, ...settings});
  cleanups.push(() => client.close());
  const polls: JSONRPCRequest[] = [];
  let catalog = initial;
  transport.handle = req => {
    if (req.method === 'events/list') transport.respond(req, {events: catalog});
    else polls.push(req);
  };
  return {client, transport, polls, setCatalog(events: unknown[]) {catalog = events;}};
}

describe('per-event schema support', () => {
  it('lists all descriptors while supported watches deliver and unsupported watches fail specifically', async () => {
    const h = setup([unavailable, available]);
    await h.client.connect();
    expect(h.client.list()).toEqual([unavailable, available]);
    expect(h.client.catalogIssues).toMatchObject([{name: unavailable.name, error: {
      code: ErrorCode.UnsupportedEventSchema, details: {name: unavailable.name}, cause: expect.any(Error),
    }}]);
    expect(() => h.client.watch({...options, name: unavailable.name}, () => {})).toThrowError(expect.objectContaining({
      code: ErrorCode.UnsupportedEventSchema, details: {name: unavailable.name},
    }));
    expect(h.polls).toHaveLength(0);
    const observations: Observation[] = [];
    const watch = h.client.watch(options, item => {observations.push(item);});
    await vi.waitFor(() => expect(h.polls).toHaveLength(1));
    h.transport.respond(h.polls[0]!, batch);
    await watch.ready;
    expect(observations).toMatchObject([{type: 'event', event}]);
    expect(watch.cursor).toBe('after');
  });

  it.each(['inputSchema', 'payloadSchema'])('isolates unsupported or invalid %s objects', async field => {
    const broken = [
      {...available, name: 'dialect', [field]: {$schema: 'https://example.com/schema'}},
      {...available, name: 'invalid', [field]: {type: 'invalid'}},
      {...available, name: 'reference', [field]: {$ref: '#/$defs/missing'}},
    ];
    const h = setup([...broken, available]);
    await h.client.connect();
    expect(h.client.list()).toHaveLength(4);
    expect(h.client.catalogIssues.map(issue => issue.name)).toEqual(['dialect', 'invalid', 'reference']);
    for (const descriptor of broken) {
      expect(() => h.client.watch({...options, name: descriptor.name}, () => {})).toThrowError(expect.objectContaining({code: ErrorCode.UnsupportedEventSchema}));
    }
    await h.client.watch(options, () => {}).stop();
  });

  it('clears schema issues when types are repaired or removed', async () => {
    const h = setup();
    await h.client.connect();
    const repaired = {...unavailable, payloadSchema: true};
    h.setCatalog([available, repaired]);
    expect(await h.client.refresh()).toEqual([available, repaired]);
    expect(h.client.catalogIssues).toEqual([]);
    await h.client.watch({...options, name: repaired.name}, () => {}).stop();
    h.setCatalog([unavailable]);
    await h.client.refresh();
    expect(h.client.catalogIssues).toHaveLength(1);
    h.setCatalog([]);
    await h.client.refresh();
    expect(h.client.catalogIssues).toEqual([]);
    expect(() => h.client.watch({...options, name: unavailable.name}, () => {})).toThrowError(expect.objectContaining({code: ErrorCode.EventNotFound}));
  });

  it('stops only the affected watch when a schema becomes unusable after refresh', async () => {
    const h = setup([available, {...unavailable, payloadSchema: true}]);
    await h.client.connect();
    const affected = h.client.watch({...options, name: unavailable.name}, () => {});
    const healthy = h.client.watch(options, () => {});
    await vi.waitFor(() => expect(h.polls).toHaveLength(2));
    h.setCatalog([available, unavailable]);
    await h.client.refresh();
    h.transport.respond(h.polls[0]!, {...batch, events: [{...event, name: unavailable.name}]});
    await expect(affected.closed).rejects.toMatchObject({code: ErrorCode.UnsupportedEventSchema});
    h.transport.respond(h.polls[1]!, batch);
    await healthy.ready;
    expect(healthy.status).toBe('active');
    expect(healthy.cursor).toBe('after');
  });

  it('preserves the complete previous catalog and its issues when a later page fails', async () => {
    const h = setup();
    await h.client.connect();
    h.transport.handle = req => h.transport.respond(req, req.params?.cursor
      ? {events: [{...available, payloadSchema: null}]}
      : {events: [available], nextCursor: 'page-two'});
    await expect(h.client.refresh()).rejects.toMatchObject({code: ErrorCode.InvalidResponse});
    expect(h.client.list()).toEqual([available, unavailable]);
    expect(h.client.catalogIssues.map(issue => issue.name)).toEqual([unavailable.name]);
  });

  it.each(['duplicate', 'limit'])('counts unsupported descriptors for catalog %s checks', async kind => {
    const h = setup([], kind === 'limit' ? {maxCatalogEntries: 1} : {});
    await h.client.connect();
    h.transport.handle = req => h.transport.respond(req, req.params?.cursor
      ? {events: [kind === 'duplicate' ? {...unavailable, payloadSchema: true} : available]}
      : {events: [unavailable], nextCursor: 'page-two'});
    await expect(h.client.refresh()).rejects.toMatchObject({code: kind === 'duplicate' ? ErrorCode.InvalidCatalog : ErrorCode.LimitExceeded});
    expect(h.client.list()).toEqual([]);
    expect(h.client.catalogIssues).toEqual([]);
  });

  it('accepts an entirely unsupported catalog and reports each affected type', async () => {
    const h = setup([unavailable]);
    await expect(h.client.connect()).resolves.toBeUndefined();
    expect(h.client.list()).toEqual([unavailable]);
    expect(h.client.catalogIssues.map(issue => issue.name)).toEqual([unavailable.name]);
  });

  it('treats false schemas as valid schemas that reject values', async () => {
    const h = setup([{...available, inputSchema: false}]);
    await h.client.connect();
    expect(h.client.catalogIssues).toEqual([]);
    expect(() => h.client.watch(options, () => {})).toThrowError(expect.objectContaining({code: ErrorCode.InvalidArguments}));
    h.setCatalog([{...available, payloadSchema: false}]);
    await h.client.refresh();
    expect(h.client.catalogIssues).toEqual([]);
    const checkpoint = vi.fn();
    const watch = h.client.watch(options, () => {}, checkpoint);
    await vi.waitFor(() => expect(h.polls).toHaveLength(1));
    h.transport.respond(h.polls[0]!, batch);
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.InvalidEvent});
    expect(checkpoint).not.toHaveBeenCalled();
    expect(watch.cursor).toBe('before');
  });
});
