import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, type ClientOptions, type Observation} from '../src/client.ts';
import {EventsClientErrorCode as ErrorCode, EventProtocolError} from '../src/index.ts';
import {EventsWire} from '../src/wire.ts';
import {TestTransport} from './transport.ts';

const descriptor = {
  name: 'example.changed', delivery: ['push'], inputSchema: {type: 'object'},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'number'}}},
};
const options = {name: descriptor.name, arguments: {}, mode: 'push' as const, cursor: 'start'};
const event = (eventId: string, cursor = eventId) => ({
  eventId, cursor, name: descriptor.name, timestamp: '2026-10-25T12:00:00Z', data: {value: 1},
});
const cleanups: Array<() => Promise<void>> = [];
beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, 'random').mockReturnValue(0);
});
afterEach(async () => {
  await Promise.all(cleanups.splice(0).map(close => close()));
  vi.restoreAllMocks();
  vi.useRealTimers();
});
const flush = () => vi.advanceTimersByTimeAsync(0);
function setup(settings: ClientOptions = {}) {
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {heartbeatMs: 100, retryMs: 10, maxRetries: 2, ...settings});
  const requests: JSONRPCRequest[] = [];
  transport.handle = request => {
    if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
    else requests.push(request);
  };
  cleanups.push(() => client.close());
  const active = (request: JSONRPCRequest, cursor: string | null = 'active', truncated = false) => {
    transport.notify(request, 'active', {cursor, truncated});
  };
  const disconnect = (request: JSONRPCRequest) => {
    transport.messages.find(({message}) => 'id' in message && message.id === request.id)!.options!.onRequestStreamEnd!();
  };
  return {client, transport, requests, active, disconnect};
}

describe('event streaming', () => {
  it('clears retrying on a quiet reconnection and on heartbeat checkpoint recovery', async () => {
    const {client, transport, requests, active, disconnect} = setup();
    await client.connect();
    let checkpoints = 0;
    const watch = client.watch(options, () => {}, cursor => {
      if (cursor === 'heartbeat' && ++checkpoints === 1) throw new Error('temporarily unavailable');
    });
    expect(watch.status).toBe('starting');
    await flush(); active(requests[0]!); await watch.ready;
    expect(watch.status).toBe('active');
    disconnect(requests[0]!);
    await flush();
    expect(watch.status).toBe('retrying');
    await vi.advanceTimersByTimeAsync(10);
    active(requests[1]!); await flush();
    expect(watch.status).toBe('active');
    transport.notify(requests[1]!, 'heartbeat', {cursor: 'heartbeat'});
    await flush();
    expect(watch.status).toBe('retrying');
    await vi.advanceTimersByTimeAsync(10);
    expect(watch.status).toBe('active');
    await watch.stop();
    expect(watch.status).toBe('stopped');
  });
  it('demultiplexes watches, deduplicates per subscription, and cancels only the stopped stdio stream', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const left: Observation[] = [];
    const right: Observation[] = [];
    const a = client.watch({...options, maxAgeMs: 500}, item => { left.push(item); });
    const b = client.watch({...options, arguments: {group: 'b'}}, item => { right.push(item); });
    await flush();
    expect(requests[0]!.params).toMatchObject({name: descriptor.name, cursor: 'start', maxAgeMs: 500});
    expect(requests[0]!.params).not.toHaveProperty('maxEvents');
    requests.forEach(request => active(request));
    await Promise.all([a.ready, b.ready]);
    transport.notify(requests[0]!, 'event', event('shared', 'a'));
    transport.notify(requests[0]!, 'event', event('shared', 'a-repeat'));
    transport.notify(requests[1]!, 'event', event('shared', 'b'));
    await flush();
    expect(left.filter(item => item.type === 'event')).toHaveLength(1);
    expect(right.filter(item => item.type === 'event')).toHaveLength(1);
    expect(a.cursor).toBe('a-repeat');
    await a.stop();
    const cancels = transport.messages.filter(({message}) => 'method' in message && message.method === 'notifications/cancelled');
    expect(cancels.map(({message}) => message)).toMatchObject([{params: {requestId: requests[0]!.id}}]);
    transport.notify(requests[1]!, 'heartbeat', {cursor: 'still-open'});
    await flush();
    expect(b.cursor).toBe('still-open');
  });

  it('waits for delivery and checkpoint acceptance, without treating a slow consumer as a dead stream', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const accepted = Promise.withResolvers<void>();
    const saved = Promise.withResolvers<void>();
    const checkpoint = vi.fn((cursor: string | null) => cursor === 'e' ? saved.promise : undefined);
    const watch = client.watch(options, item => item.type === 'event' ? accepted.promise : undefined, checkpoint);
    await flush();
    active(requests[0]!);
    await watch.ready;
    transport.notify(requests[0]!, 'event', event('e'));
    await vi.advanceTimersByTimeAsync(1000);
    expect(requests).toHaveLength(1);
    expect(watch.cursor).toBe('active');
    expect(checkpoint).not.toHaveBeenCalledWith('e');
    accepted.resolve();
    await flush();
    expect(checkpoint).toHaveBeenCalledWith('e');
    expect(watch.cursor).toBe('active');
    saved.resolve();
    await flush();
    expect(watch.cursor).toBe('e');
  });

  it('retries consumer and checkpoint failures independently without reopening the stream', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    let deliveries = 0;
    let checkpoints = 0;
    const watch = client.watch(options, item => {
      if (item.type === 'event' && ++deliveries === 1) throw new Error('busy');
    }, cursor => {
      if (cursor === 'e' && ++checkpoints === 1) throw new Error('disk busy');
    });
    await flush(); active(requests[0]!); await watch.ready;
    transport.notify(requests[0]!, 'event', event('e'));
    transport.notify(requests[0]!, 'heartbeat', {cursor: 'later'});
    await vi.advanceTimersByTimeAsync(20);
    expect(watch.cursor).toBe('later');
    expect(deliveries).toBe(2);
    expect(checkpoints).toBe(2);
    expect(requests).toHaveLength(1);
  });

  it.each([null, 'gap'])('accepts repeated active messages and ignores truncation on null cursor: %s', async cursor => {
    const {client, requests, active} = setup();
    await client.connect();
    const observations: Observation[] = [];
    const checkpoint = vi.fn();
    const watch = client.watch(options, item => { observations.push(item); }, checkpoint);
    await flush(); active(requests[0]!); await watch.ready;
    active(requests[0]!, cursor, true);
    await flush();
    expect(observations.filter(item => item.type === 'gap')).toEqual(cursor === null ? [] : [{type: 'gap', cursor}]);
    expect(watch.cursor).toBe(cursor);
    expect(checkpoint).toHaveBeenLastCalledWith(cursor);
  });

  it('reports a recoverable source error without closing or advancing the stream', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const observations: Observation[] = [];
    const watch = client.watch(options, item => { observations.push(item); });
    await flush(); active(requests[0]!); await watch.ready;
    transport.notify(requests[0]!, 'error', {error: {code: -32603, message: 'temporarily unavailable'}});
    await flush();
    expect(observations[0]).toMatchObject({type: 'status', status: 'source-error', error: {code: -32603}});
    expect((observations[0] as {error: unknown}).error).toBeInstanceOf(EventProtocolError);
    expect(watch.cursor).toBe('active');
    transport.notify(requests[0]!, 'event', event('e'));
    await flush();
    expect(watch.cursor).toBe('e');
    expect(requests).toHaveLength(1);
  });

  it.each([-32024, -32603])('stops on explicit termination even for otherwise retryable code %i', async code => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await flush(); active(requests[0]!); await watch.ready;
    transport.notify(requests[0]!, 'terminated', {error: {code, message: 'ended'}});
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.SubscriptionTerminated, cause: {code}});
    expect(requests).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(['before-active', 'invalid-payload', 'invalid-heartbeat', 'unknown', 'missing-active'])('rejects malformed stream: %s', async kind => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await flush();
    if (kind !== 'before-active' && kind !== 'missing-active') { active(requests[0]!); await watch.ready; }
    if (kind === 'before-active') transport.notify(requests[0]!, 'event', event('e'));
    if (kind === 'invalid-payload') transport.notify(requests[0]!, 'event', {...event('e'), data: {value: 'bad'}});
    if (kind === 'invalid-heartbeat') transport.notify(requests[0]!, 'heartbeat', {cursor: 42});
    if (kind === 'unknown') transport.notify(requests[0]!, 'unknown', {});
    if (kind === 'missing-active') transport.respond(requests[0]!, {});
    await expect(watch.closed).rejects.toMatchObject({code: kind === 'invalid-payload' ? ErrorCode.InvalidEvent : ErrorCode.InvalidResponse});
    expect(requests).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(['disconnect', 'complete'])('resumes at the accepted cursor after %s and resets retries after sustained health', async ending => {
    const {client, transport, requests, active, disconnect} = setup({maxRetries: 1});
    await client.connect();
    const accepted = vi.fn();
    const watch = client.watch(options, item => { if (item.type === 'event') accepted(item.event.eventId); });
    for (let i = 0; i < 4; i++) {
      await flush();
      const request = requests[i]!;
      expect(request.params?.cursor).toBe(i === 0 ? 'start' : `position-${i - 1}`);
      active(request);
      transport.notify(request, 'event', event('replayed'));
      transport.notify(request, 'heartbeat', {cursor: `position-${i}`});
      await flush();
      for (let beat = 0; beat < 2; beat++) {
        await vi.advanceTimersByTimeAsync(100);
        transport.notify(request, 'heartbeat', {cursor: `position-${i}`});
        await flush();
      }
      if (ending === 'disconnect') disconnect(request);
      else transport.respond(request, {});
      await vi.advanceTimersByTimeAsync(10);
    }
    expect(requests).toHaveLength(5);
    expect(accepted).toHaveBeenCalledOnce();
    await watch.stop();
  });

  it('bounds active-only reconnects and exposes retry exhaustion', async () => {
    const {client, requests, active, disconnect} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await flush();
    for (let i = 0; i < 3; i++) {
      active(requests[i]!);
      await flush();
      disconnect(requests[i]!);
      await vi.advanceTimersByTimeAsync(10 * 2 ** i);
    }
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted});
    expect(requests).toHaveLength(3);
  });

  it('reconnects a silent stream after twice the heartbeat interval and cancels timers on stop', async () => {
    const {client, requests, active} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await flush(); active(requests[0]!); await watch.ready;
    await vi.advanceTimersByTimeAsync(199);
    expect(requests).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(11);
    expect(requests).toHaveLength(2);
    expect(requests[1]!.params?.cursor).toBe('active');
    await watch.stop();
    await vi.advanceTimersByTimeAsync(1000);
    expect(requests).toHaveLength(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('supports heartbeat intervals beyond the native timer limit', async () => {
    const {client, transport, requests} = setup({heartbeatMs: 2 ** 31});
    await client.connect();
    const watch = client.watch(options, () => {});
    await flush();
    await vi.advanceTimersByTimeAsync(2 ** 32 - 1);
    expect(requests).toHaveLength(1);
    expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'notifications/cancelled')).toHaveLength(0);
    await watch.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('rediscovers once after schema termination and stops persistent incompatibility', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const observations: Observation[] = [];
    const watch = client.watch(options, item => { observations.push(item); });
    await flush(); active(requests[0]!); await watch.ready;
    const terminate = (request: JSONRPCRequest) => transport.notify(request, 'terminated', {
      error: {code: -32026, message: 'changed', data: {reason: 'schema_changed'}},
    });
    terminate(requests[0]!);
    await vi.advanceTimersByTimeAsync(10);
    expect(requests[1]!.params?.cursor).toBeNull();
    expect(observations.filter(item => item.type === 'gap')).toEqual([{type: 'gap', cursor: null}]);
    active(requests[1]!);
    await flush(); terminate(requests[1]!);
    await expect(watch.closed).rejects.toMatchObject({code: ErrorCode.SubscriptionTerminated, cause: {code: -32026}});
    expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'events/list')).toHaveLength(2);
  });

  it('aborts HTTP streams and drains an in-flight consumer before stop completes', async () => {
    const {client, transport, requests, active} = setup();
    transport.hasPerRequestStream = true;
    await client.connect();
    const gate = Promise.withResolvers<void>();
    const checkpoint = vi.fn();
    const watch = client.watch(options, item => item.type === 'event' ? gate.promise : undefined, checkpoint);
    await flush(); active(requests[0]!); await watch.ready;
    transport.notify(requests[0]!, 'event', event('e'));
    await flush();
    const stopped = vi.fn();
    const stopping = watch.stop().then(stopped);
    await flush();
    expect(stopped).not.toHaveBeenCalled();
    gate.resolve();
    await stopping;
    expect(watch.cursor).toBe('active');
    expect(checkpoint).not.toHaveBeenCalledWith('e');
    const stream = transport.messages.find(({message}) => 'method' in message && message.method === 'events/stream')!;
    expect(stream.options?.requestSignal?.aborted).toBe(true);
    expect(transport.messages.some(({message}) => 'method' in message && message.method === 'notifications/cancelled')).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });
  it('drains acceptance after connection loss before reconnecting with its checkpoint', async () => {
    const transports: TestTransport[] = [];
    const requests: JSONRPCRequest[] = [];
    const client = new EventsClient(new EventsWire(() => {
      const transport = new TestTransport();
      transport.handle = request => {
        if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
        else requests.push(request);
      };
      transports.push(transport);
      return transport;
    }), {retryMs: 10});
    cleanups.push(() => client.close());
    await client.connect();
    const gate = Promise.withResolvers<void>();
    const watch = client.watch(options, item => item.type === 'event' ? gate.promise : undefined);
    await flush();
    transports[0]!.notify(requests[0]!, 'active', {cursor: 'active', truncated: false});
    await watch.ready;
    transports[0]!.notify(requests[0]!, 'event', event('accepted'));
    await flush();
    await transports[0]!.close();
    await vi.advanceTimersByTimeAsync(50);
    expect(transports).toHaveLength(1);
    gate.resolve();
    await vi.advanceTimersByTimeAsync(10);
    expect(transports).toHaveLength(2);
    expect(requests[1]!.params?.cursor).toBe('accepted');
  });

  it('keeps idle heartbeat cursors current and treats source errors as insufficient liveness', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const watch = client.watch(options, () => {});
    await flush(); active(requests[0]!); await watch.ready;
    for (let i = 0; i < 4; i++) {
      await vi.advanceTimersByTimeAsync(100);
      transport.notify(requests[0]!, 'heartbeat', {cursor: `idle-${i}`});
      await flush();
    }
    expect(requests).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(100);
    transport.notify(requests[0]!, 'error', {error: {code: -32603, message: 'upstream unavailable'}});
    await vi.advanceTimersByTimeAsync(110);
    expect(requests).toHaveLength(2);
    expect(requests[1]!.params?.cursor).toBe('idle-3');
  });

  it('validates schema recovery against the refreshed catalog and keeps stable event IDs deduplicated', async () => {
    const {client, transport, requests, active} = setup();
    await client.connect();
    const accepted: string[] = [];
    const watch = client.watch(options, item => { if (item.type === 'event') accepted.push(item.event.eventId); });
    await flush(); active(requests[0]!); await watch.ready;
    transport.notify(requests[0]!, 'event', event('known'));
    await flush();
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [{...descriptor,
        payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'string'}}},
      }]});
      else requests.push(request);
    };
    transport.notify(requests[0]!, 'terminated', {error: {code: -32026, message: 'changed', data: {reason: 'schema_changed'}}});
    await vi.advanceTimersByTimeAsync(10);
    active(requests[1]!);
    for (const id of ['known', 'new']) transport.notify(requests[1]!, 'event', {...event(id), data: {value: 'updated'}});
    await flush();
    expect(accepted).toEqual(['known', 'new']);
    expect(watch.cursor).toBe('new');
  });

});
