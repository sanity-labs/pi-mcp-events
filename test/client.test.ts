import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient, type Observation} from '../src/client.ts';
import {EventsWire, nameHeader} from '../src/wire.ts';
import {PROTOCOL_VERSION, SUBSCRIPTION_ID} from '../src/protocol.ts';
import {TestTransport, testDescriptor, event} from './transport.ts';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(cleanups.splice(0).map(close => close()));
  vi.useRealTimers();
});
function setup(settings = {}) {
  const transport = new TestTransport();
  const wire = new EventsWire(transport);
  const client = new EventsClient(wire, {retryMs: 1, pollFloorMs: 10, heartbeatMs: 1000, ...settings});
  const requests: JSONRPCRequest[] = [];
  transport.handle = request => {
    if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
    else requests.push(request);
  };
  cleanups.push(() => client.close());
  return {transport, wire, client, requests};
}
describe('event transport contract', () => {
  it('refuses events requests without advertised support', async () => {
    const {client, transport} = setup();
    transport.supportsEvents = false;
    await expect(client.connect()).rejects.toThrow('does not advertise');
    expect(transport.messages.map(({message}) => 'method' in message && message.method)).toEqual(['server/discover']);
  });
  it('encodes event routing headers using the MCP sentinel rules', () => {
    expect(nameHeader('example.changed')).toBe('example.changed');
    expect(nameHeader('a\tb')).toBe('a\tb');
    for (const value of ['café', ' padded ', '=?base64?YWJj?=']) {
      expect(nameHeader(value)).toBe(`=?base64?${Buffer.from(value).toString('base64')}?=`);
    }
  });
  it('demultiplexes concurrent streams and cancels only the requested stdio stream', async () => {
    const {client, transport, requests} = setup();
    await client.connect();
    const left: Observation[] = [];
    const right: Observation[] = [];
    const a = client.watch({name: testDescriptor.name, arguments: {group: 'a'}, mode: 'push'}, value => {left.push(value);});
    const b = client.watch({name: testDescriptor.name, arguments: {group: 'b'}, mode: 'push'}, value => {right.push(value);});
    await vi.waitFor(() => expect(requests).toHaveLength(2));
    const [ra, rb] = requests;
    for (const request of requests) transport.notify(request!, 'active', {truncated: false});
    await Promise.all([a.ready, b.ready]);
    transport.notify(rb!, 'event', event('shared-id', 'right'));
    transport.notify(ra!, 'event', event('shared-id', 'left'));
    transport.notify(ra!, 'event', event('shared-id', 'left-again'));
    await vi.waitFor(() => expect(a.cursor).toBe('left-again'));
    expect(left.filter(value => value.type === 'event')).toHaveLength(1);
    expect(right.filter(value => value.type === 'event')).toHaveLength(1);
    expect(b.cursor).toBe('right');
    const streamSend = transport.messages.find(({message}) => 'id' in message && message.id === ra!.id)!;
    expect(streamSend.options?.headers?.['Mcp-Name']).toBe(testDescriptor.name);
    expect(ra!.params?._meta?.['io.modelcontextprotocol/protocolVersion']).toBe(PROTOCOL_VERSION);
    await a.stop();
    const cancels = transport.messages.filter(({message}) => 'method' in message && message.method === 'notifications/cancelled');
    expect(cancels).toHaveLength(1);
    expect(cancels[0]!.message).toMatchObject({params: {requestId: ra!.id}});
    transport.notify(rb!, 'heartbeat', {cursor: 'still-open'});
    await vi.waitFor(() => expect(b.cursor).toBe('still-open'));
  });
  it('aborts an HTTP request without sending a cancellation notification', async () => {
    const {client, transport, requests} = setup();
    transport.hasPerRequestStream = true;
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {});
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    transport.notify(requests[0]!, 'active', {cursor: null, truncated: false});
    await watch.ready;
    const stream = transport.messages.find(({message}) => 'method' in message && message.method === 'events/stream')!;
    await watch.stop();
    expect(stream.options?.requestSignal?.aborted).toBe(true);
    expect(transport.messages.some(({message}) => 'method' in message && message.method === 'notifications/cancelled')).toBe(false);
  });
  it('advances a poll cursor only after the consumer accepts the batch and drains hasMore immediately', async () => {
    const {client, transport, requests} = setup();
    await client.connect();
    const accepted = Promise.withResolvers<void>();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'poll', cursor: 'start'}, () => accepted.promise);
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    transport.respond(requests[0]!, {events: [event('e')], cursor: 'next', truncated: false, hasMore: true, nextPollMs: 100_000});
    await new Promise(resolve => setImmediate(resolve));
    expect(watch.cursor).toBe('start');
    expect(requests).toHaveLength(1);
    accepted.resolve();
    await watch.ready;
    await vi.waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1]!.params?.cursor).toBe('next');
    transport.respond(requests[1]!, {events: [], truncated: true, hasMore: false, nextPollMs: 100_000});
    await vi.waitFor(() => expect(watch.cursor).toBe(null));
  });
  it.each([-32024, -32001])('keeps recoverable streams open and stops on server termination %i', async code => {
    const {client, transport, requests} = setup();
    await client.connect();
    const observations: Observation[] = [];
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, value => {observations.push(value);});
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    const request = requests[0]!;
    transport.notify(request, 'active', {cursor: 'a', truncated: false});
    await watch.ready;
    transport.notify(request, 'active', {cursor: 'gap', truncated: true});
    transport.notify(request, 'error', {error: {code: -32603, message: 'temporary'}});
    transport.notify(request, 'event', event('e', 'after-gap'));
    await vi.waitFor(() => expect(watch.cursor).toBe('after-gap'));
    expect(requests).toHaveLength(1);
    expect(observations).toContainEqual({type: 'gap', cursor: 'gap'});
    transport.notify(request, 'terminated', {error: {code, message: 'Subscription ended'}});
    await expect(watch.closed).rejects.toMatchObject({code});
    expect(requests).toHaveLength(1);
  });
  it('rediscoveries after schema termination and resumes against the current descriptor', async () => {
    const {client, transport, requests} = setup();
    await client.connect();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {});
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    transport.notify(requests[0]!, 'active', {cursor: 'old', truncated: false});
    await watch.ready;
    transport.notify(requests[0]!, 'terminated', {error: {code: -32026, message: 'changed', data: {feature: 'payloadSchema', reason: 'schema_changed'}}});
    await vi.waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1]!.params?.cursor).toBe(null);
    expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'events/list')).toHaveLength(2);
  });
  it('refreshes the catalog only on the acknowledged extension stream', async () => {
    const {client, transport, requests} = setup();
    transport.listChanged = true;
    await client.connect();
    const request = requests[0]!;
    expect(request.method).toBe('subscriptions/listen');
    transport.onmessage?.({jsonrpc: '2.0', method: 'notifications/subscriptions/acknowledged', params: {notifications: {eventsListChanged: true}, _meta: {[SUBSCRIPTION_ID]: request.id}}});
    transport.notify(request, 'list_changed', {});
    await vi.waitFor(() => expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'events/list')).toHaveLength(3));
  });
});
