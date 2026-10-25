import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsClient} from '../src/client.ts';
import {EVENTS_EXTENSION, PROTOCOL_VERSION, completeResult} from '../src/protocol.ts';
import {createHttpTransport, EventsWire, nameHeader} from '../src/wire.ts';
import {event, testDescriptor, TestTransport} from './transport.ts';

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });
describe('delivery recovery', () => {
  it('sends required routing headers through the actual SDK HTTP transport', async () => {
    const sent: Array<{body: JSONRPCRequest; headers: Headers}> = [];
    const transport = createHttpTransport(new URL('http://localhost/mcp'), {fetch: async (_url, init) => {
      const body = JSON.parse(String(init?.body)) as JSONRPCRequest;
      sent.push({body, headers: new Headers(init?.headers)});
      const result = body.method === 'server/discover'
        ? {supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}}
        : {};
      return new Response(JSON.stringify({jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', ...result}}), {headers: {'content-type': 'application/json'}});
    }});
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await wire.connect();
    for (const method of ['events/poll', 'events/subscribe', 'events/unsubscribe']) {
      await wire.request(method, {name: 'exemple.modifié', arguments: {}}, completeResult);
    }
    await wire.stream('events/stream', {name: 'exemple.modifié', arguments: {}}, new AbortController().signal, () => {});
    await wire.request('events/list', {}, completeResult);
    for (const {body, headers} of sent.filter(entry => entry.body.method !== 'server/discover' && entry.body.method !== 'events/list')) {
      expect(headers.get('mcp-name')).toBe(nameHeader('exemple.modifié'));
      expect(headers.get('mcp-method')).toBe(body.method);
      expect(headers.get('mcp-protocol-version')).toBe(PROTOCOL_VERSION);
    }
    expect(sent.at(-1)!.headers.has('mcp-name')).toBe(false);
  });
  it('restarts a disconnected transport, repeats discovery, and resumes the saved cursor', async () => {
    const transports: TestTransport[] = [];
    const streams: JSONRPCRequest[] = [];
    const wire = new EventsWire(() => {
      const transport = new TestTransport();
      transport.handle = request => {
        if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
        if (request.method === 'events/stream') {
          streams.push(request);
          transport.notify(request, 'active', {cursor: request.params?.cursor ?? 'start', truncated: false});
        }
      };
      transports.push(transport);
      return transport;
    });
    const client = new EventsClient(wire, {retryMs: 1});
    cleanups.push(() => client.close());
    await client.connect();
    const checkpoint = vi.fn();
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'push'}, () => {}, checkpoint);
    await watch.ready;
    transports[0]!.notify(streams[0]!, 'heartbeat', {cursor: 'quiet-progress'});
    await vi.waitFor(() => expect(watch.cursor).toBe('quiet-progress'));
    expect(checkpoint).toHaveBeenLastCalledWith('quiet-progress');
    await transports[0]!.close();
    await vi.waitFor(() => expect(transports).toHaveLength(2));
    expect(streams[1]!.params?.cursor).toBe('quiet-progress');
    expect(transports[1]!.messages[0]!.message).toMatchObject({method: 'server/discover'});
  });
  it('retries delivery after the consumer fills up, even when status reporting also fails', async () => {
    const transport = new TestTransport();
    let polls = 0;
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [testDescriptor]});
      if (request.method === 'events/poll') {
        polls++;
        transport.respond(request, {events: [event('e')], cursor: 'next', truncated: false, hasMore: false, nextPollMs: 100_000});
      }
    };
    const client = new EventsClient(new EventsWire(transport), {retryMs: 1});
    cleanups.push(() => client.close());
    await client.connect();
    let attempts = 0;
    const watch = client.watch({name: testDescriptor.name, arguments: {}, mode: 'poll', cursor: 'start'}, observation => {
      if (observation.type === 'status' || ++attempts === 1) throw new Error('inbox full');
    });
    await watch.ready;
    expect(polls).toBe(1);
    expect(attempts).toBe(2);
    expect(watch.cursor).toBe('next');
  });
});
