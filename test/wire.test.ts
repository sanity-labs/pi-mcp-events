import {afterEach, describe, expect, it, vi} from 'vitest';
import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {EVENTS_EXTENSION, PROTOCOL_VERSION, completeResult} from '../src/protocol.ts';
import {createHttpTransport, nameHeader, EventsWire} from '../src/wire.ts';
import {TestTransport} from './transport.ts';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0)) await close(); });

describe('MCP wire adapter in isolation', () => {
  it('rejects a server that does not advertise the Events extension', async () => {
    const transport = new TestTransport();
    transport.supportsEvents = false;
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await expect(wire.connect()).rejects.toThrow('does not advertise');
  });

  it('retries failed discovery with a fresh SDK HTTP transport', async () => {
    let transports = 0;
    const wire = new EventsWire(() => {
      const attempt = ++transports;
      return createHttpTransport(new URL('http://localhost/mcp'), {fetch: async (_url, init) => {
        if (attempt === 1) return new Response('Temporarily unavailable', {status: 503});
        const request = JSON.parse(String(init?.body)) as JSONRPCRequest;
        return new Response(JSON.stringify({jsonrpc: '2.0', id: request.id, result: {
          resultType: 'complete', supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}},
        }}), {headers: {'content-type': 'application/json'}});
      }});
    });
    cleanups.push(() => wire.close());
    await expect(wire.connect()).rejects.toThrow();
    await expect(wire.connect()).resolves.toEqual({});
    await expect(wire.connect()).resolves.toEqual({});
    expect(transports).toBe(2);
  });

  it('shares one fresh connection when callers reconnect after transport closure', async () => {
    const transports: TestTransport[] = [];
    const wire = new EventsWire(() => {
      const transport = new TestTransport();
      transports.push(transport);
      return transport;
    });
    cleanups.push(() => wire.close());
    await wire.connect();
    await transports[0]!.close();
    await Promise.all([wire.connect(), wire.connect()]);
    expect(transports).toHaveLength(2);
    expect(transports[1]!.messages.filter(({message}) => 'method' in message && message.method === 'server/discover')).toHaveLength(1);
  });

  it('keeps the wire closed when an in-flight connection finishes after shutdown', async () => {
    const wire = new EventsWire(new TestTransport());
    cleanups.push(() => wire.close());
    const connected = Promise.withResolvers<void>();
    vi.spyOn(wire.client, 'connect').mockImplementation(() => connected.promise);
    const connecting = wire.connect();
    await wire.close();
    connected.resolve();
    await expect(connecting).rejects.toThrow('client is closed');
    await expect(wire.stream('events/stream', {}, new AbortController().signal, () => {})).rejects.toThrow('client is closed');
  });

  it('drains accepted messages and the terminal result before handling stream EOF', async () => {
    const transport = new TestTransport();
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await wire.connect();
    const accepted = Promise.withResolvers<void>();
    const received = vi.fn(() => accepted.promise);
    transport.handle = (request, options) => {
      transport.notify(request, 'active', {cursor: null, truncated: false});
      transport.respond(request, {});
      options?.onRequestStreamEnd?.();
    };
    const stream = wire.stream('events/stream', {name: 'example.changed'}, new AbortController().signal, received);
    await vi.waitFor(() => expect(received).toHaveBeenCalledTimes(1));
    accepted.resolve();
    await expect(stream).resolves.toBeUndefined();
  });

  it('closes old draining streams after the wire reconnects on a replacement transport', async () => {
    const transports: TestTransport[] = [];
    const wire = new EventsWire(() => {
      const transport = new TestTransport();
      transports.push(transport);
      transport.handle = request => {
        transport.notify(request, 'active', {cursor: null, truncated: false});
        transport.notify(request, 'event', {eventId: 'queued'});
      };
      return transport;
    });
    cleanups.push(() => wire.close());
    await wire.connect();
    const gate = Promise.withResolvers<void>();
    const received = vi.fn(() => gate.promise);
    const stream = wire.stream('events/stream', {name: 'example.changed'}, new AbortController().signal, received);
    const closed = expect(stream).rejects.toThrow('MCP Events client closed');
    await vi.waitFor(() => expect(received).toHaveBeenCalledOnce());
    await transports[0]!.close();
    await wire.connect();
    expect(transports).toHaveLength(2);
    await wire.close();
    gate.resolve();
    await closed;
    expect(received).toHaveBeenCalledOnce();
  });

  it('correlates concurrent streams and cancels only the aborted request', async () => {
    const transport = new TestTransport();
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await wire.connect();
    const left = new AbortController();
    const right = new AbortController();
    const received = vi.fn();
    transport.handle = request => transport.notify(request, 'active', {cursor: String(request.params?.name), truncated: false});
    const a = wire.stream('events/stream', {name: 'left'}, left.signal, received).catch(() => {});
    const b = wire.stream('events/stream', {name: 'right'}, right.signal, received).catch(() => {});
    await vi.waitFor(() => expect(received).toHaveBeenCalledTimes(2));
    left.abort();
    await a;
    const cancels = transport.messages.filter(({message}) => 'method' in message && message.method === 'notifications/cancelled');
    expect(cancels).toHaveLength(1);
    const rightRequest = transport.messages.find(({message}) => 'method' in message && message.method === 'events/stream' && message.params?.name === 'right')!;
    expect(rightRequest.options?.headers?.['Mcp-Name']).toBe('right');
    right.abort();
    await b;
  });
  it('encodes event routing headers using the MCP sentinel rules', () => {
    expect(nameHeader('example.changed')).toBe('example.changed');
    expect(nameHeader('a\tb')).toBe('a\tb');
    for (const value of ['café', ' padded ', '=?base64?YWJj?=']) {
      expect(nameHeader(value)).toBe(`=?base64?${Buffer.from(value).toString('base64')}?=`);
    }
  });
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
  it('aborts a per-request HTTP stream without a cancellation notification', async () => {
    const transport = new TestTransport();
    transport.hasPerRequestStream = true;
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await wire.connect();
    const abort = new AbortController();
    const stream = wire.stream('events/stream', {name: 'example.changed'}, abort.signal, () => {});
    const closed = expect(stream).rejects.toMatchObject({name: 'AbortError'});
    await vi.waitFor(() => expect(transport.messages.some(({message}) => 'method' in message && message.method === 'events/stream')).toBe(true));
    const request = transport.messages.find(({message}) => 'method' in message && message.method === 'events/stream')!;
    abort.abort();
    await closed;
    expect(request.options?.requestSignal?.aborted).toBe(true);
    expect(transport.messages.some(({message}) => 'method' in message && message.method === 'notifications/cancelled')).toBe(false);
  });
});
