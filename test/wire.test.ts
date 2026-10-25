import {afterEach, describe, expect, it, vi} from 'vitest';
import {EventsWire} from '../src/wire.ts';
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
});
