import {expect, it, vi} from 'vitest';
import {createServer} from 'node:http';
import {gzipSync} from 'node:zlib';
import {boundedResponse} from '../src/http-limits.ts';
import {createHttpTransport, EventsWire} from '../src/wire.ts';
import {completeResult, EVENTS_EXTENSION, PROTOCOL_VERSION} from '../src/protocol.ts';
import {EventsClientErrorCode as Code} from '../src/errors.ts';

const encode = (value: string) => new TextEncoder().encode(value);
function source(chunks: Uint8Array[], headers = {'content-type': 'application/json'}) {
  let read = 0; const cancelled = vi.fn();
  const body = new ReadableStream<Uint8Array>({pull(controller) {
    const chunk = chunks[read++];
    if (chunk) controller.enqueue(chunk);
    else controller.close();
  }, cancel: cancelled}, {highWaterMark: 0});
  return {response: new Response(body, {headers}), cancelled, reads: () => read};
}

it.each([undefined, '1', '999999'])('bounds actual JSON bytes regardless of Content-Length %s', async length => {
  const h = source(Array.from({length: 100}, () => encode('x'.repeat(100))), {'content-type': 'application/json', ...(length ? {'content-length': length} : {})});
  const failed = vi.fn();
  await expect(boundedResponse(h.response, 250, 250, failed).text()).rejects.toMatchObject({code: Code.LimitExceeded});
  expect(h.reads()).toBe(3);
  expect(h.cancelled).toHaveBeenCalledOnce(); expect(failed).toHaveBeenCalledOnce();
});

it('passes an exactly bounded JSON body and propagates consumer cancellation', async () => {
  const h = source([encode('abc')]);
  expect(await boundedResponse(h.response, 3, 3, vi.fn()).text()).toBe('abc');
  const cancel = source(Array.from({length: 100}, () => encode('abc')));
  const reason = new Error('Stopped');
  await boundedResponse(cancel.response, 3, 3, vi.fn()).body!.cancel(reason);
  await vi.waitFor(() => expect(cancel.cancelled).toHaveBeenCalledExactlyOnceWith(reason));
});

it('preserves upstream body errors without reporting a limit failure', async () => {
  const error = new Error('Source failed');
  const failed = vi.fn();
  const body = new ReadableStream<Uint8Array>({pull(controller) {controller.error(error);}});
  await expect(boundedResponse(new Response(body), 100, 100, failed).text()).rejects.toBe(error);
  expect(failed).not.toHaveBeenCalled();
});

it('rejects oversized bodies even if upstream cancellation never completes', async () => {
  const cancel = vi.fn(() => new Promise<void>(() => {}));
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {controller.enqueue(encode('oversized'));}, cancel,
  }, {highWaterMark: 0});
  await expect(boundedResponse(new Response(body), 3, 3, vi.fn()).text()).rejects.toMatchObject({code: Code.LimitExceeded});
  await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
});

it.each([202, 400, 401, 403, 429, 500])('bounds the whole SSE-labelled HTTP %i body that the SDK consumes as text', async status => {
  const h = source(Array.from({length: 100}, () => encode('data: x\n\n')), {'content-type': 'text/event-stream'});
  const response = new Response(h.response.body, {status, headers: h.response.headers});
  await expect(boundedResponse(response, 32, 32, vi.fn()).text()).rejects.toMatchObject({code: Code.LimitExceeded});
  expect(h.cancelled).toHaveBeenCalledOnce();
  expect(h.reads()).toBe(4);
});

it.each([
  {method: 'notifications/test'},
  {id: 'server-ping', result: {}},
  {id: 'server-ping', error: {code: -32603, message: 'Failed'}},
])('bounds a successful acknowledgement of %j even when it claims SSE', async message => {
  const h = source(Array.from({length: 1000}, () => encode('data: x\n\n')), {'content-type': 'text/event-stream'});
  const wire = new EventsWire(createHttpTransport(new URL('http://localhost/mcp'), {maxResponseBytes: 500, fetch: async (_url, init) => {
    const body = JSON.parse(String(init?.body));
    if (body.method !== 'server/discover') return h.response;
    return new Response(JSON.stringify({jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}}}), {headers: {'content-type': 'application/json'}});
  }}));
  try {
    await wire.connect();
    await wire.client.transport!.send({jsonrpc: '2.0', ...message});
    expect(h.cancelled).toHaveBeenCalledOnce();
    expect(h.reads()).toBe(56);
  } finally {await wire.close();}
});

it.each(['\n', '\r', '\r\n'])('limits SSE frames across arbitrary chunk boundaries with newline %j', async newline => {
  const frame = `data: ok${newline}${newline}`;
  const text = frame.repeat(100);
  const h = source([...encode(text)].map(byte => Uint8Array.of(byte)), {'content-type': 'text/event-stream; charset=utf-8'});
  expect(await boundedResponse(h.response, 1, encode(frame).length, vi.fn()).text()).toBe(text);
});

it.each(['data: ', ': ', 'id: '])('rejects an oversized unterminated SSE field %s', async prefix => {
  const h = source([encode(prefix), ...Array.from({length: 100}, () => encode('x'.repeat(100)))], {'content-type': 'text/event-stream'});
  await expect(boundedResponse(h.response, 1000, 250, vi.fn()).text()).rejects.toMatchObject({code: Code.LimitExceeded});
  expect(h.reads()).toBe(4);
  expect(h.cancelled).toHaveBeenCalledOnce();
});

it('counts UTF-8 bytes and the full multiline frame, including comments and separators', async () => {
  const text = 'data: ☀️\n: comment\ndata: more\n\n';
  const bytes = encode(text);
  for (const size of [bytes.length, bytes.length - 1]) {
    const h = source([bytes], {'content-type': 'text/event-stream'});
    const reading = boundedResponse(h.response, 1000, size, vi.fn()).text();
    if (size === bytes.length) expect(await reading).toBe(text);
    else await expect(reading).rejects.toMatchObject({code: Code.LimitExceeded});
  }
});

it('preserves typed stream limit failures and leaves other HTTP streams running', async () => {
  const sent: Array<{body: Record<string, unknown>; raw: ReturnType<typeof source>}> = [];
  const wire = new EventsWire(createHttpTransport(new URL('http://localhost/mcp'), {maxResponseBytes: 500, fetch: async (_url, init) => {
    const body = JSON.parse(String(init?.body));
    if (body.method === 'server/discover') return new Response(JSON.stringify({jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}}}), {headers: {'content-type': 'application/json'}});
    if (body.params.name === 'bad') {
      const raw = source([encode('data: ' + 'x'.repeat(1000))], {'content-type': 'text/event-stream'});
      sent.push({body, raw}); return raw.response;
    }
    return new Response(new ReadableStream({start(controller) {controller.enqueue(encode(': waiting\n\n'));}}), {headers: {'content-type': 'text/event-stream'}});
  }}));
  const stop = new AbortController();
  try {
    await wire.connect();
    let closed = false;
    const good = wire.stream('events/stream', {name: 'good'}, stop.signal, () => {}).catch(() => {}).finally(() => {closed = true;});
    await expect(wire.stream('events/stream', {name: 'bad'}, new AbortController().signal, () => {})).rejects.toMatchObject({code: Code.LimitExceeded});
    expect(sent[0]!.raw.cancelled).toHaveBeenCalled();
    expect(closed).toBe(false);
    stop.abort(); await good;
  } finally {stop.abort(); await wire.close();}
});

it('reapplies client limits to replacements without loosening stricter transport limits', async () => {
  const transports: ReturnType<typeof createHttpTransport>[] = [];
  const wire = new EventsWire(() => {
    const transport = createHttpTransport(new URL('http://localhost/mcp'), {maxResponseBytes: 400, fetch: async (_url, init) => {
      const body = JSON.parse(String(init?.body));
      const result = body.method === 'server/discover' ? {supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}} : {data: 'x'.repeat(1000)};
      return new Response(JSON.stringify({jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', ...result}}), {headers: {'content-type': 'application/json'}});
    }});
    transports.push(transport); return transport;
  });
  wire.setResponseLimits({maxResponseBytes: 500, maxPollBytes: 250});
  try {
    await wire.connect();
    await expect(wire.request('events/poll', {name: 'test'}, completeResult)).rejects.toMatchObject({code: Code.LimitExceeded, details: {max: 250}});
    await expect(wire.request('events/list', {}, completeResult)).rejects.toMatchObject({code: Code.LimitExceeded, details: {max: 400}});
    await transports[0]!.close();
    await wire.connect(); expect(transports).toHaveLength(2);
    await expect(wire.request('events/poll', {name: 'test'}, completeResult)).rejects.toMatchObject({code: Code.LimitExceeded, details: {max: 250}});
    await expect(wire.request('events/list', {}, completeResult)).rejects.toMatchObject({code: Code.LimitExceeded, details: {max: 400}});
  } finally { await wire.close(); }
});

it.each(['chunked', 'gzip', 'sse'])('bounds real %s HTTP responses before JSON parsing', async encoding => {
  const marker = 'private-response-body';
  const parse = JSON.parse;
  const parsedBodies: string[] = [];
  const spy = vi.spyOn(JSON, 'parse').mockImplementation((text, reviver) => {
    if (String(text).includes(marker)) parsedBodies.push(String(text));
    return parse(text, reviver);
  });
  const server = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = parse(raw);
    const result = body.method === 'server/discover'
      ? {supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}}
      : {value: marker + 'x'.repeat(128 * 1024)};
    const bytes = Buffer.from(JSON.stringify({jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', ...result}}));
    if (encoding === 'sse') {
      response.writeHead(200, {'content-type': 'text/event-stream'});
      response.end(`data: ${bytes.toString()}\n\n`);
    } else if (encoding === 'gzip') {
      const compressed = gzipSync(bytes);
      response.writeHead(200, {'content-type': 'application/json', 'content-encoding': 'gzip', 'content-length': compressed.length});
      response.end(compressed);
    } else {
      response.writeHead(200, {'content-type': 'application/json'});
      for (let offset = 0; offset < bytes.length; offset += 64) response.write(bytes.subarray(offset, offset + 64));
      response.end();
    }
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as {port: number};
  const wire = new EventsWire(createHttpTransport(new URL(`http://127.0.0.1:${address.port}/mcp`), {maxPollBytes: 512}));
  try {
    await wire.connect();
    if (encoding === 'sse') {
      const failed = Promise.withResolvers<Error>();
      wire.client.onerror = failed.resolve;
      await wire.client.transport!.send({jsonrpc: '2.0', id: 'poll-test', method: 'events/poll', params: {name: 'test'}});
      await expect(failed.promise).resolves.toMatchObject({message: expect.stringContaining('maxStreamFrameBytes (512)')});
    } else await expect(wire.request('events/poll', {name: 'test'}, completeResult)).rejects.toMatchObject({code: Code.LimitExceeded});
    expect(parsedBodies).toEqual([]);
  } finally {
    spy.mockRestore(); await wire.close(); server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});
