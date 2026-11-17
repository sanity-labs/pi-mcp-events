import {afterEach, describe, expect, it, vi} from 'vitest';
import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {argumentHeaderDeclarations, argumentHeaders, decodeHeaderValue, encodeHeaderValue} from '../src/argument-headers.ts';
import {EventsClient} from '../src/client.ts';
import {completeResult, EVENTS_EXTENSION, PROTOCOL_VERSION} from '../src/protocol.ts';
import {createHttpTransport, EventsWire} from '../src/wire.ts';
import {TestTransport} from './transport.ts';

const property = (type = 'string', name: unknown = 'Region') => ({type, 'x-mcp-header': name});
const schema = (value: unknown) => ({type: 'object', properties: {region: value}});
const descriptor = {name: 'example.changed', delivery: ['poll', 'push', 'webhook'],
  inputSchema: schema(property()), payloadSchema: {type: 'object'}};
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { await Promise.all(cleanups.splice(0).map(close => close())); });

describe('subscription argument headers', () => {
  it('follows nested properties, preserves primitive values, and omits absent or null values', () => {
    const declarations = argumentHeaderDeclarations({properties: {scope: {properties: {
      region: property(), enabled: property('boolean', 'Enabled'), count: property('integer', 'Count'),
      absent: property('string', 'Absent'), nullable: property('string', 'Nullable'),
    }}}});
    expect(argumentHeaders(declarations, {scope: {region: 'us-west1', enabled: false, count: 0, nullable: null}}))
      .toEqual({'Mcp-Param-Region': 'us-west1', 'Mcp-Param-Enabled': 'false', 'Mcp-Param-Count': '0'});
    expect(argumentHeaders(declarations, {})).toEqual({});
    expect(argumentHeaders(declarations, Object.create({scope: {region: 'inherited'}}))).toEqual({});
  });

  it.each([Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER])('accepts the safe integer boundary %i', value => {
    const declarations = argumentHeaderDeclarations(schema(property('integer')));
    expect(argumentHeaders(declarations, {region: value})).toEqual({'Mcp-Param-Region': String(value)});
  });
  it.each([Number.MAX_SAFE_INTEGER + 1, Number.MIN_SAFE_INTEGER - 1, 1.5, Infinity, NaN, '12', true])('rejects non-safe integer %s', value => {
    const declarations = argumentHeaderDeclarations(schema(property('integer')));
    expect(() => argumentHeaders(declarations, {region: value})).toThrow('safe range');
  });
  it.each(['', ' with spaces', 'trailing ', 'é', 'line\nbreak', 'null\0byte', '=?base64?YWJj?='])('encodes and decodes %j', value => {
    const encoded = `=?base64?${Buffer.from(value).toString('base64')}?=`;
    expect(encodeHeaderValue(value)).toBe(encoded);
    expect(decodeHeaderValue(encoded)).toBe(value);
  });
  it.each(['us-west1', 'true', '-12', 'a\tb', '=?base64?incomplete'])('preserves ASCII %j', value => {
    expect(encodeHeaderValue(value)).toBe(value);
    expect(decodeHeaderValue(value)).toBe(value);
  });
  it.each(['=?base64?%%%?=', '=?base64?YQ?=', '=?base64?/w==?='])('rejects malformed Base64 or UTF-8 %j', value => {
    expect(decodeHeaderValue(value)).toBeUndefined();
  });

  it.each(['', 'with space', 'with:colon', 'é', 1, null])('rejects invalid header annotation names %j', name => {
    expect(() => argumentHeaderDeclarations(schema(property('string', name)))).toThrow('tokens');
  });
  it.each(['number', 'object', 'array', 'null', ['string', 'null'], undefined])('rejects annotated type %j', type => {
    expect(() => argumentHeaderDeclarations(schema({type, 'x-mcp-header': 'Region'}))).toThrow('string, integer or boolean');
  });
  it('rejects case-insensitive duplicates across different paths', () => {
    expect(() => argumentHeaderDeclarations({properties: {a: property(), nested: {properties: {b: property('boolean', 'region')}}}})).toThrow('unique');
  });
  it.each([
    property(),
    {items: schema(property())},
    {prefixItems: [schema(property())]},
    {oneOf: [schema(property())]},
    {anyOf: [schema(property())]},
    {allOf: [schema(property())]},
    {if: schema(property())},
    {$defs: {unused: schema(property())}},
    {definitions: {unused: schema(property())}},
    {additionalProperties: property()},
    {patternProperties: {'.*': property()}},
    {dependentSchemas: {x: schema(property())}},
    {dependencies: {x: schema(property())}},
  ])('rejects annotations outside a chain of properties: %j', value => {
    expect(() => argumentHeaderDeclarations(value)).toThrow('chain of properties');
  });
  it('ignores instance data inside defaults and examples', () => {
    expect(argumentHeaderDeclarations({default: {'x-mcp-header': false}, examples: [property()]})).toEqual([]);
  });

  it.each([false, true])('isolates invalid declarations, with HTTP=%s', async http => {
    const transport = new TestTransport();
    transport.hasPerRequestStream = http;
    const client = new EventsClient(new EventsWire(transport));
    cleanups.push(() => client.close());
    const invalid = {...descriptor, name: 'invalid', inputSchema: schema(property('number'))};
    transport.handle = request => transport.respond(request, {events: [descriptor, invalid]});
    await client.connect();
    expect(client.list().map(event => event.name)).toEqual(http ? [descriptor.name] : [descriptor.name, 'invalid']);
    expect(client.catalogIssues).toHaveLength(http ? 1 : 0);
    if (http) expect(() => client.watch({name: descriptor.name, mode: 'poll', arguments: {region: 1}}, () => {})).toThrow('inputSchema');
  });

  it('rejects unsafe annotated integers before starting a watch', async () => {
    const transport = new TestTransport();
    transport.hasPerRequestStream = true;
    const client = new EventsClient(new EventsWire(transport));
    cleanups.push(() => client.close());
    transport.handle = request => transport.respond(request, {events: [{...descriptor, inputSchema: schema(property('integer'))}]});
    await client.connect();
    expect(() => client.watch({name: descriptor.name, mode: 'poll', arguments: {region: Number.MAX_SAFE_INTEGER + 1}}, () => {})).toThrow('inputSchema');
    expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'events/poll')).toHaveLength(0);
  });
});

describe('HTTP argument header transport and recovery', () => {
  function setup() {
    const sent: Array<{body: JSONRPCRequest; headers: Headers}> = [];
    let current = descriptor;
    let failure: ((body: JSONRPCRequest, headers: Headers) => boolean) | undefined;
    const wire = new EventsWire(() => createHttpTransport(new URL('http://localhost/mcp'), {
      requestInit: {headers: {'Mcp-Param-Stale': 'stale', 'Mcp-Param-Region': 'configured', Authorization: 'Bearer test'}},
      fetch: async (_url, init) => {
        const body = JSON.parse(String(init?.body)) as JSONRPCRequest;
        const headers = new Headers(init?.headers);
        sent.push({body, headers});
        if (failure?.(body, headers)) return Response.json({jsonrpc: '2.0', id: body.id, error: {code: -32020, message: 'HeaderMismatch'}}, {status: 400});
        const result = body.method === 'server/discover'
          ? {supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}}
          : {};
        return Response.json({jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', ...result,
          ...(body.method === 'events/list' ? {events: [current]} : {})}});
      },
    }));
    const client = new EventsClient(wire);
    cleanups.push(() => client.close());
    return {wire, client, sent, change(name: string) { current = {...descriptor, inputSchema: schema(property('string', name))}; },
      fail(shouldFail: typeof failure) { failure = shouldFail; }};
  }

  it('mirrors arguments on poll, push, subscribe/renewal/unsubscribe through the actual SDK', async () => {
    const {wire, client, sent} = setup();
    await client.connect();
    const params = {name: descriptor.name, arguments: {region: 'café'}, cursor: 'saved'};
    for (const method of ['events/poll', 'events/subscribe', 'events/subscribe', 'events/unsubscribe']) {
      await wire.request(method, params, completeResult);
    }
    await wire.stream('events/stream', params, new AbortController().signal, () => {});
    for (const {body, headers} of sent) {
      expect(headers.get('authorization')).toBe('Bearer test');
      expect(headers.get('Mcp-Param-Stale')).toBeNull();
      if (['server/discover', 'events/list'].includes(body.method)) expect(headers.get('Mcp-Param-Region')).toBeNull();
      else {
        expect(headers.get('Mcp-Param-Region')).toBe('=?base64?Y2Fmw6k=?=');
        expect(body.params?.arguments).toEqual(params.arguments);
      }
    }
    await wire.request('events/poll', {name: descriptor.name, arguments: {}}, completeResult);
    expect(sent.at(-1)?.headers.get('Mcp-Param-Region')).toBeNull();
  });

  it.each(['events/poll', 'events/subscribe', 'events/unsubscribe', 'events/stream'])('refreshes a stale catalog once and preserves cursor for %s', async method => {
    const fixture = setup();
    const {wire, client, sent} = fixture;
    await client.connect();
    fixture.change('Zone');
    fixture.fail((body, headers) => body.method === method && headers.get('Mcp-Param-Zone') !== 'us-west1');
    const params = {name: descriptor.name, arguments: {region: 'us-west1'}, cursor: 'accepted-cursor'};
    if (method === 'events/stream') await wire.stream(method, params, new AbortController().signal, () => {});
    else await wire.request(method, params, completeResult);
    const attempts = sent.filter(item => item.body.method === method);
    expect(attempts).toHaveLength(2);
    expect(attempts.map(item => item.body.params?.cursor)).toEqual(['accepted-cursor', 'accepted-cursor']);
    expect(attempts[0]!.headers.get('Mcp-Param-Region')).toBe('us-west1');
    expect(attempts[1]!.headers.get('Mcp-Param-Region')).toBeNull();
    expect(attempts[1]!.headers.get('Mcp-Param-Zone')).toBe('us-west1');
    expect(sent.filter(item => item.body.method === 'events/list')).toHaveLength(2);
  });

  it('bounds repeated HeaderMismatch recovery', async () => {
    const fixture = setup();
    await fixture.client.connect();
    fixture.fail(body => body.method === 'events/poll');
    await expect(fixture.wire.request('events/poll', {name: descriptor.name, arguments: {region: 'x'}}, completeResult)).rejects.toMatchObject({code: -32020});
    expect(fixture.sent.filter(item => item.body.method === 'events/poll')).toHaveLength(2);
    expect(fixture.sent.filter(item => item.body.method === 'events/list')).toHaveLength(2);
  });

  it('does not retry when cancellation arrives during catalog refresh', async () => {
    const transport = new TestTransport();
    transport.hasPerRequestStream = true;
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await wire.connect();
    const refresh = vi.fn();
    const controller = new AbortController();
    wire.setEventHeaders(() => ({}), async () => { refresh(); controller.abort(); });
    transport.handle = request => transport.fail(request, -32020);
    await expect(wire.request('events/poll', {name: descriptor.name}, completeResult, {signal: controller.signal})).rejects.toThrow();
    expect(refresh).toHaveBeenCalledOnce();
    expect(transport.messages.filter(({message}) => 'method' in message && message.method === 'events/poll')).toHaveLength(1);
  });

  it('leaves a stream that already delivered notifications to the watch recovery loop', async () => {
    const transport = new TestTransport();
    transport.hasPerRequestStream = true;
    const wire = new EventsWire(transport);
    cleanups.push(() => wire.close());
    await wire.connect();
    const refresh = vi.fn();
    wire.setEventHeaders(() => ({}), refresh);
    transport.handle = request => { transport.notify(request, 'active', {cursor: 'advanced'}); transport.fail(request, -32020); };
    await expect(wire.stream('events/stream', {name: descriptor.name, cursor: 'saved'}, new AbortController().signal, () => {})).rejects.toMatchObject({code: -32020});
    expect(refresh).not.toHaveBeenCalled();
  });
});
