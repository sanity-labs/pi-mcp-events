import {execFileSync} from 'node:child_process';
import {createHmac} from 'node:crypto';
import {once} from 'node:events';
import {mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync} from 'node:fs';
import {createServer} from 'node:http';
import {request} from 'node:https';
import {tmpdir} from 'node:os';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {createReceiver, createTransport, EventsClient, EventsWire, EVENTS_EXTENSION, PROTOCOL_VERSION, parseConfig, readConfig, EventsClientErrorCode as ErrorCode} from '../src/index.ts';

const servers = {example: {transport: 'http', url: 'https://example.invalid/mcp'}};
const watch = {id: 'one', server: 'example', name: 'test.changed', mode: 'poll'};
const webhook = {publicUrl: 'https://localhost/hooks', port: 0};
const directories: string[] = [];
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const directory of directories.splice(0)) rmSync(directory, {recursive: true, force: true});
  vi.unstubAllEnvs();
});
function directory() {
  const path = mkdtempSync(join(tmpdir(), 'pi-events-config-')); directories.push(path); return path;
}
function file(value: unknown) {
  const path = join(directory(), 'events.json'); writeFileSync(path, JSON.stringify(value)); return path;
}
const invalid = {code: ErrorCode.InvalidConfiguration};

describe('configuration', () => {
  it('loads watch defaults and exposes the same client and receiver settings as the APIs', () => {
    const config = readConfig(file({servers, webhook, watches: [watch], client: {maxSubscriptions: 40, heartbeatMs: 1000}}));
    expect(config.watches[0]).toEqual({...watch, arguments: {}, maxEvents: 50});
    expect(config.client).toMatchObject({maxSubscriptions: 40, heartbeatMs: 1000, maxRetries: 10, webhookRefreshMs: 300_000});
    expect(config.webhook).toMatchObject({maxRegistrations: 32, maxPendingDeliveries: 16});
    expect(parseConfig({servers}).watches).toEqual([]);
    expect(createReceiver(parseConfig({servers}), '/unused/events.json')).toBeUndefined();
  });

  it.each([
    {watches: [watch, {...watch, arguments: {different: true}}]},
    {watches: [watch, {...watch, id: 'two'}]},
    {watches: [{...watch, server: 'missing'}]},
    {watches: [{...watch, server: 'toString'}]},
    {watches: [{...watch, mode: 'webhook'}]},
    {client: {maxSubscriptions: 0}}, {client: {maxRetires: 2}},
    {watches: [{...watch, typo: true}]}, {typo: true},
    {servers: {example: {transport: 'http', url: servers.example.url, command: 'unused'}}},
    {servers: {example: {transport: 'stdio', command: ''}}},
    {watches: [{...watch, arguments: {invalid: undefined}}]},
  ])('rejects invalid references, duplicates, options, and unknown fields: %j', value => {
    expect(() => parseConfig({servers, ...value})).toThrow(expect.objectContaining(invalid));
  });

  it('canonicalizes argument objects when detecting duplicate subscriptions', () => {
    expect(() => parseConfig({servers, watches: [
      {...watch, arguments: {a: 1, b: 2}}, {...watch, id: 'two', arguments: {b: 2, a: 1}},
    ]})).toThrow(expect.objectContaining(invalid));
    expect(parseConfig({servers, watches: [watch, {...watch, id: 'push', mode: 'push'}]}).watches).toHaveLength(2);
  });

  it('uses configurable subscription limits per server and receiver limits across servers', () => {
    const watches = Array.from({length: 33}, (_, id) => ({...watch, id: String(id), arguments: {id}}));
    expect(() => parseConfig({servers, watches})).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded, details: {limit: 'maxSubscriptions', max: 32}}));
    expect(parseConfig({servers, watches, client: {maxSubscriptions: 33}}).watches).toHaveLength(33);
    expect(parseConfig({servers, client: {maxSubscriptions: 600}, watches: Array.from({length: 513}, (_, id) => ({...watch, id: String(id), arguments: {id}}))}).watches).toHaveLength(513);
    const shared = {servers: {...servers, other: servers.example}, client: {maxSubscriptions: 1}, watches: [
      {...watch, mode: 'webhook'}, {...watch, mode: 'webhook', id: 'two', server: 'other'},
    ]};
    expect(() => parseConfig({...shared, webhook: {...webhook, maxRegistrations: 1}})).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded, details: {limit: 'maxRegistrations', max: 1}}));
    expect(parseConfig({...shared, webhook: {...webhook, maxRegistrations: 2}}).watches).toHaveLength(2);
  });

  it.each(['not a URL', 'file:///tmp/socket', 'ftp://example.invalid', 'https://user:secret@example.invalid', 'https://example.invalid/#fragment'])('rejects unsupported HTTP URL %s', url => {
    expect(() => parseConfig({servers: {example: {transport: 'http', url}}})).toThrow(expect.objectContaining(invalid));
  });

  it.each([
    {keyFile: 'key.pem'}, {certFile: 'cert.pem'}, {host: '0.0.0.0'},
    {publicUrl: 'http://localhost'}, {publicUrl: 'https://localhost/?query=value'},
    {publicUrl: 'https://user:secret@localhost'}, {publicUrl: 'https://localhost/#fragment'},
    {maxPendingDeliveries: 0}, {keyFile: '', certFile: ''},
  ])('rejects invalid webhook settings during parsing: %j', settings => {
    expect(() => parseConfig({servers, webhook: {...webhook, ...settings}})).toThrow(expect.objectContaining(invalid));
  });

  it('does not resolve credentials or read TLS files until resources are constructed', async () => {
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', undefined);
    const config = parseConfig({servers: {example: {...servers.example, headers: {Authorization: {env: 'PI_EVENTS_TEST_TOKEN', prefix: 'Bearer '}}}}, webhook: {...webhook, host: '0.0.0.0', keyFile: 'missing.key', certFile: 'missing.crt'}});
    expect(() => createTransport(config.servers.example!, '/tmp')).toThrow(expect.objectContaining({code: ErrorCode.MissingEnvironmentVariable, details: {variable: 'PI_EVENTS_TEST_TOKEN'}}));
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', 'synthetic-token');
    const transport = createTransport(config.servers.example!, '/tmp'); await transport.close();
    expect(() => createReceiver(config, '/tmp/events.json')).toThrow(expect.objectContaining({code: ErrorCode.ConfigurationReadFailed}));
  });

  it('reports file and JSON errors without including file contents or credential values', () => {
    expect(() => readConfig(join(directory(), 'absent.json'))).toThrow(expect.objectContaining({code: ErrorCode.ConfigurationReadFailed}));
    const path = file({}); writeFileSync(path, 'synthetic-secret invalid JSON');
    try { readConfig(path); throw new Error('Expected failure'); }
    catch (error) { expect(error).toMatchObject(invalid); expect(String(error)).not.toContain('synthetic-secret'); expect(error).not.toHaveProperty('cause'); }
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', 'synthetic-secret\ninvalid');
    const config = parseConfig({servers: {example: {...servers.example, headers: {Authorization: {env: 'PI_EVENTS_TEST_TOKEN'}}}}});
    try { createTransport(config.servers.example!, '/tmp'); throw new Error('Expected failure'); }
    catch (error) { expect(error).toMatchObject(invalid); expect(String(error)).not.toContain('synthetic-secret'); expect(error).not.toHaveProperty('cause'); }
  });

  it('reads updated HTTP credentials when reconnecting through the real MCP transport', async () => {
    const headers: Array<Record<string, unknown>> = [];
    const server = createServer(async (req, res) => {
      let body = ''; for await (const part of req) body += part;
      const message = JSON.parse(body);
      headers.push(req.headers);
      const result = message.method === 'server/discover'
        ? {supportedVersions: [PROTOCOL_VERSION], capabilities: {extensions: {[EVENTS_EXTENSION]: {}}}}
        : {events: []};
      res.writeHead(200, {'content-type': 'application/json'});
      res.end(JSON.stringify({jsonrpc: '2.0', id: message.id, result: {resultType: 'complete', ...result}}));
    });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    cleanup.push(async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
    const address = server.address(); if (!address || typeof address === 'string') throw new Error('No address');
    const config = parseConfig({servers: {example: {transport: 'http', url: `http://127.0.0.1:${address.port}`, headers: {Authorization: {env: 'PI_EVENTS_TEST_TOKEN', prefix: 'Bearer '}, 'x-literal': 'fixed'}}}});
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', 'one');
    let current: ReturnType<typeof createTransport>;
    const wire = new EventsWire(() => { current = createTransport(config.servers.example!, '/tmp'); return current; });
    const client = new EventsClient(wire, config.client); cleanup.push(() => client.close());
    await client.connect();
    expect(headers[0]).toMatchObject({authorization: 'Bearer one', 'x-literal': 'fixed'});
    await current!.close();
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', 'two');
    await client.connect();
    expect(headers.at(-1)).toMatchObject({authorization: 'Bearer two', 'x-literal': 'fixed'});
    expect(config.servers.example).toHaveProperty('headers.Authorization', {env: 'PI_EVENTS_TEST_TOKEN', prefix: 'Bearer '});
  });

  it('starts stdio in the configured directory with arguments and selected environment values', async () => {
    const root = directory(); mkdirSync(join(root, 'child'));
    vi.stubEnv('PI_EVENTS_SOURCE', 'from-env'); vi.stubEnv('PI_EVENTS_UNLISTED', 'must-not-inherit');
    const config = parseConfig({servers: {local: {transport: 'stdio', command: process.execPath,
      args: [fileURLToPath(new URL('./fixtures/config-server.mjs', import.meta.url)), 'an argument with spaces'],
      cwd: 'child', env: {PI_EVENTS_TEST_TOKEN: {env: 'PI_EVENTS_SOURCE', prefix: 'prefix:'}, PI_EVENTS_LITERAL: 'literal'},
    }}});
    const client = new EventsClient(new EventsWire(() => createTransport(config.servers.local!, root)), config.client);
    cleanup.push(() => client.close()); await client.connect();
    const details = JSON.parse(client.list()[0]!.description!);
    expect(details).toEqual({cwd: realpathSync(join(root, 'child')), args: ['an argument with spaces'], token: 'prefix:from-env', literal: 'literal'});
  });

  it('loads relative TLS files and accepts a signed challenge over trusted HTTPS', async () => {
    const root = directory(); mkdirSync(join(root, 'tls'));
    execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', join(root, 'tls/key.pem'), '-out', join(root, 'tls/cert.pem'), '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], {stdio: 'ignore'});
    const path = join(root, 'events.json');
    writeFileSync(path, JSON.stringify({servers, webhook: {...webhook, keyFile: 'tls/key.pem', certFile: 'tls/cert.pem'}}));
    const receiver = createReceiver(readConfig(path), path)!; cleanup.push(() => receiver.close()); await receiver.start();
    const route = receiver.register(() => {});
    const raw = JSON.stringify({type: 'verification', challenge: 'nonce'});
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = createHmac('sha256', Buffer.from(route.secret.slice(6), 'base64')).update(`msg_tls.${timestamp}.${raw}`).digest('base64');
    const address = receiver.address(); if (!address || typeof address === 'string') throw new Error('No address');
    const response = await new Promise<{status: number; body: string}>((resolve, reject) => {
      const req = request({hostname: '127.0.0.1', servername: 'localhost', port: address.port, path: new URL(route.url).pathname, method: 'POST', ca: readFileSync(join(dirname(path), 'tls/cert.pem')), headers: {
        'content-type': 'application/json', 'webhook-id': 'msg_tls', 'webhook-timestamp': timestamp, 'webhook-signature': `v1,${signature}`, 'x-mcp-subscription-id': 'sub_tls',
      }}, res => { let body = ''; res.on('data', chunk => { body += chunk; }); res.on('end', () => resolve({status: res.statusCode!, body})); res.on('error', reject); });
      req.on('error', reject); req.end(raw);
    });
    expect(response).toEqual({status: 200, body: JSON.stringify({challenge: 'nonce'})});
  });
});
