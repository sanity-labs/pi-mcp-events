import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {z} from 'zod';
import {EventsClient, EventsWire, WebhookReceiver, createHttpTransport} from '../dist/index.js';
import {startWeatherServer} from '../dev/weather-server.mjs';
import {startHttpsWebhookReceiver} from './fixtures/https-webhook-receiver.mjs';
import {cliPath, modelPath, root, RpcClient} from './fixtures/installed-package.mjs';

const result = z.object({}).passthrough();

async function availablePort() {
  const socket = createServer();
  await new Promise(resolve => socket.listen(0, '127.0.0.1', resolve));
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  return port;
}

test('installed Pi receives a weather event over webhook', {timeout: 30_000}, async t => {
  const directory = mkdtempSync(join(tmpdir(), 'weather-pi-webhook-'));
  const keyPath = join(directory, 'key.pem');
  const certPath = join(directory, 'cert.pem');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyPath,
    '-out', certPath, '-days', '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'], {stdio: 'ignore'});
  const port = await availablePort();
  const origin = `https://127.0.0.1:${port}`;
  const server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'test-token',
    webhookCa: readFileSync(certPath), webhookAllowedOrigins: [origin]});
  const agent = join(directory, 'agent'); mkdirSync(agent);
  writeFileSync(join(agent, 'settings.json'), JSON.stringify({packages: [root]}));
  mkdirSync(join(directory, '.pi'));
  writeFileSync(join(directory, '.pi', 'mcp-events.json'), JSON.stringify({
    servers: {weather: {transport: 'http', url: server.url, headers: {Authorization: 'Bearer test-token'}}},
    webhook: {publicUrl: `${origin}/hooks`, host: '127.0.0.1', port, keyFile: keyPath, certFile: certPath},
    watches: [{id: 'readings', server: 'weather', name: 'weather.reading', mode: 'webhook'}],
  }));
  const rpc = new RpcClient({cliPath, cwd: directory, provider: 'events-test', model: 'local',
    env: {PI_CODING_AGENT_DIR: agent, EVENTS_TEST_CONTEXTS: join(directory, 'contexts.jsonl'),
      EVENTS_TEST_OBSERVATIONS: join(directory, 'observations.jsonl')},
    args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '-e', modelPath, '--no-session'],
  });
  t.after(async () => { await rpc.stop(); await server.close(); rmSync(directory, {recursive: true, force: true}); });
  await rpc.start();
  const deadline = Date.now() + 10_000;
  let active = false;
  while (!active && Date.now() < deadline) {
    await rpc.prompt('/mcp-events status');
    active = (await rpc.getMessages()).some(message => message.customType === 'mcp-events-status' && message.content.includes('readings: active'));
    if (!active) await delay(50);
  }
  assert.equal(active, true, 'weather webhook becomes active');
  const [reading] = server.advance();
  let found = false;
  while (!found && Date.now() < deadline) {
    found = (await rpc.getMessages()).some(message => message.customType === 'mcp-events'
      && message.details.events.some(item => item.observation.type === 'event' && item.observation.event.eventId === reading.eventId));
    if (!found) await delay(50);
  }
  assert.equal(found, true, 'Pi receives the weather event');
});

test('the Events client receives weather webhooks through its own HTTPS receiver', {timeout: 15_000}, async t => {
  const directory = mkdtempSync(join(tmpdir(), 'weather-events-tls-'));
  const keyPath = join(directory, 'key.pem');
  const certPath = join(directory, 'cert.pem');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyPath,
    '-out', certPath, '-days', '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'], {stdio: 'ignore'});
  const port = await availablePort();
  const origin = `https://127.0.0.1:${port}`;
  const cert = readFileSync(certPath);
  const receiver = new WebhookReceiver({publicUrl: `${origin}/hooks`, host: '127.0.0.1', port,
    tls: {key: readFileSync(keyPath, 'utf8'), cert: cert.toString()}});
  const server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'test-token',
    webhookCa: cert, webhookAllowedOrigins: [origin]});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {
    requestInit: {headers: {Authorization: 'Bearer test-token'}},
  }));
  const client = new EventsClient(wire, {webhookReceiver: receiver});
  t.after(async () => { await client.close(); await receiver.close(); await server.close(); rmSync(directory, {recursive: true, force: true}); });
  await client.connect();
  const observations = [];
  const watch = client.watch({name: 'weather.reading', arguments: {stationId: 'harbor'}, mode: 'webhook', ttlMs: 1000}, value => observations.push(value));
  await watch.ready;
  await delay(1600);
  const [reading] = server.advance();
  const deadline = Date.now() + 5000;
  while (!observations.some(value => value.type === 'event') && Date.now() < deadline) await delay(10);
  assert.equal(observations.find(value => value.type === 'event')?.event.eventId, reading.eventId);
  await watch.stop();
});

test('weather webhook verifies, delivers, refreshes, and unsubscribes over HTTPS', {timeout: 15_000}, async t => {
  let attempts = 0;
  const receiver = await startHttpsWebhookReceiver({response: () => ({status: ++attempts === 1 ? 503 : 204})});
  const callbackUrl = receiver.url.replace('127.0.0.1', 'localhost');
  const server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'test-token',
    webhookCa: receiver.ca, webhookAllowedUrls: [callbackUrl], webhookDeliveryOptions: {retryBaseMs: 10}});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {
    requestInit: {headers: {Authorization: 'Bearer test-token'}},
  }));
  const client = new EventsClient(wire);
  t.after(async () => { await client.close(); await server.close(); await receiver.close(); });
  await client.connect();
  const params = {name: 'weather.reading', arguments: {stationId: 'harbor'},
    delivery: {mode: 'webhook', url: callbackUrl, secret: receiver.secret}, ttlMs: 60_000, cursor: null};
  const first = await wire.request('events/subscribe', params, result);
  assert.equal(receiver.requests[0].body.type, 'verification');
  assert.equal(receiver.requests[0].headers['x-mcp-subscription-id'], first.id);
  server.advance();
  await server.waitForWebhookIdle();
  const deliveries = receiver.requests.filter(request => request.body?.eventId);
  assert.equal(deliveries.length, 2);
  assert.ok(deliveries.every(request => request.verified));
  assert.equal(deliveries[0].headers['webhook-id'], deliveries[1].headers['webhook-id']);
  assert.equal(deliveries[0].body.data.stationId, 'harbor');
  const refreshed = await wire.request('events/subscribe', params, result);
  assert.equal(refreshed.id, first.id);
  assert.ok(refreshed.cursor);
  await wire.request('events/unsubscribe', {name: params.name, arguments: params.arguments,
    delivery: {url: callbackUrl}}, result);
  server.advance();
  server.advance();
  await server.waitForWebhookIdle();
  assert.equal(receiver.requests.filter(request => request.body?.eventId).length, 2);
});

test('weather webhook replays from the last cursor without skipping an unacknowledged event', {timeout: 15_000}, async t => {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const receiver = await startHttpsWebhookReceiver({response: async () => { await gate; return {status: 204}; }});
  const server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'test-token',
    webhookCa: receiver.ca, webhookAllowedUrls: [receiver.url]});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {
    requestInit: {headers: {Authorization: 'Bearer test-token'}},
  }));
  const client = new EventsClient(wire);
  t.after(async () => { release(); await client.close(); await server.close(); await receiver.close(); });
  await client.connect();
  const params = {name: 'weather.reading', arguments: {stationId: 'harbor'},
    delivery: {mode: 'webhook', url: receiver.url, secret: receiver.secret}, ttlMs: 60_000, cursor: null};
  const initial = await wire.request('events/subscribe', params, result);
  await wire.request('events/unsubscribe', {name: params.name, arguments: params.arguments, delivery: {url: receiver.url}}, result);
  const [reading] = server.advance();
  const replay = await wire.request('events/subscribe', {...params, cursor: initial.cursor}, result);
  assert.equal(replay.cursor, initial.cursor);
  const refresh = await wire.request('events/subscribe', {...params, cursor: initial.cursor}, result);
  assert.equal(refresh.cursor, initial.cursor);
  release();
  await server.waitForWebhookIdle();
  const deliveries = receiver.requests.filter(request => request.body?.eventId === reading.eventId);
  assert.equal(deliveries.length, 1);
  const accepted = await wire.request('events/subscribe', {...params, cursor: initial.cursor}, result);
  assert.notEqual(accepted.cursor, initial.cursor);
  await server.waitForWebhookIdle();
  assert.equal(receiver.requests.filter(request => request.body?.eventId === reading.eventId).length, 1);
});

test('weather webhook reports a replay gap after history expires', {timeout: 15_000}, async t => {
  const receiver = await startHttpsWebhookReceiver();
  const server = await startWeatherServer({port: 0, intervalMs: 0, historyLimit: 1, webhookToken: 'test-token',
    webhookCa: receiver.ca, webhookAllowedUrls: [receiver.url]});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {
    requestInit: {headers: {Authorization: 'Bearer test-token'}},
  }));
  const client = new EventsClient(wire);
  t.after(async () => { await client.close(); await server.close(); await receiver.close(); });
  await client.connect();
  const params = {name: 'weather.reading', arguments: {},
    delivery: {mode: 'webhook', url: receiver.url, secret: receiver.secret}, ttlMs: 60_000, cursor: null};
  const initial = await wire.request('events/subscribe', params, result);
  await wire.request('events/unsubscribe', {name: params.name, arguments: params.arguments, delivery: {url: receiver.url}}, result);
  server.advance();
  const [latest] = server.advance();
  const replay = await wire.request('events/subscribe', {...params, cursor: initial.cursor}, result);
  assert.equal(replay.truncated, true);
  await server.waitForWebhookIdle();
  assert.equal(receiver.requests.filter(request => request.body?.eventId === latest.eventId).length, 1);
});

test('weather webhook subscriptions require the configured bearer token', {timeout: 15_000}, async t => {
  const server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'test-token'});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url)));
  const client = new EventsClient(wire);
  t.after(async () => { await client.close(); await server.close(); });
  await client.connect();
  await assert.rejects(wire.request('events/subscribe', {name: 'weather.reading', arguments: {},
    delivery: {mode: 'webhook', url: 'https://example.com/hook', secret: 'whsec_invalid'}}, result),
  error => error.code === -32024);
});

test('embedding the weather server does not read webhook credentials from the environment', async t => {
  const previous = process.env.WEATHER_WEBHOOK_TOKEN;
  process.env.WEATHER_WEBHOOK_TOKEN = 'ambient-token';
  t.after(() => {
    if (previous === undefined) delete process.env.WEATHER_WEBHOOK_TOKEN;
    else process.env.WEATHER_WEBHOOK_TOKEN = previous;
  });
  const server = await startWeatherServer({port: 0, intervalMs: 0});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url)));
  const client = new EventsClient(wire);
  t.after(async () => { await client.close(); await server.close(); });
  await client.connect();
  assert.ok(client.list().every(event => !event.delivery.includes('webhook')));
});

test('weather server rejects new requests while webhook deliveries drain on shutdown', {timeout: 15_000}, async t => {
  let release;
  let started;
  const gate = new Promise(resolve => { release = resolve; });
  const deliveryStarted = new Promise(resolve => { started = resolve; });
  const url = 'https://127.0.0.1:12345/hook';
  const server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'test-token', webhookAllowedUrls: [url],
    webhookDeliveryOptions: {post: async ({body}) => {
      const payload = JSON.parse(body.toString());
      if (payload.type === 'verification') return {status: 200, body: Buffer.from(JSON.stringify({challenge: payload.challenge}))};
      started();
      await gate;
      return {status: 204};
    }},
  });
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {
    requestInit: {headers: {Authorization: 'Bearer test-token'}},
  }));
  const client = new EventsClient(wire);
  let closing = null;
  t.after(async () => { release(); await client.close(); await (closing ?? server.close()); });
  await client.connect();
  await wire.request('events/subscribe', {name: 'weather.reading', arguments: {},
    delivery: {mode: 'webhook', url, secret: 'whsec_MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY='}}, result);
  server.advance();
  await deliveryStarted;
  closing = server.close();
  const response = await fetch(new URL('/advance', server.url), {method: 'POST', headers: {'content-type': 'application/json'}, body: '{}'});
  assert.equal(response.status, 503);
  release();
  await closing;
});
