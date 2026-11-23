import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {z} from 'zod';
import {startWeatherServer} from '../dev/weather-server.mjs';
import {EventsWire, createHttpTransport} from '../dist/index.js';
import {startHttpsWebhookReceiver} from './fixtures/https-webhook-receiver.mjs';
import {startWeatherConformanceServer} from './fixtures/weather-conformance-server.mjs';

const result = z.looseObject({});
const checkName = 'weather.station.tick';

function connect(t, server, token) {
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {
    requestInit: token === undefined ? undefined : {headers: {Authorization: `Bearer ${token}`}},
  }));
  t.after(() => wire.close());
  return wire;
}

async function until(condition) {
  const deadline = Date.now() + 4000;
  while (Date.now() < deadline) {
    const value = condition();
    if (value) return value;
    await delay(20);
  }
  assert.fail('Timed out waiting for Events notification');
}

function stream(wire, t, name) {
  const frames = [];
  const controller = new AbortController();
  t.after(() => controller.abort());
  const closed = wire.stream('events/stream', {name}, controller.signal, (method, params) => {
    frames.push({method, ...params});
  });
  void closed.catch(() => {});
  const find = (kind, match = () => true) => frames.find(frame => frame.method === `notifications/events/${kind}` && match(frame));
  return {wait: (kind, match) => until(() => find(kind, match)), closed, frames};
}

test('weather reference exercises checked-source failure, gap, recovery, and schema changes over HTTP', {timeout: 15_000}, async t => {
  const server = await startWeatherConformanceServer();
  const wire = connect(t, server);
  t.after(() => server.close());
  const control = async (suffix, args) => {
    const response = await wire.request('tools/call', {name: `events_conformance_${suffix}`, arguments: args}, result);
    assert.equal(response.isError, undefined);
    return response.content[0].text;
  };
  const station = stream(wire, t, checkName);
  await station.wait('active', frame => !frame.truncated);
  await control('yield_error', {name: checkName});
  assert.equal((await station.wait('error')).error.code, -32603);
  await control('yield_gap', {name: checkName});
  const gap = await station.wait('active', frame => frame.truncated);
  const reading = await station.wait('event', frame => Number(frame.cursor) > Number(gap.cursor));
  assert.equal(reading.eventId, `station-${reading.data.tick}`);
  const pulse = stream(wire, t, 'weather.pulse');
  assert.equal((await pulse.wait('active')).cursor, null);
  await control('yield_gap', {name: 'weather.pulse'});
  assert.equal((await pulse.wait('event')).cursor, null);
  assert.equal(pulse.frames.filter(frame => frame.truncated).length, 0);

  const name = (await control('catalog', {op: 'add'})).slice(4);
  const dynamic = stream(wire, t, name);
  await dynamic.wait('active');
  await control('catalog', {op: 'extend', name});
  assert.equal(dynamic.frames.length, 1);
  await control('catalog', {op: 'retype', name});
  const terminal = await dynamic.wait('terminated');
  assert.equal(terminal.error.code, -32026);
  assert.deepEqual(terminal.error.data, {feature: 'payloadSchema', reason: 'schema_changed'});
  await dynamic.closed;
  const replacement = stream(wire, t, name);
  await replacement.wait('active');
  await control('catalog', {op: 'remove', name});
  assert.equal((await replacement.wait('terminated')).error.code, -32023);
  await replacement.closed;
  await assert.rejects(wire.request('events/poll', {name}, result), error => error.code === -32023);
  await assert.rejects(wire.request('events/poll', {name: 'weather.pulse'}, result),
    error => error.code === -32026 && error.data?.value === 'poll');
});

test('live-only poll subscriptions isolate principals and release their lease on revocation', {timeout: 10_000}, async t => {
  const seen = new Map();
  let stationTick = 0;
  const server = await startWeatherServer({
    port: 0, intervalMs: 0, maxSubscriptions: 2, authenticate: token => token || undefined,
    eventTypes: [['weather.live', {
      replay: false, payloadSchema: z.object({tick: z.number()}),
      check({subscriptionId}) {
        const previous = seen.get(subscriptionId);
        seen.set(subscriptionId, stationTick);
        const events = previous === undefined || previous === stationTick ? []
          : [{eventId: `weather-${stationTick}`, timestamp: new Date().toISOString(), data: {tick: stationTick}}];
        return {events, cursor: null, truncated: true};
      },
      onEnd({subscriptionId}) { seen.delete(subscriptionId); },
    }]],
  });
  t.after(() => server.close());
  const alice = connect(t, server, 'alice');
  const bob = connect(t, server, 'bob');
  const carol = connect(t, server, 'carol');
  const poll = wire => wire.request('events/poll', {name: 'weather.live'}, result);
  for (const client of [alice, bob]) assert.equal((await poll(client)).cursor, null);
  assert.equal(seen.size, 2);
  await assert.rejects(poll(carol), error => error.code === -32025 && error.data?.limit === 'subscriptions');
  stationTick++;
  for (const client of [alice, bob]) {
    const page = await poll(client);
    assert.equal(page.events[0].eventId, 'weather-1');
    assert.equal(page.events[0].cursor, null);
    assert.equal(page.truncated, false);
  }
  await server.eventTypes.terminate('weather.live', {principal: 'alice'});
  assert.equal(seen.size, 1);
  await assert.rejects(poll(alice), error => error.code === -32024);
  assert.equal((await poll(carol)).events.length, 0);
});

test('a shared webhook registration survives restart and access revocation sends a signed termination', {timeout: 10_000}, async t => {
  const receiver = await startHttpsWebhookReceiver();
  t.after(() => receiver.close());
  const records = new Map();
  const store = {
    async get(id) { return records.get(id); },
    async put(record) { records.set(record.id, structuredClone(record)); },
    async delete(id) { return records.delete(id); },
    async *listByName(name) { for (const entry of records.values()) if (entry.name === name) yield entry; },
  };
  const settings = {port: 0, intervalMs: 0, webhookToken: 'trusted', webhookStore: store,
    webhookCa: receiver.ca, webhookAllowedUrls: [receiver.url]};
  const first = await startWeatherServer(settings);
  const wire = connect(t, first, 'trusted');
  t.after(() => first.close());
  const name = 'weather.reading';
  const subscribe = {name, delivery: {mode: 'webhook', url: receiver.url, secret: receiver.secret}};
  const grant = await wire.request('events/subscribe', subscribe, result);
  assert.equal(records.size, 1);
  await wire.close();
  await first.close();
  const restarted = await startWeatherServer(settings);
  t.after(() => restarted.close());
  const event = restarted.advance()[0];
  const delivery = await until(() => receiver.requests.find(request => request.body?.eventId === event.eventId));
  assert.ok(delivery.verified);
  assert.equal(delivery.headers['x-mcp-subscription-id'], grant.id);
  assert.equal(receiver.requests.filter(request => request.body?.type === 'verification').length, 1);
  await restarted.eventTypes.terminate(name, {principal: 'weather-reference-user'});
  const termination = receiver.requests.find(request => request.body?.type === 'terminated');
  assert.ok(termination?.verified);
  assert.equal(termination.body.error.code, -32024);
  assert.equal(records.size, 0);
  const next = connect(t, restarted, 'trusted');
  await assert.rejects(next.request('events/subscribe', subscribe, result), error => error.code === -32024);
});
