import assert from 'node:assert/strict';
import {existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {request} from 'node:http';
import {setTimeout as delay} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {test} from 'node:test';
import {z} from 'zod';
import {startWeatherServer} from '../dev/weather-server.mjs';
import {cliPath, modelPath, root, RpcClient} from './fixtures/installed-package.mjs';

const {EventsClient, EventsWire, createHttpTransport} = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const result = z.object({}).passthrough();
async function until(check) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) { if (await check()) return; await delay(25); }
  assert.fail('Timed out waiting for weather delivery');
}
async function setup(t, options) {
  let seconds = 0;
  const server = await startWeatherServer({port: 0, intervalMs: 0, now: () => Date.UTC(2026, 9, 28, 12) + seconds * 1000, ...options});
  const wire = new EventsWire(() => createHttpTransport(new URL(server.url)));
  const client = new EventsClient(wire, {retryMs: 10});
  t.after(async () => { await client.close(); await server.close(); });
  await client.connect();
  const poll = (cursor = null, extra = {}) => wire.request('events/poll', {
    name: 'weather.reading', arguments: {}, cursor, ...extra,
  }, result);
  return {server, client, wire, poll, setTime(value) { seconds = value; }};
}

for (const fault of ['stalled-page', 'quota']) {
  test(`weather reference: polling backs off ${fault} responses and recovers real readings`, {timeout: 20_000}, async t => {
    const server = await startWeatherServer({port: 0, intervalMs: 0});
    const polls = [];
    const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {fetch: async (url, init) => {
      const request = JSON.parse(init.body);
      if (request.method !== 'events/poll') return fetch(url, init);
      polls.push({at: performance.now(), cursor: request.params.cursor});
      const response = await fetch(url, init);
      const body = await response.json();
      if (polls.length === 2 || polls.length === 3) {
        if (fault === 'quota') return Response.json({jsonrpc: '2.0', id: request.id,
          error: {code: -32025, message: 'Upstream quota exhausted', data: {limit: 'upstream_requests'}}});
        return Response.json({...body, result: {...body.result, events: [], cursor: request.params.cursor, hasMore: true}});
      }
      return Response.json({...body, result: {...body.result, nextPollMs: 0}});
    }}));
    const client = new EventsClient(wire, {pollFloorMs: 50, retryMs: 10});
    t.after(async () => { await client.close(); await server.close(); });
    await client.connect();
    const events = [];
    const watch = client.watch({name: 'weather.reading', arguments: {}, mode: 'poll', maxEvents: 1}, item => {
      if (item.type === 'event') events.push(item.event);
    });
    await watch.ready;
    const initialCursor = watch.cursor;
    const readings = Array.from({length: 3}, () => server.advance()[0]);
    await until(() => events.length === 3);
    assert.deepEqual(events.map(event => event.eventId), readings.map(event => event.eventId));
    assert.deepEqual(polls.slice(1, 4).map(poll => poll.cursor), [initialCursor, initialCursor, initialCursor]);
    assert.ok(polls[2].at - polls[1].at >= 49, 'first retry respects the floor');
    assert.ok(polls[3].at - polls[2].at >= 99, 'second retry backs off exponentially');
    assert.notEqual(watch.cursor, initialCursor);
    assert.equal(watch.status, 'active');
    await watch.stop();
  });
}

for (const mode of ['poll', 'push']) {
  test(`weather reference: ${mode} preserves basic-offset event timestamps`, {timeout: 20_000}, async t => {
    const {server, client} = await setup(t, {
      onEvent(event) { event.timestamp = event.timestamp.replace(/Z$/, '+0000'); },
    });
    const events = [];
    const watch = client.watch({name: 'weather.reading', arguments: {}, mode}, item => {
      if (item.type === 'event') events.push(item.event);
    });
    await watch.ready;
    const [reading] = server.advance();
    await until(() => events.length === 1);
    assert.match(reading.timestamp, /\+0000$/);
    assert.equal(events[0].timestamp, reading.timestamp);
    assert.equal(events[0].eventId, reading.eventId);
    assert.deepEqual(events[0].data, reading.data);
    assert.ok(watch.cursor);
    assert.equal(watch.status, 'active');
    await watch.stop();
  });

  test(`weather reference: ${mode} delivers alongside an unsupported event schema`, {timeout: 20_000}, async t => {
    const server = await startWeatherServer({port: 0, intervalMs: 0});
    const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {fetch: async (url, init) => {
      const response = await fetch(url, init);
      if (JSON.parse(init.body).method !== 'events/list') return response;
      const body = await response.json();
      const reading = body.result.events.find(event => event.name === 'weather.reading');
      reading.inputSchema = true;
      reading.payloadSchema = true;
      body.result.events.push({name: 'weather.experimental', delivery: ['poll', 'push'], inputSchema: true,
        payloadSchema: {$schema: 'https://example.com/unsupported-schema'}});
      return Response.json(body);
    }}));
    const client = new EventsClient(wire, {retryMs: 10});
    t.after(async () => { await client.close(); await server.close(); });
    await client.connect();
    assert.equal(client.list().length, 4);
    assert.deepEqual(client.catalogIssues.map(issue => issue.name), ['weather.experimental']);
    assert.throws(() => client.watch({name: 'weather.experimental', arguments: {}, mode}, () => {}), {code: 'UNSUPPORTED_EVENT_SCHEMA'});
    const events = [];
    const watch = client.watch({name: 'weather.reading', arguments: {}, mode}, item => {
      if (item.type === 'event') events.push(item.event);
    });
    await watch.ready;
    await client.refresh();
    const [reading] = server.advance();
    await until(() => events.length === 1);
    assert.equal(events[0].eventId, reading.eventId);
    assert.deepEqual(events[0].data, reading.data);
    assert.equal(watch.status, 'active');
    await watch.stop();
  });

  test(`weather reference: ${mode} recovers a reading rejected by a stale catalog`, {timeout: 20_000}, async t => {
    const server = await startWeatherServer({port: 0, intervalMs: 0});
    let catalogReads = 0;
    const wire = new EventsWire(() => createHttpTransport(new URL(server.url), {fetch: async (url, init) => {
      const response = await fetch(url, init);
      if (JSON.parse(init.body).method !== 'events/list') return response;
      const body = await response.json();
      const reading = body.result.events.find(event => event.name === 'weather.reading');
      reading._meta = {'example.com/source': 'weather-reference'};
      if (++catalogReads === 1) {
        delete reading.payloadSchema.properties.daylight;
        reading.payloadSchema.required = reading.payloadSchema.required.filter(name => name !== 'daylight');
      }
      return Response.json(body);
    }}));
    const client = new EventsClient(wire, {retryMs: 10});
    t.after(async () => { await client.close(); await server.close(); });
    await client.connect();
    assert.equal(client.list()[0].payloadSchema.properties.daylight, undefined);
    assert.deepEqual(client.list()[0]._meta, {'example.com/source': 'weather-reference'});
    const events = [];
    const watch = client.watch({name: 'weather.reading', arguments: {}, mode}, item => {
      if (item.type === 'event') events.push(item.event);
    });
    await watch.ready;
    const [reading] = server.advance();
    await until(() => events.length === 1);
    assert.equal(catalogReads, 2, 'payload mismatch reads the current weather catalog once');
    assert.deepEqual(events[0].data, reading.data);
    assert.equal(events[0].eventId, reading.eventId);
    assert.equal(watch.status, 'active');
    assert.ok(watch.cursor);
    assert.deepEqual(client.list()[0]._meta, {'example.com/source': 'weather-reference'});
    await watch.stop();
  });
}

test('weather reference: discovery, validation, filtering, pagination, and stable replay IDs', async t => {
  const {server, client, wire, poll} = await setup(t);
  assert.deepEqual(client.list().map(event => [event.name, event.delivery]), [
    ['weather.reading', ['poll', 'push']], ['weather.changed', ['poll', 'push']], ['weather.alert', ['poll', 'push']],
  ]);
  const initial = await poll();
  assert.deepEqual(initial.events, []);
  for (let i = 0; i < 5; i++) server.advance();
  const first = await poll(initial.cursor, {maxEvents: 2});
  assert.deepEqual(first.events.map(event => event.data.condition), ['cloudy', 'clear']);
  assert.equal(first.hasMore, true);
  assert.equal((await poll(initial.cursor, {maxEvents: 2})).events[0].eventId, first.events[0].eventId);
  const rest = await poll(first.cursor);
  assert.deepEqual(rest.events.map(event => event.data.condition), ['cloudy', 'clear', 'cloudy']);
  assert.equal(rest.hasMore, false);
  const filtered = await poll(null, {arguments: {stationId: 'harbor'}});
  server.advance();
  const empty = await poll(filtered.cursor, {arguments: {stationId: 'harbor'}});
  assert.deepEqual(empty.events, []);
  assert.notEqual(empty.cursor, filtered.cursor);
  for (const params of [{name: 'missing'}, {arguments: {island: 'other'}}, {maxEvents: 0}, {cursor: 'garbage'}, {cursor: initial.cursor, arguments: {stationId: 'other'}}]) {
    await assert.rejects(poll(null, params), error => [-32602, -32023].includes(error.code));
  }
  await assert.rejects(wire.request('events/subscribe', {name: 'weather.reading'}, result), /Method not found/);
});

test('weather reference: HTTP routing headers and loopback guards reject invalid requests', async t => {
  const {server} = await setup(t);
  const advance = await fetch(new URL('/advance', server.url), {
    method: 'POST', headers: {'content-type': 'application/json; charset=utf-8'}, body: '{}',
  });
  assert.equal(advance.status, 200);
  assert.equal((await advance.json())[0].data.condition, 'cloudy');
  for (const header of [undefined, 'weather.wrong']) {
    const response = await fetch(server.url, {method: 'POST',
      headers: {'content-type': 'application/json', ...(header === undefined ? {} : {'Mcp-Name': header})},
      body: JSON.stringify({jsonrpc: '2.0', id: 1, method: 'events/poll', params: {name: 'weather.reading', arguments: {}}}),
    });
    assert.equal(response.status, 400);
    assert.equal((await response.json()).error.code, -32020);
  }
  assert.equal((await fetch(server.url, {headers: {Origin: 'https://untrusted.example'}})).status, 403);
  const status = await new Promise((resolve, reject) => {
    const incoming = request(server.url, {headers: {Host: 'untrusted.example'}}, response => {
      response.resume(); resolve(response.statusCode);
    });
    incoming.on('error', reject); incoming.end();
  });
  assert.equal(status, 403);
});

test('weather reference: retention, age, and server-restart gaps are explicit', async t => {
  const {server, poll} = await setup(t, {historyLimit: 2});
  const initial = await poll();
  for (let i = 0; i < 5; i++) server.advance();
  assert.equal((await poll(initial.cursor)).truncated, true);
  const second = await setup(t);
  assert.equal((await second.poll(initial.cursor)).truncated, true);
  const before = await second.poll();
  second.server.advance();
  await delay(5);
  const aged = await second.poll(before.cursor, {maxAgeMs: 0});
  assert.equal(aged.truncated, true);
  assert.deepEqual(aged.events, []);
});

test('weather reference: weather follows elapsed time, not bursts, and warnings persist independently', async t => {
  const {server, setTime} = await setup(t);
  const pair = seconds => {
    setTime(seconds);
    return [...server.advance(), ...server.advance()];
  };
  const initial = pair(0);
  assert.deepEqual(initial.map(event => event.data.condition), ['cloudy', 'clear']);
  for (let i = 0; i < 20; i++) assert.deepEqual(server.advance().map(event => event.name), ['weather.reading']);
  assert.ok(!pair(19).some(event => event.name === 'weather.alert'), 'storms do not start before twenty seconds');
  const raised = pair(20).filter(event => event.name === 'weather.alert');
  assert.deepEqual(raised.map(event => [event.data.stationId, event.data.status]), [['harbor', 'raised']]);
  for (const seconds of [21, 64]) {
    const events = pair(seconds);
    assert.ok(!events.some(event => event.name === 'weather.alert'), 'no repeated warning or early all-clear');
    assert.equal(events.find(event => event.name === 'weather.reading' && event.data.stationId === 'harbor').data.condition, 'storm');
  }
  assert.deepEqual(pair(65).filter(event => event.name === 'weather.alert').map(event => [event.data.stationId, event.data.status]), [['hilltop', 'raised']]);
  for (const seconds of [66, 90, 109]) {
    const events = pair(seconds);
    assert.ok(!events.some(event => event.name === 'weather.alert'), 'warnings persist without repeated alerts');
    assert.ok(events.filter(event => event.name === 'weather.reading').every(event => event.data.condition === 'storm'));
  }
  const cleared = pair(110).filter(event => event.name === 'weather.alert');
  assert.deepEqual(cleared.map(event => [event.data.stationId, event.data.status]), [['harbor', 'cleared']]);
  const beforeHilltopClears = pair(154);
  assert.ok(!beforeHilltopClears.some(event => event.name === 'weather.alert'));
  assert.equal(beforeHilltopClears.find(event => event.name === 'weather.reading' && event.data.stationId === 'hilltop').data.condition, 'storm');
  assert.deepEqual(pair(155).filter(event => event.name === 'weather.alert').map(event => [event.data.stationId, event.data.status]), [['hilltop', 'cleared']]);
  const repeated = pair(180);
  assert.ok(repeated.every(event => event.data.daylight), 'a new weather cycle does not change daylight');
  assert.deepEqual(repeated.filter(event => event.name === 'weather.reading').map(event => event.data.condition), ['cloudy', 'clear']);
});

test('weather reference: automatic readings stay 10–20 seconds apart and alerts recur every 45 seconds', async t => {
  for (const intervalMs of [-1, 1, 9999, 2_147_483_648]) {
    await assert.rejects(startWeatherServer({port: 0, intervalMs}), /Interval must be/);
  }
  t.mock.timers.enable({apis: ['setTimeout', 'Date'], now: Date.UTC(2026, 9, 28, 12)});
  const started = Date.now();
  const emitted = [];
  const server = await startWeatherServer({port: 0, onEvent: event => emitted.push({at: Date.now() - started, event})});
  let closed = false;
  t.after(() => { if (!closed) return server.close(); });
  for (let seconds = 0; seconds < 200; seconds++) t.mock.timers.tick(1000);
  const readings = emitted.filter(item => item.event.name === 'weather.reading');
  const bursts = [...new Set(readings.map(item => item.at))];
  let previous = 0;
  for (const at of bursts) {
    assert.ok(at - previous >= 10_000 && at - previous <= 20_000);
    assert.ok(readings.filter(item => item.at === at).length <= 3);
    previous = at;
  }
  assert.ok(bursts.length >= 10);
  const alerts = emitted.filter(item => item.event.name === 'weather.alert');
  assert.deepEqual(alerts.map(item => [item.at, item.event.data.stationId, item.event.data.status]), [
    [20_000, 'harbor', 'raised'], [65_000, 'hilltop', 'raised'],
    [110_000, 'harbor', 'cleared'], [155_000, 'hilltop', 'cleared'], [200_000, 'harbor', 'raised'],
  ]);
  await server.close();
  closed = true;
  const before = emitted.length;
  t.mock.timers.tick(60_000);
  assert.equal(emitted.length, before, 'shutdown stops readings and alerts');
});

test('weather reference: streams deliver bursts before heartbeat, filter, replay, and stop independently', async t => {
  const {server, client, setTime} = await setup(t, {heartbeatMs: 30_000, historyLimit: 2});
  const readings = []; const changes = []; const alerts = [];
  const watch = (name, target, arguments_ = {}, cursor) => client.watch({name, mode: 'push', arguments: arguments_, cursor}, item => {
    if (item.type === 'event') target.push(item.event);
  });
  const reading = watch('weather.reading', readings);
  const change = watch('weather.changed', changes);
  const alert = watch('weather.alert', alerts, {stationId: 'harbor'});
  await Promise.all([reading.ready, change.ready, alert.ready]);
  for (const seconds of [0, 5, 20, 35, 50, 65, 110]) {
    setTime(seconds); server.advance(); server.advance();
  }
  await until(() => readings.length === 14 && changes.length === 6 && alerts.length === 2);
  assert.deepEqual(alerts.map(event => event.data.status), ['raised', 'cleared']);
  assert.ok(alerts.every(event => event.data.stationId === 'harbor'));
  assert.deepEqual(changes.slice(0, 2).map(event => [event.data.stationId, event.data.previousCondition, event.data.condition]), [
    ['harbor', 'cloudy', 'rain'], ['harbor', 'rain', 'storm'],
  ]);
  assert.ok(readings.every(event => event.data.daylight));
  const cursor = reading.cursor;
  await reading.stop();
  assert.equal(reading.status, 'stopped');
  server.advance();
  const replayed = [];
  const resumed = watch('weather.reading', replayed, {}, cursor);
  await resumed.ready;
  await until(() => replayed.length === 1);
  assert.equal(replayed[0].data.stationId, 'harbor');
  assert.equal(replayed[0].data.condition, 'rain');
  server.advance();
  for (const seconds of [140, 155, 170, 175]) {
    setTime(seconds); server.advance(); server.advance();
  }
  await until(() => changes.length === 11 && replayed.length === 10);
  assert.equal(alerts.length, 2, 'Hilltop warnings do not reach the Harbor watch');
  assert.equal(readings.length, 14);
});

test('weather reference: quiet streams advance their cursor through heartbeats', async t => {
  const {server, client} = await setup(t);
  const events = [];
  const watch = client.watch({name: 'weather.alert', mode: 'push', arguments: {}}, item => {
    if (item.type === 'event') events.push(item.event);
  });
  await watch.ready;
  const initial = watch.cursor;
  server.advance();
  await until(() => watch.cursor !== initial);
  assert.deepEqual(events, []);
  await watch.stop();
});

for (const ephemeral of [false, true]) {
  test(`weather reference: prompt precedes subscriptions and survives activation in installed Pi (${ephemeral ? '--no-session' : 'saved session'})`, {timeout: 30_000}, async t => {
    let seconds = 0;
    const server = await startWeatherServer({port: 0, intervalMs: 0, now: () => Date.UTC(2026, 9, 28, 12) + seconds * 1000});
    const directory = mkdtempSync(join(tmpdir(), 'pi-weather-'));
    const agent = join(directory, 'agent'); mkdirSync(agent);
    writeFileSync(join(agent, 'settings.json'), JSON.stringify({packages: [root]}));
    const config = join(directory, '.pi', 'mcp-events.json');
    const eventConfig = {servers: {weather: {transport: 'http', url: server.url}}, watches: [
      {id: 'readings', server: 'weather', name: 'weather.reading', mode: 'push'},
      {id: 'alerts', server: 'weather', name: 'weather.alert', mode: 'poll'},
    ]};
    const contexts = join(directory, 'contexts.jsonl');
    const rpc = new RpcClient({cliPath, cwd: directory, provider: 'events-test', model: 'local',
      env: {PI_CODING_AGENT_DIR: agent,
        EVENTS_TEST_CONTEXTS: contexts, EVENTS_TEST_OBSERVATIONS: join(directory, 'observations.jsonl')},
      args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '-e', modelPath,
        ...(ephemeral ? ['--no-session'] : ['--session-dir', join(directory, 'sessions')])],
    });
    t.after(async () => { await rpc.stop(); await server.close(); rmSync(directory, {recursive: true, force: true}); });
    await rpc.start();
    const presenter = "You are the island's radio presenter. Announce storm warnings and all-clears. Wait for the weather feed.";
    await rpc.prompt(presenter);
    await until(async () => (await rpc.getMessages()).some(message => message.role === 'assistant'));
    assert.equal((await rpc.getMessages()).some(message => message.customType === 'mcp-events'), false);
    const shell = await rpc.bash(`mkdir -p .pi && cat > .pi/mcp-events.json <<'JSON'
${JSON.stringify(eventConfig, null, 2)}
JSON`);
    assert.equal(shell.exitCode, 0, shell.output);
    assert.deepEqual(JSON.parse(readFileSync(config, 'utf8')), eventConfig);
    await rpc.prompt('/test-reload');
    const watchesReady = async () => {
      await rpc.prompt('/mcp-events status');
      return (await rpc.getMessages()).some(message => message.customType === 'mcp-events-status'
        && message.content.includes('readings: active') && message.content.includes('alerts: active'));
    };
    await until(watchesReady);
    for (const time of [0, 20, 110]) { seconds = time; server.advance(); server.advance(); }
    await until(async () => {
      const events = (await rpc.getMessages()).filter(message => message.customType === 'mcp-events')
        .flatMap(message => message.details.events).filter(item => item.observation.type === 'event');
      const alerts = events.filter(item => item.watch === 'alerts').map(item => item.observation.event.data);
      return events.filter(item => item.watch === 'readings').length === 6
        && alerts.some(event => event.status === 'raised') && alerts.some(event => event.status === 'cleared');
    });
    await until(() => existsSync(contexts) && readFileSync(contexts, 'utf8').trim().split('\n').some(line => {
      const context = JSON.parse(line);
      return JSON.stringify(context.messages).includes(presenter) && JSON.stringify(context.messages).includes('weather.reading');
    }));
  });
}
