import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import {appendFileSync, existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync} from 'node:fs';
import {createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join, relative} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {pathToFileURL} from 'node:url';
import {z} from 'zod';
import {readCompleteJsonLines} from './fixtures/jsonl.mjs';
import {cliPath, modelPath, root, RpcClient} from './fixtures/installed-package.mjs';

const checkout = join(import.meta.dirname, '..');
const weatherCli = join(checkout, 'dev/weather-server.mjs');
const fixture = join(import.meta.dirname, 'fixtures/weather-stdio-server.mjs');
const {EventsClient, EventsWire, createTransport, parseConfig} = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const result = z.object({}).passthrough();
const names = ['weather.reading', 'weather.changed', 'weather.alert'];
const watches = ['poll', 'push'].flatMap(mode => names.map(name => ({
  id: `${mode}-${name}`, server: 'weather', name, mode, arguments: {stationId: 'harbor'},
})));
async function until(check, description) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) { if (await check()) return; await delay(25); }
  assert.fail(`Timed out waiting for ${description}`);
}
function alive(pid) {
  try { process.kill(pid, 0); return true; }
  catch (error) { if (error.code === 'ESRCH') return false; throw error; }
}
function setup(t, historyLimit = 1000) {
  const directory = realpathSync(mkdtempSync(join(tmpdir(), 'weather-stdio-')));
  const controls = join(directory, 'controls.jsonl');
  const receipts = join(directory, 'receipts.jsonl');
  writeFileSync(controls, ''); writeFileSync(receipts, '');
  const cleanup = [];
  t.after(async () => {
    for (const close of cleanup.reverse()) await close();
    for (const {pid} of readCompleteJsonLines(receipts).filter(item => item.type === 'started')) {
      if (alive(pid)) process.kill(pid, 'SIGKILL');
    }
    rmSync(directory, {recursive: true, force: true});
  });
  const server = {transport: 'stdio', command: process.execPath,
    args: [relative(directory, fixture), controls, receipts, String(historyLimit)], cwd: '..'};
  const configDirectory = join(directory, '.pi'); mkdirSync(configDirectory);
  const config = parseConfig({servers: {weather: server}, watches, client: {retryMs: 10, pollFloorMs: 50}});
  writeFileSync(join(configDirectory, 'mcp-events.json'), JSON.stringify(config));
  const started = () => readCompleteJsonLines(receipts).filter(item => item.type === 'started');
  const command = async value => {
    const id = randomUUID();
    appendFileSync(controls, `${JSON.stringify({...value, id})}\n`);
    let response;
    await until(() => { response = readCompleteJsonLines(receipts).find(item => item.id === id); return response; }, 'weather control response');
    return response.events;
  };
  return {directory, configDirectory, config, cleanup, started,
    advance: seconds => command({type: 'advance', seconds}), crash: () => command({type: 'crash'})};
}
function checkEvents(items, expected) {
  for (const watch of watches) {
    const ids = items.filter(item => item.watch === watch.id && item.observation.type === 'event').map(item => item.observation.event.eventId).sort();
    assert.deepEqual(ids, expected.filter(event => event.name === watch.name && event.data.stationId === 'harbor').map(event => event.eventId).sort(), watch.id);
    assert.equal(new Set(ids).size, ids.length, 'no duplicate event IDs within a watch');
  }
}

for (const ending of ['EOF before discovery', 'EOF after discovery', 'oversized request', 'SIGINT', 'SIGTERM']) {
  test(`weather stdio CLI: JSON stdout, no HTTP listener, ${ending}`, {timeout: 15_000}, async t => {
    const reservation = createServer();
    reservation.listen(0, '127.0.0.1'); await once(reservation, 'listening');
    t.after(() => new Promise(resolve => reservation.close(resolve)));
    const child = spawn(process.execPath, [weatherCli, '--transport', 'stdio', '--port', String(reservation.address().port)], {
      stdio: ['pipe', 'pipe', 'pipe'], env: {...process.env, WEATHER_WEBHOOK_TOKEN: 'unused-in-stdio', WEATHER_WEBHOOK_CA: '/missing/unused-certificate'},
    });
    t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
    const exited = once(child, 'exit');
    let stdout = ''; let stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    await until(() => stderr.includes('Island weather station: stdio'), 'stdio startup');
    if (ending !== 'EOF before discovery') {
      const meta = {'io.modelcontextprotocol/protocolVersion': '2026-07-28',
        'io.modelcontextprotocol/clientInfo': {name: 'weather-cli-test', version: '1'},
        'io.modelcontextprotocol/clientCapabilities': {extensions: {'io.modelcontextprotocol/events': {}}}};
      const request = (id, method, params = {}) => child.stdin.write(`${JSON.stringify({jsonrpc: '2.0', id, method, params: {...params, _meta: meta}})}\n`);
      request(1, 'server/discover');
      await until(() => stdout.includes('"id":1'), 'discovery response');
      request(2, 'events/list');
      await until(() => stdout.includes('"id":2'), 'catalog response');
      const catalog = stdout.trim().split('\n').map(line => JSON.parse(line)).find(message => message.id === 2);
      assert.deepEqual(catalog.result.events.map(event => event.delivery), names.map(() => ['poll', 'push']));
      request(3, 'events/subscribe', {name: 'weather.alert', arguments: {}, delivery: {mode: 'webhook', url: 'https://example.invalid', secret: 'unused'}});
      await until(() => stdout.includes('"id":3'), 'unsupported webhook response');
      assert.equal(stdout.trim().split('\n').map(line => JSON.parse(line)).find(message => message.id === 3).error.code, -32026);
    }
    if (ending.startsWith('EOF')) child.stdin.end();
    else if (ending === 'oversized request') child.stdin.write('x'.repeat(70_000));
    else child.kill(ending);
    const [code, signal] = await exited;
    assert.equal(code, 0, stderr); assert.equal(signal, null);
    for (const line of stdout.trim().split('\n').filter(Boolean)) assert.equal(JSON.parse(line).jsonrpc, '2.0');
    if (ending === 'EOF before discovery') assert.equal(stdout, '');
  });
}

test('weather stdio: all events, filtering, poll pages, replay, independent cancellation and restart gaps', {timeout: 30_000}, async t => {
  const h = setup(t);
  const wire = new EventsWire(() => createTransport(h.config.servers.weather, h.configDirectory));
  const client = new EventsClient(wire, h.config.client);
  h.cleanup.push(() => client.close());
  await client.connect();
  assert.deepEqual(client.list().map(event => event.delivery), names.map(() => ['poll', 'push']));
  assert.equal(h.started()[0].cwd, realpathSync(h.directory));
  assert.equal(h.started()[0].url, null);
  const observations = [];
  const running = watches.map(watch => client.watch(watch, observation => { observations.push({watch: watch.id, observation}); }));
  await Promise.all(running.map(watch => watch.ready));
  const initial = await wire.request('events/poll', {name: 'weather.reading', arguments: {stationId: 'harbor'}, cursor: null}, result);
  const expected = [];
  for (const seconds of [0, 5, 20, 65, 110, 155]) expected.push(...await h.advance(seconds));
  await until(() => watches.every(watch => observations.filter(item => item.watch === watch.id && item.observation.type === 'event').length
    === expected.filter(event => event.name === watch.name && event.data.stationId === 'harbor').length), 'all six watches');
  checkEvents(observations, expected);
  const page = await wire.request('events/poll', {name: 'weather.reading', arguments: {stationId: 'harbor'}, cursor: initial.cursor, maxEvents: 1}, result);
  const replay = await wire.request('events/poll', {name: 'weather.reading', arguments: {stationId: 'harbor'}, cursor: initial.cursor, maxEvents: 1}, result);
  assert.equal(page.hasMore, true);
  assert.equal(page.events.length, 1);
  assert.deepEqual(replay.events, page.events);
  const nextPage = await wire.request('events/poll', {name: 'weather.reading', arguments: {stationId: 'harbor'}, cursor: page.cursor, maxEvents: 1}, result);
  assert.notEqual(nextPage.events[0].eventId, page.events[0].eventId);
  const stoppedIndex = watches.findIndex(watch => watch.id === 'push-weather.reading');
  const stopped = running[stoppedIndex];
  await stopped.stop();
  const [missed] = await h.advance(160);
  await until(() => observations.some(item => item.watch === 'poll-weather.reading' && item.observation.type === 'event' && item.observation.event.eventId === missed.eventId), 'poll survives stopping push');
  assert.ok(!observations.some(item => item.watch === 'push-weather.reading' && item.observation.type === 'event' && item.observation.event.eventId === missed.eventId));
  running[stoppedIndex] = client.watch({...watches[stoppedIndex], cursor: stopped.cursor}, observation => { observations.push({watch: 'push-weather.reading', observation}); });
  await until(() => observations.some(item => item.watch === 'push-weather.reading' && item.observation.type === 'event' && item.observation.event.eventId === missed.eventId), 'push replay on restart');
  await h.crash();
  await until(() => h.started().length === 2 && watches.every(watch => observations.some(item => item.watch === watch.id && item.observation.type === 'gap')), 'reconnection and explicit gaps after history loss');
  const fresh = await h.advance(20);
  await until(() => watches.filter(watch => watch.name !== 'weather.changed').every(watch => observations.some(item => item.watch === watch.id
    && item.observation.type === 'event' && fresh.some(event => event.eventId === item.observation.event.eventId))), 'new events after child restart');
  await client.close();
  await until(() => h.started().every(({pid}) => !alive(pid)), 'stdio children exit');
});

for (const ephemeral of [false, true]) {
  test(`weather stdio in installed Pi: poll/push, busy delivery, controls and restart (${ephemeral ? '--no-session' : 'saved session'})`, {timeout: 45_000}, async t => {
    const h = setup(t);
    const agent = join(h.directory, 'agent'); mkdirSync(agent);
    writeFileSync(join(agent, 'settings.json'), JSON.stringify({packages: [root]}));
    const contexts = join(h.directory, 'contexts.jsonl');
    const accepted = join(h.directory, 'observations.jsonl');
    const release = join(h.directory, 'release');
    const createRpc = sessionFile => new RpcClient({cliPath, cwd: h.directory, provider: 'events-test', model: 'local', env: {
      PI_CODING_AGENT_DIR: agent, EVENTS_TEST_CONTEXTS: contexts, EVENTS_TEST_OBSERVATIONS: accepted, EVENTS_TEST_RELEASE: release,
    }, args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '-e', modelPath,
      ...(ephemeral ? ['--no-session'] : ['--session-dir', join(h.directory, 'sessions')]), ...(sessionFile ? ['--session', sessionFile] : [])]});
    let rpc = createRpc();
    h.cleanup.push(() => rpc.stop());
    const events = async () => (await rpc.getMessages()).filter(message => message.customType === 'mcp-events').flatMap(message => message.details.events);
    const status = async () => {
      await rpc.prompt('/mcp-events status');
      return (await rpc.getMessages()).filter(message => message.customType === 'mcp-events-status').at(-1)?.content ?? '';
    };
    const active = () => until(async () => {
      const current = await status(); return watches.every(watch => current.includes(`${watch.id}: active`));
    }, 'six active Pi watches');
    await rpc.start(); await active();
    await rpc.prompt('HOLD_TEST');
    await until(() => readCompleteJsonLines(contexts).some(context => JSON.stringify(context.messages).includes('HOLD_TEST')), 'busy model');
    const expected = [];
    for (const seconds of [0, 5, 20, 65, 110, 155]) expected.push(...await h.advance(seconds));
    await until(() => {
      const items = readCompleteJsonLines(accepted);
      return watches.every(watch => items.filter(item => item.watch === watch.id && item.observation.type === 'event').length
        === expected.filter(event => event.name === watch.name && event.data.stationId === 'harbor').length);
    }, 'all weather events accepted while Pi is busy');
    assert.equal((await events()).length, 0);
    writeFileSync(release, 'ready');
    await until(async () => (await events()).filter(item => item.observation.type === 'event').length === 2 * expected.filter(event => event.data.stationId === 'harbor').length, 'conversation handoff');
    checkEvents(await events(), expected);
    await until(() => readCompleteJsonLines(contexts).some(context => JSON.stringify(context.messages).includes('weather.alert')), 'weather events in model context');
    await rpc.prompt('/mcp-events stop push-weather.reading');
    const [missed] = await h.advance(160);
    await until(async () => (await events()).some(item => item.watch === 'poll-weather.reading' && item.observation.type === 'event' && item.observation.event.eventId === missed.eventId), 'poll continues independently');
    assert.ok(!(await events()).some(item => item.watch === 'push-weather.reading' && item.observation.type === 'event' && item.observation.event.eventId === missed.eventId));
    await rpc.prompt('/mcp-events start push-weather.reading');
    await until(async () => (await events()).some(item => item.watch === 'push-weather.reading' && item.observation.type === 'event' && item.observation.event.eventId === missed.eventId), 'named start replays missed reading');
    const sessionFile = (await rpc.getState()).sessionFile;
    assert.equal(sessionFile === undefined, ephemeral);
    await rpc.prompt('/test-shutdown'); await rpc.stop();
    await until(() => h.started().every(({pid}) => !alive(pid)), 'Pi shutdown closes weather child');
    rpc = createRpc(sessionFile);
    await rpc.start(); await active();
    if (!ephemeral) await until(async () => {
      const items = await events();
      return watches.every(watch => items.some(item => item.watch === watch.id && item.observation.type === 'gap'));
    }, 'saved cursors report gaps after the weather child restarts');
    const fresh = await h.advance(20);
    await until(async () => {
      const items = await events();
      return watches.filter(watch => watch.name !== 'weather.changed').every(watch => items.some(item => item.watch === watch.id && item.observation.type === 'event'
        && fresh.some(event => event.eventId === item.observation.event.eventId)));
    }, 'fresh events after Pi restart');
    await rpc.prompt('/mcp-events stop');
    await until(() => h.started().every(({pid}) => !alive(pid)), 'stop all closes weather child');
    if (ephemeral) assert.equal(existsSync(join(agent, 'mcp-events')), false);
  });
}
