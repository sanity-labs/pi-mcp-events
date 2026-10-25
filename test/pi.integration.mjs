import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {RpcClient} from '@earendil-works/pi-coding-agent';
import {startFixture} from './fixtures/events-server.mjs';

const root = resolve(import.meta.dirname, '..');
const cliPath = join(dirname(fileURLToPath(import.meta.resolve('@earendil-works/pi-coding-agent'))), 'bundle/cli.js');
const modes = ['poll', 'push', 'webhook'];
async function until(check, description) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(25);
  }
  assert.fail(`Timed out: ${description}`);
}
const batches = messages => messages.filter(message => message.role === 'custom' && message.customType === 'mcp-events');
async function ordinaryTool(rpc) {
  await rpc.prompt('READ_TEST', undefined, 'followUp');
  await until(async () => (await rpc.getMessages()).some(message => message.role === 'toolResult' && message.toolName === 'read' && !message.isError && JSON.stringify(message.content).includes('ordinary tool use still works')), 'ordinary tool result');
}
const observations = messages => batches(messages).flatMap(message => message.details.events);
const values = messages => observations(messages).filter(item => item.observation.type === 'event').map(item => [item.watch, item.observation.event.data.value]);
function files(path) {
  if (!existsSync(path)) return {};
  return Object.fromEntries(readdirSync(path, {recursive: true, withFileTypes: true}).filter(entry => entry.isFile()).map(entry => {
    const full = join(entry.parentPath, entry.name);
    return [full.slice(path.length), readFileSync(full, 'utf8')];
  }));
}
async function setup(t, {ephemeral = true, override = true, blocked = false, configure = config => config} = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'pi-events-cli-'));
  const agentDir = join(directory, 'agent'); mkdirSync(agentDir);
  const key = join(directory, 'key.pem'); const cert = join(directory, 'cert.pem');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'], {stdio: 'ignore'});
  const fixture = await startFixture({key: readFileSync(key), cert: readFileSync(cert)});
  const stateDir = override ? join(directory, 'custom-state') : join(agentDir, 'mcp-events');
  if (blocked) writeFileSync(stateDir, 'not a directory');
  else { mkdirSync(stateDir); if (ephemeral) writeFileSync(join(stateDir, 'saved-state.json'), 'preserve existing saved-session state'); }
  const configPath = join(directory, 'events.json');
  const {url, token, callbackOrigin, callbackPort} = fixture.connection;
  writeFileSync(configPath, JSON.stringify(configure({
    ...(override ? {stateDir: 'custom-state'} : {}),
    client: {pollFloorMs: 20, retryMs: 10, webhookUnsubscribeTimeoutMs: 500},
    servers: {local: {transport: 'http', url, headers: {Authorization: `Bearer ${token}`}}},
    webhook: {publicUrl: `${callbackOrigin}/hooks`, host: '127.0.0.1', port: callbackPort},
    watches: modes.map(mode => ({id: mode, server: 'local', name: mode === 'webhook' ? 'example.live_changed' : 'example.changed', mode, arguments: {channel: mode}, ttlMs: null})),
  })));
  const contexts = join(directory, 'contexts.jsonl'); const acceptedPath = join(directory, 'accepted.jsonl'); const release = join(directory, 'release');
  writeFileSync(join(directory, 'ordinary.txt'), 'ordinary tool use still works');
  let rpc;
  const start = async sessionFile => {
    rpc = new RpcClient({cliPath, cwd: directory, provider: 'events-test', model: 'local',
      env: {PI_CODING_AGENT_DIR: agentDir, PI_MCP_EVENTS_CONFIG: configPath, EVENTS_TEST_CONTEXTS: contexts, EVENTS_TEST_OBSERVATIONS: acceptedPath, EVENTS_TEST_RELEASE: release},
      args: ['--offline', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-context-files', '--tools', 'read',
        '-e', root, '-e', join(root, 'test/fixtures/pi-model.mjs'),
        ...(ephemeral ? ['--no-session'] : ['--session-dir', join(directory, 'sessions')]), ...(sessionFile ? ['--session', sessionFile] : [])],
    });
    await rpc.start();
    await rpc.getState();
    await until(() => fixture.stats().streams === 1 && fixture.stats().webhooks === 1 && fixture.requests.some(r => r.method === 'events/poll'), 'all subscriptions active');
    return rpc;
  };
  t.after(async () => { await rpc?.stop(); await fixture.close(); rmSync(directory, {recursive: true, force: true}); });
  const emit = async value => {
    for (const channel of modes) {
      const response = await fetch(`${fixture.connection.baseUrl}/emit`, {method: 'POST', headers: {'content-type': 'application/json', authorization: `Bearer ${token}`}, body: JSON.stringify({channel, value})});
      assert.equal(response.status, 200);
    }
  };
  const received = async value => until(async () => {
    const delivered = values(await rpc.getMessages());
    return modes.every(mode => delivered.some(([watch, found]) => watch === mode && found === value));
  }, `all modes delivered ${value}`);
  const accepted = value => until(() => {
    if (!existsSync(acceptedPath)) return false;
    const items = readFileSync(acceptedPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
    return modes.every(mode => items.some(item => item.watch === mode && item.observation.type === 'event' && item.observation.event.data.value === value));
  }, `all modes accepted ${value}`);
  const hold = async () => {
    rmSync(release, {force: true});
    const before = existsSync(contexts) ? readFileSync(contexts, 'utf8').length : 0;
    await rpc.prompt('HOLD_TEST', undefined, 'followUp');
    await until(() => existsSync(contexts) && readFileSync(contexts, 'utf8').slice(before).includes('HOLD_TEST'), 'held model call');
  };
  const shutdown = async () => {
    await rpc.prompt('/test-shutdown');
    await until(() => fixture.stats().streams === 0 && fixture.stats().webhooks === 0, 'shutdown removes subscriptions');
    await rpc.stop(); rpc = undefined;
    const count = fixture.requests.filter(r => r.method === 'events/poll').length;
    await delay(1100);
    assert.equal(fixture.requests.filter(r => r.method === 'events/poll').length, count, 'polling stops');
  };
  return {directory, agentDir, stateDir, fixture, start, emit, received, accepted, hold, shutdown, contexts, release};
}

test('subscription status identifies each watch and retains stopped and failed watches', {timeout: 45_000}, async t => {
  const h = await setup(t, {configure: config => ({...config,
    servers: {...config.servers, unauthorized: {...config.servers.local, headers: {Authorization: 'Bearer private-test-credential'}}},
    watches: [...config.watches,
      {id: 'missing', server: 'local', name: 'example.missing', mode: 'poll', arguments: {secret: 'private-test-argument'}},
      {id: 'inaccessible', server: 'unauthorized', name: 'example.changed', mode: 'poll'},
    ],
  })});
  const rpc = await h.start();
  const status = async () => {
    await rpc.prompt('/mcp-events status');
    const messages = await rpc.getMessages();
    return messages.filter(message => message.customType === 'mcp-events-status').at(-1)?.content;
  };
  await until(async () => (await status())?.includes('webhook: active'), 'all watches ready for inspection');
  let content = await status();
  for (const mode of modes) {
    const name = mode === 'webhook' ? 'example.live_changed' : 'example.changed';
    assert.ok(content.includes(`${mode}: active — local / ${name} (${mode})`));
  }
  assert.ok(content.includes('3 running watch(es)'));
  assert.ok(content.includes('missing: could not start'));
  assert.ok(content.includes('inaccessible: connection failed'));
  assert.ok(content.includes('memory only'));
  for (const secret of ['private-test-credential', 'private-test-argument', h.fixture.connection.token]) assert.ok(!content.includes(secret));
  await rpc.prompt('/mcp-events stop poll');
  content = await status();
  assert.ok(content.includes('poll: stopped'));
  assert.ok(content.includes('2 running watch(es)'));
  h.fixture.terminateStreams();
  await until(async () => (await status())?.includes('push: failed'), 'terminal failure remains visible');
  content = await status();
  assert.ok(content.includes('webhook: active'));
  assert.ok(content.includes('1 running watch(es)'));
  await h.shutdown();
});

for (const override of [false, true]) {
  test(`stock Pi --no-session: all transports, busy delivery, reconnect, fresh sessions, cleanup (${override ? 'stateDir' : 'Pi agent directory'})`, {timeout: 60_000}, async t => {
    const h = await setup(t, {override}); const before = files(h.stateDir);
    let rpc = await h.start();
    assert.equal((await rpc.getState()).sessionFile, undefined);
    await rpc.prompt('HOLD_TEST');
    await until(() => existsSync(h.contexts), 'model is busy');
    await h.emit(1);
    await delay(1200);
    assert.equal(batches(await rpc.getMessages()).length, 0, 'busy conversation is not interrupted');
    writeFileSync(h.release, 'continue');
    await h.received(1);
    await h.emit(2); await h.received(2);
    const lastStream = h.fixture.requests.filter(r => r.method === 'events/stream').length;
    h.fixture.disconnect();
    await until(() => h.fixture.requests.filter(r => r.method === 'events/stream').length > lastStream, 'stream reconnects');
    const resumed = h.fixture.requests.filter(r => r.method === 'events/stream').at(-1);
    assert.notEqual(resumed.params.cursor, null, 'active-session cursor survives reconnect');
    await h.emit(3); await h.received(3);
    for (const mode of modes) for (const value of [1, 2, 3]) {
      assert.equal(values(await rpc.getMessages()).filter(([watch, found]) => watch === mode && found === value).length, 1);
    }
    const contextText = readFileSync(h.contexts, 'utf8');
    for (const mode of modes) assert.ok(contextText.includes(mode), `${mode} reaches model context`);
    assert.ok(!contextText.includes(h.fixture.connection.token)); assert.ok(!contextText.includes('whsec_'));
    assert.ok(h.fixture.requests.filter(r => r.method === 'events/subscribe').every(r => r.params.ttlMs !== null), 'ephemeral webhook requests finite/default expiry');
    assert.deepEqual(files(h.stateDir), before);
    assert.ok(!existsSync(join(h.agentDir, 'sessions')));
    await h.hold(); await h.emit(99); await h.accepted(99);
    assert.ok(!values(await rpc.getMessages()).some(([, value]) => value === 99));
    const since = h.fixture.requests.length;
    await rpc.newSession();
    writeFileSync(h.release, 'continue');
    await until(() => h.fixture.requests.slice(since).some(r => r.method === 'events/stream'), 'replacement session subscribes');
    assert.equal(h.fixture.requests.slice(since).find(r => r.method === 'events/stream').params.cursor, null);
    assert.equal(batches(await rpc.getMessages()).length, 0);
    await h.emit(4); await h.received(4);
    assert.ok(!values(await rpc.getMessages()).some(([, value]) => value === 99), 'pending events expire with the replaced ephemeral session');
    await rpc.prompt('/test-reload');
    await until(() => h.fixture.stats().streams === 1 && h.fixture.stats().webhooks === 1, 'reload reopens subscriptions');
    await h.emit(5); await h.received(5);
    await h.shutdown(); await h.emit(6);
    rpc = await h.start();
    await h.emit(7); await h.received(7);
    assert.ok(values(await rpc.getMessages()).every(([, value]) => value === 7), 'new ephemeral process starts from now');
    await ordinaryTool(rpc);
    assert.ok((await rpc.getMessages()).some(message => message.role === 'toolResult' && message.toolName === 'read' && JSON.stringify(message.content).includes('ordinary tool use still works')));
    assert.deepEqual(files(h.stateDir), before);
    await h.shutdown();
  });
}

test('saved Pi session restores cursors and pending events after resume', {timeout: 60_000}, async t => {
  const h = await setup(t, {ephemeral: false, override: false}); let rpc = await h.start();
  writeFileSync(h.release, 'continue'); await ordinaryTool(rpc);
  await h.hold(); await h.emit(10); await h.accepted(10);
  assert.equal(batches(await rpc.getMessages()).length, 0, 'accepted batch remains queued while busy');
  const sessionFile = (await rpc.getState()).sessionFile; assert.ok(sessionFile);
  assert.ok(Object.keys(files(h.stateDir)).length > 0, 'Pi agent directory holds event state');
  await h.shutdown(); await h.emit(11);
  writeFileSync(h.release, 'continue'); rpc = await h.start(sessionFile);
  await h.received(10);
  await until(async () => {
    const delivered = values(await rpc.getMessages());
    return ['poll', 'push'].every(mode => delivered.some(([watch, value]) => watch === mode && value === 11));
  }, 'replay-capable transports resume');
  await h.emit(12); await h.received(12);
  await h.shutdown();
});

test('unusable state directory gives one conversation warning and leaves events and ordinary tools working', {timeout: 45_000}, async t => {
  const h = await setup(t, {ephemeral: false, blocked: true}); const rpc = await h.start();
  writeFileSync(h.release, 'continue');
  await until(async () => (await rpc.getMessages()).some(message => message.customType === 'mcp-events-status' && message.details?.code === 'PERSISTENCE_UNAVAILABLE'), 'conversation persistence warning');
  await h.emit(20); await h.received(20); await h.emit(21); await h.received(21);
  await ordinaryTool(rpc);
  const messages = await rpc.getMessages();
  assert.equal(messages.filter(message => message.details?.code === 'PERSISTENCE_UNAVAILABLE').length, 1);
  assert.ok(messages.some(message => message.role === 'toolResult' && !message.isError));
  assert.equal(readFileSync(h.stateDir, 'utf8'), 'not a directory');
  assert.ok((await rpc.getState()).sessionFile, 'Pi conversation persistence is unchanged');
  await h.shutdown();
});

test('write failure switches an active saved session to memory and warns without stopping delivery', {timeout: 45_000}, async t => {
  const h = await setup(t, {ephemeral: false}); const rpc = await h.start();
  writeFileSync(h.release, 'continue'); await h.emit(30); await h.received(30);
  renameSync(h.stateDir, `${h.stateDir}-saved`); writeFileSync(h.stateDir, 'storage unavailable');
  await h.emit(31); await h.received(31); await h.emit(32); await h.received(32);
  const messages = await rpc.getMessages();
  assert.equal(messages.filter(message => message.details?.code === 'PERSISTENCE_UNAVAILABLE').length, 1);
  assert.equal(readFileSync(h.stateDir, 'utf8'), 'storage unavailable');
  await ordinaryTool(rpc);
  assert.ok((await rpc.getMessages()).some(message => message.role === 'toolResult' && !message.isError));
  await h.shutdown();
});
