import assert from 'node:assert/strict';
import {once} from 'node:events';
import {execFileSync} from 'node:child_process';
import {existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {cliPath, modelPath, root, RpcClient} from './fixtures/installed-package.mjs';
import {startFixture} from './fixtures/events-server.mjs';

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
async function watchStatus(rpc) {
  const statuses = async () => (await rpc.getMessages()).filter(message => message.customType === 'mcp-events-status' && message.content.startsWith('MCP Events:'));
  const before = (await statuses()).length;
  await rpc.prompt('/mcp-events status');
  await until(async () => (await statuses()).length > before, 'fresh watch status');
  return (await statuses()).at(-1).content;
}
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
async function setup(t, {ephemeral = true, override = true, blocked = false, catalogUpdates = false, extraEvents = [], noExpiryWebhooks = false, configure = config => config} = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'pi-events-cli-'));
  const agentDir = join(directory, 'agent'); mkdirSync(agentDir);
  execFileSync(process.execPath, [cliPath, 'install', root], {
    cwd: directory, env: {...process.env, PI_CODING_AGENT_DIR: agentDir}, stdio: 'pipe', timeout: 30_000,
  });
  const key = join(directory, 'key.pem'); const cert = join(directory, 'cert.pem');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'], {stdio: 'ignore'});
  const fixture = await startFixture({key: readFileSync(key), cert: readFileSync(cert)}, {catalogUpdates, extraEvents, noExpiryWebhooks});
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
      args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '--tools', 'read',
        '-e', modelPath,
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
  const crash = async () => {
    const child = rpc.process;
    const exited = once(child, 'exit');
    child.kill('SIGKILL');
    await exited;
    rpc = undefined;
    await until(() => fixture.stats().streams === 0, 'crash closes streams');
  };
  return {directory, agentDir, stateDir, fixture, start, crash, emit, received, accepted, hold, shutdown, contexts, release};
}

test('unsupported event schemas leave other watches working and can be retried after repair', {timeout: 45_000}, async t => {
  const unavailable = {name: 'example.experimental', delivery: ['poll'], inputSchema: true,
    payloadSchema: {$schema: 'https://private-test-schema.invalid/secret'}};
  const h = await setup(t, {catalogUpdates: true, extraEvents: [unavailable], configure: config => ({...config, watches: [...config.watches,
    {id: 'experimental', server: 'local', name: unavailable.name, mode: 'poll', arguments: {channel: 'experimental'}},
  ]})});
  const rpc = await h.start();
  const status = await watchStatus(rpc);
  assert.ok(status.includes('experimental: unsupported schema'));
  assert.ok(status.includes('Unavailable event: local / example.experimental'));
  assert.ok(status.includes('3 running watch(es)'));
  const messages = JSON.stringify(await rpc.getMessages());
  assert.ok(messages.includes('event schemas are unsupported or invalid'));
  assert.ok(!messages.includes('private-test-schema'));
  assert.ok(!existsSync(h.contexts), 'schema diagnostic does not start a model turn');
  await h.emit(700); await h.received(700);
  h.fixture.setExtraEvents([{...unavailable, payloadSchema: true}]);
  await rpc.prompt('/mcp-events start experimental');
  await until(async () => (await watchStatus(rpc)).includes('experimental: active'), 'repaired schema permits the watch to start');
  const repaired = await watchStatus(rpc);
  assert.ok(!repaired.includes('Unavailable event:'));
  assert.ok(repaired.includes('4 running watch(es)'));
  await h.emit(701); await h.received(701);
  h.fixture.setExtraEvents([unavailable]);
  await until(async () => (await watchStatus(rpc)).includes('experimental: unsupported schema'), 'only the affected watch stops after a schema change');
  const stopped = await watchStatus(rpc);
  assert.ok(stopped.includes('3 running watch(es)'));
  assert.equal(h.fixture.stats().catalogs, 1, 'catalog monitoring survives the unsupported schema');
  assert.ok(JSON.stringify(await rpc.getMessages()).includes('stopped because its event schemas are unsupported or invalid'));
  await h.emit(702); await h.received(702);
});

test('catalog monitoring failures warn without a model turn and keep watches working', {timeout: 45_000}, async t => {
  const h = await setup(t, {catalogUpdates: true});
  const rpc = await h.start();
  await until(() => h.fixture.stats().catalogs === 1, 'catalog monitoring active');
  h.fixture.failCatalogUpdates();
  const warning = 'MCP Events catalog updates stopped for local. Watch definitions may be outdated. Use /reload to reconnect.';
  await until(async () => (await rpc.getMessages()).some(message => message.content === warning), 'catalog failure warning');
  const content = await watchStatus(rpc);
  assert.ok(content.includes('Catalog updates stopped for local.'));
  assert.ok(content.includes('3 running watch(es)'));
  assert.ok(!JSON.stringify(await rpc.getMessages()).includes('private-test-detail'));
  await delay(200);
  assert.ok(!existsSync(h.contexts), 'catalog warning and status do not start a model turn');
  assert.equal((await rpc.getMessages()).filter(message => message.content === warning).length, 1);
  await h.emit(600); await h.received(600);
  await rpc.prompt('/mcp-events stop');
  assert.ok(!(await watchStatus(rpc)).includes('Catalog updates stopped for local.'));
  assert.equal((await rpc.getMessages()).filter(message => message.content === warning).length, 1, 'intentional shutdown adds no warning');
});

test('subscription status identifies each watch and retains stopped and failed watches', {timeout: 45_000}, async t => {
  const h = await setup(t, {configure: config => ({...config,
    servers: {...config.servers, unauthorized: {...config.servers.local, headers: {Authorization: 'Bearer private-test-credential'}}},
    watches: [...config.watches,
      {id: 'missing', server: 'local', name: 'example.missing', mode: 'poll', arguments: {secret: 'private-test-argument'}},
      {id: 'inaccessible', server: 'unauthorized', name: 'example.changed', mode: 'poll'},
    ],
  })});
  const rpc = await h.start();
  const status = () => watchStatus(rpc);
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
  await rpc.prompt('/mcp-events start push');
  await until(async () => (await status())?.includes('push: active'), 'failed watch starts again and clears its failure');
  await rpc.prompt('/mcp-events start missing');
  assert.ok((await status()).includes('missing: could not start'));
  assert.ok((await status()).includes('2 running watch(es)'));
  await h.shutdown();
});

test('watch control confirmations reach model context without starting extra turns, including while busy', {timeout: 45_000}, async t => {
  const h = await setup(t); const rpc = await h.start();
  const contexts = () => existsSync(h.contexts)
    ? readFileSync(h.contexts, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line)) : [];
  const messageText = message => typeof message.content === 'string'
    ? message.content : message.content.filter(part => part.type === 'text').map(part => part.text).join('\n');
  const commands = [
    ['stop push', 'MCP Events watch "push" stopped.'],
    ['start push', 'MCP Events watch "push" is starting.'],
    ['start push', 'MCP Events watch "push" is already running.'],
    ['stop', 'MCP Events subscriptions stopped.'],
  ];
  for (const [command, confirmation] of commands) {
    await rpc.prompt(`/mcp-events ${command}`);
    await until(async () => (await rpc.getMessages()).some(message => message.customType === 'mcp-events-status' && message.content === confirmation), confirmation);
  }
  await delay(200);
  assert.equal(contexts().length, 0, 'control confirmations do not trigger model turns');
  await rpc.prompt('CONTROL_CONTEXT_TEST');
  await until(() => contexts().some(context => JSON.stringify(context.messages.at(-1)?.content).includes('CONTROL_CONTEXT_TEST')), 'control confirmations reach model context');
  const context = contexts().findLast(context => JSON.stringify(context.messages.at(-1)?.content).includes('CONTROL_CONTEXT_TEST'));
  const positions = commands.map(([, confirmation]) => context.messages.findIndex(message => messageText(message).includes(confirmation)));
  assert.ok(positions.every(position => position >= 0));
  assert.deepEqual(positions, [...positions].sort((a, b) => a - b), 'control confirmations remain in command order');
  await h.hold();
  const before = contexts().length;
  await rpc.prompt('/mcp-events start push');
  assert.equal(contexts().length, before, 'a confirmation does not interrupt a busy model');
  writeFileSync(h.release, 'continue');
  await rpc.prompt('CONTROL_AFTER_BUSY', undefined, 'followUp');
  await until(() => contexts().some(context => JSON.stringify(context.messages.at(-1)?.content).includes('CONTROL_AFTER_BUSY')), 'busy confirmation reaches next model turn');
  const afterBusy = contexts().findLast(context => JSON.stringify(context.messages.at(-1)?.content).includes('CONTROL_AFTER_BUSY'));
  assert.equal(afterBusy.messages.filter(message => messageText(message).includes('MCP Events watch "push" is starting.')).length, 2);
  await h.shutdown();
});

for (const ephemeral of [true, false]) {
  test(`named starts resume watches without restarting others (${ephemeral ? '--no-session' : 'saved session'})`, {timeout: 60_000}, async t => {
    const h = await setup(t, {ephemeral}); const before = files(h.stateDir); const rpc = await h.start();
    const status = () => watchStatus(rpc);
    const delivered = async (watch, value) => until(async () => values(await rpc.getMessages()).some(([id, found]) => id === watch && found === value), `${watch} delivered ${value}`);
    const starts = () => h.fixture.requests.filter(request => ['events/stream', 'events/subscribe'].includes(request.method)).length;
    await h.emit(100); await h.received(100);
    const initial = starts();
    for (const command of ['start', 'start unknown', ...modes.map(mode => `start ${mode}`)]) await rpc.prompt(`/mcp-events ${command}`);
    assert.equal(starts(), initial, 'unknown, incomplete, and already-running starts leave watches unchanged');
    assert.ok((await status()).includes('3 running watch(es)'));
    for (const [index, mode] of modes.entries()) {
      const missed = 200 + index; const fresh = 300 + index;
      await rpc.prompt(`/mcp-events stop ${mode}`);
      const stopped = await status();
      assert.ok(stopped.includes(`${mode}: stopped`), stopped);
      await h.emit(missed);
      for (const other of modes.filter(candidate => candidate !== mode)) await delivered(other, missed);
      assert.ok(!values(await rpc.getMessages()).some(([id, value]) => id === mode && value === missed));
      const since = h.fixture.requests.length; const verifications = h.fixture.stats().verifications;
      await rpc.prompt(`/mcp-events start ${mode}`);
      await until(async () => (await status()).includes(`${mode}: active`), `${mode} active again`);
      const method = {poll: 'events/poll', push: 'events/stream', webhook: 'events/subscribe'}[mode];
      const resumed = h.fixture.requests.slice(since).find(request => request.method === method);
      assert.ok(resumed);
      if (mode === 'webhook') {
        assert.equal(resumed.params.cursor, null, 'emit-only source starts from now');
        assert.equal(h.fixture.stats().verifications, verifications + 1, 'new callback verifies before delivery');
        if (ephemeral) assert.notEqual(resumed.params.ttlMs, null, 'ephemeral webhook still requests finite expiry');
      } else {
        assert.notEqual(resumed.params.cursor, null, 'retained cursor resumes the stopped watch');
        await delivered(mode, missed);
      }
      await h.emit(fresh); await h.received(fresh);
      assert.equal(values(await rpc.getMessages()).filter(([id, value]) => id === mode && value === fresh).length, 1);
      if (mode === 'webhook') assert.ok(!values(await rpc.getMessages()).some(([id, value]) => id === mode && value === missed));
    }
    await rpc.prompt('/mcp-events stop');
    await until(() => h.fixture.stats().streams === 0 && h.fixture.stats().webhooks === 0, 'stop all closes remote subscriptions');
    const stopped = await status();
    assert.ok(stopped.includes('0 running watch(es)'));
    for (const mode of modes) assert.ok(stopped.includes(`${mode}: stopped`));
    const polls = h.fixture.requests.filter(request => request.method === 'events/poll').length;
    await delay(1100);
    assert.equal(h.fixture.requests.filter(request => request.method === 'events/poll').length, polls);
    await h.emit(400);
    const streams = h.fixture.requests.filter(request => request.method === 'events/stream').length;
    await Promise.all([rpc.prompt('/mcp-events start push'), rpc.prompt('/mcp-events start push')]);
    await delivered('push', 400);
    assert.equal(h.fixture.requests.filter(request => request.method === 'events/stream').length, streams + 1, 'concurrent named starts create one stream');
    assert.equal(h.fixture.requests.filter(request => request.method === 'events/poll').length, polls, 'other watches stay stopped');
    assert.equal(h.fixture.stats().webhooks, 0);
    await rpc.prompt('/mcp-events start poll'); await rpc.prompt('/mcp-events start webhook');
    await until(async () => (await status()).includes('webhook: active'), 'webhook receiver reopens after stop all');
    await h.emit(401); await h.received(401);
    await h.hold(); await h.emit(500); await h.accepted(500);
    await rpc.prompt('/mcp-events stop');
    writeFileSync(h.release, 'continue');
    await h.received(500);
    await ordinaryTool(rpc);
    if (ephemeral) {
      assert.equal((await rpc.getState()).sessionFile, undefined);
      assert.deepEqual(files(h.stateDir), before, 'named starts do not write event state in --no-session');
    }
    await h.shutdown();
  });
}

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

test('saved Pi session renews the same no-expiry webhook after a crash', {timeout: 60_000}, async t => {
  const h = await setup(t, {ephemeral: false, noExpiryWebhooks: true,
    configure: config => ({...config, client: {...config.client, webhookRefreshMs: 100}})});
  let rpc = await h.start();
  writeFileSync(h.release, 'continue'); await ordinaryTool(rpc);
  const sessionFile = (await rpc.getState()).sessionFile; assert.ok(sessionFile);
  const initial = h.fixture.requests.find(r => r.method === 'events/subscribe').params;
  assert.equal(initial.ttlMs, null);
  await h.crash();
  assert.equal(h.fixture.stats().webhooks, 1, 'remote no-expiry subscription survives the crash');
  const before = h.fixture.requests.filter(r => r.method === 'events/subscribe').length;
  rpc = await h.start(sessionFile);
  await until(() => h.fixture.requests.filter(r => r.method === 'events/subscribe').length > before, 'resumed webhook renewal');
  const resumed = h.fixture.requests.filter(r => r.method === 'events/subscribe').at(-1).params;
  assert.deepEqual(resumed.delivery, initial.delivery, 'callback URL and signing secret survive restart');
  assert.equal(resumed.ttlMs, null);
  assert.equal(h.fixture.stats().webhooks, 1, 'resuming does not create an orphan subscription');
  assert.equal(h.fixture.stats().verifications, 1, 'server reuses the original subscription');
  await h.emit(42); await h.received(42);
  await h.shutdown();
});

test('unusable state directory gives one conversation warning and leaves events and ordinary tools working', {timeout: 45_000}, async t => {
  const h = await setup(t, {ephemeral: false, blocked: true}); const rpc = await h.start();
  assert.ok(h.fixture.requests.filter(r => r.method === 'events/subscribe').every(r => r.params.ttlMs !== null), 'unavailable durable identity forces finite expiry');
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
