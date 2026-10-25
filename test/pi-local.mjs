import assert from 'node:assert/strict';
import {existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve, join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createAgentSession, createEventBus, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager} from '@earendil-works/pi-coding-agent';
import {fauxAssistantMessage, fauxProvider} from '@earendil-works/pi-ai';

const connection = JSON.parse(readFileSync(process.env.LOCAL_EVENTS_CONNECTION_FILE, 'utf8'));
const ephemeral = process.argv.includes('--no-session');
assert.equal(new URL(connection.url).hostname, '127.0.0.1');
const extensionPath = resolve(process.env.LOCAL_EVENTS_EXTENSION_PATH ?? '.');
const tlsDir = resolve(process.env.LOCAL_EVENTS_TLS_DIR ?? '.local/tls');
const directory = mkdtempSync(join(tmpdir(), 'pi-events-'));
const agentDir = join(directory, 'agent');
const stateDir = join(directory, 'state');
mkdirSync(agentDir);
const configPath = join(directory, 'events.json');
const modes = ['poll', 'push', 'webhook'];
const channels = Object.fromEntries(modes.map(mode => [mode, `${directory}:${mode}`]));
writeFileSync(configPath, JSON.stringify({
  stateDir,
  servers: {mellon: {transport: 'http', url: connection.url, headers: {Authorization: `Bearer ${connection.token}`}}},
  webhook: {publicUrl: `${connection.callbackOrigin}/hooks`, host: '127.0.0.1', port: connection.callbackPort ?? Number(new URL(connection.callbackOrigin).port), ...(connection.callbackTls === false ? {} : {keyFile: join(tlsDir, 'localhost.key'), certFile: join(tlsDir, 'localhost.crt')})},
  watches: modes.map(mode => ({id: mode, server: 'mellon', name: mode === 'webhook' ? (connection.webhookEventName ?? 'example.changed') : 'example.changed', arguments: {channel: channels[mode]}, mode, ...(ephemeral ? {ttlMs: null} : {})})),
}), {mode: 0o600});
const previousConfig = process.env.PI_MCP_EVENTS_CONFIG;
process.env.PI_MCP_EVENTS_CONFIG = configPath;
const errors = [];
let session;
let release;
const gate = new Promise(resolve => {release = resolve;});
async function until(check, description) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(20);
  }
  throw new Error(`Timed out: ${description}`);
}
async function status() {
  const response = await fetch(`${connection.baseUrl}/status`, {headers: {Authorization: `Bearer ${connection.token}`}});
  assert.ok(response.ok);
  return response.json();
}
function readInbox() {
  return readdirSync(stateDir).filter(name => name.endsWith('.json') && !name.endsWith('.cursors.json')).flatMap(name => JSON.parse(readFileSync(join(stateDir, name), 'utf8')));
}
async function emitAll(value) {
  for (const mode of modes) {
    const response = await fetch(`${connection.baseUrl}/emit`, {method: 'POST', headers: {'Content-Type': 'application/json', Authorization: `Bearer ${connection.token}`}, body: JSON.stringify({channel: channels[mode], value})});
    assert.ok(response.ok);
  }
}
function reachedModel(contexts, value, expectedModes = modes) {
  const text = JSON.stringify(contexts);
  return expectedModes.every(mode => text.includes(JSON.stringify({channel: channels[mode], value}).replaceAll('"', '\\"')));
}
function assertEphemeralStorage(manager) {
  assert.equal(manager.getSessionFile(), undefined);
  assert.ok(!existsSync(stateDir), 'Ephemeral events must not create inbox or checkpoint files');
  assert.ok(!existsSync(join(directory, 'sessions')), 'Ephemeral Pi must not create transcript files');
}
try {
  const settingsManager = SettingsManager.inMemory();
  const eventBus = createEventBus();
  const observations = [];
  eventBus.on('mcp-events:observation', item => observations.push(item));
  const resourceLoader = new DefaultResourceLoader({cwd: directory, agentDir, settingsManager, eventBus,
    noExtensions: true, additionalExtensionPaths: [extensionPath], disabledBuiltinExtensions: ['mcp'],
    noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
  });
  await resourceLoader.reload();
  const runtime = await ModelRuntime.create({authPath: join(agentDir, 'auth.json'), modelsPath: null, allowModelNetwork: false, refreshOnCreate: false});
  const faux = fauxProvider({provider: 'events-test', api: 'events-test', models: [{id: 'deterministic'}], tokensPerSecond: 100000});
  runtime.registerNativeProvider(faux.provider);
  const contexts = [];
  faux.setResponses([
    async context => {contexts.push(context); await gate; return fauxAssistantMessage('Ready.');},
    context => {contexts.push(context); return fauxAssistantMessage('Events received.');},
  ]);
  const sessionManager = ephemeral ? SessionManager.inMemory(directory) : SessionManager.create(directory, join(directory, 'sessions'));
  const created = await createAgentSession({cwd: directory, agentDir, resourceLoader, settingsManager, modelRuntime: runtime, model: faux.getModel(), sessionManager, noTools: 'all'});
  session = created.session;
  session.subscribe(event => {
    if (!ephemeral && event.type === 'message_end' && event.message.role === 'custom' && event.message.customType === 'mcp-events') {
      assert.ok(readInbox().length > 0, 'message_end is emitted before Pi persists the batch; the durable inbox must still retain it');
    }
  });
  assert.deepEqual(created.extensionsResult.errors, []);
  assert.equal(created.extensionsResult.extensions.length, 1, 'Only the packaged Events extension is loaded');
  await session.bindExtensions({onError: error => errors.push(error)});
  await until(async () => {const current = await status(); return current.streams === 1 && current.webhooks === 1;}, 'server listeners ready');
  const prompt = session.prompt('Wait while the integration test sends events.');
  await until(() => faux.state.callCount === 1, 'first model request');
  await emitAll(42);
  await until(() => modes.every(mode => observations.some(item => item.watch === mode && item.observation.type === 'event' && item.observation.event.data.value === 42)), 'all three modes accepted while Pi is busy');
  if (ephemeral) assertEphemeralStorage(sessionManager);
  else assert.equal(readInbox().filter(item => item.observation.type === 'event').length, 3);
  assert.equal(faux.state.callCount, 1, 'Events must not start a concurrent agent turn');
  release();
  await prompt;
  await until(() => contexts.length >= 2 && !session.isStreaming, 'event follow-up turn');
  const contextText = JSON.stringify(contexts[1]);
  for (const mode of modes) assert.ok(contextText.includes(channels[mode]), `${mode} reached the model`);
  assert.ok(!contextText.includes(connection.token), 'MCP credentials stay out of model context');
  assert.ok(!contextText.includes('whsec_'), 'Webhook signing secret stays out of model context');
  if (ephemeral) {
    faux.appendResponses(Array.from({length: 6}, () => context => {contexts.push(context); return fauxAssistantMessage('More events received.');}));
    await emitAll(43);
    await until(() => reachedModel(contexts, 43) && !session.isStreaming, 'successive ephemeral batches reach the model');
    assertEphemeralStorage(sessionManager);
  } else {
    await until(() => readInbox().length === 0, 'Pi message acknowledgement clears the durable inbox');
  }
  assert.deepEqual(errors, []);
  await session.extensionRunner.emit({type: 'session_shutdown', reason: 'quit'});
  await until(async () => {const current = await status(); return current.streams === 0 && current.webhooks === 0;}, 'server listeners released');
  session.dispose();
  session = undefined;
  await emitAll(84);
  const recoveryContexts = [];
  faux.setResponses(Array.from({length: 6}, () => context => {recoveryContexts.push(context); return fauxAssistantMessage('Received after restart.');}));
  await resourceLoader.reload();
  const restoredManager = ephemeral ? SessionManager.inMemory(directory) : SessionManager.open(sessionManager.getSessionFile());
  const restored = await createAgentSession({cwd: directory, agentDir, resourceLoader, settingsManager, modelRuntime: runtime, model: faux.getModel(), sessionManager: restoredManager, noTools: 'all'});
  session = restored.session;
  assert.deepEqual(restored.extensionsResult.errors, []);
  await session.bindExtensions({onError: error => errors.push(error)});
  if (ephemeral) {
    await delay(1200);
    assert.equal(recoveryContexts.length, 0, 'A new ephemeral session must not replay events from the previous session');
    assert.ok(!observations.some(item => item.observation.type === 'event' && item.observation.event.data.value === 84));
    await emitAll(85);
    await until(() => reachedModel(recoveryContexts, 85) && !session.isStreaming, 'fresh ephemeral session receives new events');
    assertEphemeralStorage(restoredManager);
  } else {
    const replayModes = connection.webhookReplay === false ? ['poll', 'push'] : modes;
    await until(() => reachedModel(recoveryContexts, 84, replayModes), 'offline events recovered where replay is supported');
    if (connection.webhookReplay === false) {
      assert.ok(!observations.some(item => item.watch === 'webhook' && item.observation.type === 'event' && item.observation.event.data.value === 84), 'Null-cursor webhook does not promise offline replay');
      await until(async () => {const current = await status(); return current.webhooks === 1;}, 'restored webhook ready');
      await emitAll(85);
      await until(() => reachedModel(recoveryContexts, 85), 'all transports deliver after restart');
    }
    await until(() => !session.isStreaming && readInbox().length === 0, 'recovery acknowledgement');
  }
  assert.deepEqual(errors, []);
  await session.extensionRunner.emit({type: 'session_shutdown', reason: 'quit'});
  await until(async () => {const current = await status(); return current.streams === 0 && current.webhooks === 0;}, 'server listeners released');
  console.log(ephemeral
    ? 'PASS: stock Pi in-memory sessions delivered all three modes while busy and in successive batches, created no event state files, started fresh after restart, and cleaned up server subscriptions.'
    : 'PASS: stock Pi loaded the standalone extension; all three modes delivered while busy, reached the next model turn, cleared the durable inbox, recovered offline events where supported after session resume, and cleaned up server subscriptions.');
} finally {
  release();
  if (session) {
    await session.abort();
    await session.extensionRunner.emit({type: 'session_shutdown', reason: 'quit'});
    session.dispose();
  }
  if (previousConfig === undefined) delete process.env.PI_MCP_EVENTS_CONFIG;
  else process.env.PI_MCP_EVENTS_CONFIG = previousConfig;
  rmSync(directory, {recursive: true, force: true});
}
