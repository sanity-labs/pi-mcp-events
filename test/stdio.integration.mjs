import assert from 'node:assert/strict';
import {appendFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {test} from 'node:test';
import {readCompleteJsonLines} from './fixtures/jsonl.mjs';
import {cliPath, modelPath, root as installedRoot, RpcClient} from './fixtures/installed-package.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const {createTransport, EventsClient, EventsWire, parseConfig} = await import(pathToFileURL(join(root, 'dist/index.js')).href);

async function until(check, description) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(10);
  }
  assert.fail(`Timed out waiting for ${description}`);
}

test('real stdio streams route concurrently, cancel independently, and resume after EOF', {timeout: 30_000}, async t => {
  const directory = mkdtempSync(join(tmpdir(), 'pi-events-stdio-'));
  const historyPath = join(directory, 'history.jsonl');
  const requestLogPath = join(directory, 'requests.jsonl');
  const exitPath = join(directory, 'exit');
  const fixturePath = fileURLToPath(new URL('./fixtures/stdio-events-server.mjs', import.meta.url));
  writeFileSync(historyPath, '');
  writeFileSync(requestLogPath, '');

  const config = parseConfig({servers: {local: {
    transport: 'stdio', command: process.execPath, args: [fixturePath, historyPath, requestLogPath, exitPath],
  }}});
  const transports = [];
  const wire = new EventsWire(() => {
    const transport = createTransport(config.servers.local, directory);
    transports.push(transport);
    return transport;
  });
  const client = new EventsClient(wire, {retryMs: 500, maxRetries: 5});
  const leftEvents = [];
  const rightEvents = [];
  t.after(async () => {
    await client.close();
    rmSync(directory, {recursive: true, force: true});
  });

  await client.connect();
  const left = client.watch({name: 'example.changed', arguments: {channel: 'left'}, mode: 'push'}, observation => {
    if (observation.type === 'event') leftEvents.push(observation.event);
  });
  const right = client.watch({name: 'example.changed', arguments: {channel: 'right'}, mode: 'push'}, observation => {
    if (observation.type === 'event') rightEvents.push(observation.event);
  });
  await Promise.all([left.ready, right.ready]);

  function appendEvent(cursor, channel, value) {
    appendFileSync(historyPath, `${JSON.stringify({
      eventId: `event-${cursor}`, name: 'example.changed', timestamp: new Date().toISOString(),
      data: {channel, value}, cursor: String(cursor),
    })}\n`);
  }
  function readLog() {
    return readCompleteJsonLines(requestLogPath);
  }

  appendEvent(1, 'left', 1);
  appendEvent(2, 'right', 2);
  await until(() => leftEvents.length === 1 && rightEvents.length === 1, 'both concurrent streams to receive their event');
  assert.deepEqual(leftEvents.map(event => event.data.value), [1]);
  assert.deepEqual(rightEvents.map(event => event.data.value), [2]);

  const leftRequest = readLog().find(record => record.kind === 'stream' && record.channel === 'left');
  assert.ok(leftRequest, 'fixture observed a real stdio events/stream request');
  await left.stop();
  await until(() => readLog().some(record => record.kind === 'cancel' && record.requestId === leftRequest.id),
    'notifications/cancelled for the left stream');

  appendEvent(3, 'left', 3);
  appendEvent(4, 'right', 4);
  await until(() => rightEvents.length === 2, 'the surviving right stream to continue');
  assert.deepEqual(leftEvents.map(event => event.data.value), [1]);
  assert.deepEqual(rightEvents.map(event => event.data.value), [2, 4]);
  await until(() => right.cursor === '4', 'the right watch to checkpoint its latest cursor');

  writeFileSync(exitPath, 'exit');
  await until(() => readLog().some(record => record.kind === 'exit'), 'the original server to exit');
  appendEvent(5, 'right', 5);
  await until(() => transports.length === 2 && rightEvents.length === 3 && right.status === 'active',
    'the client to reconnect after child EOF and replay the event');
  assert.deepEqual(rightEvents.map(event => event.data.value), [2, 4, 5]);

  const rightRequests = readLog().filter(record => record.kind === 'stream' && record.channel === 'right');
  assert.deepEqual(rightRequests.map(record => record.cursor), [null, '4']);
  assert.equal(right.cursor, '5');
  await right.stop();
});

for (const ephemeral of [false, true]) {
  test(`installed Pi routes, cancels, and recovers stdio streams (${ephemeral ? '--no-session' : 'saved session'})`, {timeout: 30_000}, async t => {
    const directory = mkdtempSync(join(tmpdir(), 'pi-stdio-installed-'));
    const history = join(directory, 'history.jsonl');
    const requests = join(directory, 'requests.jsonl');
    const exit = join(directory, 'exit');
    writeFileSync(history, ''); writeFileSync(requests, '');
    const agent = join(directory, 'agent'); mkdirSync(agent);
    writeFileSync(join(agent, 'settings.json'), JSON.stringify({packages: [installedRoot]}));
    const configDirectory = join(directory, '.pi'); mkdirSync(configDirectory);
    writeFileSync(join(configDirectory, 'mcp-events.json'), JSON.stringify({
      servers: {local: {transport: 'stdio', command: process.execPath,
        args: [fileURLToPath(new URL('./fixtures/stdio-events-server.mjs', import.meta.url)), history, requests, exit]}},
      client: {retryMs: 100},
      watches: ['left', 'right'].map(channel => ({id: channel, server: 'local', name: 'example.changed', mode: 'push', arguments: {channel}})),
    }));
    const rpc = new RpcClient({cliPath, cwd: directory, provider: 'events-test', model: 'local',
      env: {PI_CODING_AGENT_DIR: agent, EVENTS_TEST_CONTEXTS: join(directory, 'contexts.jsonl'),
        EVENTS_TEST_OBSERVATIONS: join(directory, 'observations.jsonl')},
      args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '-e', modelPath,
        ...(ephemeral ? ['--no-session'] : ['--session-dir', join(directory, 'sessions')])],
    });
    t.after(async () => { await rpc.stop(); rmSync(directory, {recursive: true, force: true}); });
    const log = () => readCompleteJsonLines(requests);
    const values = async channel => (await rpc.getMessages()).filter(message => message.customType === 'mcp-events')
      .flatMap(message => message.details.events).filter(item => item.watch === channel && item.observation.type === 'event')
      .map(item => item.observation.event.data.value);
    const append = (position, channel) => appendFileSync(history, `${JSON.stringify({eventId: `event-${position}`,
      name: 'example.changed', timestamp: new Date().toISOString(), data: {channel, value: position}, cursor: String(position)})}\n`);
    await rpc.start();
    await until(() => log().filter(item => item.kind === 'stream').length === 2, 'both Pi stdio watches');
    append(1, 'left'); append(2, 'right');
    await until(async () => (await values('left')).includes(1) && (await values('right')).includes(2), 'independent Pi event routing');
    const left = log().find(item => item.kind === 'stream' && item.channel === 'left');
    await rpc.prompt('/mcp-events stop left');
    await until(() => log().some(item => item.kind === 'cancel' && item.requestId === left.id), 'Pi cancels only the left stream');
    append(3, 'left'); append(4, 'right');
    await until(async () => (await values('right')).includes(4), 'the surviving Pi stdio watch');
    assert.deepEqual(await values('left'), [1]);
    writeFileSync(exit, 'exit');
    await until(() => log().some(item => item.kind === 'exit'), 'stdio child exit');
    append(5, 'right');
    await until(async () => (await values('right')).includes(5), 'Pi reconnects and replays after child exit');
    assert.deepEqual(await values('right'), [2, 4, 5]);
    assert.deepEqual(log().filter(item => item.kind === 'stream' && item.channel === 'right').map(item => item.cursor), [null, '4']);
    assert.equal(log().filter(item => item.kind === 'stream' && item.channel === 'left').length, 1);
    await rpc.prompt('/mcp-events stop');
    await until(() => log().filter(item => item.kind === 'cancel').length === 2, 'Pi shuts down remaining stdio streams');
  });
}
