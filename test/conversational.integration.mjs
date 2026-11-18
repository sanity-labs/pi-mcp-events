import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {appendFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {cliPath, modelPath, root, RpcClient} from './fixtures/installed-package.mjs';
import {readCompleteJsonLines} from './fixtures/jsonl.mjs';

async function until(check, description) {
  const end = Date.now() + 15_000;
  while (Date.now() < end) { if (await check()) return; await delay(25); }
  assert.fail(`Timed out: ${description}`);
}
for (const saved of [false, true]) {
  test(`installed Pi chat tools: Harbor alerts, changes, stops, and ${saved ? 'saved restoration' : 'memory lifetime'}`, {timeout: 60_000}, async t => {
    const directory = mkdtempSync(join(tmpdir(), 'pi-chat-watches-'));
    const agentDir = join(directory, 'agent'); mkdirSync(agentDir);
    const controls = join(directory, 'controls.jsonl'); const receipts = join(directory, 'receipts.jsonl');
    writeFileSync(controls, ''); writeFileSync(receipts, '');
    const configPath = join(directory, 'events.json');
    writeFileSync(configPath, JSON.stringify({stateDir: 'state', servers: {weather: {
      transport: 'stdio', command: process.execPath,
      args: [join(import.meta.dirname, 'fixtures/weather-stdio-server.mjs'), controls, receipts, '1000'],
    }}, client: {pollFloorMs: 20, retryMs: 10}}));
    execFileSync(process.execPath, [cliPath, 'install', root], {cwd: directory, env: {...process.env, PI_CODING_AGENT_DIR: agentDir}, stdio: 'pipe'});
    let rpc;
    t.after(async () => { await rpc?.stop(); rmSync(directory, {recursive: true, force: true}); });
    const start = async sessionFile => {
      rpc = new RpcClient({cliPath, cwd: directory, provider: 'events-test', model: 'local',
        env: {PI_CODING_AGENT_DIR: agentDir, PI_MCP_EVENTS_CONFIG: configPath,
          EVENTS_TEST_CONTEXTS: join(directory, 'contexts'), EVENTS_TEST_OBSERVATIONS: join(directory, 'observations')},
        args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '-e', modelPath,
          ...(saved ? ['--session-dir', join(directory, 'sessions')] : ['--no-session']), ...(sessionFile ? ['--session', sessionFile] : [])],
      });
      await rpc.start(); await rpc.getState();
    };
    const call = async (action, args = {}, expectError = false) => {
      const name = `mcp_events_${action}`;
      const results = async () => (await rpc.getMessages()).filter(message => message.role === 'toolResult' && message.toolName === name);
      const count = (await results()).length;
      await rpc.prompt(`EVENT_TOOL ${JSON.stringify({name, arguments: args})}`, undefined, 'followUp');
      await until(async () => (await results()).length > count, name);
      const result = (await results()).at(-1);
      assert.equal(Boolean(result.isError), expectError, JSON.stringify(result));
      return result.details;
    };
    const advance = async seconds => {
      const id = randomUUID(); appendFileSync(controls, `${JSON.stringify({id, type: 'advance', seconds})}\n`);
      await until(() => readCompleteJsonLines(receipts).some(item => item.id === id), 'weather control');
    };
    const observations = async () => (await rpc.getMessages()).filter(message => message.customType === 'mcp-events')
      .flatMap(message => message.details.events).filter(item => item.observation.type === 'event');
    const input = {server: 'weather', name: 'weather.alert', arguments: {stationId: 'harbor'}};
    await start();
    assert.equal(readCompleteJsonLines(receipts).length, 0, 'no connection before discovery');
    const discovered = await call('discover');
    assert.ok(discovered[0].events.some(event => event.name === 'weather.alert' && event.inputSchema));
    const original = (await call('start', input)).watch;
    assert.equal(original.status, 'active'); assert.equal(original.mode, 'push');
    assert.match(original.lifetime, saved ? /saved session/ : /memory only/);
    assert.equal((await call('start', input)).watch.id, original.id);
    await advance(25);
    await until(async () => (await observations()).some(item => item.observation.event.data.status === 'raised'), 'Harbor warning reaches Pi');
    await advance(125);
    await until(async () => (await observations()).some(item => item.observation.event.data.status === 'cleared'), 'Harbor all-clear reaches Pi');
    await call('update', {...input, id: original.id, mode: 'webhook'}, true);
    assert.equal((await call('list')).watches.find(watch => watch.id === original.id).status, 'active');
    const replacement = (await call('update', {...input, id: original.id, mode: 'poll'})).watch;
    assert.equal(replacement.mode, 'poll'); assert.equal(replacement.status, 'active');
    assert.equal((await call('list')).watches.find(watch => watch.id === original.id).status, 'stopped');
    const sessionFile = (await rpc.getState()).sessionFile;
    await rpc.stop(); rpc = undefined;
    await start(sessionFile);
    const resumed = (await call('list')).watches;
    if (saved) {
      assert.equal(resumed.find(watch => watch.id === replacement.id).status, 'active');
      assert.equal(resumed.find(watch => watch.id === original.id).phase, 'stopped');
      await call('stop', {id: replacement.id});
      await rpc.stop(); rpc = undefined; await start(sessionFile);
      assert.equal((await call('list')).watches.find(watch => watch.id === replacement.id).phase, 'stopped');
      await rpc.newSession();
      assert.deepEqual((await call('list')).watches, []);
    } else assert.deepEqual(resumed, []);
  });
}
