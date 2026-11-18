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
test(`configured-watch changes in installed Pi: ${saved ? 'saved overrides' : 'in-memory defaults'}`, {timeout: 60000}, async t => {
    const directory = mkdtempSync(join(tmpdir(), 'pi-chat-watches-'));
    const agentDir = join(directory, 'agent'); mkdirSync(agentDir);
    const controls = join(directory, 'controls.jsonl'); const receipts = join(directory, 'receipts.jsonl');
    writeFileSync(controls, ''); writeFileSync(receipts, '');
    const configPath = join(directory, 'events.json');
    writeFileSync(configPath, JSON.stringify({stateDir: 'state', servers: {weather: {
      transport: 'stdio', command: process.execPath,
      args: [join(import.meta.dirname, 'fixtures/weather-stdio-server.mjs'), controls, receipts, '1000'],
    }}, client: {pollFloorMs: 20, retryMs: 10}, watches: [{id: 'configured', server: 'weather', name: 'weather.alert', arguments: {stationId: 'harbor'}, mode: 'push'}]}));
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
    await call('start', {...input, durationMs: 500}, true);
    const expiry = Date.now() + 60_000;
    const timed = (await call('update', {...input, id: 'configured', expiresAt: expiry})).watch;
    assert.equal(timed.id, 'configured');
    assert.equal(timed.expiresAt, expiry);
    const replacement = (await call('update', {...input, id: 'configured', mode: 'poll'})).watch;
    assert.equal(replacement.status, 'active');
    let watches = (await call('list')).watches;
    assert.equal(watches.filter(w => w.status === 'active').length, 1);
    const sessionFile = (await rpc.getState()).sessionFile;
    await rpc.stop(); rpc = undefined;
    await start(sessionFile);
    watches = (await call('list')).watches;
    const active = watches.filter(w => w.status === 'active');
    assert.equal(active.length, 1);
    assert.equal(active[0].id, saved ? replacement.id : 'configured');
    assert.equal(active[0].mode, saved ? 'poll' : 'push');
    const oldCount = (await observations()).length;
    await advance(25);
    await until(async () => (await observations()).length > oldCount, 'warning after restart');
    await delay(1200);
    const latest = (await observations()).slice(oldCount);
    assert.equal(latest.length, 1, 'one warning reaches Pi after restart');
    assert.equal(latest[0].watch, active[0].id);
    await call('stop', {id: active[0].id});
    await rpc.stop(); rpc = undefined;
    await start(sessionFile);
    watches = (await call('list')).watches;
    if (saved) {
      assert.ok(watches.every(w => w.status === 'stopped'));
      await rpc.newSession();
      assert.deepEqual((await call('list')).watches.map(w => ({id: w.id, status: w.status})), [{id: 'configured', status: 'active'}]);
    } else assert.equal(watches[0].status, 'active');
});
}
