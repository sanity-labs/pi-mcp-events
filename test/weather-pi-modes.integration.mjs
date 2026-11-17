import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync} from 'node:fs';
import {request as httpRequest} from 'node:http';
import {createServer as createHttpsServer} from 'node:https';
import {createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {test} from 'node:test';
import {startWeatherServer} from '../dev/weather-server.mjs';
import {readCompleteJsonLines} from './fixtures/jsonl.mjs';
import {cliPath, modelPath, root, RpcClient} from './fixtures/installed-package.mjs';

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return server.address().port;
}

async function until(check, description) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(25);
  }
  assert.fail(`Timed out waiting for ${description}`);
}

const delivered = messages => messages.filter(message => message.customType === 'mcp-events')
  .flatMap(message => message.details.events).filter(item => item.observation.type === 'event');

for (const ephemeral of [false, true]) {
  for (const proxyTls of [false, true]) {
    test(`weather in installed Pi: all events and modes, ${ephemeral ? '--no-session' : 'saved session'}, ${proxyTls ? 'HTTPS proxy' : 'direct TLS'}`,
      {timeout: 45_000}, async t => {
        const directory = mkdtempSync(join(tmpdir(), 'weather-pi-modes-'));
        let rpc;
        let server = null;
        let proxy;
        t.after(async () => {
          await rpc?.stop();
          await server?.close();
          if (proxy) {
            proxy.closeAllConnections();
            await new Promise(resolve => proxy.close(resolve));
          }
          rmSync(directory, {recursive: true, force: true});
        });
        const keyPath = join(directory, 'key.pem');
        const certPath = join(directory, 'cert.pem');
        execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyPath,
          '-out', certPath, '-days', '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'], {stdio: 'ignore'});
        const reservation = createServer();
        const receiverPort = await listen(reservation);
        await new Promise(resolve => reservation.close(resolve));
        let publicPort = receiverPort;
        if (proxyTls) {
          proxy = createHttpsServer({key: readFileSync(keyPath), cert: readFileSync(certPath)}, (request, response) => {
            const upstream = httpRequest({hostname: '127.0.0.1', port: receiverPort, path: request.url,
              method: request.method, headers: request.headers}, incoming => {
              response.writeHead(incoming.statusCode, incoming.headers);
              incoming.pipe(response);
            });
            upstream.on('error', () => { response.writeHead(502); response.end(); });
            request.pipe(upstream);
          });
          publicPort = await listen(proxy);
        }
        const origin = `https://127.0.0.1:${publicPort}`;
        let seconds = 0;
        const started = Date.now();
        server = await startWeatherServer({port: 0, intervalMs: 0, webhookToken: 'matrix-token',
          webhookCa: readFileSync(certPath), webhookAllowedOrigins: [origin],
          now: () => started + seconds * 1000});
        const agent = join(directory, 'agent'); mkdirSync(agent);
        execFileSync(process.execPath, [cliPath, 'install', root], {
          cwd: directory, env: {...process.env, PI_CODING_AGENT_DIR: agent}, stdio: 'pipe', timeout: 30_000,
        });
        const configDirectory = join(directory, '.pi'); mkdirSync(configDirectory);
        const modes = ['poll', 'push', 'webhook'];
        const names = ['weather.reading', 'weather.changed', 'weather.alert'];
        const watches = modes.flatMap(mode => names.map(name => ({
          id: `${mode}-${name}`, server: 'weather', name, mode, arguments: {stationId: 'harbor'}, ttlMs: 600_000,
        })));
        writeFileSync(join(configDirectory, 'mcp-events.json'), JSON.stringify({
          ...(proxyTls ? {stateDir: '../custom-state'} : {}),
          client: {webhookRefreshMs: 250},
          servers: {weather: {transport: 'http', url: server.url,
            headers: {Authorization: {env: 'WEATHER_MATRIX_TOKEN', prefix: 'Bearer '}}}},
          webhook: {publicUrl: `${origin}/hooks`, host: '127.0.0.1', port: receiverPort,
            ...(proxyTls ? {} : {keyFile: '../key.pem', certFile: '../cert.pem'})},
          watches,
        }));
        const contexts = join(directory, 'contexts.jsonl');
        const observations = join(directory, 'observations.jsonl');
        const release = join(directory, 'release');
        writeFileSync(join(directory, 'ordinary.txt'), 'ordinary tool use still works');
        const createRpc = sessionFile => new RpcClient({cliPath, cwd: directory, provider: 'events-test', model: 'local',
          env: {PI_CODING_AGENT_DIR: agent, WEATHER_MATRIX_TOKEN: 'matrix-token',
            EVENTS_TEST_CONTEXTS: contexts, EVENTS_TEST_OBSERVATIONS: observations, EVENTS_TEST_RELEASE: release},
          args: ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '--tools', 'read', '-e', modelPath,
            ...(ephemeral ? ['--no-session'] : ['--session-dir', join(directory, 'sessions')]),
            ...(sessionFile ? ['--session', sessionFile] : [])],
        });
        rpc = createRpc();
        await rpc.start();
        const status = async () => {
          await rpc.prompt('/mcp-events status');
          return (await rpc.getMessages()).filter(message => message.customType === 'mcp-events-status').at(-1)?.content ?? '';
        };
        await until(async () => {
          const current = await status();
          return watches.every(watch => current.includes(`${watch.id}: active`));
        }, 'all nine watches active');
        await rpc.prompt('HOLD_TEST');
        await until(() => readCompleteJsonLines(contexts).some(context => JSON.stringify(context.messages).includes('HOLD_TEST')), 'busy model');
        const expected = [];
        for (const time of [0, 5, 20, 65, 110, 155]) {
          seconds = time;
          expected.push(...server.advance(), ...server.advance());
        }
        const expectedFor = watch => expected.filter(event => event.name === watch.name && event.data.stationId === 'harbor')
          .map(event => event.eventId).sort();
        const assertDeliveries = items => {
          for (const watch of watches) {
            const actual = items.filter(item => item.watch === watch.id).map(item => item.observation.event.eventId).sort();
            assert.deepEqual(actual, expectedFor(watch), watch.id);
          }
        };
        await until(() => watches.every(watch => {
          const items = readCompleteJsonLines(observations).filter(item => item.observation.type === 'event' && item.watch === watch.id);
          return items.length >= expectedFor(watch).length;
        }), 'every event accepted while the model is busy');
        assertDeliveries(readCompleteJsonLines(observations).filter(item => item.observation.type === 'event'));
        writeFileSync(release, 'continue');
        await until(async () => delivered(await rpc.getMessages()).length >= watches.reduce((count, watch) => count + expectedFor(watch).length, 0), 'all modes reach the conversation');
        assertDeliveries(delivered(await rpc.getMessages()));
        for (const mode of modes) {
          const id = `${mode}-weather.reading`;
          await rpc.prompt(`/mcp-events stop ${id}`);
          await until(async () => (await status()).includes(`${id}: stopped`), `${id} stopped`);
          const [missed] = server.advance();
          server.advance();
          await until(async () => {
            const items = delivered(await rpc.getMessages());
            return modes.filter(other => other !== mode).every(other =>
              items.some(item => item.watch === `${other}-weather.reading` && item.observation.event.eventId === missed.eventId));
          }, 'other modes continue while one is stopped');
          assert.ok(!delivered(await rpc.getMessages()).some(item => item.watch === id && item.observation.event.eventId === missed.eventId));
          await rpc.prompt(`/mcp-events start ${id}`);
          await until(async () => delivered(await rpc.getMessages()).some(item => item.watch === id && item.observation.event.eventId === missed.eventId), `${id} replays a stopped event`);
          assert.equal(delivered(await rpc.getMessages()).filter(item => item.watch === id && item.observation.event.eventId === missed.eventId).length, 1);
        }
        await rpc.prompt('READ_TEST', undefined, 'followUp');
        await until(async () => (await rpc.getMessages()).some(message => message.role === 'toolResult'
          && message.toolName === 'read' && !message.isError), 'ordinary Pi tools still work');
        if (ephemeral) {
          assert.equal((await rpc.getState()).sessionFile, undefined);
          const statePath = proxyTls ? join(directory, 'custom-state') : join(agent, 'mcp-events');
          assert.ok(!existsSync(statePath) || readdirSync(statePath).length === 0, '--no-session creates no event state');
        } else {
          assert.ok((await rpc.getState()).sessionFile);
        }
        const sessionFile = (await rpc.getState()).sessionFile;
        await rpc.prompt('/test-shutdown');
        await rpc.stop();
        const [offline] = server.advance(); server.advance();
        rpc = createRpc(sessionFile);
        await rpc.start();
        await until(async () => {
          const current = await status();
          return watches.every(watch => current.includes(`${watch.id}: active`));
        }, 'all modes active after Pi restarts');
        const [fresh] = server.advance(); server.advance();
        await until(async () => {
          const items = delivered(await rpc.getMessages());
          return modes.every(mode => items.some(item => item.watch === `${mode}-weather.reading` && item.observation.event.eventId === fresh.eventId));
        }, 'fresh readings after Pi restarts');
        if (!ephemeral) {
          await until(async () => {
            const items = delivered(await rpc.getMessages());
            return modes.every(mode => items.some(item => item.watch === `${mode}-weather.reading` && item.observation.event.eventId === offline.eventId));
          }, 'saved cursors replay readings produced while Pi was down');
        }
        for (const mode of modes) {
          const items = delivered(await rpc.getMessages()).filter(item => item.watch === `${mode}-weather.reading`
            && item.observation.event.eventId === offline.eventId);
          assert.equal(items.length, ephemeral ? 0 : 1, `${mode} respects session persistence after restart`);
        }
        await rpc.prompt('/mcp-events stop');
        await until(async () => {
          const current = await status();
          return watches.every(watch => current.includes(`${watch.id}: stopped`));
        }, 'all watches stopped');
        const count = delivered(await rpc.getMessages()).length;
        server.advance(); server.advance();
        await server.waitForWebhookIdle();
        await delay(1100);
        assert.equal(delivered(await rpc.getMessages()).length, count, 'stop all ends delivery in every mode');
      });
  }
}
