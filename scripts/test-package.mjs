import assert from 'node:assert/strict';
import {spawn, execFileSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {startFixture} from '../test/fixtures/server.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const directory = mkdtempSync(join(tmpdir(), 'pi-events-package-'));
let fixture;
async function run(command, args, options = {}) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {cwd: root, stdio: 'inherit', ...options});
    child.on('error', reject);
    child.on('exit', code => code === 0 ? resolve() : reject(new Error(`${command} exited ${code}`)));
  });
}
try {
  await run('npm', ['run', 'build']);
  const packed = JSON.parse(execFileSync('npm', ['pack', '--ignore-scripts', '--json', '--pack-destination', directory], {cwd: root, encoding: 'utf8'}))[0];
  const archive = join(directory, packed.filename);
  for (const path of ['docs/events-draft.md', 'docs/architecture.md', 'docs/development.md']) {
    assert.ok(packed.files.some(file => file.path === path), `Missing linked documentation: ${path}`);
  }
  for (const file of packed.files) {
    assert.ok(file.path.startsWith('dist/') || ['package.json', 'README.md', 'LICENSE', 'docs/events-draft.md', 'docs/architecture.md', 'docs/development.md'].includes(file.path), `Unexpected archive entry: ${file.path}`);
    assert.ok(!file.path.endsWith('.test.js'), 'Tests must not ship in the package');
  }
  const install = join(directory, 'consumer');
  mkdirSync(install);
  writeFileSync(join(install, 'package.json'), JSON.stringify({private: true, type: 'module'}));
  await run('npm', ['install', '--ignore-scripts', '--legacy-peer-deps', '--no-audit', '--no-fund', archive], {cwd: install});
  await run(process.execPath, ['--input-type=module', '-e', `
    import assert from 'node:assert/strict';
    import {EventsClient, EventsWire, WebhookReceiver, PROTOCOL_VERSION} from '@sanity-labs/pi-mcp-events';
    import {OBSERVATION_EVENT} from '@sanity-labs/pi-mcp-events/pi';
    assert.equal(PROTOCOL_VERSION, '2026-07-28');
    assert.equal(OBSERVATION_EVENT, 'mcp-events:observation');
    for (const value of [EventsClient, EventsWire, WebhookReceiver]) assert.equal(typeof value, 'function');
  `], {cwd: install});
  const installed = join(install, 'node_modules/@sanity-labs/pi-mcp-events');
  const manifest = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'));
  assert.equal(manifest.private, true, 'Publishing stays disabled during private iteration');
  assert.deepEqual(manifest.pi.extensions, ['./dist/extension.js']);
  const cliAgentDir = join(directory, 'cli-agent');
  await run(process.execPath, [join(root, 'node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js'), 'install', installed], {
    cwd: install, env: {...process.env, PI_CODING_AGENT_DIR: cliAgentDir},
  });
  const settings = JSON.parse(readFileSync(join(cliAgentDir, 'settings.json'), 'utf8'));
  assert.ok(settings.packages.some(source => resolve(cliAgentDir, source) === installed), 'Pi CLI registers the installed package directory');
  const keyFile = join(directory, 'localhost.key');
  const certFile = join(directory, 'localhost.crt');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyFile, '-out', certFile, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'], {stdio: 'ignore'});
  fixture = await startFixture({key: readFileSync(keyFile), cert: readFileSync(certFile)});
  const connectionFile = join(directory, 'connection.json');
  writeFileSync(connectionFile, JSON.stringify(fixture.connection), {mode: 0o600});
  const env = {...process.env, LOCAL_EVENTS_CONNECTION_FILE: connectionFile, LOCAL_EVENTS_EXTENSION_PATH: installed};
  for (const args of [[], ['--no-session']]) {
    await run(process.execPath, [resolve(root, 'test/pi-local.mjs'), ...args], {env});
    assert.deepEqual({streams: fixture.stats().streams, webhooks: fixture.stats().webhooks}, {streams: 0, webhooks: 0});
  }
  assert.ok(fixture.stats().verifications >= 4);
  assert.ok(fixture.stats().deliveries >= 5);
  console.log('PASS: clean tarball installation, public exports, signed HTTPS callbacks, and stock Pi in both session modes.');
} finally {
  await fixture?.close();
  rmSync(directory, {recursive: true, force: true});
}
