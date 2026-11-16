import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';

test('a fresh production installation imports the Events client without installing Pi', {timeout: 180_000}, t => {
  const consumer = mkdtempSync(join(tmpdir(), 'pi-events-client-'));
  t.after(() => rmSync(consumer, {recursive: true, force: true}));
  const checkout = join(import.meta.dirname, '..');
  const npm = (args, cwd) => execFileSync('npm', args, {cwd, encoding: 'utf8', timeout: 150_000});
  const [archive] = JSON.parse(npm(['pack', '--ignore-scripts', '--json', '--pack-destination', consumer], checkout));
  writeFileSync(join(consumer, 'package.json'), JSON.stringify({name: 'events-client-consumer', private: true, type: 'module'}));
  npm(['install', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund', join(consumer, archive.filename)], consumer);
  const lock = JSON.parse(readFileSync(join(consumer, 'package-lock.json'), 'utf8'));
  assert.ok(Object.keys(lock.packages).every(path => !path.includes('node_modules/@earendil-works/')), 'Client installation includes Pi host packages');
  writeFileSync(join(consumer, 'verify.mjs'), `
    import assert from 'node:assert/strict';
    import {EventsClient, WebhookReceiver, createHttpTransport} from '@sanity-labs/pi-mcp-events';
    import {MESSAGE_TYPE} from '@sanity-labs/pi-mcp-events/pi';
    assert.equal(typeof EventsClient, 'function');
    assert.equal(typeof WebhookReceiver, 'function');
    assert.equal(typeof createHttpTransport, 'function');
    assert.equal(MESSAGE_TYPE, 'mcp-events');
  `);
  execFileSync(process.execPath, ['verify.mjs'], {cwd: consumer, encoding: 'utf8', timeout: 30_000});
  npm(['audit', '--omit=dev', '--audit-level=high'], consumer);
});
