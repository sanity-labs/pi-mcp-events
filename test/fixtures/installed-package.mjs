import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {copyFileSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {after} from 'node:test';
import {fileURLToPath, pathToFileURL} from 'node:url';

const checkout = resolve(import.meta.dirname, '../..');
const consumer = mkdtempSync(join(tmpdir(), 'pi-events-installed-'));
after(() => rmSync(consumer, {recursive: true, force: true}));
const manifest = JSON.parse(readFileSync(join(checkout, 'package.json'), 'utf8'));
const npm = (args, cwd) => execFileSync('npm', args, {cwd, encoding: 'utf8', timeout: 180_000});
let resolveInstalled;

try {
  const [archive] = JSON.parse(npm(['pack', '--json', '--pack-destination', consumer], checkout));
  const paths = archive.files.map(file => file.path);
  for (const path of paths) {
    assert.match(path, /^(dist\/[^/]+\.(js|d\.ts)|package\.json|README\.md|LICENSE)$/);
  }
  for (const path of ['package.json', 'README.md', 'LICENSE']) assert.ok(paths.includes(path), `Missing ${path}`);
  for (const entry of Object.values(manifest.exports)) {
    for (const path of Object.values(entry)) assert.ok(paths.includes(path.replace(/^\.\//, '')), `Missing export ${path}`);
  }
  writeFileSync(join(consumer, 'package.json'), JSON.stringify({private: true, type: 'module'}));
  npm(['install', '--no-audit', '--no-fund', '--prefer-offline', join(consumer, archive.filename),
    ...['@earendil-works/pi-coding-agent', '@earendil-works/pi-ai'].map(name => `${name}@${manifest.devDependencies[name]}`)], consumer);
  const resolver = join(consumer, 'resolve.mjs');
  writeFileSync(resolver, 'export const resolve = name => import.meta.resolve(name);');
  resolveInstalled = (await import(pathToFileURL(resolver).href)).resolve;
  const entry = fileURLToPath(resolveInstalled(manifest.name));
  assert.ok(realpathSync(entry).startsWith(realpathSync(consumer) + '/'), 'Package resolves outside consumer');
  for (const suffix of ['', '/pi', '/extension']) {
    await import(resolveInstalled(manifest.name + suffix));
  }
  copyFileSync(join(checkout, 'test/fixtures/pi-model.mjs'), join(consumer, 'pi-model.mjs'));
} catch (error) {
  rmSync(consumer, {recursive: true, force: true});
  throw error;
}

export const root = dirname(dirname(fileURLToPath(resolveInstalled(manifest.name))));
export const modelPath = join(consumer, 'pi-model.mjs');
export const cliPath = join(dirname(fileURLToPath(resolveInstalled('@earendil-works/pi-coding-agent'))), 'bundle/cli.js');
export const {RpcClient} = await import(resolveInstalled('@earendil-works/pi-coding-agent'));
