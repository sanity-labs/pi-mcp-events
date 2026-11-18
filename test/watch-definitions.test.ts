import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, expect, it} from 'vitest';
import {WatchDefinitions, type WatchDefinition} from '../src/watch-definitions.ts';

const directories: string[] = [];
afterEach(() => { for (const directory of directories.splice(0)) rmSync(directory, {recursive: true, force: true}); });
const definition = (id: string, phase: WatchDefinition['phase'] = 'stopped'): WatchDefinition => ({
  id, server: 'weather', name: 'weather.alert', arguments: {stationId: 'harbor'}, mode: 'push', phase,
});
function file() {
  const directory = mkdtempSync(join(tmpdir(), 'watch-definitions-'));
  directories.push(directory);
  return join(directory, 'watches.json');
}

it('reads existing definition files and saves overrides atomically with their definitions', () => {
  const path = file();
  writeFileSync(path, JSON.stringify({version: 1, watches: [definition('old')]}));
  const store = new WatchDefinitions(path);
  store.set({...definition('replacement', 'active'), configuredId: 'default'}, 'default');
  expect(JSON.parse(readFileSync(path, 'utf8'))).toMatchObject({overrides: {default: 'replacement'}});
  const reopened = new WatchDefinitions(path);
  expect(reopened.configuredOverrides()).toEqual({default: 'replacement'});
  expect(reopened.list()).toHaveLength(2);
});

it('prunes terminal definitions while keeping overrides and interrupted replacement targets', () => {
  const store = new WatchDefinitions();
  store.set({...definition('override'), configuredId: 'default'}, 'default');
  store.set(definition('target'));
  store.set({...definition('replacement', 'active'), replaces: 'target'});
  for (let i = 0; i < 600; i++) store.set(definition(`old-${i}`));
  expect(store.list()).toHaveLength(512);
  expect(store.list().map(w => w.id)).toEqual(expect.arrayContaining(['override', 'target', 'replacement', 'old-599']));
  expect(store.list().some(w => w.id === 'old-0')).toBe(false);
});

it('reclaims byte capacity from stopped history and keeps active watches', () => {
  const store = new WatchDefinitions();
  store.set(definition('active', 'active'));
  store.set({...definition('large-old'), arguments: {data: 'a'.repeat(150_000)}});
  store.set({...definition('large-new', 'active'), arguments: {data: 'b'.repeat(150_000)}});
  expect(store.list().map(w => w.id)).toEqual(['active', 'large-new']);
});

it('keeps the store unchanged when every definition is protected', () => {
  const store = new WatchDefinitions();
  for (let i = 0; i < 512; i++) store.set(definition(`active-${i}`, 'active'));
  expect(() => store.set(definition('overflow', 'active'))).toThrow();
  expect(store.list()).toHaveLength(512);
  expect(store.list().some(w => w.id === 'overflow')).toBe(false);
});

it('rejects dangling overrides instead of silently suppressing a configured watch', () => {
  const path = file();
  writeFileSync(path, JSON.stringify({version: 1, watches: [], overrides: {default: 'missing'}}));
  expect(() => new WatchDefinitions(path)).toThrow('Invalid configured watch override');
});
