import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {createReceiver, createTransport, readConfig} from '../src/config.ts';

const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) rmSync(directory, {recursive: true, force: true});
  vi.unstubAllEnvs();
});
function config(value: unknown) {
  const directory = mkdtempSync(join(tmpdir(), 'events-config-'));
  directories.push(directory);
  const path = join(directory, 'events.json');
  writeFileSync(path, JSON.stringify(value));
  return {value: readConfig(path), path};
}

describe('extension configuration', () => {
  const servers = {example: {transport: 'http', url: 'https://example.invalid/mcp'}};
  const watch = {id: 'one', server: 'example', name: 'example.changed', mode: 'poll'};

  it('defaults arguments and rejects ambiguous or missing watch references', () => {
    expect(config({servers, watches: [watch]}).value.watches[0]?.arguments).toEqual({});
    expect(() => config({servers, watches: [watch, watch]})).toThrow('Duplicate');
    expect(() => config({servers: {}, watches: [watch]})).toThrow('unknown server');
    expect(() => config({servers, watches: [watch, {...watch, id: 'two'}]})).toThrow('Duplicate configured subscription');
    expect(() => config({servers, watches: [{...watch, mode: 'webhook'}]})).toThrow('Webhook watches require');
  });

  it('requires environment credentials before opening a transport', () => {
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', undefined);
    const {value} = config({servers: {example: {...servers.example, headers: {Authorization: {env: 'PI_EVENTS_TEST_TOKEN', prefix: 'Bearer '}}}}});
    expect(() => createTransport(value.servers.example!, '/tmp')).toThrow('Missing environment variable PI_EVENTS_TEST_TOKEN');
    vi.stubEnv('PI_EVENTS_TEST_TOKEN', 'synthetic-token');
    expect(() => createTransport(value.servers.example!, '/tmp')).not.toThrow();
  });

  it('rejects configured watches beyond client or session limits before connecting', () => {
    const watches = Array.from({length: 33}, (_, index) => ({...watch, id: String(index), arguments: {channel: String(index)}}));
    expect(() => config({servers, watches})).toThrow('32 watches');
    expect(() => config({servers, watches: Array.from({length: 513}, (_, index) => ({...watch, id: String(index)}))})).toThrow('512 watches');
  });

  it('requires paired TLS files and loopback binding behind a proxy', () => {
    const webhook = {publicUrl: 'https://example.invalid/events', port: 8181};
    expect(() => config({servers, webhook: {...webhook, keyFile: 'key.pem'}})).toThrow('both');
    const valid = config({servers, webhook});
    expect(createReceiver(valid.value, valid.path)).toBeDefined();
    const invalid = config({servers, webhook: {...webhook, host: '0.0.0.0'}});
    expect(() => createReceiver(invalid.value, invalid.path)).toThrow('loopback');
  });
});
