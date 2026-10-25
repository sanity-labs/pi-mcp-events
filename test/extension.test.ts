import {createHash} from 'node:crypto';
import {mkdtempSync, mkdirSync, writeFileSync, rmSync, readdirSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type {ExtensionAPI, ExtensionContext} from '@earendil-works/pi-coding-agent';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {Inbox} from '../src/inbox.ts';
import type {Observation, WatchOptions} from '../src/client.ts';
import extension from '../src/extension.ts';

const state = vi.hoisted(() => ({
  ready: Promise.resolve(),
  subscriptions: [] as Array<{options: WatchOptions; accept: (observation: Observation) => void; checkpoint: (cursor: string | null) => void}>,
}));
vi.mock('../src/client.ts', () => ({EventsClient: class {
  connect() {return Promise.resolve();}
  close() {return Promise.resolve();}
  watch(options: WatchOptions, accept: (observation: Observation) => void, checkpoint: (cursor: string | null) => void) {
    state.subscriptions.push({options, accept, checkpoint});
    return {options, ready: state.ready, closed: new Promise(() => {}), stop: async () => {}};
  }
}}));
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  state.subscriptions = [];
  state.ready = Promise.resolve();
});
function setup(count: number, watches = false, ephemeral = false) {
  const directory = mkdtempSync(join(tmpdir(), 'events-extension-'));
  cleanups.push(() => rmSync(directory, {recursive: true, force: true}));
  const path = join(directory, 'config.json');
  const stateDir = join(directory, 'state');
  mkdirSync(stateDir);
  writeFileSync(path, JSON.stringify({stateDir, servers: watches ? {test: {transport: 'http', url: 'http://localhost/mcp'}} : {}, watches: watches ? [{id: 'watch', server: 'test', name: 'example.changed', mode: 'poll', ttlMs: null}] : []}));
  const inboxPath = join(stateDir, `${createHash('sha256').update(`${path}\0session`).digest('hex')}.json`);
  const inbox = new Inbox(inboxPath);
  for (let i = 0; i < count; i++) inbox.accept({server: 'test', watch: 'watch', observation: {type: 'gap', cursor: String(i)}});
  vi.stubEnv('PI_MCP_EVENTS_CONFIG', path);
  const handlers = new Map<string, (event: unknown, context: ExtensionContext) => unknown>();
  const sendMessage = vi.fn();
  const api = {on: (name: string, handler: (event: unknown, context: ExtensionContext) => unknown) => handlers.set(name, handler), registerCommand: vi.fn(), sendMessage, events: {emit: vi.fn()}};
  let idle = true;
  const entries: unknown[] = [];
  const context = {cwd: directory, ui: {notify: vi.fn()}, isIdle: () => idle, sessionManager: {getSessionId: () => 'session', getSessionFile: () => ephemeral ? undefined : join(directory, 'session.jsonl'), getEntries: () => entries}} as unknown as ExtensionContext;
  extension(api as unknown as ExtensionAPI);
  cleanups.push(async () => {await handlers.get('session_shutdown')!({}, context);});
  return {handlers, context, sendMessage, inboxPath, stateDir, entries, configPath: path, setIdle(value: boolean) {idle = value;}};
}
describe('Pi extension lifecycle', () => {
  it('restores each watch cursor independently and resets it when the endpoint changes', async () => {
    const fixture = setup(0);
    const config = {
      stateDir: fixture.stateDir,
      servers: {test: {transport: 'http', url: 'http://localhost/first'}},
      watches: ['poll', 'push'].map(mode => ({id: mode, server: 'test', name: 'example.changed', mode})),
    };
    writeFileSync(fixture.configPath, JSON.stringify(config));
    await fixture.handlers.get('session_start')!({}, fixture.context);
    state.subscriptions[0]!.checkpoint('poll-position');
    state.subscriptions[1]!.checkpoint('push-position');
    await fixture.handlers.get('session_start')!({}, fixture.context);
    expect(state.subscriptions[2]!.options.cursor).toBe('poll-position');
    expect(state.subscriptions[3]!.options.cursor).toBe('push-position');
    config.servers.test.url = 'http://localhost/second';
    writeFileSync(fixture.configPath, JSON.stringify(config));
    await fixture.handlers.get('session_start')!({}, fixture.context);
    expect(state.subscriptions[4]!.options.cursor).toBeNull();
    expect(state.subscriptions[5]!.options.cursor).toBeNull();
    state.subscriptions[4]!.checkpoint('new-endpoint');
    config.watches[0]!.id = 'renamed';
    writeFileSync(fixture.configPath, JSON.stringify(config));
    await fixture.handlers.get('session_start')!({}, fixture.context);
    expect(state.subscriptions[6]!.options.cursor).toBeNull();
    const checkpointFile = readdirSync(fixture.stateDir).find(name => name.endsWith('.cursors.json'))!;
    expect(JSON.parse(readFileSync(join(fixture.stateDir, checkpointFile), 'utf8'))).toEqual({});
  });
  it('drains a restored full inbox before a configured watch becomes ready', async () => {
    vi.useFakeTimers();
    const ready = Promise.withResolvers<void>();
    state.ready = ready.promise;
    const fixture = setup(100, true);
    const starting = fixture.handlers.get('session_start')!({}, fixture.context);
    await vi.advanceTimersByTimeAsync(100);
    expect(fixture.sendMessage).toHaveBeenCalledTimes(1);
    ready.resolve();
    await starting;
    expect(new Inbox(fixture.inboxPath).size).toBe(100);
  });
  it('waits through compaction and clears the inbox only after a durable transcript handoff', async () => {
    vi.useFakeTimers();
    const fixture = setup(1);
    fixture.setIdle(false);
    await fixture.handlers.get('session_start')!({}, fixture.context);
    await vi.advanceTimersByTimeAsync(500);
    expect(fixture.sendMessage).not.toHaveBeenCalled();
    fixture.setIdle(true);
    await vi.advanceTimersByTimeAsync(100);
    expect(fixture.sendMessage).toHaveBeenCalledTimes(1);
    await fixture.handlers.get('agent_end')!({}, fixture.context);
    expect(new Inbox(fixture.inboxPath).size).toBe(1);
    const batch = fixture.sendMessage.mock.calls[0]![0];
    writeFileSync(fixture.context.sessionManager.getSessionFile()!, JSON.stringify({type: 'custom_message', ...batch}) + '\n');
    await fixture.handlers.get('agent_end')!({}, fixture.context);
    expect(new Inbox(fixture.inboxPath).size).toBe(0);
  });
  it('delivers successive ephemeral batches only after in-memory handoff, without creating event state files', async () => {
    vi.useFakeTimers();
    const fixture = setup(0, true, true);
    await fixture.handlers.get('session_start')!({}, fixture.context);
    const subscription = state.subscriptions[0]!;
    expect(subscription.options.ttlMs).toBeUndefined();
    fixture.setIdle(false);
    subscription.accept({type: 'gap', cursor: 'first'});
    subscription.checkpoint('first');
    await vi.advanceTimersByTimeAsync(200);
    expect(fixture.sendMessage).not.toHaveBeenCalled();
    fixture.setIdle(true);
    await vi.advanceTimersByTimeAsync(100);
    expect(fixture.sendMessage).toHaveBeenCalledTimes(1);
    subscription.accept({type: 'gap', cursor: 'second'});
    await fixture.handlers.get('agent_end')!({}, fixture.context);
    await vi.advanceTimersByTimeAsync(200);
    expect(fixture.sendMessage).toHaveBeenCalledTimes(1);
    fixture.entries.push({type: 'custom_message', ...fixture.sendMessage.mock.calls[0]![0]});
    await fixture.handlers.get('agent_end')!({}, fixture.context);
    await vi.advanceTimersByTimeAsync(100);
    expect(fixture.sendMessage).toHaveBeenCalledTimes(2);
    expect(fixture.sendMessage.mock.calls[1]![0].details.events).toEqual([
      {server: 'test', watch: 'watch', observation: {type: 'gap', cursor: 'second'}},
    ]);
    expect(readdirSync(fixture.stateDir)).toEqual([]);
    await fixture.handlers.get('session_shutdown')!({}, fixture.context);
    expect(() => subscription.accept({type: 'gap', cursor: 'late'})).toThrow('Pi session changed');
    await fixture.handlers.get('session_start')!({}, fixture.context);
    expect(state.subscriptions[1]!.options.cursor).toBeNull();
    await vi.advanceTimersByTimeAsync(200);
    expect(fixture.sendMessage).toHaveBeenCalledTimes(2);
    expect(readdirSync(fixture.stateDir)).toEqual([]);
  });
  it('does not restore or overwrite saved event state in an ephemeral session', async () => {
    vi.useFakeTimers();
    const fixture = setup(1, true, true);
    await fixture.handlers.get('session_start')!({}, fixture.context);
    await vi.advanceTimersByTimeAsync(100);
    expect(fixture.sendMessage).not.toHaveBeenCalled();
    state.subscriptions[0]!.checkpoint('memory-only');
    state.subscriptions[0]!.accept({type: 'gap', cursor: 'memory-only'});
    expect(new Inbox(fixture.inboxPath).snapshot()[0]!.observation).toEqual({type: 'gap', cursor: '0'});
    expect(readdirSync(fixture.stateDir)).toHaveLength(1);
  });
});
