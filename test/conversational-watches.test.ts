import {createHash} from 'node:crypto';
import {mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import type {ExtensionAPI, ExtensionContext, ExtensionToolContext, ToolDefinition} from '@earendil-works/pi-coding-agent';
import {afterEach, expect, it, vi} from 'vitest';
import * as configModule from '../src/config.ts';
import extension from '../src/extension.ts';
import {SessionState} from '../src/session-state.ts';
import type {WatchDefinition} from '../src/watch-definitions.ts';
import {TestTransport} from './transport.ts';

vi.mock('../src/config.ts', async original => ({...await original<typeof configModule>(), createTransport: vi.fn()}));
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});
const descriptor = {name: 'weather.alert', description: 'Raised and cleared storm warnings', delivery: ['poll', 'push'],
  inputSchema: {type: 'object', required: ['stationId'], properties: {stationId: {type: 'string'}}, additionalProperties: false}, payloadSchema: {type: 'object'}};
const input = {server: 'weather', name: descriptor.name, arguments: {stationId: 'harbor'}};
const event = (id: string, timestamp = new Date().toISOString()) => ({eventId: id, name: descriptor.name, timestamp, cursor: id, data: {warning: 'raised'}});
type Listed = {watches: Array<{id: string; configuredId?: string; arguments: Record<string, unknown>; expiresAt?: number; status: string; phase?: string; pending: number; mode: string; warnings: string[]; lifetime: string}>};
async function setup({saved = false, configured = false, maxSubscriptions = 32} = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'pi-conversational-'));
  cleanups.push(() => rmSync(directory, {recursive: true, force: true}));
  const configPath = join(directory, 'events.json');
  const writeConfig = (watches: unknown[]) => writeFileSync(configPath, JSON.stringify({stateDir: '.',
    servers: {weather: {transport: 'stdio', command: 'unused'}}, client: {retryMs: 1, maxRetries: 0, pollFloorMs: 1, maxSubscriptions}, watches}));
  writeConfig(configured ? [{...input, id: 'configured', mode: 'push'}] : []);
  vi.stubEnv('PI_MCP_EVENTS_CONFIG', configPath);
  let transport: TestTransport;
  let streams: JSONRPCRequest[] = [];
  let fail = false;
  let pollEvents: ReturnType<typeof event>[] = [];
  let truncated = false;
  vi.mocked(configModule.createTransport).mockImplementation(() => {
    transport = new TestTransport(); streams = [];
    transport.handle = request => {
      if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
      if (request.method === 'events/stream') {
        if (fail) { transport.fail(request, -32024, {private: 'hidden-secret'}); return; }
        streams.push(request);
        transport.notify(request, 'active', {cursor: request.params?.cursor ?? 'initial', truncated});
      }
      if (request.method === 'events/poll') {
        transport.respond(request, {events: pollEvents, cursor: pollEvents.at(-1)?.cursor ?? request.params?.cursor ?? 'initial', truncated, hasMore: false, nextPollMs: 100_000});
      }
    };
    return transport;
  });
  const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => Promise<void>>();
  const tools = new Map<string, ToolDefinition>();
  let command: {handler: (args: string, ctx: ExtensionContext) => Promise<void>};
  const messages = vi.fn(); const emit = vi.fn();
  const pi = {registerMessageRenderer: vi.fn(), registerCommand: (_name: string, value: typeof command) => {command = value;},
    on: (name: string, handler: (event: unknown, ctx: ExtensionContext) => Promise<void>) => handlers.set(name, handler),
    registerTool: (tool: ToolDefinition) => tools.set(tool.name, tool), sendMessage: messages, events: {emit}} as unknown as ExtensionAPI;
  extension(pi);
  let sessionId = 'one';
  const ctx = {cwd: directory, isIdle: () => false, ui: {notify: vi.fn()}, sessionManager: {
    getSessionFile: () => saved ? join(directory, 'session.jsonl') : undefined,
    getSessionId: () => sessionId, getEntries: () => [],
  }} as unknown as ExtensionContext;
  const reload = () => handlers.get('session_start')!({}, ctx);
  cleanups.push(() => handlers.get('session_shutdown')!({}, ctx));
  await reload();
  const call = (action: string, params: Record<string, unknown> = {}) => tools.get(`mcp_events_${action}`)!.execute('call', params, undefined, undefined, ctx as ExtensionToolContext);
  const list = async () => (await call('list')).details as Listed;
  const statePath = () => join(directory, `${createHash('sha256').update(`${configPath}\0${sessionId}`).digest('hex')}.json`);
  return {call, list, reload, directory, command: (args: string) => command.handler(args, ctx), config: () => readFileSync(configPath, 'utf8'), statePath, messages, emit, writeConfig,
    newSession: async () => {sessionId = 'two'; await reload();},
    setFail: (value: boolean) => {fail = value;},
    replay: (values: ReturnType<typeof event>[]) => {pollEvents = values;},
    setGap: () => {truncated = true;},
    send: (id: string, timestamp?: string) => transport!.notify(streams.at(-1)!, 'event', event(id, timestamp)),
    getTransport: () => transport,
  };
}

it('discovers an unwatched server, defaults to push, returns duplicates, and retains accepted events on stop', async () => {
  const h = await setup();
  expect(h.getTransport()).toBeUndefined();
  expect(JSON.stringify((await h.call('discover')).details)).toContain('stationId');
  const started = (await h.call('start', input)).details as {watch: {id: string; status: string; mode: string; lifetime: string}};
  expect(started.watch).toMatchObject({status: 'active', mode: 'push'});
  expect(started.watch.lifetime).toContain('memory only');
  expect((await h.call('start', input)).details).toMatchObject({existing: true, watch: {id: started.watch.id}});
  h.send('accepted');
  await vi.waitFor(async () => expect((await h.list()).watches[0]!.pending).toBe(1));
  expect((await h.call('stop', {id: started.watch.id})).details).toMatchObject({watch: {status: 'stopped', phase: 'stopped', pending: 1}});
  h.send('too-late');
  expect((await h.list()).watches[0]!.pending).toBe(1);
  await h.reload();
  expect((await h.list()).watches).toEqual([]);
  expect(readdirSync(h.directory)).toEqual(['events.json']);
});

it('validates arguments, event names, modes, and receivers without false success or secrets', async () => {
  const h = await setup();
  for (const params of [{...input, arguments: {}}, {...input, name: 'missing'}, {...input, mode: 'webhook'}, {...input, mode: 'invalid'}, {...input, server: 'missing'}]) {
    const result = await h.call('start', params);
    expect(result.isError).toBe(true);
    expect(JSON.stringify(result)).not.toContain('hidden-secret');
  }
  expect((await h.list()).watches.every(watch => watch.status !== 'active')).toBe(true);
});

it('keeps the original after a failed replacement, including limits, then starts a valid replacement first', async () => {
  const h = await setup();
  const original = (await h.call('start', input)).details as {watch: {id: string}};
  h.setFail(true);
  const replacement = {...input, id: original.watch.id, arguments: {stationId: 'other'}};
  expect((await h.call('update', replacement)).isError).toBe(true);
  expect((await h.list()).watches.find(watch => watch.id === original.watch.id)!.status).toBe('active');
  h.setFail(false);
  expect((await h.call('update', replacement)).isError).not.toBe(true);
  expect((await h.list()).watches.find(watch => watch.id === original.watch.id)!.phase).toBe('stopped');
  const limited = await setup({maxSubscriptions: 1});
  const first = (await limited.call('start', input)).details as {watch: {id: string}};
  expect((await limited.call('update', {...replacement, id: first.watch.id})).isError).toBe(true);
  expect((await limited.list()).watches[0]!.status).toBe('active');
});

it('restores saved definitions and cursors, preserves stops, and isolates different sessions', async () => {
  const h = await setup({saved: true});
  const started = (await h.call('start', input)).details as {watch: {id: string}};
  h.send('saved-cursor');
  await vi.waitFor(() => expect(new SessionState(h.statePath()).size).toBe(1));
  await h.reload();
  expect((await h.list()).watches[0]).toMatchObject({id: started.watch.id, status: 'active', pending: 1});
  const stream = h.getTransport().messages.find(({message}) => 'method' in message && message.method === 'events/stream')!;
  expect('params' in stream.message && stream.message.params?.cursor).toBe('saved-cursor');
  await h.call('stop', {id: started.watch.id}); await h.reload();
  expect((await h.list()).watches[0]).toMatchObject({phase: 'stopped', status: 'stopped', pending: 1});
  await h.newSession();
  expect((await h.list()).watches).toEqual([]);
});

it('restores expired saved watches with bounded poll catch-up, including out-of-order eligible events', async () => {
  const h = await setup({saved: true});
  const expiry = Date.now() + 60_000;
  const started = (await h.call('start', {...input, expiresAt: expiry})).details as {watch: {id: string}};
  h.send('before');
  await vi.waitFor(() => expect(new SessionState(h.statePath()).size).toBe(1));
  // Simulate time spent disconnected without waiting for the wall clock.
  const state = new SessionState(h.statePath());
  const definition = state.watches[0]!;
  const expired = Date.now() - 1000;
  state.saveWatch({...definition, expiresAt: expired});
  h.replay([event('later', new Date(expired + 1).toISOString()), event('eligible', new Date(expired).toISOString())]);
  await h.reload();
  const listed = (await h.list()).watches[0]!;
  expect(listed).toMatchObject({id: started.watch.id, phase: 'expired', status: 'stopped', pending: 2});
  expect(new SessionState(h.statePath()).snapshot().map(item => item.observation.type === 'event' && item.observation.event.eventId)).toEqual(['before', 'eligible']);
  await h.reload();
  expect((await h.list()).watches[0]!.phase).toBe('expired');
});

it('reports null replay cursors, interrupted catch-up, and restoration conflicts without blocking other watches', async () => {
  const h = await setup({saved: true});
  await h.call('start', input);
  const state = new SessionState(h.statePath());
  const definition = state.watches[0]!;
  state.saveWatch({...definition, phase: 'catching-up', expiresAt: Date.now() - 1000});
  state.retainCursors([]);
  await h.reload();
  expect((await h.list()).watches[0]).toMatchObject({phase: 'expired'});
  expect(JSON.stringify(h.messages.mock.calls)).toContain('interrupted catch-up');
  expect(JSON.stringify(h.messages.mock.calls)).toContain('without a saved cursor');
  const second: WatchDefinition = {...definition, id: 'conflict', phase: 'active'};
  new SessionState(h.statePath()).saveWatch(second);
  h.writeConfig([{...input, id: 'configured', mode: 'push'}]);
  await h.reload();
  expect((await h.list()).watches.find(watch => watch.id === 'configured')!.status).toBe('active');
  expect((await h.list()).watches.find(watch => watch.id === 'conflict')!.status).toBe('could not start');
});

it('expires a live watch and cancels later catch-up after an explicit stop', async () => {
  const h = await setup({saved: true});
  const expiry = Date.now() + 100;
  await h.call('start', {...input, expiresAt: expiry});
  await vi.waitFor(async () => expect((await h.list()).watches[0]!.phase).toBe('expired'));
  expect((await h.list()).watches[0]!.status).toBe('stopped');
  const next = (await h.call('start', {...input, expiresAt: Date.now() + 100})).details as {watch: {id: string}};
  await h.call('stop', {id: next.watch.id});
  await new Promise(resolve => setTimeout(resolve, 130));
  await h.reload();
  expect((await h.list()).watches.find(watch => watch.id === next.watch.id)!.phase).toBe('stopped');
});

it('retains replay gaps and terminal failures in status and the conversation', async () => {
  const h = await setup(); h.setGap();
  await h.call('start', input);
  expect((await h.list()).watches[0]!.status).toBe('active');
  expect((await h.list()).watches[0]!.warnings.join(' ')).toContain('Events may have been missed');
  h.getTransport().onclose?.();
  await vi.waitFor(async () => expect((await h.list()).watches[0]!.status).toBe('failed'));
  expect(JSON.stringify(h.messages.mock.calls)).toContain('stopped.');
});

it('finishes an interrupted replacement on restoration and keeps unrelated watches after a server disappears', async () => {
  const h = await setup({saved: true});
  const started = (await h.call('start', input)).details as {watch: {id: string}};
  const state = new SessionState(h.statePath());
  const original = state.watches[0]!;
  state.saveWatch({...original, id: 'replacement', replaces: started.watch.id, arguments: {stationId: 'hilltop'}});
  state.saveWatch({...original, id: 'missing-server', server: 'removed'});
  await h.reload();
  const listed = (await h.list()).watches;
  expect(listed.find(watch => watch.id === 'replacement')!.status).toBe('active');
  expect(listed.find(watch => watch.id === original.id)!.phase).toBe('stopped');
  expect(listed.find(watch => watch.id === 'missing-server')!.status).toBe('connection failed');
  expect(new SessionState(h.statePath()).watches.find(watch => watch.id === 'replacement')!.replaces).toBeUndefined();
});

it('calculates a duration from the client clock and rejects conflicting expiry inputs', async () => {
  const h = await setup();
  const before = Date.now();
  const result = (await h.call('start', {...input, durationMs: 60_000})).details as {watch: {expiresAt: number}};
  expect(result.watch.expiresAt).toBeGreaterThanOrEqual(before + 60_000);
  expect(result.watch.expiresAt).toBeLessThanOrEqual(Date.now() + 60_000);
  expect((await h.call('start', {...input, durationMs: 1000, expiresAt: Date.now() + 1000})).isError).toBe(true);
});

it.each(['push', 'poll'])('keeps only the %s replacement of a configured watch after saved reload', async mode => {
  const h = await setup({saved: true, configured: true});
  const config = h.config();
  const result = await h.call('update', {...input, id: 'configured', mode, arguments: {stationId: 'hilltop'}});
  expect(result.isError).not.toBe(true);
  const replacement = (result.details as {watch: {id: string}}).watch;
  await h.reload();
  expect((await h.list()).watches.filter(w => w.status === 'active')).toEqual([
    expect.objectContaining({id: replacement.id, configuredId: 'configured', mode, arguments: {stationId: 'hilltop'}}),
  ]);
  expect(h.config()).toBe(config);
  await h.call('stop', {id: replacement.id});
  await h.reload();
  expect((await h.list()).watches.every(w => w.status === 'stopped')).toBe(true);
  await h.newSession();
  expect((await h.list()).watches).toEqual([expect.objectContaining({id: 'configured', status: 'active'})]);
});

it('saves a configured stop and can explicitly restart the same ID and cursor', async () => {
  const h = await setup({saved: true, configured: true});
  h.send('config-cursor');
  await vi.waitFor(() => expect(new SessionState(h.statePath()).size).toBe(1));
  await h.call('stop', {id: 'configured'});
  await h.reload();
  expect((await h.list()).watches).toEqual([expect.objectContaining({id: 'configured', phase: 'stopped', pending: 1})]);
  await h.command('start configured');
  expect((await h.list()).watches[0]).toMatchObject({id: 'configured', status: 'active'});
  const stream = h.getTransport().messages.find(({message}) => 'method' in message && message.method === 'events/stream')!;
  expect('params' in stream.message && stream.message.params?.cursor).toBe('config-cursor');
  await h.reload();
  expect((await h.list()).watches[0].status).toBe('active');
});

it('discards in-memory configured overrides on reload', async () => {
  const h = await setup({configured: true});
  await h.call('update', {...input, id: 'configured', mode: 'poll'});
  await h.reload();
  expect((await h.list()).watches).toEqual([expect.objectContaining({id: 'configured', mode: 'push', status: 'active'})]);
  await h.call('stop', {id: 'configured'});
  await h.reload();
  expect((await h.list()).watches[0].status).toBe('active');
});

it('keeps the previous configured override after a failed replacement or a stop of its old ID', async () => {
  const h = await setup({saved: true, configured: true});
  const replacement = ((await h.call('update', {...input, id: 'configured', arguments: {stationId: 'hilltop'}})).details as {watch: {id: string}}).watch;
  h.setFail(true);
  expect((await h.call('update', {...input, id: replacement.id, arguments: {stationId: 'other'}})).isError).toBe(true);
  await h.call('stop', {id: 'configured'});
  await h.command('start configured');
  expect((await h.call('update', {...input, id: 'configured'})).isError).toBe(true);
  h.setFail(false);
  await h.reload();
  expect((await h.list()).watches.filter(w => w.status === 'active')).toEqual([expect.objectContaining({id: replacement.id})]);
});

it('rejects duplicate starts with a different explicit expiry and updates expiry without reconnecting', async () => {
  const h = await setup({saved: true, configured: true, maxSubscriptions: 1});
  expect((await h.call('start', {...input, durationMs: 60_000})).isError).toBe(true);
  expect((await h.call('start', input)).details).toMatchObject({existing: true, watch: {id: 'configured'}});
  const before = h.getTransport().messages.length;
  const expiry = Date.now() + 60_000;
  expect((await h.call('update', {...input, id: 'configured', expiresAt: expiry})).details).toMatchObject({watch: {id: 'configured', expiresAt: expiry}});
  expect(h.getTransport().messages).toHaveLength(before);
  await h.reload();
  expect((await h.list()).watches[0]).toMatchObject({id: 'configured', expiresAt: expiry, status: 'active'});
  expect((await h.call('start', {...input, expiresAt: expiry})).details).toMatchObject({existing: true});
  expect((await h.call('start', {...input, durationMs: 120_000})).isError).toBe(true);
  await h.call('update', {...input, id: 'configured'});
  expect((await h.list()).watches[0].expiresAt).toBeUndefined();
});

it('arms expiry when changing a configured watch without replacing its subscription', async () => {
  const h = await setup({saved: true, configured: true});
  await h.call('update', {...input, id: 'configured', durationMs: 100});
  await vi.waitFor(async () => expect((await h.list()).watches[0].phase).toBe('expired'));
  await h.reload();
  expect((await h.list()).watches[0]).toMatchObject({phase: 'expired', status: 'stopped'});
});

it('cleans up 520 failed starts so a valid watch still starts after reload', async () => {
  const h = await setup({saved: true});
  for (let i = 0; i < 520; i++) {
    expect((await h.call('start', {...input, arguments: {invalid: i}})).isError).toBe(true);
  }
  expect((await h.list()).watches).toEqual([]);
  await h.reload();
  expect((await h.call('start', input)).isError).not.toBe(true);
}, 30_000);

it('prunes old stopped history without dropping accepted inbox events', async () => {
  const h = await setup({saved: true});
  const first = ((await h.call('start', input)).details as {watch: {id: string}}).watch;
  h.send('retained-event');
  await vi.waitFor(() => expect(new SessionState(h.statePath()).size).toBe(1));
  await h.call('stop', {id: first.id});
  const state = new SessionState(h.statePath());
  const stopped = state.watches[0];
  for (let i = 0; i < 511; i++) state.saveWatch({...stopped, id: `old-${i}`});
  await h.reload();
  expect((await h.call('start', input)).isError).not.toBe(true);
  expect((await h.list()).watches).toHaveLength(512);
  expect((await h.list()).watches.some(w => w.id === first.id)).toBe(false);
  expect(new SessionState(h.statePath()).size).toBe(1);
}, 30_000);

it('keeps cursor capacity available across more than 512 successful starts and stops', async () => {
  const h = await setup({configured: true});
  const other = {...input, arguments: {stationId: 'hilltop'}};
  for (let i = 0; i < 520; i++) {
    const result = await h.call('start', other);
    expect(result.isError).not.toBe(true);
    const id = (result.details as {watch: {id: string}}).watch.id;
    expect((await h.call('stop', {id})).isError).not.toBe(true);
  }
  expect((await h.list()).watches).toHaveLength(512);
  expect((await h.call('start', other)).isError).not.toBe(true);
}, 30_000);

it('retains declared delivery options when a stopped watch resumes', async () => {
  const h = await setup({saved: true});
  h.writeConfig([{...input, id: 'configured', mode: 'poll', maxEvents: 7, maxAgeMs: 1000, ttlMs: 2000}]);
  await h.reload();
  await h.call('stop', {id: 'configured'});
  await h.reload();
  await h.command('start configured');
  const request = h.getTransport().messages.find(({message}) => 'method' in message && message.method === 'events/poll')!;
  expect('params' in request.message && request.message.params).toMatchObject({maxEvents: 7, maxAgeMs: 1000});
  expect(new SessionState(h.statePath()).watches[0]).toMatchObject({maxEvents: 7, maxAgeMs: 1000, ttlMs: 2000});
});

it('finishes an interrupted configured replacement without restoring both watches', async () => {
  const h = await setup({saved: true, configured: true});
  const definition: WatchDefinition = {...input, id: 'replacement', configuredId: 'configured', replaces: 'configured', mode: 'poll', phase: 'active'};
  new SessionState(h.statePath()).saveWatch(definition);
  await h.reload();
  expect((await h.list()).watches.filter(w => w.status === 'active')).toEqual([expect.objectContaining({id: 'replacement'})]);
  expect(new SessionState(h.statePath()).configuredOverrides).toEqual({configured: 'replacement'});
  await h.reload();
  expect((await h.list()).watches.filter(w => w.status === 'active')).toEqual([expect.objectContaining({id: 'replacement'})]);
});
