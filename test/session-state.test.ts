import * as fs from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {Checkpoints, EventsClient, EventsWire, Inbox, SessionState, EventsClientErrorCode as ErrorCode, type InboxItem, type PersistenceWarning} from '../src/index.ts';
import {TestTransport} from './transport.ts';

vi.mock('node:fs', async importOriginal => ({...await importOriginal<typeof import('node:fs')>()}));
const directories: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const path of directories.splice(0)) fs.rmSync(path, {recursive: true, force: true});
});
function file() {
  const directory = fs.mkdtempSync(join(tmpdir(), 'pi-events-session-')); directories.push(directory);
  return join(directory, 'state.json');
}
const item = (id: string): InboxItem => ({server: 'server', watch: 'watch', observation: {type: 'event', event: {
  eventId: id, cursor: id, name: 'example.changed', timestamp: '2026-10-25T12:00:00Z', data: {value: id},
}}});
const denied = () => { throw Object.assign(new Error('private OS error'), {code: 'EACCES'}); };
function seeded(path: string) {
  const state = new SessionState(path);
  state.accept(item('first')); state.setCursor('watch', 'first');
  return state;
}

describe('session persistence', () => {
  it('restores saved observations and cursors, including acknowledgements and cursor removal', () => {
    const path = file(); const state = seeded(path);
    state.accept(item('second')); state.acknowledge(1);
    state.setCursor('other', 'ignored'); state.retainCursors(['watch']);
    const restored = new SessionState(path);
    expect(restored.mode).toBe('persistent'); expect(restored.warning).toBeUndefined();
    expect(restored.snapshot()).toEqual([item('second')]); expect(restored.getCursor('watch')).toBe('first');
    expect(restored.getCursor('other')).toBeNull();
    restored.setCursor('watch', null); expect(new SessionState(path).getCursor('watch')).toBeNull();
  });

  it('does no filesystem work or warnings when persistence is not requested', () => {
    const open = vi.spyOn(fs, 'openSync'); const mkdir = vi.spyOn(fs, 'mkdirSync'); const onWarning = vi.fn();
    const state = new SessionState(undefined, {onWarning});
    state.accept(item('first')); state.setCursor('watch', 'first'); state.acknowledge(1); state.retainCursors([]);
    expect(state.mode).toBe('memory'); expect(state.warning).toBeUndefined();
    expect(new SessionState().size).toBe(0); expect(new SessionState().getCursor('watch')).toBeNull();
    expect(open).not.toHaveBeenCalled(); expect(mkdir).not.toHaveBeenCalled(); expect(onWarning).not.toHaveBeenCalled();
  });

  it.each(['inbox', 'checkpoints'] as const)('preserves a corrupt %s file and continues with recoverable state', broken => {
    const path = file(); seeded(path);
    const corrupt = broken === 'inbox' ? path : `${path}.cursors.json`;
    fs.writeFileSync(corrupt, 'broken private contents');
    const files = [fs.readFileSync(path), fs.readFileSync(`${path}.cursors.json`)];
    const onWarning = vi.fn(); const state = new SessionState(path, {onWarning});
    expect(state.mode).toBe('degraded'); expect(state.getCursor('watch')).toBeNull();
    expect(state.snapshot()).toEqual(broken === 'inbox' ? [] : [item('first')]);
    state.accept(item('next')); state.setCursor('watch', 'next');
    expect(state.snapshot().at(-1)).toEqual(item('next')); expect(state.getCursor('watch')).toBe('next');
    expect([fs.readFileSync(path), fs.readFileSync(`${path}.cursors.json`)]).toEqual(files);
    expect(onWarning).toHaveBeenCalledTimes(1);
    expect(state.warning).toMatchObject({code: 'PERSISTENCE_UNAVAILABLE', operation: 'read', error: {code: ErrorCode.InvalidState}});
    expect(state.warning!.message).not.toContain('private contents');
  });

  it('falls back when reads are denied, while invalid options still fail', () => {
    vi.spyOn(fs, 'openSync').mockImplementation(denied);
    const state = new SessionState(file());
    state.accept(item('next')); state.setCursor('watch', 'next');
    expect(state.size).toBe(1); expect(state.warning?.error.code).toBe(ErrorCode.StateReadFailed);
    expect(() => new SessionState(file(), {checkpoints: {maxEntries: 0}})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect(() => new SessionState('')).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
  });

  it('preserves oversized saved state and enforces capacity in the replacement memory store', () => {
    const path = file(); const original = seeded(path); original.accept(item('second'));
    const saved = fs.readFileSync(path);
    const state = new SessionState(path, {inbox: {maxItems: 1}});
    expect(state.mode).toBe('degraded'); state.accept(item('live'));
    expect(() => state.accept(item('overflow'))).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    expect(state.snapshot()).toEqual([item('live')]); expect(fs.readFileSync(path)).toEqual(saved);
  });

  it.each(['accept', 'checkpoint', 'acknowledge', 'retain'] as const)('switches both stores to memory after a failed %s', operation => {
    const path = file(); const state = seeded(path); state.accept(item('second'));
    const saved = [fs.readFileSync(path), fs.readFileSync(`${path}.cursors.json`)];
    const writes = vi.spyOn(fs, 'renameSync').mockImplementation(denied);
    if (operation === 'accept') state.accept(item('third'));
    if (operation === 'checkpoint') state.setCursor('watch', 'second');
    if (operation === 'acknowledge') state.acknowledge(1);
    if (operation === 'retain') state.retainCursors((function* () { yield 'other'; })());
    expect(state.mode).toBe('degraded'); expect(state.warning?.operation).toBe('write');
    if (operation === 'accept') expect(state.snapshot()).toEqual([item('first'), item('second'), item('third')]);
    if (operation === 'checkpoint') expect(state.getCursor('watch')).toBe('second');
    if (operation === 'acknowledge') expect(state.snapshot()).toEqual([item('second')]);
    if (operation === 'retain') expect(state.getCursor('watch')).toBeNull();
    state.accept(item('later')); state.setCursor('watch', 'later');
    expect(state.snapshot().at(-1)).toEqual(item('later')); expect(state.getCursor('watch')).toBe('later');
    expect(writes).toHaveBeenCalledTimes(1);
    expect([fs.readFileSync(path), fs.readFileSync(`${path}.cursors.json`)]).toEqual(saved);
    writes.mockRestore();
    state.acknowledge(1); state.setCursor('watch', 'still-memory');
    expect(state.mode).toBe('degraded');
    expect([fs.readFileSync(path), fs.readFileSync(`${path}.cursors.json`)]).toEqual(saved);
  });

  it.skipIf(process.platform === 'win32').each(['accept', 'acknowledge', 'checkpoint'] as const)('does not repeat %s when replacement succeeded before sync failed', operation => {
    const path = file(); const state = seeded(path); state.accept(item('second'));
    const sync = fs.fsyncSync;
    vi.spyOn(fs, 'fsyncSync').mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) denied();
      sync(fd);
    });
    if (operation === 'accept') state.accept(item('third'));
    if (operation === 'acknowledge') state.acknowledge(1);
    if (operation === 'checkpoint') state.setCursor('watch', 'second');
    expect(state.mode).toBe('degraded'); expect(state.warning?.error.details?.replaced).toBe(true);
    expect(state.snapshot()).toEqual(new Inbox(path).snapshot());
    expect(state.getCursor('watch')).toBe(new Checkpoints(`${path}.cursors.json`).get('watch'));
    const pending = state.snapshot(); state.accept(item('later')); state.setCursor('watch', 'later');
    expect(state.snapshot()).toEqual([...pending, item('later')]); expect(state.getCursor('watch')).toBe('later');
    expect(new Inbox(path).snapshot()).toEqual(pending);
  });

  it.each(['throw', 'reject'] as const)('retains the warning and continues after a callback %s', failure => {
    const path = file();
    const onWarning = vi.fn(() => {
      if (failure === 'throw') throw new Error('conversation unavailable');
      return Promise.reject(new Error('conversation unavailable'));
    });
    const state = new SessionState(path, {onWarning});
    vi.spyOn(fs, 'renameSync').mockImplementation(denied);
    state.accept(item('one')); state.accept(item('two')); state.setCursor('watch', 'two');
    expect(state.size).toBe(2); expect(onWarning).toHaveBeenCalledTimes(1);
    expect(onWarning).toHaveBeenCalledWith(state.warning);
    expect(state.warning!.message).toContain('continue in memory');
    expect(state.warning!.message).toContain('will not be saved');
    expect(state.warning!.message).not.toContain('private OS error');
  });

  it.each(['persistent', 'degraded'] as const)('enforces validation and capacity in %s mode', mode => {
    const path = file(); const onWarning = vi.fn();
    const state = new SessionState(path, {inbox: {maxItems: 1}, checkpoints: {maxEntries: 1}, onWarning});
    state.accept(item('one')); state.setCursor('watch', 'one');
    if (mode === 'degraded') {
      vi.spyOn(fs, 'renameSync').mockImplementation(denied);
      state.setCursor('watch', 'new-position');
    }
    expect(() => state.accept(item('two'))).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    expect(() => state.setCursor('other', 'two')).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    expect(() => state.accept({...item('one'), server: ''})).toThrow(expect.objectContaining({code: ErrorCode.InvalidState}));
    expect(() => state.acknowledge(2)).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect(state.mode).toBe(mode); expect(onWarning).toHaveBeenCalledTimes(mode === 'degraded' ? 1 : 0);
    expect(new SessionState(path).snapshot()).toEqual([item('one')]);
  });

  it('continues client delivery and cursor resume after storage failure without consuming a retry', async () => {
    const path = file(); const warnings: PersistenceWarning[] = [];
    const state = new SessionState(path, {onWarning: warning => { warnings.push(warning); }});
    vi.spyOn(fs, 'renameSync').mockImplementation(denied);
    const cursors: unknown[] = [];
    for (const id of ['one', 'two', 'three']) {
      const transport = new TestTransport();
      transport.handle = request => {
        if (request.method === 'events/list') transport.respond(request, {events: [{name: 'example.changed', delivery: ['poll'], inputSchema: {}, payloadSchema: {}}]});
        if (request.method === 'events/poll') {
          cursors.push(request.params!.cursor);
          const observation = item(id).observation;
          transport.respond(request, {events: observation.type === 'event' ? [observation.event] : [], cursor: id, truncated: false, hasMore: false, nextPollMs: 60_000});
        }
      };
      const client = new EventsClient(new EventsWire(transport), {maxRetries: 0});
      try {
        await client.connect();
        const watch = client.watch({name: 'example.changed', mode: 'poll', arguments: {}, cursor: state.getCursor('watch')},
          observation => state.accept({server: 'server', watch: 'watch', observation}), cursor => state.setCursor('watch', cursor));
        await watch.ready; await watch.stop();
      } finally { await client.close(); }
      expect(state.snapshot()).toEqual([item(id)]); state.acknowledge(1);
    }
    expect(cursors).toEqual([null, 'one', 'two']); expect(warnings).toHaveLength(1);
    expect(state.getCursor('watch')).toBe('three'); expect(state.size).toBe(0);
    expect(new SessionState(path).getCursor('watch')).toBeNull();
  });
});
