import * as fs from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {Checkpoints, Inbox, EventsClient, EventsWire, EventProtocolError, EventsClientErrorCode as ErrorCode, type InboxItem, type Observation} from '../src/index.ts';

import {TestTransport} from './transport.ts';

vi.mock('node:fs', async importOriginal => ({...await importOriginal<typeof import('node:fs')>()}));
const directories: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const path of directories.splice(0)) fs.rmSync(path, {recursive: true, force: true});
});
function directory() { const path = fs.mkdtempSync(join(tmpdir(), 'pi-events-state-')); directories.push(path); return path; }
function item(id = 'e'): InboxItem {
  return {server: 'server', watch: 'watch', observation: {type: 'event', event: {
    eventId: id, name: 'example.changed', timestamp: '2026-10-25T12:00:00Z', cursor: id, data: {nested: {value: 'café'}},
  }}};
}
const storedInbox = (items: InboxItem[]) => JSON.stringify({version: 1, items});
const storedCursors = (cursors: Array<[string, string]>) => JSON.stringify({version: 1, cursors});

describe('event inbox', () => {
  it('restores pending observations and acknowledges only the captured prefix', () => {
    const path = join(directory(), 'inbox.json');
    let inbox = new Inbox(path); inbox.accept(item('one')); inbox.accept(item('two'));
    inbox = new Inbox(path);
    const batch = inbox.snapshot(); expect(batch).toEqual([item('one'), item('two')]);
    inbox.accept(item('three')); inbox.acknowledge(batch.length);
    expect(new Inbox(path).snapshot()).toEqual([item('three')]);
    inbox.acknowledge(1); expect(new Inbox(path).size).toBe(0);
  });

  it('detaches incoming items and snapshots, including nested event payloads', () => {
    const inbox = new Inbox(); const source = item(); inbox.accept(source);
    if (source.observation.type === 'event') source.observation.event.data.nested = {value: 'changed'};
    const snapshot = inbox.snapshot();
    snapshot[0]!.server = 'changed';
    if (snapshot[0]!.observation.type === 'event') snapshot[0]!.observation.event.data.nested = {value: 'also changed'};
    expect(inbox.snapshot()).toEqual([item()]);
  });

  it('preserves all observation variants and protocol error details across restart', () => {
    const path = join(directory(), 'inbox.json');
    const error = new EventProtocolError({code: -32001, message: 'source unavailable', data: {retry: true}}, {cause: new Error('internal only')});
    const observations: Observation[] = [
      item().observation, {type: 'gap', cursor: null}, {type: 'status', status: 'retrying'},
      {type: 'status', status: 'stopped'}, {type: 'status', status: 'source-error', error},
      {type: 'status', status: 'webhook-delivery', deliveryStatus: {active: false, lastError: 'timeout', retryAfterMs: 100}},
    ];
    const inbox = new Inbox(path);
    observations.forEach(observation => inbox.accept({server: 's', watch: 'w', observation}));
    const restored = new Inbox(path).snapshot();
    const expected = [...observations];
    expected[4] = {type: 'status', status: 'source-error', error: new EventProtocolError({code: error.code, message: error.message, data: error.data})};
    expect(restored.map(item => item.observation)).toEqual(expected);
    const status = restored[4]!.observation;
    expect(status.type === 'status' && status.status === 'source-error' && status.error).toBeInstanceOf(EventProtocolError);
    expect(fs.readFileSync(path, 'utf8')).not.toMatch(/internal only|"stack"|"cause"/);
    if (status.type === 'status' && status.status === 'source-error') {
      (status.error.data as {retry: boolean}).retry = false;
      expect(inbox.snapshot()[4]!.observation).toMatchObject({error: {data: {retry: true}}});
    }
  });

  it.each([-1, 0.5, NaN, Infinity, 2])('rejects invalid acknowledgement counts: %s', count => {
    const inbox = new Inbox(); inbox.accept(item());
    expect(() => inbox.acknowledge(count)).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect(inbox.size).toBe(1);
  });

  it('bounds items and UTF-8 bytes on acceptance and reload', () => {
    const path = join(directory(), 'inbox.json');
    const inbox = new Inbox(path, {maxItems: 1}); inbox.accept(item());
    expect(() => inbox.accept(item('overflow'))).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded, details: {limit: 'maxItems', max: 1}}));
    expect(new Inbox(path).snapshot()).toEqual([item()]);
    fs.writeFileSync(path, storedInbox([item(), item('two')]));
    expect(() => new Inbox(path, {maxItems: 1})).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    const bytes = Buffer.byteLength(storedInbox([item()]));
    const exact = new Inbox(undefined, {maxBytes: bytes}); exact.accept(item());
    const short = new Inbox(undefined, {maxBytes: bytes - 1});
    expect(() => short.accept(item())).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded, details: {limit: 'maxBytes', max: bytes - 1}}));
    expect(short.size).toBe(0);
  });

  it.each([
    '{bad json', JSON.stringify([]), JSON.stringify({version: 2, items: []}),
    JSON.stringify({version: 1, items: [{server: 's', watch: 'w', observation: {type: 'unknown'}}]}),
    Buffer.from([0xff]),
  ])('rejects corrupt or unsupported files without changing them: %s', contents => {
    const path = join(directory(), 'inbox.json'); fs.writeFileSync(path, contents);
    expect(() => new Inbox(path)).toThrow(expect.objectContaining({code: ErrorCode.InvalidState}));
    expect(fs.readFileSync(path)).toEqual(Buffer.from(contents));
  });

  it('rejects invalid observations and unserializable payloads without changing the inbox', () => {
    const inbox = new Inbox(); inbox.accept(item());
    expect(() => inbox.accept({server: '', watch: 'w', observation: {type: 'gap', cursor: null}})).toThrow(expect.objectContaining({code: ErrorCode.InvalidState}));
    const source = item();
    if (source.observation.type === 'event') source.observation.event.data.circular = source;
    expect(() => inbox.accept(source)).toThrow(expect.objectContaining({code: ErrorCode.InvalidState}));
    for (const value of [undefined, NaN, Infinity, () => {}, new Date(), 1n]) {
      const source = item();
      if (source.observation.type === 'event') source.observation.event.data.invalid = value;
      expect(() => inbox.accept(source)).toThrow(expect.objectContaining({code: ErrorCode.InvalidState}));
    }
    expect(inbox.snapshot()).toEqual([item()]);
  });
});

describe('event checkpoints', () => {
  it('restores opaque keys and cursors, removes null entries, and retains only active keys', () => {
    const path = join(directory(), 'cursors.json'); let checkpoints = new Checkpoints(path);
    expect(checkpoints.get('missing')).toBeNull();
    checkpoints.set('one', 'a'); checkpoints.set('two', ''); checkpoints.set('__proto__', 'prototype'); checkpoints.set('toString', 'method');
    checkpoints = new Checkpoints(path);
    expect(checkpoints.get('__proto__')).toBe('prototype'); expect(checkpoints.get('toString')).toBe('method'); expect(checkpoints.get('two')).toBe('');
    checkpoints.set('one', null); checkpoints.retain(['two', '__proto__']);
    const restored = new Checkpoints(path);
    expect(restored.get('one')).toBeNull(); expect(restored.get('toString')).toBeNull();
    expect(restored.get('two')).toBe(''); expect(restored.get('__proto__')).toBe('prototype');
  });

  it('bounds entry counts and bytes on updates and reload', () => {
    const path = join(directory(), 'cursors.json'); const checkpoints = new Checkpoints(path, {maxEntries: 1});
    checkpoints.set('one', 'a'); checkpoints.set('one', 'replacement');
    expect(() => checkpoints.set('two', 'b')).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    expect(new Checkpoints(path).get('one')).toBe('replacement');
    fs.writeFileSync(path, storedCursors([['one', 'a'], ['two', 'b']]));
    expect(() => new Checkpoints(path, {maxEntries: 1})).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    const limited = new Checkpoints(undefined, {maxBytes: 50});
    expect(() => limited.set('one', 'x'.repeat(100))).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    expect(limited.get('one')).toBeNull();
  });

  it.each([storedCursors([['same', 'a'], ['same', 'b']]), JSON.stringify({version: 1, cursors: [['x', null]]}), JSON.stringify({version: 2, cursors: []})])('rejects duplicate, invalid, or unsupported persisted checkpoints', contents => {
    const path = join(directory(), 'cursors.json'); fs.writeFileSync(path, contents);
    expect(() => new Checkpoints(path)).toThrow(expect.objectContaining({code: ErrorCode.InvalidState}));
  });

  it('does not write for unchanged cursors or retain sets', () => {
    const path = join(directory(), 'cursors.json'); const checkpoints = new Checkpoints(path); checkpoints.set('one', 'a');
    const writes = vi.spyOn(fs, 'writeFileSync');
    checkpoints.set('one', 'a'); checkpoints.set('missing', null); checkpoints.retain(['one']);
    expect(writes).not.toHaveBeenCalled();
    expect(() => checkpoints.set('', 'a')).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
  });
});

describe('state files', () => {
  it('does no filesystem work in memory-only mode and starts fresh for new instances', () => {
    const calls = [vi.spyOn(fs, 'openSync'), vi.spyOn(fs, 'readSync'), vi.spyOn(fs, 'mkdirSync'), vi.spyOn(fs, 'writeFileSync'), vi.spyOn(fs, 'renameSync')];
    const inbox = new Inbox(); const checkpoints = new Checkpoints();
    inbox.accept(item()); inbox.snapshot(); inbox.acknowledge(1);
    checkpoints.set('one', 'a'); checkpoints.get('one'); checkpoints.retain([]);
    expect(new Inbox().size).toBe(0); expect(new Checkpoints().get('one')).toBeNull();
    calls.forEach(call => expect(call).not.toHaveBeenCalled());
  });

  it('starts missing files empty without creating them until the first write', () => {
    const root = directory(); const path = join(root, 'nested', 'inbox.json'); const inbox = new Inbox(path);
    expect(inbox.size).toBe(0); expect(fs.existsSync(join(root, 'nested'))).toBe(false);
    inbox.acknowledge(0); expect(fs.existsSync(path)).toBe(false);
    inbox.accept(item()); expect(new Inbox(path).snapshot()).toEqual([item()]);
    if (process.platform !== 'win32') {
      expect(fs.statSync(path).mode & 0o777).toBe(0o600);
      expect(fs.statSync(join(root, 'nested')).mode & 0o777).toBe(0o700);
    }
    expect(fs.readdirSync(join(root, 'nested'))).toEqual(['inbox.json']);
  });

  it.each(['writeFileSync', 'fsyncSync', 'renameSync'] as const)('preserves prior state and removes its temporary file when %s fails', operation => {
    const root = directory(); const path = join(root, 'inbox.json'); const inbox = new Inbox(path); inbox.accept(item('one'));
    vi.spyOn(fs, operation).mockImplementationOnce(() => { throw new Error('simulated write failure'); });
    expect(() => inbox.accept(item('two'))).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    expect(inbox.snapshot()).toEqual([item('one')]); expect(new Inbox(path).snapshot()).toEqual([item('one')]);
    expect(fs.readdirSync(root)).toEqual(['inbox.json']);
    inbox.accept(item('two')); expect(new Inbox(path).snapshot()).toEqual([item('one'), item('two')]);
  });

  it('keeps acknowledgements and checkpoint mutations retryable after write failure', () => {
    const root = directory(); const path = join(root, 'inbox.json'); const inbox = new Inbox(path); inbox.accept(item());
    vi.spyOn(fs, 'renameSync').mockImplementationOnce(() => { throw new Error('rename failed'); });
    expect(() => inbox.acknowledge(1)).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    expect(inbox.size).toBe(1); inbox.acknowledge(1); expect(new Inbox(path).size).toBe(0);
    const cursorPath = join(root, 'cursors.json'); const checkpoints = new Checkpoints(cursorPath); checkpoints.set('one', 'a');
    vi.spyOn(fs, 'renameSync').mockImplementationOnce(() => { throw new Error('rename failed'); });
    expect(() => checkpoints.set('one', 'b')).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    expect(checkpoints.get('one')).toBe('a'); expect(new Checkpoints(cursorPath).get('one')).toBe('a');
    checkpoints.set('one', 'b'); expect(new Checkpoints(cursorPath).get('one')).toBe('b');
  });

  it.skipIf(process.platform === 'win32')('preserves replaced state and requires reopening after a directory sync failure', () => {
    const root = directory(); const path = join(root, 'inbox.json'); const inbox = new Inbox(path); inbox.accept(item('one'));
    const sync = fs.fsyncSync;
    vi.spyOn(fs, 'fsyncSync').mockImplementationOnce(sync).mockImplementationOnce(() => { throw new Error('directory sync failed'); });
    expect(() => inbox.accept(item('two'))).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed, details: expect.objectContaining({replaced: true})}));
    expect(inbox.snapshot()).toEqual([item('one'), item('two')]);
    expect(new Inbox(path).snapshot()).toEqual(inbox.snapshot());
    expect(() => inbox.accept(item('three'))).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    expect(() => inbox.acknowledge(1)).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    const reopened = new Inbox(path); reopened.accept(item('three'));
    expect(new Inbox(path).snapshot()).toEqual([item('one'), item('two'), item('three')]);
    const cursorPath = join(root, 'cursors.json'); const checkpoints = new Checkpoints(cursorPath); checkpoints.set('one', 'a');
    vi.spyOn(fs, 'fsyncSync').mockImplementationOnce(sync).mockImplementationOnce(() => { throw new Error('directory sync failed'); });
    expect(() => checkpoints.set('one', 'b')).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    expect(checkpoints.get('one')).toBe('b'); expect(new Checkpoints(cursorPath).get('one')).toBe('b');
    expect(() => checkpoints.set('one', 'b')).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    expect(() => checkpoints.retain([])).toThrow(expect.objectContaining({code: ErrorCode.StateWriteFailed}));
    const resumed = new Checkpoints(cursorPath); resumed.set('two', 'c');
    expect(new Checkpoints(cursorPath).get('one')).toBe('b');
  });

  it('resumes a client from its saved cursor while keeping unacknowledged observations', async () => {
    const root = directory(); const inboxPath = join(root, 'inbox.json'); const cursorPath = join(root, 'cursors.json');
    const requests: Array<string | null> = [];
    for (let run = 0; run < 2; run++) {
      const inbox = new Inbox(inboxPath); const checkpoints = new Checkpoints(cursorPath);
      const transport = new TestTransport();
      transport.handle = request => {
        if (request.method === 'events/list') transport.respond(request, {events: [{name: 'example.changed', delivery: ['poll'], inputSchema: {}, payloadSchema: {}}]});
        if (request.method === 'events/poll') {
          requests.push(request.params!.cursor as string | null);
          const first = item().observation;
          const events = run === 0 && first.type === 'event' ? [first.event] : [];
          transport.respond(request, {events, cursor: 'accepted', truncated: false, hasMore: false, nextPollMs: 60_000});
        }
      };
      const client = new EventsClient(new EventsWire(transport));
      try {
        await client.connect();
        const watch = client.watch({name: 'example.changed', arguments: {}, mode: 'poll', cursor: checkpoints.get('watch')},
          observation => inbox.accept({server: 'server', watch: 'watch', observation}), cursor => checkpoints.set('watch', cursor));
        await watch.ready; await watch.stop();
      } finally { await client.close(); }
    }
    expect(requests).toEqual([null, 'accepted']);
    expect(new Inbox(inboxPath).snapshot()).toEqual([item()]);
    expect(new Checkpoints(cursorPath).get('watch')).toBe('accepted');
  });

  it('keeps in-memory progress across reconnects, delivers successive batches, and starts new sessions fresh', async () => {
    const requests: Array<string | null> = [];
    const session = () => ({inbox: new Inbox(), checkpoints: new Checkpoints()});
    const receive = async (state: ReturnType<typeof session>, id: string) => {
      const transport = new TestTransport();
      transport.handle = request => {
        if (request.method === 'events/list') transport.respond(request, {events: [{name: 'example.changed', delivery: ['poll'], inputSchema: {}, payloadSchema: {}}]});
        if (request.method === 'events/poll') {
          requests.push(request.params!.cursor as string | null);
          const observation = item(id).observation;
          transport.respond(request, {events: observation.type === 'event' ? [observation.event] : [], cursor: id, truncated: false, hasMore: false, nextPollMs: 60_000});
        }
      };
      const client = new EventsClient(new EventsWire(transport));
      try {
        await client.connect();
        const watch = client.watch({name: 'example.changed', arguments: {}, mode: 'poll', cursor: state.checkpoints.get('watch')},
          observation => state.inbox.accept({server: 'server', watch: 'watch', observation}), cursor => state.checkpoints.set('watch', cursor));
        await watch.ready; await watch.stop();
      } finally { await client.close(); }
    };
    const active = session();
    await receive(active, 'first');
    const first = active.inbox.snapshot(); expect(first).toEqual([item('first')]); active.inbox.acknowledge(first.length);
    await receive(active, 'second');
    const second = active.inbox.snapshot(); expect(second).toEqual([item('second')]); active.inbox.acknowledge(second.length);
    await receive(active, 'pending'); expect(active.inbox.size).toBe(1);
    const fresh = session(); expect(fresh.inbox.size).toBe(0); expect(fresh.checkpoints.get('watch')).toBeNull();
    await receive(fresh, 'fresh'); expect(fresh.inbox.snapshot()).toEqual([item('fresh')]);
    expect(requests).toEqual([null, 'first', 'second', null]);
  });

  it('reads at most the byte limit plus one byte before rejecting an oversized file', () => {
    const path = join(directory(), 'inbox.json'); fs.writeFileSync(path, 'x'.repeat(100_000));
    const reads = vi.spyOn(fs, 'readSync');
    expect(() => new Inbox(path, {maxBytes: 32})).toThrow(expect.objectContaining({code: ErrorCode.LimitExceeded}));
    expect(reads.mock.results.reduce((total, result) => total + Number(result.value), 0)).toBe(33);
  });

  it('reports read failures rather than treating them as empty state', () => {
    vi.spyOn(fs, 'openSync').mockImplementationOnce(() => { throw Object.assign(new Error('denied'), {code: 'EACCES'}); });
    expect(() => new Inbox('/unreadable/inbox.json')).toThrow(expect.objectContaining({code: ErrorCode.StateReadFailed}));
  });

  it.each([0, -1, 0.5, Infinity])('rejects invalid storage limits: %s', value => {
    expect(() => new Inbox(undefined, {maxItems: value})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
    expect(() => new Checkpoints(undefined, {maxBytes: value})).toThrow(expect.objectContaining({code: ErrorCode.InvalidOptions}));
  });
});
