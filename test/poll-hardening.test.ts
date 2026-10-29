import type {JSONRPCRequest} from '@modelcontextprotocol/client';
import {afterEach, beforeEach, expect, it, vi} from 'vitest';
import {EventsClient, EventsWire, EventsClientErrorCode as ErrorCode, type ClientOptions, type Observation} from '../src/index.ts';
import {TestTransport} from './transport.ts';

const descriptor = {name: 'example.changed', delivery: ['poll'], inputSchema: true, payloadSchema: true};
const options = {name: descriptor.name, arguments: {}, mode: 'poll' as const, cursor: 'saved'};
const event = {eventId: 'e', name: descriptor.name, timestamp: '2026-10-29T12:00:00Z', data: {value: 1}};
const batch = {events: [], cursor: 'saved', truncated: false, hasMore: true, nextPollMs: 60_000};
const cleanups: Array<() => Promise<void>> = [];
beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, 'random').mockReturnValue(0.5);
});
afterEach(async () => {
  for (const close of cleanups.splice(0)) await close();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

async function setup(settings: ClientOptions = {}) {
  const transport = new TestTransport();
  const client = new EventsClient(new EventsWire(transport), {retryMs: 100, ...settings});
  cleanups.push(() => client.close());
  const polls: JSONRPCRequest[] = [];
  let respond = (request: JSONRPCRequest) => transport.respond(request, batch);
  transport.handle = request => {
    if (request.method === 'events/list') transport.respond(request, {events: [descriptor]});
    if (request.method === 'events/poll') { polls.push(request); respond(request); }
  };
  await client.connect();
  return {client, transport, polls, respond(handler: typeof respond) { respond = handler; }};
}

it.each(['empty', 'duplicate-only', 'absent-cursor'])('backs off %s stalled pages with jitter, a floor, and a cap', async kind => {
  const s = await setup();
  s.respond(req => s.transport.respond(req, {...batch,
    cursor: kind === 'absent-cursor' ? undefined : 'saved',
    events: kind === 'duplicate-only' ? [event] : [],
  }));
  const accepted: Observation[] = [];
  const watch = s.client.watch({...options, cursor: kind === 'absent-cursor' ? null : 'saved'}, item => { accepted.push(item); });
  await vi.advanceTimersByTimeAsync(0);
  const firstStall = kind === 'duplicate-only' ? 2 : 1;
  expect(s.polls).toHaveLength(firstStall);
  expect(watch.status).toBe('retrying');
  for (const [index, wait] of [1500, 3000, 6000, 12000, 24000, 30000].entries()) {
    await vi.advanceTimersByTimeAsync(wait - 1);
    expect(s.polls).toHaveLength(firstStall + index);
    await vi.advanceTimersByTimeAsync(1);
    expect(s.polls).toHaveLength(firstStall + index + 1);
  }
  expect(accepted.filter(item => item.type === 'event')).toHaveLength(kind === 'duplicate-only' ? 1 : 0);
  expect(watch.cursor).toBe(kind === 'absent-cursor' ? null : 'saved');
});

it.each(['cursor', 'event'])('resets backoff when a page makes progress through its %s', async progress => {
  const s = await setup();
  s.respond(req => {
    const advancing = s.polls.length === 3;
    s.transport.respond(req, {...batch, cursor: advancing && progress === 'cursor' ? 'advanced' : req.params?.cursor,
      events: advancing && progress === 'event' ? [event] : []});
  });
  const watch = s.client.watch(options, () => {});
  await vi.advanceTimersByTimeAsync(0);
  await vi.advanceTimersByTimeAsync(1500);
  expect(s.polls).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(3000);
  expect(s.polls).toHaveLength(4);
  expect(watch.cursor).toBe(progress === 'cursor' ? 'advanced' : 'saved');
  await vi.advanceTimersByTimeAsync(1499);
  expect(s.polls).toHaveLength(4);
  await vi.advanceTimersByTimeAsync(1);
  expect(s.polls).toHaveLength(5);
});

it('drains progressing empty pages immediately and applies the floor after the backlog ends', async () => {
  const s = await setup();
  s.respond(req => s.transport.respond(req, {...batch, cursor: String(s.polls.length), hasMore: s.polls.length < 4, nextPollMs: 0}));
  s.client.watch(options, () => {});
  await vi.advanceTimersByTimeAsync(0);
  expect(s.polls).toHaveLength(4);
  await vi.advanceTimersByTimeAsync(999);
  expect(s.polls).toHaveLength(4);
  await vi.advanceTimersByTimeAsync(1);
  expect(s.polls).toHaveLength(5);
});

it('recovers quota failures from the saved cursor and delivers the recovered batch once', async () => {
  const s = await setup();
  s.respond(req => {
    if (s.polls.length <= 2) s.transport.fail(req, -32025, {limit: 'upstream_requests'});
    else s.transport.respond(req, {...batch, cursor: 'recovered', hasMore: false, events: [event]});
  });
  const accepted: Observation[] = [];
  const watch = s.client.watch(options, item => { accepted.push(item); });
  await vi.advanceTimersByTimeAsync(0);
  expect(watch.status).toBe('retrying');
  expect(watch.cursor).toBe('saved');
  await vi.advanceTimersByTimeAsync(1499);
  expect(s.polls).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(s.polls).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(3000);
  await watch.ready;
  expect(s.polls.map(req => req.params?.cursor)).toEqual(['saved', 'saved', 'saved']);
  expect(accepted.filter(item => item.type === 'event')).toEqual([{type: 'event', event: {...event, cursor: null}}]);
  expect(watch.cursor).toBe('recovered');
  expect(watch.status).toBe('active');
});

it.each(['quota', 'stall', 'alternating'])('bounds %s retries and preserves the last cursor on exhaustion', async kind => {
  const s = await setup({maxRetries: 2});
  s.respond(req => {
    if (kind === 'quota' || (kind === 'alternating' && s.polls.length % 2 === 0)) s.transport.fail(req, -32025, {limit: 'upstream_requests'});
    else s.transport.respond(req, batch);
  });
  const watch = s.client.watch(options, () => {});
  const failed = expect(watch.closed).rejects.toMatchObject({code: ErrorCode.RetryExhausted, details: {maxRetries: 2}});
  await vi.advanceTimersByTimeAsync(4500);
  await failed;
  expect(s.polls).toHaveLength(3);
  expect(watch.cursor).toBe('saved');
  expect(watch.status).toBe('stopped');
});

it.each(['quota', 'stall'])('cancels a pending %s retry without another poll', async kind => {
  const s = await setup();
  if (kind === 'quota') s.respond(req => s.transport.fail(req, -32025));
  const watch = s.client.watch(options, () => {});
  await vi.advanceTimersByTimeAsync(0);
  expect(watch.status).toBe('retrying');
  await watch.stop();
  await vi.advanceTimersByTimeAsync(60_000);
  expect(s.polls).toHaveLength(1);
  expect(vi.getTimerCount()).toBe(0);
});
