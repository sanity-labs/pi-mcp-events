import {afterEach, expect, it, vi} from 'vitest';
import {EventsClient, type Observation} from '../src/client.ts';
import {EventsWire} from '../src/wire.ts';
import {TestTransport} from './transport.ts';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0)) await close(); vi.useRealTimers(); });
const descriptor = {name: 'example.changed', delivery: ['poll', 'push'], inputSchema: true, payloadSchema: true};
const cutoff = Date.parse('2026-11-18T12:00:00Z');
const event = (eventId: string, offset: number) => ({eventId, name: descriptor.name, data: {}, cursor: eventId, timestamp: new Date(cutoff + offset).toISOString()});
async function setup() {
  const transport = new TestTransport();
  transport.handle = request => transport.respond(request, {events: [descriptor]});
  const client = new EventsClient(new EventsWire(transport), {maxRetries: 0, pollFloorMs: 1});
  cleanups.push(() => client.close());
  await client.connect();
  return {client, transport};
}
const options = {name: descriptor.name, arguments: {}, mode: 'poll' as const, cursor: 'saved'};

it('drains every available poll page, filters timestamps without assuming order, and checkpoints only after acceptance', async () => {
  const {client, transport} = await setup();
  const observations: Observation[] = [];
  const persisted: string[] = [];
  const accepted = Promise.withResolvers<void>();
  let polls = 0;
  transport.handle = request => {
    polls++;
    transport.respond(request, {events: polls === 1 ? [event('after', 1), event('at', 0)] : [event('before', -1)],
      cursor: String(polls), truncated: polls === 1, hasMore: polls === 1, nextPollMs: 0});
  };
  const watch = client.watch(options, async value => {
    observations.push(value);
    if (value.type === 'event' && value.event.eventId === 'at') await accepted.promise;
  }, cursor => {persisted.push(cursor!);}, {until: cutoff});
  await vi.waitFor(() => expect(observations).toHaveLength(2));
  expect(persisted).toEqual([]);
  expect(watch.cursor).toBe('saved');
  accepted.resolve();
  await watch.closed;
  expect(observations.map(item => item.type === 'event' ? item.event.eventId : item.type)).toEqual(['gap', 'at', 'before']);
  expect(persisted).toEqual(['1', '2']);
  expect(watch.replayComplete).toBe(false);
  expect(watch.status).toBe('stopped');
});

it('bounds poll catch-up by pages and leaves completeness unknown', async () => {
  const {client, transport} = await setup();
  let pages = 0;
  transport.handle = request => {
    transport.respond(request, {events: [], cursor: String(++pages), truncated: false, hasMore: true, nextPollMs: 0});
  };
  const watch = client.watch(options, () => {}, undefined, {until: cutoff, maxPages: 2});
  await watch.closed;
  expect(pages).toBe(2);
  expect(watch.replayComplete).toBe(false);
  expect(watch.cursor).toBe('2');
});

it.each(['poll', 'push'] as const)('bounds %s catch-up even when the server never finishes', async mode => {
  const {client, transport} = await setup();
  vi.useFakeTimers();
  transport.handle = request => {
    if (mode === 'push') transport.notify(request, 'active', {cursor: 'saved', truncated: false});
  };
  const watch = client.watch({...options, mode}, () => {}, undefined, {until: cutoff, maxMs: 100});
  await vi.advanceTimersByTimeAsync(100);
  await watch.closed;
  expect(watch.replayComplete).toBe(false);
  expect(watch.status).toBe('stopped');
});

it('reports unavailable replay when a poll response drops the saved cursor', async () => {
  const {client, transport} = await setup();
  const observations: Observation[] = [];
  transport.handle = request => transport.respond(request, {events: [], cursor: null, truncated: true, hasMore: false, nextPollMs: 0});
  const watch = client.watch(options, value => {observations.push(value);}, undefined, {until: cutoff});
  await watch.closed;
  expect(watch.replayComplete).toBe(false);
  expect(observations).toEqual([{type: 'gap', cursor: null}]);
});
