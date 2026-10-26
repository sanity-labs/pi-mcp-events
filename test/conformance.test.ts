import fc from 'fast-check';
import {describe, expect, it} from 'vitest';
import {EventsClient, EventsWire, SessionState, type InboxItem} from '../src/index.ts';
import {TestTransport} from './transport.ts';

const runs = Number(process.env.FC_RUNS ?? 100);
const seed = Number(process.env.FC_SEED ?? 20261025);
if (!Number.isSafeInteger(runs) || runs < 1) throw new Error('FC_RUNS must be a positive integer');
if (!Number.isSafeInteger(seed) || process.env.FC_SEED?.trim() === '') throw new Error('FC_SEED must be an integer');
const parameters = {numRuns: runs, seed};
const descriptor = {
  name: 'example.changed', delivery: ['poll'], inputSchema: {type: 'object'},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'integer'}}},
};
const event = (id: number) => ({
  eventId: String(id), name: descriptor.name, timestamp: '2026-10-25T12:00:00Z', data: {value: id}, cursor: null,
});

describe('generated public-behavior conformance', () => {
  it('preserves first delivery order across overlapping poll pages and temporary consumer failures', async () => {
    await fc.assert(fc.asyncProperty(
      fc.array(fc.array(fc.integer({min: 0, max: 20}), {minLength: 1, maxLength: 25}), {minLength: 1, maxLength: 8}),
      fc.boolean(),
      async (pages, rejectOnce) => {
        const transport = new TestTransport();
        const client = new EventsClient(new EventsWire(transport), {pollFloorMs: 1, retryMs: 1});
        const accepted: string[] = [];
        const completed = Promise.withResolvers<void>();
        let page = 0;
        let rejected = false;
        transport.handle = request => {
          if (request.method === 'events/list') { transport.respond(request, {events: [descriptor]}); return; }
          if (request.method !== 'events/poll') return;
          expect(request.params?.cursor ?? null).toBe(page === 0 ? null : `page-${page}`);
          const ids = pages[page++]!;
          transport.respond(request, {
            events: ids.map(event), cursor: `page-${page}`, truncated: false,
            hasMore: page < pages.length, nextPollMs: 60_000,
          });
        };
        try {
          await client.connect();
          const watch = client.watch({name: descriptor.name, arguments: {}, mode: 'poll'}, observation => {
            if (observation.type !== 'event') return;
            if (rejectOnce && !rejected) { rejected = true; throw new Error('Temporary consumer failure'); }
            accepted.push(observation.event.eventId);
          }, cursor => {
            if (cursor === `page-${pages.length}`) completed.resolve();
          });
          await Promise.race([completed.promise, watch.closed.then(() => { throw new Error('Watch closed before final checkpoint'); })]);
          expect(accepted).toEqual([...new Set(pages.flat().map(String))]);
          expect(rejected).toBe(rejectOnce);
          await watch.stop();
          expect(watch.status).toBe('stopped');
        } finally { await client.close(); }
      },
    ), parameters);
  }, 120_000);

  it('retains accepted observations until acknowledged through arbitrary inbox operations', () => {
    const operation = fc.oneof(
      fc.record({kind: fc.constant('accept' as const), value: fc.integer()}),
      fc.record({kind: fc.constant('acknowledge' as const), value: fc.nat({max: 20})}),
    );
    fc.assert(fc.property(fc.array(operation, {maxLength: 200}), operations => {
      const state = new SessionState(undefined, {inbox: {maxItems: 8}});
      const expected: InboxItem[] = [];
      for (const operation of operations) {
        if (operation.kind === 'acknowledge') {
          const count = Math.min(operation.value, expected.length);
          state.acknowledge(count); expected.splice(0, count);
        } else {
          const item: InboxItem = {server: 'server', watch: 'watch', observation: {type: 'event', event: event(operation.value)}};
          if (expected.length === 8) expect(() => state.accept(item)).toThrow();
          else { state.accept(item); expected.push(item); }
        }
        expect(state.snapshot()).toEqual(expected);
        expect(state.mode).toBe('memory');
      }
      expect(new SessionState().snapshot()).toEqual([]);
    }), parameters);
  });

  it('preserves JSON event payloads and opaque cursors without exposing mutable state', () => {
    fc.assert(fc.property(fc.jsonValue({maxDepth: 4}), fc.string(), (value, cursor) => {
      const state = new SessionState();
      const item: InboxItem = {server: 'server', watch: 'watch', observation: {
        type: 'event', event: {...event(1), data: {value}},
      }};
      state.accept(item);
      state.setCursor('__proto__', cursor);
      const expected = structuredClone(item);
      item.observation = {type: 'gap', cursor: 'changed'};
      const snapshot = state.snapshot();
      snapshot[0]!.observation = {type: 'gap', cursor: 'changed again'};
      expect(state.snapshot()).toEqual([expected]);
      expect(state.getCursor('__proto__')).toBe(cursor);
    }), parameters);
  });
});
