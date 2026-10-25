import {setTimeout as sleep} from 'node:timers/promises';
import {z} from 'zod';
import {
  canonical, catalogResult, EventProtocolError, pollResult,
  type EventDescriptor, type EventOccurrence,
} from './protocol.ts';
import {EventContract} from './validation.ts';
import {EventsWire} from './wire.ts';

export type WatchOptions = {
  name: string;
  arguments: Record<string, unknown>;
  mode: 'poll';
  cursor?: string | null;
  maxAgeMs?: number;
  maxEvents?: number;
};
export type Observation =
  | {type: 'event'; event: EventOccurrence}
  | {type: 'gap'; cursor: string | null}
  | {type: 'status'; status: 'retrying' | 'stopped'};
export type Watch = {
  readonly ready: Promise<void>;
  readonly closed: Promise<void>;
  readonly cursor: string | null;
  readonly options: WatchOptions;
  stop(): Promise<void>;
};
export type ClientOptions = {
  pollFloorMs?: number;
  retryMs?: number;
  maxRetries?: number;
};
const watchSchema = z.object({
  name: z.string().min(1), arguments: z.record(z.string(), z.unknown()),
  mode: z.literal('poll'), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(), maxEvents: z.number().int().positive().optional(),
});
const clientSchema = z.object({
  pollFloorMs: z.number().int().positive().max(2 ** 31 - 1).default(1000),
  retryMs: z.number().int().positive().max(30_000).default(1000),
  maxRetries: z.number().int().nonnegative().default(10),
});

function remoteCode(error: unknown): number | undefined {
  if (typeof error !== 'object' || error === null || !('code' in error)) return undefined;
  return typeof error.code === 'number' ? error.code : undefined;
}
function needsRediscovery(error: unknown): boolean {
  const code = remoteCode(error);
  if (code === -32023) return true;
  if (code !== -32026 || typeof error !== 'object' || error === null || !('data' in error)) return false;
  const data = error.data;
  return typeof data === 'object' && data !== null && 'reason' in data && data.reason === 'schema_changed';
}
function fatal(error: unknown): boolean {
  return [-32601, -32602, -32023, -32024, -32026].includes(remoteCode(error) ?? 0);
}

export class EventsClient {
  private readonly wire: EventsWire;
  private readonly settings: Required<ClientOptions>;
  private readonly lifetime = new AbortController();
  private readonly watches = new Map<string, Watch>();
  private contracts = new Map<string, EventContract>();
  private refreshing?: Promise<EventDescriptor[]>;

  constructor(wire: EventsWire, settings: ClientOptions = {}) {
    this.wire = wire;
    this.settings = clientSchema.parse(settings);
  }
  async connect() {
    await this.wire.connect(this.lifetime.signal);
    await this.refresh();
  }
  list() { return [...this.contracts.values()].map(contract => contract.descriptor); }
  refresh(): Promise<EventDescriptor[]> {
    if (this.refreshing) return this.refreshing;
    const run = async () => {
      const contracts = new Map<string, EventContract>();
      const cursors = new Set<string>();
      let cursor: string | undefined;
      do {
        const result = await this.wire.request('events/list', cursor === undefined ? {} : {cursor}, catalogResult, {signal: this.lifetime.signal});
        for (const descriptor of result.events) {
          if (contracts.has(descriptor.name)) throw new Error('Duplicate event name in catalog');
          if (contracts.size >= 1000) throw new Error('Event catalog exceeds 1000 descriptors');
          contracts.set(descriptor.name, new EventContract(descriptor));
        }
        cursor = result.nextCursor;
        if (cursor !== undefined) {
          if (cursors.has(cursor) || cursors.size >= 100) throw new Error('Event catalog pagination did not terminate');
          cursors.add(cursor);
        }
      } while (cursor !== undefined);
      this.contracts = contracts;
      return this.list();
    };
    this.refreshing = run().finally(() => { this.refreshing = undefined; });
    return this.refreshing;
  }
  private contract(options: WatchOptions) {
    const contract = this.contracts.get(options.name);
    if (!contract) throw new EventProtocolError({code: -32023, message: 'Event is not in the current catalog'});
    if (!contract.descriptor.delivery.includes(options.mode)) throw new EventProtocolError({code: -32026, message: 'Event does not offer this delivery mode'});
    try { contract.arguments(options.arguments); }
    catch (error) {
      throw new EventProtocolError({code: -32602, message: error instanceof Error ? error.message : 'Invalid subscription arguments'});
    }
    return contract;
  }
  watch(options: WatchOptions, accept: (observation: Observation) => Promise<void> | void, checkpoint?: (cursor: string | null) => Promise<void> | void): Watch {
    options = watchSchema.parse(options);
    this.contract(options);
    if (this.lifetime.signal.aborted) throw new Error('MCP Events client is closed');
    const key = canonical([options.name, options.arguments, options.mode]);
    if (this.watches.has(key)) throw new Error('This subscription is already running');
    if (this.watches.size >= 32) throw new Error('At most 32 event subscriptions may run per server');
    const local = new AbortController();
    const signal = AbortSignal.any([local.signal, this.lifetime.signal]);
    const ready = Promise.withResolvers<void>();
    void ready.promise.catch(() => {});
    let cursor = options.cursor ?? null;
    let failures = 0;
    const seen = new Set<string>();
    const emit = async (event: EventOccurrence) => {
      this.contract(options).event(event);
      if (!seen.has(event.eventId)) {
        await accept({type: 'event', event});
        failures = 0;
        seen.add(event.eventId);
        if (seen.size > 4096) seen.delete(seen.values().next().value!);
      }
    };
    const advance = async (next: string | null) => {
      signal.throwIfAborted();
      await checkpoint?.(next);
      cursor = next;
    };
    const params = () => ({
      name: options.name, arguments: options.arguments, cursor,
      ...(options.maxAgeMs === undefined ? {} : {maxAgeMs: options.maxAgeMs}),
    });
    const status = async (value: Extract<Observation, {type: 'status'}>['status']) => {
      try { await accept({type: 'status', status: value}); }
      catch { /* A full consumer inbox must not prevent delivery retries. */ }
    };
    const maxEvents = Math.min(options.maxEvents ?? 50, 50);
    let pendingPoll: {result: z.infer<typeof pollResult>; index: number; gapAccepted: boolean} | undefined;
    let rediscovery: {gapAccepted: boolean} | undefined;
    const poll = async () => {
      while (!signal.aborted) {
        if (!pendingPoll) {
          const result = await this.wire.request('events/poll', {...params(), maxEvents}, pollResult, {signal});
          if (result.events.length > maxEvents || Buffer.byteLength(JSON.stringify(result)) > 256 * 1024) throw new Error('Poll response exceeds the requested batch or memory limit');
          pendingPoll = {result, index: 0, gapAccepted: false};
        }
        const {result} = pendingPoll;
        signal.throwIfAborted();
        if (!pendingPoll.gapAccepted) {
          if (result.cursor !== null && result.truncated) await accept({type: 'gap', cursor: result.cursor});
          pendingPoll.gapAccepted = true;
        }
        while (pendingPoll.index < result.events.length) {
          signal.throwIfAborted();
          await emit(result.events[pendingPoll.index]!);
          pendingPoll.index++;
        }
        await advance(result.cursor);
        pendingPoll = undefined;
        failures = 0;
        ready.resolve();
        if (!result.hasMore) {
          let remaining = Math.max(this.settings.pollFloorMs, result.nextPollMs);
          while (remaining > 0) {
            const interval = Math.min(remaining, 2 ** 31 - 1);
            await sleep(interval, undefined, {signal});
            remaining -= interval;
          }
        }
      }
    };
    const run = async () => {
      try {
        while (!signal.aborted) {
          try {
            if (rediscovery) {
              await this.refresh();
              this.contract(options);
              if (!rediscovery.gapAccepted) {
                await accept({type: 'gap', cursor: null});
                rediscovery.gapAccepted = true;
              }
              await advance(null);
              pendingPoll = undefined;
              rediscovery = undefined;
            }
            this.contract(options);
            await poll();
          } catch (error) {
            if (signal.aborted) break;
            if (needsRediscovery(error)) {
              if (rediscovery) throw error;
              rediscovery = {gapAccepted: false};
            } else if (fatal(error)) throw error;
            if (failures >= this.settings.maxRetries) throw new Error('Event delivery retry limit reached');
            await status('retrying');
            await this.delay(failures++, signal);
            continue;
          }
          failures = 0;
          if (!signal.aborted) await this.delay(0, signal);
        }
      } catch (error) {
        if (!signal.aborted) {
          ready.reject(error);
          await status('stopped');
          throw error;
        }
      } finally {
        ready.reject(new Error('Subscription stopped before becoming active'));
        this.watches.delete(key);
      }
    };
    const closed = Promise.resolve().then(run);
    void closed.catch(() => {});
    const watch: Watch = {
      options, ready: ready.promise, closed, get cursor() { return cursor; },
      stop: async () => { local.abort(); await closed.catch(() => {}); },
    };
    this.watches.set(key, watch);
    return watch;
  }
  private delay(failures: number, signal: AbortSignal) {
    return sleep(Math.min(30_000, this.settings.retryMs * 2 ** Math.min(failures, 5)), undefined, {signal});
  }
  async close() {
    this.lifetime.abort();
    await Promise.allSettled([...this.watches.values()].map(watch => watch.stop()));
    await this.wire.close();
  }
}
