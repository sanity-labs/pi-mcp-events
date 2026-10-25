import {ProtocolError, SdkError, SdkErrorCode, type StandardSchemaV1} from '@modelcontextprotocol/client';
import delay from 'delay';
import {z} from 'zod';
import {
  canonical, catalogResult, EventProtocolError, EventProtocolErrorCode, pollResult,
  type EventDescriptor, type EventOccurrence,
} from './protocol.ts';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import {retryOperation} from './retry.ts';
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
  maxSubscriptions?: number;
  maxCatalogEntries?: number;
  maxCatalogPages?: number;
  maxPollBytes?: number;
  maxTrackedEventIds?: number;
};
const watchSchema = z.object({
  name: z.string().min(1), arguments: z.record(z.string(), z.unknown()),
  mode: z.literal('poll'), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(), maxEvents: z.number().int().positive().default(50),
});
const clientSchema = z.object({
  pollFloorMs: z.number().int().positive().default(1000),
  retryMs: z.number().int().positive().max(30_000).default(1000),
  maxRetries: z.number().int().nonnegative().default(10),
  maxSubscriptions: z.number().int().positive().default(32),
  maxCatalogEntries: z.number().int().positive().default(1000),
  maxCatalogPages: z.number().int().positive().default(100),
  maxPollBytes: z.number().int().positive().default(256 * 1024),
  maxTrackedEventIds: z.number().int().positive().default(4096),
});

function needsRediscovery(error: unknown): boolean {
  if (!(error instanceof EventProtocolError)) return false;
  if (error.code === EventProtocolErrorCode.NotFound) return true;
  const data = error.data;
  return error.code === EventProtocolErrorCode.Unsupported
    && typeof data === 'object' && data !== null && 'reason' in data && data.reason === 'schema_changed';
}

function normalizeRequestError(error: unknown) {
  if (error instanceof EventsClientError || error instanceof EventProtocolError) return error;
  if (error instanceof ProtocolError) return new EventProtocolError(error, {cause: error});
  if (error instanceof SdkError && error.code === SdkErrorCode.InvalidResult) {
    return new EventsClientError(ErrorCode.InvalidResponse, 'Invalid MCP Events response', {cause: error});
  }
  return new EventsClientError(ErrorCode.RequestFailed, 'MCP Events request failed', {cause: error});
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
    const parsed = clientSchema.safeParse(settings);
    if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid MCP Events client options', {cause: parsed.error});
    this.settings = parsed.data;
  }
  async connect() {
    this.assertOpen();
    try { await this.wire.connect(this.lifetime.signal); }
    catch (error) {
      this.lifetime.signal.throwIfAborted();
      if (error instanceof ProtocolError || error instanceof EventProtocolError) throw normalizeRequestError(error);
      throw new EventsClientError(ErrorCode.ConnectionFailed, 'MCP Events connection failed', {cause: error});
    }
    await this.refresh();
  }
  list() { return [...this.contracts.values()].map(contract => contract.descriptor); }
  refresh(): Promise<EventDescriptor[]> {
    if (this.refreshing) return this.refreshing;
    this.refreshing = this.readCatalog().finally(() => { this.refreshing = undefined; });
    return this.refreshing;
  }
  private async request<S extends StandardSchemaV1>(method: string, params: Record<string, unknown>, schema: S, signal: AbortSignal) {
    this.assertOpen();
    try { return await this.wire.request(method, params, schema, {signal}); }
    catch (error) { signal.throwIfAborted(); throw normalizeRequestError(error); }
  }
  private async readCatalog() {
    const contracts = new Map<string, EventContract>();
    const cursors = new Set<string>();
    let cursor: string | undefined;
    let pages = 0;
    do {
      if (pages++ >= this.settings.maxCatalogPages) throw limitExceeded('maxCatalogPages', this.settings.maxCatalogPages);
      const result = await this.request('events/list', cursor === undefined ? {} : {cursor}, catalogResult, this.lifetime.signal);
      for (const descriptor of result.events) {
        if (contracts.has(descriptor.name)) throw new EventsClientError(ErrorCode.InvalidCatalog, 'Duplicate event name in catalog', {details: {name: descriptor.name}});
        if (contracts.size >= this.settings.maxCatalogEntries) throw limitExceeded('maxCatalogEntries', this.settings.maxCatalogEntries);
        try { contracts.set(descriptor.name, new EventContract(descriptor)); }
        catch (error) { throw new EventsClientError(ErrorCode.InvalidCatalog, 'Invalid event schema in catalog', {cause: error, details: {name: descriptor.name}}); }
      }
      cursor = result.nextCursor;
      if (cursor !== undefined && cursors.has(cursor)) throw new EventsClientError(ErrorCode.InvalidCatalog, 'Event catalog pagination repeated a cursor');
      if (cursor !== undefined) cursors.add(cursor);
    } while (cursor !== undefined);
    this.contracts = contracts;
    return this.list();
  }
  private assertOpen() {
    if (this.lifetime.signal.aborted) throw new EventsClientError(ErrorCode.Closed, 'MCP Events client is closed');
  }
  private contract(options: WatchOptions) {
    const contract = this.contracts.get(options.name);
    if (!contract) throw new EventsClientError(ErrorCode.EventNotFound, 'Event is not in the current catalog', {details: {name: options.name}});
    if (!contract.descriptor.delivery.includes(options.mode)) throw new EventsClientError(ErrorCode.UnsupportedDeliveryMode, 'Event does not offer this delivery mode', {details: {name: options.name, mode: options.mode}});
    try { contract.arguments(options.arguments); }
    catch (error) { throw new EventsClientError(ErrorCode.InvalidArguments, 'Subscription arguments do not match inputSchema', {cause: error}); }
    return contract;
  }
  watch(input: WatchOptions, accept: (observation: Observation) => Promise<void> | void, checkpoint?: (cursor: string | null) => Promise<void> | void): Watch {
    this.assertOpen();
    const parsed = watchSchema.safeParse(input);
    if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid event watch options', {cause: parsed.error});
    const options = parsed.data;
    this.contract(options);
    let key: string;
    try { key = canonical([options.name, options.arguments, options.mode]); }
    catch (error) { throw new EventsClientError(ErrorCode.InvalidArguments, 'Event arguments must be JSON values', {cause: error}); }
    if (this.watches.has(key)) throw new EventsClientError(ErrorCode.AlreadyWatching, 'This subscription is already running');
    if (this.watches.size >= this.settings.maxSubscriptions) throw limitExceeded('maxSubscriptions', this.settings.maxSubscriptions);
    const local = new AbortController();
    const signal = AbortSignal.any([local.signal, this.lifetime.signal]);
    const ready = Promise.withResolvers<void>();
    void ready.promise.catch(() => {});
    let cursor = options.cursor ?? null;
    const seen = new Set<string>();
    const status = async (value: 'retrying' | 'stopped') => {
      try { await accept({type: 'status', status: value}); }
      catch { /* A full consumer inbox must not prevent delivery retries. */ }
    };
    const retry = <T>(operation: () => Promise<T>) => retryOperation(operation, {
      ...this.settings, signal, onRetry: () => status('retrying'),
    });
    const deliver = (observation: Observation) => retry(async () => {
      try { await accept(observation); }
      catch (error) { throw new EventsClientError(ErrorCode.ConsumerRejected, 'Event consumer rejected delivery', {cause: error}); }
    });
    const advance = async (next: string | null) => {
      await retry(async () => {
        try { await checkpoint?.(next); }
        catch (error) { throw new EventsClientError(ErrorCode.CheckpointFailed, 'Event checkpoint failed', {cause: error}); }
      });
      cursor = next;
    };
    const rediscover = async () => {
      await retry(() => this.refresh());
      this.contract(options);
      await deliver({type: 'gap', cursor: null});
      await advance(null);
    };
    const fetchPoll = () => retry(() => this.request('events/poll', {
      name: options.name, arguments: options.arguments, cursor, maxEvents: options.maxEvents,
      ...(options.maxAgeMs === undefined ? {} : {maxAgeMs: options.maxAgeMs}),
    }, pollResult, signal));
    const readPoll = async () => {
      try { return await fetchPoll(); }
      catch (error) {
        if (!needsRediscovery(error)) throw error;
        await rediscover();
        return fetchPoll();
      }
    };
    const poll = async () => {
      while (!signal.aborted) {
        this.contract(options);
        const result = await readPoll();
        if (result.events.length > options.maxEvents) throw new EventsClientError(ErrorCode.InvalidResponse, 'Poll response exceeds the requested batch size', {details: {maxEvents: options.maxEvents}});
        if (Buffer.byteLength(JSON.stringify(result)) > this.settings.maxPollBytes) throw limitExceeded('maxPollBytes', this.settings.maxPollBytes);
        if (result.cursor !== null && result.truncated) await deliver({type: 'gap', cursor: result.cursor});
        for (const event of result.events) {
          signal.throwIfAborted();
          try { this.contract(options).event(event); }
          catch (error) {
            if (error instanceof EventsClientError) throw error;
            throw new EventsClientError(ErrorCode.InvalidEvent, 'Event does not match its advertised name or payloadSchema', {cause: error});
          }
          if (seen.has(event.eventId)) continue;
          await deliver({type: 'event', event});
          seen.add(event.eventId);
          if (seen.size > this.settings.maxTrackedEventIds) seen.delete(seen.values().next().value!);
        }
        await advance(result.cursor);
        ready.resolve();
        if (!result.hasMore) await delay(Math.max(this.settings.pollFloorMs, result.nextPollMs), {signal});
      }
    };
    const run = async () => {
      try { await poll(); }
      catch (error) {
        if (!signal.aborted) {
          ready.reject(error);
          await status('stopped');
          throw error;
        }
      } finally {
        ready.reject(new EventsClientError(ErrorCode.Stopped, 'Subscription stopped before becoming active'));
        this.watches.delete(key);
      }
    };
    const closed = Promise.resolve().then(run);
    void closed.catch(() => {});
    const watch: Watch = {
      options, ready: ready.promise, closed, get cursor() { return cursor; },
      stop: async () => {
        local.abort(new EventsClientError(ErrorCode.Stopped, 'Event subscription stopped'));
        await closed.catch(() => {});
      },
    };
    this.watches.set(key, watch);
    return watch;
  }
  async close() {
    this.lifetime.abort(new EventsClientError(ErrorCode.Closed, 'MCP Events client is closed'));
    await Promise.allSettled([...this.watches.values()].map(watch => watch.stop()));
    try { await this.wire.close(); }
    catch (error) { throw new EventsClientError(ErrorCode.ConnectionFailed, 'MCP Events connection could not close', {cause: error}); }
  }
}
