import {addAbortListener} from 'node:events';
import {ProtocolError, type StandardSchemaV1} from '@modelcontextprotocol/client';
import delay from 'delay';
import {z} from 'zod';
import {
  canonical, catalogResult, EventMethod, EventProtocolError, EventProtocolErrorCode, pollResult,
  type EventDescriptor, type EventOccurrence, type WebhookDeliveryStatus,
} from './protocol.ts';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import {retryable, retryOperation} from './retry.ts';
import {clientSettingsSchema, watchOptionsSchema} from './options.ts';
import {WebhookReceiver} from './webhook.ts';
import {subscribeWebhook} from './webhook-subscription.ts';
import {streamEvents} from './stream.ts';
import {listenCatalog} from './catalog.ts';
import {normalizeRequestError} from './request-error.ts';
import {EventContract} from './validation.ts';
import {EventsWire} from './wire.ts';

export type WatchOptions = {
  name: string;
  arguments: Record<string, unknown>;
  mode: 'poll' | 'push' | 'webhook';
  ttlMs?: number | null;
  cursor?: string | null;
  maxAgeMs?: number;
  maxEvents?: number;
};
export type Observation =
  | {type: 'event'; event: EventOccurrence}
  | {type: 'gap'; cursor: string | null}
  | {type: 'status'; status: 'retrying' | 'stopped'}
  | {type: 'status'; status: 'webhook-delivery'; deliveryStatus: WebhookDeliveryStatus}
  | {type: 'status'; status: 'source-error'; error: EventProtocolError};
export type WatchStatus = 'starting' | 'active' | 'retrying' | 'suspended' | 'stopped';
export type Watch = {
  readonly status: WatchStatus;
  readonly ready: Promise<void>;
  readonly closed: Promise<void>;
  readonly cursor: string | null;
  readonly options: WatchOptions;
  stop(): Promise<void>;
};
export type ClientOptions = {
  webhookReceiver?: WebhookReceiver;
  webhookRefreshMs?: number;
  webhookUnsubscribeTimeoutMs?: number;
  pollFloorMs?: number;
  heartbeatMs?: number;
  catalogAckTimeoutMs?: number;
  retryMs?: number;
  maxRetries?: number;
  maxSubscriptions?: number;
  maxCatalogEntries?: number;
  maxCatalogPages?: number;
  maxPollBytes?: number;
  maxTrackedEventIds?: number;
};
const clientSchema = clientSettingsSchema.extend({webhookReceiver: z.instanceof(WebhookReceiver).optional()});
type CatalogEntry = EventContract | {descriptor: EventDescriptor; error: EventsClientError};

function needsRediscovery(error: unknown): boolean {
  if (!(error instanceof EventProtocolError)) return false;
  if (error.code === EventProtocolErrorCode.NotFound) return true;
  const data = error.data;
  return error.code === EventProtocolErrorCode.Unsupported
    && typeof data === 'object' && data !== null && 'reason' in data && data.reason === 'schema_changed';
}

export class EventsClient {
  private readonly wire: EventsWire;
  private readonly settings: z.output<typeof clientSchema>;
  private readonly lifetime = new AbortController();
  private readonly watches = new Map<string, Watch>();
  private catalogEntries = new Map<string, CatalogEntry>();
  private refreshing?: Promise<EventDescriptor[]>;
  private catalog?: Readonly<Pick<Watch, 'ready' | 'closed'>>;
  private catalogRunning = false;

  get catalogUpdates() { return this.catalog; }
  get catalogIssues() {
    return [...this.catalogEntries.values()].flatMap(entry => 'error' in entry
      ? [{name: entry.descriptor.name, error: entry.error}] : []);
  }

  constructor(wire: EventsWire, settings: ClientOptions = {}) {
    this.wire = wire;
    const parsed = clientSchema.safeParse(settings);
    if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid MCP Events client options', {cause: parsed.error});
    this.settings = parsed.data;
  }
  async connect() {
    this.assertOpen();
    let extension: Record<string, unknown>;
    try { extension = await this.wire.connect(this.lifetime.signal); }
    catch (error) {
      this.lifetime.signal.throwIfAborted();
      if (error instanceof ProtocolError || error instanceof EventProtocolError) throw normalizeRequestError(error);
      throw new EventsClientError(ErrorCode.ConnectionFailed, 'MCP Events connection failed', {cause: error});
    }
    await this.refresh();
    this.assertOpen();
    if (extension.listChanged === true && !this.catalogRunning) {
      this.catalogRunning = true;
      this.catalog = listenCatalog(this.wire, {
        ...this.settings, signal: this.lifetime.signal,
        refresh: async () => {
          // A notification must trigger a read that starts after that notification.
          await this.refreshing?.catch(() => {});
          await this.refresh();
        },
      });
      void this.catalog.closed.finally(() => { this.catalogRunning = false; }).catch(() => {});
    }
  }
  list() { return [...this.catalogEntries.values()].map(entry => entry.descriptor); }
  refresh(): Promise<EventDescriptor[]> {
    if (this.refreshing) return this.refreshing;
    this.refreshing = this.readCatalog().finally(() => { this.refreshing = undefined; });
    return this.refreshing;
  }
  private async refreshAfterCurrent(signal: AbortSignal) {
    signal.throwIfAborted();
    const aborted = Promise.withResolvers<never>();
    const listener = addAbortListener(signal, () => aborted.reject(signal.reason));
    const refresh = async () => {
      await this.refreshing?.catch(() => {});
      signal.throwIfAborted();
      return this.refresh();
    };
    try { return await Promise.race([refresh(), aborted.promise]); }
    finally { listener[Symbol.dispose](); }
  }
  private async request<S extends StandardSchemaV1>(method: string, params: Record<string, unknown>, schema: S, signal: AbortSignal) {
    this.assertOpen();
    try { return await this.wire.request(method, params, schema, {signal}); }
    catch (error) { signal.throwIfAborted(); throw normalizeRequestError(error); }
  }
  private async readCatalog() {
    const entries = new Map<string, CatalogEntry>();
    const cursors = new Set<string>();
    let cursor: string | undefined;
    let pages = 0;
    do {
      if (pages++ >= this.settings.maxCatalogPages) throw limitExceeded('maxCatalogPages', this.settings.maxCatalogPages);
      const result = await this.request(EventMethod.List, cursor === undefined ? {} : {cursor}, catalogResult, this.lifetime.signal);
      for (const descriptor of result.events) {
        if (entries.has(descriptor.name)) throw new EventsClientError(ErrorCode.InvalidCatalog, 'Duplicate event name in catalog', {details: {name: descriptor.name}});
        if (entries.size >= this.settings.maxCatalogEntries) throw limitExceeded('maxCatalogEntries', this.settings.maxCatalogEntries);
        try { entries.set(descriptor.name, new EventContract(descriptor)); }
        catch (error) {
          entries.set(descriptor.name, {descriptor, error: new EventsClientError(
            ErrorCode.UnsupportedEventSchema, 'Event schemas are unsupported or invalid', {cause: error, details: {name: descriptor.name}},
          )});
        }
      }
      cursor = result.nextCursor;
      if (cursor !== undefined && cursors.has(cursor)) throw new EventsClientError(ErrorCode.InvalidCatalog, 'Event catalog pagination repeated a cursor');
      if (cursor !== undefined) cursors.add(cursor);
    } while (cursor !== undefined);
    this.catalogEntries = entries;
    return this.list();
  }
  private assertOpen() {
    if (this.lifetime.signal.aborted) throw new EventsClientError(ErrorCode.Closed, 'MCP Events client is closed');
  }
  private contract(options: WatchOptions) {
    const contract = this.catalogEntries.get(options.name);
    if (!contract) throw new EventsClientError(ErrorCode.EventNotFound, 'Event is not in the current catalog', {details: {name: options.name}});
    if ('error' in contract) throw contract.error;
    if (!contract.descriptor.delivery.includes(options.mode)) throw new EventsClientError(ErrorCode.UnsupportedDeliveryMode, 'Event does not offer this delivery mode', {details: {name: options.name, mode: options.mode}});
    try { contract.arguments(options.arguments); }
    catch (error) { throw new EventsClientError(ErrorCode.InvalidArguments, 'Subscription arguments do not match inputSchema', {cause: error}); }
    return contract;
  }
  watch(input: WatchOptions, accept: (observation: Observation) => Promise<void> | void, checkpoint?: (cursor: string | null) => Promise<void> | void): Watch {
    this.assertOpen();
    const parsed = watchOptionsSchema.safeParse(input);
    if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid event watch options', {cause: parsed.error});
    const options = parsed.data;
    this.contract(options);
    if (options.mode === 'webhook' && !this.settings.webhookReceiver) {
      throw new EventsClientError(ErrorCode.InvalidOptions, 'Webhook watches require a webhookReceiver');
    }
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
    let currentStatus: WatchStatus = 'starting';
    const seen = new Set<string>();
    const status = async (value: 'retrying' | 'stopped') => {
      currentStatus = value;
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
    const validateEvent = (event: EventOccurrence) => {
      try { this.contract(options).event(event); }
      catch (error) {
        if (error instanceof EventsClientError) throw error;
        throw new EventsClientError(ErrorCode.InvalidEvent, 'Event does not match its advertised name or payloadSchema', {cause: error});
      }
    };
    const emit = async (event: EventOccurrence) => {
      try { validateEvent(event); }
      catch (error) {
        if (event.name !== options.name || !(error instanceof EventsClientError) || error.code !== ErrorCode.InvalidEvent) throw error;
        await retry(() => this.refreshAfterCurrent(signal));
        signal.throwIfAborted();
        validateEvent(event);
      }
      if (seen.has(event.eventId)) return;
      await deliver({type: 'event', event});
      seen.add(event.eventId);
      if (seen.size > this.settings.maxTrackedEventIds) seen.delete(seen.values().next().value!);
    };
    const rediscover = async () => {
      await retry(() => this.refresh());
      this.contract(options);
      await deliver({type: 'gap', cursor: null});
      await advance(null);
    };
    const fetchPoll = () => retry(() => this.request(EventMethod.Poll, {
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
          await emit(event);
        }
        await advance(result.cursor);
        currentStatus = 'active';
        ready.resolve();
        if (!result.hasMore) await delay(Math.max(this.settings.pollFloorMs, result.nextPollMs), {signal});
      }
    };
    const push = async () => {
      let rediscovered = false;
      while (!signal.aborted) {
        await retry(async () => {
          this.contract(options);
          let progressed = false;
          const progress = () => { progressed = true; rediscovered = false; currentStatus = 'active'; };
          try {
            await streamEvents(this.wire, {
              name: options.name, arguments: options.arguments, cursor,
              ...(options.maxAgeMs === undefined ? {} : {maxAgeMs: options.maxAgeMs}),
            }, signal, this.settings.heartbeatMs, {
              active: async (next, truncated) => {
                if (next !== null && truncated) await deliver({type: 'gap', cursor: next});
                await advance(next);
                currentStatus = 'active';
                ready.resolve();
              },
              event: async event => { await emit(event); await advance(event.cursor); progress(); },
              heartbeat: async next => { await advance(next); progress(); },
              sourceError: async error => {
                try { await accept({type: 'status', status: 'source-error', error}); }
                catch { /* Status delivery must not stop the source. */ }
              },
            });
          } catch (error) {
            signal.throwIfAborted();
            const reason = error instanceof EventsClientError && error.code === ErrorCode.SubscriptionTerminated
              ? error.cause : error;
            if (needsRediscovery(reason) && !rediscovered) {
              rediscovered = true;
              await rediscover();
              return;
            }
            const normalized = normalizeRequestError(error);
            if (progressed && retryable(normalized) && this.settings.maxRetries > 0) return;
            throw normalized;
          }
        });
        await status('retrying');
        await delay(this.settings.retryMs, {signal});
      }
    };
    const webhook = async () => {
      let rediscovered = false;
      let suspended = false;
      while (!signal.aborted) {
        try {
          await subscribeWebhook(this.wire, this.settings.webhookReceiver!, {...this.settings, ...options}, signal, {
            validate: () => { this.contract(options); },
            cursor: () => cursor,
            active: async (next, truncated, deliveryStatus) => {
              if (next !== null && truncated) await deliver({type: 'gap', cursor: next});
              await advance(next);
              if (deliveryStatus) suspended = !deliveryStatus.active;
              currentStatus = suspended ? 'suspended' : 'active';
              ready.resolve();
            },
            event: async event => {
              await emit(event); await advance(event.cursor); rediscovered = false;
              currentStatus = suspended ? 'suspended' : 'active';
            },
            gap: async next => {
              await deliver({type: 'gap', cursor: next}); await advance(next);
              currentStatus = suspended ? 'suspended' : 'active';
            },
            deliveryStatus: async deliveryStatus => {
              try { await accept({type: 'status', status: 'webhook-delivery', deliveryStatus}); }
              catch { /* Status delivery must not stop the source. */ }
            },
            refreshCatalog: async () => { await this.refresh(); this.contract(options); },
            retrying: () => status('retrying'),
          });
        } catch (error) {
          signal.throwIfAborted();
          const reason = error instanceof EventsClientError && error.code === ErrorCode.SubscriptionTerminated
            ? error.cause : error;
          if (!needsRediscovery(reason) || rediscovered) throw error;
          rediscovered = true;
          await rediscover();
        }
      }
    };
    const run = async () => {
      try {
        if (options.mode === 'poll') await poll();
        else if (options.mode === 'push') await push();
        else await webhook();
      }
      catch (error) {
        if (!signal.aborted) {
          ready.reject(error);
          await status('stopped');
          throw error;
        }
      } finally {
        currentStatus = 'stopped';
        ready.reject(new EventsClientError(ErrorCode.Stopped, 'Subscription stopped before becoming active'));
        this.watches.delete(key);
      }
    };
    const closed = Promise.resolve().then(run);
    void closed.catch(() => {});
    const watch: Watch = {
      options, ready: ready.promise, closed, get cursor() { return cursor; }, get status() { return currentStatus; },
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
    await this.catalog?.closed.catch(() => {});
    try { await this.wire.close(); }
    catch (error) { throw new EventsClientError(ErrorCode.ConnectionFailed, 'MCP Events connection could not close', {cause: error}); }
  }
}
