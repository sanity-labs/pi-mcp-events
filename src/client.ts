import {setTimeout as sleep} from 'node:timers/promises';
import {z} from 'zod';
import {
  activeNotification, canonical, catalogResult, completeResult, errorNotification,
  EventProtocolError, heartbeatNotification, occurrence, pollResult, subscribeResult,
  type DeliveryMode, type EventDescriptor, type EventOccurrence,
} from './protocol.ts';
import {EventContract} from './validation.ts';
import {EventsWire} from './wire.ts';
import {type WebhookRegistration, WebhookReceiver} from './webhook.ts';

export type WatchOptions = {
  name: string;
  arguments: Record<string, unknown>;
  mode: DeliveryMode;
  cursor?: string | null;
  maxAgeMs?: number;
  maxEvents?: number;
  ttlMs?: number | null;
};
export type Observation =
  | {type: 'event'; event: EventOccurrence}
  | {type: 'gap'; cursor: string | null}
  | {type: 'status'; status: 'retrying' | 'stopped' | 'source-error' | 'delivery-suspended'};
export type Watch = {
  readonly ready: Promise<void>;
  readonly closed: Promise<void>;
  readonly cursor: string | null;
  readonly options: WatchOptions;
  stop(): Promise<void>;
};
export type ClientOptions = {
  pollFloorMs?: number;
  heartbeatMs?: number;
  retryMs?: number;
  maxRetries?: number;
  receiver?: WebhookReceiver;
};
const watchSchema = z.object({
  name: z.string().min(1), arguments: z.record(z.string(), z.unknown()),
  mode: z.enum(['poll', 'push', 'webhook']), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(), maxEvents: z.number().int().positive().optional(),
  ttlMs: z.number().int().positive().nullable().optional(),
});

class SubscriptionTerminatedError extends EventProtocolError {}

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
  private readonly settings: Required<Omit<ClientOptions, 'receiver'>> & Pick<ClientOptions, 'receiver'>;
  private readonly lifetime = new AbortController();
  private readonly watches = new Map<string, Watch>();
  private contracts = new Map<string, EventContract>();
  private refreshing?: Promise<EventDescriptor[]>;
  private catalogLoop?: Promise<void>;

  constructor(wire: EventsWire, settings: ClientOptions = {}) {
    this.wire = wire;
    this.settings = {
      pollFloorMs: 1000, heartbeatMs: 30_000, retryMs: 1000, maxRetries: 10, ...settings,
    };
  }
  async connect() {
    const extension = await this.wire.connect(this.lifetime.signal);
    await this.refresh();
    if (extension.listChanged === true) this.catalogLoop = this.listenCatalog();
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
  private async listenCatalog() {
    let failures = 0;
    while (!this.lifetime.signal.aborted && failures <= this.settings.maxRetries) {
      const attempt = new AbortController();
      const signal = AbortSignal.any([this.lifetime.signal, attempt.signal]);
      let acknowledged = false;
      const timer = setTimeout(() => attempt.abort(new Error('Catalog subscription was not acknowledged')), 30_000);
      try {
        await this.wire.stream('subscriptions/listen', {notifications: {eventsListChanged: true}}, signal, async (method, params) => {
          if (method === 'notifications/subscriptions/acknowledged') {
            const ack = z.object({notifications: z.object({eventsListChanged: z.boolean().optional()})}).parse(params);
            if (!ack.notifications.eventsListChanged) throw new Error('Server did not acknowledge event catalog changes');
            acknowledged = true;
            clearTimeout(timer);
            await this.refresh();
            failures = 0;
            return;
          }
          if (!acknowledged) throw new Error('Catalog notification arrived before acknowledgement');
          if (method === 'notifications/events/list_changed') await this.refresh();
        });
      } catch (error) {
        if (this.lifetime.signal.aborted || fatal(error)) return;
      } finally { clearTimeout(timer); }
      if (failures >= this.settings.maxRetries) return;
      await this.delay(failures++, this.lifetime.signal).catch(() => {});
    }
  }
  private contract(options: WatchOptions) {
    const contract = this.contracts.get(options.name);
    if (!contract) throw new EventProtocolError({code: -32023, message: 'Event is not in the current catalog'});
    if (!contract.descriptor.delivery.includes(options.mode)) throw new EventProtocolError({code: -32026, message: 'Event does not offer this delivery mode'});
    contract.arguments(options.arguments);
    return contract;
  }
  watch(options: WatchOptions, accept: (observation: Observation) => Promise<void> | void, checkpoint?: (cursor: string | null) => Promise<void> | void): Watch {
    options = watchSchema.parse(options);
    this.contract(options);
    if (this.lifetime.signal.aborted) throw new Error('MCP Events client is closed');
    if (options.mode === 'webhook' && !this.settings.receiver) throw new Error('Webhook delivery requires a configured HTTPS receiver');
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
    const advance = async (next: string | null, truncated: boolean) => {
      if (next !== null && truncated) await accept({type: 'gap', cursor: next});
      await checkpoint?.(next);
      cursor = next;
    };
    const params = () => ({
      name: options.name, arguments: options.arguments, cursor,
      ...(options.maxAgeMs === undefined ? {} : {maxAgeMs: options.maxAgeMs}),
    });
    const markReady = () => { ready.resolve(); };
    const status = async (value: Extract<Observation, {type: 'status'}>['status']) => {
      try { await accept({type: 'status', status: value}); }
      catch { /* A full consumer inbox must not prevent delivery retries. */ }
    };
    const maxEvents = Math.min(options.maxEvents ?? 50, 50);
    let pendingPoll: {result: z.infer<typeof pollResult>; index: number; gapAccepted: boolean} | undefined;
    let rediscover = false;
    const poll = async () => {
      while (!signal.aborted) {
        if (!pendingPoll) {
          const result = await this.wire.request('events/poll', {...params(), maxEvents}, pollResult, {signal});
          if (result.events.length > maxEvents || Buffer.byteLength(JSON.stringify(result)) > 256 * 1024) throw new Error('Poll response exceeds the requested batch or memory limit');
          pendingPoll = {result, index: 0, gapAccepted: false};
        }
        const {result} = pendingPoll;
        if (!pendingPoll.gapAccepted) {
          if (result.cursor !== null && result.truncated) await accept({type: 'gap', cursor: result.cursor});
          pendingPoll.gapAccepted = true;
        }
        while (pendingPoll.index < result.events.length) {
          await emit(result.events[pendingPoll.index]!);
          pendingPoll.index++;
        }
        await advance(result.cursor, false);
        pendingPoll = undefined;
        failures = 0;
        markReady();
        if (!result.hasMore) await sleep(Math.max(this.settings.pollFloorMs, result.nextPollMs), undefined, {signal});
      }
    };
    const push = async () => {
      const attempt = new AbortController();
      let lastSeen = Date.now();
      let active = false;
      const timer = setInterval(() => {
        if (Date.now() - lastSeen > this.settings.heartbeatMs * 2) attempt.abort(new Error('Event stream heartbeat expired'));
      }, this.settings.heartbeatMs);
      try {
        await this.wire.stream('events/stream', params(), AbortSignal.any([signal, attempt.signal]), async (method, value) => {
          if (method === 'notifications/events/active') {
            const message = activeNotification.parse(value);
            await advance(message.cursor, message.truncated);
            active = true;
            lastSeen = Date.now();
            markReady();
            return;
          }
          if (!active) throw new Error('Event stream did not begin with active');
          switch (method) {
            case 'notifications/events/event': {
              const event = occurrence.parse(value);
              await emit(event);
              await advance(event.cursor, false);
              lastSeen = Date.now();
              break;
            }
            case 'notifications/events/heartbeat':
              await advance(heartbeatNotification.parse(value).cursor, false);
              lastSeen = Date.now();
              failures = 0;
              break;
            case 'notifications/events/error':
              errorNotification.parse(value);
              await status('source-error');
              break;
            case 'notifications/events/terminated':
              throw new SubscriptionTerminatedError(errorNotification.parse(value).error);
            default: throw new Error('Unexpected notification on event stream');
          }
        });
        if (!active) throw new Error('Event stream did not begin with active');
      } finally { clearInterval(timer); }
    };
    let registration: WebhookRegistration | undefined;
    let webhookAbort: AbortController | undefined;
    const webhook = async () => {
      const receiver = this.settings.receiver!;
      await receiver.start();
      webhookAbort = new AbortController();
      const attemptSignal = AbortSignal.any([signal, webhookAbort.signal]);
      registration ??= receiver.register(async body => {
        if (signal.aborted) throw new Error('Subscription is stopping');
        if ('type' in body) {
          if (body.type === 'gap') await advance(body.cursor, true);
          if (body.type === 'terminated') {
            webhookAbort?.abort(new SubscriptionTerminatedError(body.error));
          }
        } else {
          await emit(body);
          await advance(body.cursor, false);
        }
      });
      let granted = false;
      while (!attemptSignal.aborted) {
        let result;
        try {
          result = await this.wire.request('events/subscribe', {
            ...params(), delivery: {mode: 'webhook', url: registration.url, secret: registration.secret},
            ...(options.ttlMs === undefined ? {} : {ttlMs: options.ttlMs}),
          }, subscribeResult, {signal: attemptSignal});
        } catch (error) {
          if (webhookAbort.signal.aborted) throw webhookAbort.signal.reason;
          throw error;
        }
        attemptSignal.throwIfAborted();
        registration.bind(result.id);
        if (result.refreshBefore === null && options.ttlMs !== null) throw new Error('Server granted unrequested no-expiry subscription');
        await advance(result.cursor, result.truncated);
        if (result.deliveryStatus?.active === false) await status('delivery-suspended');
        const remaining = result.refreshBefore === null ? 5 * 60_000 : Date.parse(result.refreshBefore) - Date.now();
        if (remaining <= 0) throw new Error('Server granted an already expired webhook subscription');
        if (granted) failures = 0;
        granted = true;
        markReady();
        try { await sleep(Math.min(remaining * 0.75, 5 * 60_000), undefined, {signal: attemptSignal}); }
        catch (error) { if (webhookAbort.signal.aborted) throw webhookAbort.signal.reason; throw error; }
      }
      attemptSignal.throwIfAborted();
    };
    const run = async () => {
      try {
        while (!signal.aborted) {
          try {
            if (rediscover) {
              await this.refresh();
              this.contract(options);
              await accept({type: 'gap', cursor: null});
              await advance(null, false);
              pendingPoll = undefined;
              rediscover = false;
            }
            this.contract(options);
            if (options.mode === 'poll') await poll();
            else if (options.mode === 'push') await push();
            else await webhook();
          } catch (error) {
            if (signal.aborted) break;
            if (needsRediscovery(error)) {
              if (rediscover) throw error;
              rediscover = true;
            } else if (error instanceof SubscriptionTerminatedError || fatal(error)) throw error;
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
        if (registration) {
          try {
            await this.wire.request('events/unsubscribe', {
              name: options.name, arguments: options.arguments, delivery: {url: registration.url},
            }, completeResult, {timeout: 5000});
          } catch { /* TTL is the fallback when the server is unreachable. */ }
          registration.close();
        }
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
    await this.catalogLoop;
    await this.wire.close();
  }
}
