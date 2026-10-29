import delay from 'delay';
import {EventsClientError, EventsClientErrorCode as ErrorCode} from './errors.ts';
import {completeResult, EventMethod, EventProtocolError, normalizeTimestampOffset, subscribeResult, type EventOccurrence, type WebhookDeliveryStatus} from './protocol.ts';
import {normalizeRequestError} from './request-error.ts';
import {retryOperation} from './retry.ts';
import type {WebhookIdentity} from './webhook-identities.ts';
import type {WebhookReceiver} from './webhook.ts';
import type {EventsWire} from './wire.ts';

type SubscriptionOptions = {
  webhookIdentity?: WebhookIdentity;
  name: string;
  arguments: Record<string, unknown>;
  maxAgeMs?: number;
  ttlMs?: number | null;
  webhookRefreshMs: number;
  webhookUnsubscribeTimeoutMs: number;
  retryMs: number;
  maxRetries: number;
};
type Callbacks = {
  validate(): void;
  cursor(): string | null;
  active(cursor: string | null, truncated: boolean, deliveryStatus?: WebhookDeliveryStatus): Promise<void>;
  event(event: EventOccurrence): Promise<void>;
  gap(cursor: string | null): Promise<void>;
  deliveryStatus(status: WebhookDeliveryStatus): Promise<void>;
  refreshCatalog(): Promise<void>;
  retrying(): Promise<void>;
};

export async function subscribeWebhook(wire: EventsWire, receiver: WebhookReceiver, options: SubscriptionOptions, watchSignal: AbortSignal, callbacks: Callbacks) {
  const stopped = new AbortController();
  const signal = AbortSignal.any([watchSignal, stopped.signal]);
  let tail = Promise.resolve();
  // Grants and POSTs share one checkpoint queue, in the order they arrive.
  const enqueue = (operation: () => Promise<void>) => {
    const pending = tail.then(async () => { signal.throwIfAborted(); await operation(); });
    tail = pending.catch(error => { stopped.abort(error); });
    return pending;
  };
  type Grant = {cursor: string | null; truncated: boolean; deliveryStatus?: WebhookDeliveryStatus};
  let pendingUpdate: {result?: Grant; gapCursor?: string; retrying: boolean} | undefined;
  // Coalesce waiting updates without moving a grant watermark across a delivery.
  const queueUpdate = (result?: Grant) => {
    const gapCursor = result?.truncated && result.cursor !== null ? result.cursor : undefined;
    if (pendingUpdate) {
      if (result) pendingUpdate.result = result;
      pendingUpdate.gapCursor = gapCursor ?? pendingUpdate.gapCursor;
      pendingUpdate.retrying = result === undefined;
      return;
    }
    const update = {result, gapCursor, retrying: result === undefined};
    pendingUpdate = update;
    void enqueue(async () => {
      if (pendingUpdate === update) pendingUpdate = undefined;
      const grant = update.result;
      if (grant) {
        const truncated = update.gapCursor !== undefined && update.gapCursor === grant.cursor;
        if (update.gapCursor !== undefined && !truncated) await callbacks.gap(update.gapCursor);
        await callbacks.active(grant.cursor, truncated, grant.deliveryStatus);
        if (grant.deliveryStatus) await callbacks.deliveryStatus(grant.deliveryStatus);
      }
      if (update.retrying) await callbacks.retrying();
    }).catch(() => {});
  };
  signal.throwIfAborted();
  await receiver.start();
  signal.throwIfAborted();
  const registration = receiver.register(body => {
    signal.throwIfAborted();
    if ('type' in body && body.type === 'terminated') {
      stopped.abort(new EventsClientError(ErrorCode.SubscriptionTerminated, 'Webhook subscription terminated', {cause: new EventProtocolError(body.error)}));
      return;
    }
    pendingUpdate = undefined;
    return enqueue(async () => {
      if ('type' in body) await callbacks.gap(body.cursor);
      else await callbacks.event(body);
    });
  }, options.webhookIdentity);
  const retry = <T>(operation: () => Promise<T>) => retryOperation(operation, {
    ...options, signal, onRetry: async () => { queueUpdate(); },
  });
  let attempted = false;
  let noExpiry = false;
  try {
    while (!signal.aborted) {
      if (noExpiry) await retry(callbacks.refreshCatalog);
      const result = await retry(async () => {
        signal.throwIfAborted();
        callbacks.validate();
        attempted = true;
        try {
          return await wire.request(EventMethod.Subscribe, {
            name: options.name, arguments: options.arguments, cursor: callbacks.cursor(),
            delivery: {mode: 'webhook', url: registration.url, secret: registration.secret},
            ...(options.maxAgeMs === undefined ? {} : {maxAgeMs: options.maxAgeMs}),
            ...(options.ttlMs === undefined ? {} : {ttlMs: options.ttlMs}),
          }, subscribeResult, {signal});
        } catch (error) { signal.throwIfAborted(); throw normalizeRequestError(error); }
      });
      signal.throwIfAborted();
      noExpiry = result.refreshBefore === null;
      if (noExpiry && options.ttlMs !== null) throw new EventsClientError(ErrorCode.InvalidResponse, 'Server granted no expiry without an explicit request');
      const remaining = result.refreshBefore === null ? Infinity : Date.parse(normalizeTimestampOffset(result.refreshBefore)) - Date.now();
      if (remaining <= 0) throw new EventsClientError(ErrorCode.InvalidResponse, 'Webhook subscription grant has already expired');
      try { registration.bind(result.id); }
      catch (error) { throw new EventsClientError(ErrorCode.InvalidResponse, 'Server changed the webhook subscription ID', {cause: error}); }
      const renewAt = Date.now() + Math.min(remaining * 0.75, options.webhookRefreshMs);
      queueUpdate(result);
      await delay(Math.max(0, renewAt - Date.now()), {signal});
    }
    signal.throwIfAborted();
  } catch (error) {
    signal.throwIfAborted();
    throw error;
  } finally {
    stopped.abort(new EventsClientError(ErrorCode.Stopped, 'Webhook subscription stopped'));
    await registration.close();
    await tail;
    if (attempted) {
      const cleanup = new AbortController();
      try {
        await Promise.race([
          wire.request(EventMethod.Unsubscribe, {
            name: options.name, arguments: options.arguments, delivery: {url: registration.url},
          }, completeResult, {signal: cleanup.signal}),
          delay(options.webhookUnsubscribeTimeoutMs, {signal: cleanup.signal}),
        ]);
      } catch { /* Finite grants expire if eager cleanup cannot reach the server. */ }
      finally { cleanup.abort(); }
    }
  }
}
