import delay from 'delay';
import {z} from 'zod';
import {EventsClientError, EventsClientErrorCode as ErrorCode} from './errors.ts';
import {normalizeRequestError} from './request-error.ts';
import {retryable, retryOperation} from './retry.ts';
import type {EventsWire} from './wire.ts';

const acknowledgement = z.object({notifications: z.object({eventsListChanged: z.literal(true)})});
type CatalogOptions = {
  catalogAckTimeoutMs: number;
  maxRetries: number;
  retryMs: number;
  signal: AbortSignal;
  refresh(): Promise<void>;
};

export function listenCatalog(wire: EventsWire, options: CatalogOptions) {
  const {signal} = options;
  const ready = Promise.withResolvers<void>();
  void ready.promise.catch(() => {});
  const retry = <T>(operation: () => Promise<T>) => retryOperation(operation, {...options, onRetry: async () => {}});
  const run = async () => {
    while (!signal.aborted) {
      await retry(async () => {
        const attempt = new AbortController();
        const deadline = new AbortController();
        const streamSignal = AbortSignal.any([signal, attempt.signal]);
        let acknowledged = false;
        let refreshed = false;
        const timer = delay(options.catalogAckTimeoutMs, {signal: AbortSignal.any([signal, deadline.signal])}).then(() => {
          attempt.abort(new EventsClientError(ErrorCode.RequestFailed, 'Catalog subscription was not acknowledged before the deadline'));
        }, () => {});
        const refresh = async () => {
          await retry(options.refresh);
          refreshed = true;
        };
        try {
          await wire.stream('subscriptions/listen', {notifications: {eventsListChanged: true}}, streamSignal, async (method, params) => {
            if (method === 'notifications/subscriptions/acknowledged') {
              if (acknowledged || !acknowledgement.safeParse(params).success) {
                throw new EventsClientError(ErrorCode.InvalidResponse, 'Invalid event catalog subscription acknowledgement');
              }
              acknowledged = true;
              deadline.abort();
              await refresh();
              ready.resolve();
              return;
            }
            if (!acknowledged) throw new EventsClientError(ErrorCode.InvalidResponse, 'Catalog notification arrived before acknowledgement');
            if (method !== 'notifications/events/list_changed') {
              throw new EventsClientError(ErrorCode.InvalidResponse, 'Unexpected notification on catalog stream', {details: {method}});
            }
            await refresh();
          });
          throw new EventsClientError(ErrorCode.RequestFailed, 'Catalog stream ended');
        } catch (error) {
          signal.throwIfAborted();
          const failure = normalizeRequestError(attempt.signal.aborted ? attempt.signal.reason : error);
          if (refreshed && retryable(failure) && options.maxRetries > 0) return;
          throw failure;
        } finally {
          deadline.abort();
          await timer;
        }
      });
      await delay(options.retryMs, {signal});
    }
  };
  const closed = Promise.resolve().then(run).catch(error => {
    ready.reject(error);
    if (!signal.aborted) throw error;
  }).finally(() => {
    ready.reject(new EventsClientError(ErrorCode.Stopped, 'Catalog subscription stopped before becoming active'));
  });
  void closed.catch(() => {});
  return {ready: ready.promise, closed};
}
