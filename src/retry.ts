import pRetry from 'p-retry';
import delay from 'delay';
import {EventsClientError, EventsClientErrorCode} from './errors.ts';
import {EventProtocolError, EventProtocolErrorCode} from './protocol.ts';

type RetryOptions = {
  maxRetries: number;
  retryMs: number;
  signal: AbortSignal;
  onRetry(): Promise<void>;
  shouldRetry?(error: unknown): boolean;
  randomize?: boolean;
  retryFloorMs?: number;
};

export function retryable(error: unknown): boolean {
  if (error instanceof EventsClientError) {
    return error.code === EventsClientErrorCode.RequestFailed
      || error.code === EventsClientErrorCode.ConsumerRejected
      || error.code === EventsClientErrorCode.CheckpointFailed;
  }
  if (!(error instanceof EventProtocolError)) return false;
  return !([
    EventProtocolErrorCode.MethodNotFound, EventProtocolErrorCode.InvalidParams,
    EventProtocolErrorCode.NotFound, EventProtocolErrorCode.Forbidden,
    EventProtocolErrorCode.ResourceExhausted, EventProtocolErrorCode.Unsupported,
  ] as number[]).includes(error.code);
}

export async function retryOperation<T>(operation: () => Promise<T>, options: RetryOptions): Promise<T> {
  const shouldRetry = options.shouldRetry ?? retryable;
  const minimumDelay = Math.max(options.retryMs, options.retryFloorMs ?? 0);
  try {
    return await pRetry(operation, {
      retries: options.maxRetries, minTimeout: Math.min(minimumDelay, 30_000), maxTimeout: 30_000, factor: 2,
      randomize: options.randomize,
      signal: options.signal,
      shouldRetry: ({error}) => shouldRetry(error),
      onFailedAttempt: async ({error, retriesLeft, retryDelay}) => {
        if (retriesLeft <= 0 || !shouldRetry(error)) return;
        await options.onRetry();
        // delay supports floors longer than p-retry's native timer can safely wait.
        if (minimumDelay > retryDelay) await delay(minimumDelay - retryDelay, {signal: options.signal});
      },
    });
  } catch (error) {
    options.signal.throwIfAborted();
    if (!shouldRetry(error)) throw error;
    throw new EventsClientError(EventsClientErrorCode.RetryExhausted, 'Event delivery retry limit reached', {
      cause: error, details: {maxRetries: options.maxRetries},
    });
  }
}
