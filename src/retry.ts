import pRetry from 'p-retry';
import {EventsClientError, EventsClientErrorCode} from './errors.ts';
import {EventProtocolError, EventProtocolErrorCode} from './protocol.ts';

type RetryOptions = {
  maxRetries: number;
  retryMs: number;
  signal: AbortSignal;
  onRetry(): Promise<void>;
};

function retryable(error: unknown): boolean {
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
  try {
    return await pRetry(operation, {
      retries: options.maxRetries, minTimeout: options.retryMs, maxTimeout: 30_000, factor: 2,
      signal: options.signal,
      shouldRetry: ({error}) => retryable(error),
      onFailedAttempt: async ({error, retriesLeft}) => {
        if (retriesLeft > 0 && retryable(error)) await options.onRetry();
      },
    });
  } catch (error) {
    options.signal.throwIfAborted();
    if (!retryable(error)) throw error;
    throw new EventsClientError(EventsClientErrorCode.RetryExhausted, 'Event delivery retry limit reached', {
      cause: error, details: {maxRetries: options.maxRetries},
    });
  }
}
