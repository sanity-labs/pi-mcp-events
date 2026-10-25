import {afterEach, expect, it, vi} from 'vitest';
import {EventsClientError, EventsClientErrorCode as ErrorCode} from '../src/index.ts';
import {retryOperation} from '../src/retry.ts';

afterEach(() => vi.useRealTimers());

it('grows exponential backoff beyond 32 times the base and caps it at 30 seconds', async () => {
  vi.useFakeTimers();
  const operation = vi.fn(async () => {
    if (operation.mock.calls.length <= 10) throw new EventsClientError(ErrorCode.RequestFailed, 'offline');
    return 'connected';
  });
  const result = retryOperation(operation, {maxRetries: 10, retryMs: 100, signal: new AbortController().signal, onRetry: async () => {}});
  await vi.advanceTimersByTimeAsync(0);
  for (const [index, interval] of [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 30000].entries()) {
    await vi.advanceTimersByTimeAsync(interval - 1);
    expect(operation).toHaveBeenCalledTimes(index + 1);
    await vi.advanceTimersByTimeAsync(1);
    expect(operation).toHaveBeenCalledTimes(index + 2);
  }
  await expect(result).resolves.toBe('connected');
});

it('preserves the final failure as the cause of retry exhaustion', async () => {
  const cause = new EventsClientError(ErrorCode.CheckpointFailed, 'disk full');
  const operation = vi.fn(async () => { throw cause; });
  await expect(retryOperation(operation, {maxRetries: 1, retryMs: 1, signal: new AbortController().signal, onRetry: async () => {}}))
    .rejects.toMatchObject({code: ErrorCode.RetryExhausted, cause, details: {maxRetries: 1}});
  expect(operation).toHaveBeenCalledTimes(2);
});

it('does not retry permanent local errors or report that they are retrying', async () => {
  const error = new EventsClientError(ErrorCode.InvalidArguments, 'invalid');
  const operation = vi.fn(async () => { throw error; });
  const onRetry = vi.fn(async () => {});
  await expect(retryOperation(operation, {maxRetries: 10, retryMs: 1, signal: new AbortController().signal, onRetry})).rejects.toBe(error);
  expect(operation).toHaveBeenCalledOnce();
  expect(onRetry).not.toHaveBeenCalled();
});

it('cancels a pending retry delay without another attempt', async () => {
  vi.useFakeTimers();
  const controller = new AbortController();
  const reason = new EventsClientError(ErrorCode.Stopped, 'stopped');
  const operation = vi.fn(async () => { throw new EventsClientError(ErrorCode.ConsumerRejected, 'busy'); });
  const result = retryOperation(operation, {maxRetries: 10, retryMs: 1000, signal: controller.signal, onRetry: async () => {}});
  const rejected = expect(result).rejects.toBe(reason);
  await vi.advanceTimersByTimeAsync(0);
  controller.abort(reason);
  await rejected;
  await vi.advanceTimersByTimeAsync(30_000);
  expect(operation).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});
