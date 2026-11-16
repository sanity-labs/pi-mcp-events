import delay from 'delay';
import {z} from 'zod';
import {EventsClientError, EventsClientErrorCode as ErrorCode} from './errors.ts';
import {
  activeNotification, errorNotification, EventMethod, EventNotification, EventProtocolError, heartbeatNotification, occurrence,
  type EventOccurrence,
} from './protocol.ts';
import type {EventsWire} from './wire.ts';

type StreamHandlers = {
  active(cursor: string | null, truncated: boolean): Promise<void>;
  event(event: EventOccurrence): Promise<void>;
  heartbeat(cursor: string | null): Promise<void>;
  sourceError(error: EventProtocolError): Promise<void>;
};

export async function streamEvents(
  wire: EventsWire, params: Record<string, unknown>, signal: AbortSignal,
  heartbeatMs: number, handlers: StreamHandlers,
): Promise<void> {
  const attempt = new AbortController();
  const streamSignal = AbortSignal.any([signal, attempt.signal]);
  let active = false;
  let grantedHeartbeatMs = heartbeatMs;
  let deadline = new AbortController();
  const resetDeadline = (timeoutMs: number) => {
    deadline.abort();
    deadline = new AbortController();
    const deadlineSignal = AbortSignal.any([streamSignal, deadline.signal]);
    void delay(timeoutMs, {signal: deadlineSignal}).then(() => {
      attempt.abort(new EventsClientError(ErrorCode.RequestFailed, 'Event stream heartbeat expired'));
    }).catch(() => {});
  };
  resetDeadline(heartbeatMs * 2);
  try {
    await wire.stream(EventMethod.Stream, params, streamSignal, async (method, value) => {
      deadline.abort();
      try {
        if (method === EventNotification.Active) {
          const message = activeNotification.parse(value);
          grantedHeartbeatMs = message.heartbeatMs ?? grantedHeartbeatMs;
          await handlers.active(message.cursor, message.truncated);
          active = true;
          return;
        }
        if (!active) throw new EventsClientError(ErrorCode.InvalidResponse, 'Event stream did not begin with active');
        switch (method) {
          case EventNotification.Event:
            await handlers.event(occurrence.parse(value));
            return;
          case EventNotification.Heartbeat:
            await handlers.heartbeat(heartbeatNotification.parse(value).cursor);
            return;
          case EventNotification.Error:
            await handlers.sourceError(new EventProtocolError(errorNotification.parse(value).error));
            return;
          case EventNotification.Terminated:
            throw new EventsClientError(ErrorCode.SubscriptionTerminated, 'Event subscription terminated by the server', {
              cause: new EventProtocolError(errorNotification.parse(value).error),
            });
          default:
            throw new EventsClientError(ErrorCode.InvalidResponse, 'Unexpected notification on event stream', {details: {method}});
        }
      } catch (error) {
        if (error instanceof z.ZodError) throw new EventsClientError(ErrorCode.InvalidResponse, 'Invalid event stream notification', {cause: error});
        throw error;
      } finally {
        if (active && !streamSignal.aborted) resetDeadline(grantedHeartbeatMs * 3);
      }
    });
    if (!active) throw new EventsClientError(ErrorCode.InvalidResponse, 'Event stream did not begin with active');
    throw new EventsClientError(ErrorCode.RequestFailed, 'Event stream ended');
  } catch (error) {
    streamSignal.throwIfAborted();
    throw error;
  } finally {
    attempt.abort();
    deadline.abort();
  }
}
