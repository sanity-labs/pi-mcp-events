import {z} from 'zod';
import {createRequire} from 'node:module';
import {ProtocolErrorCode} from '@modelcontextprotocol/client';

// Pinned draft: https://github.com/modelcontextprotocol/modelcontextprotocol/blob/b0efd116faca84c9caad109ccf0778da4dc49579/seps/0000-events-extension.md
export const PROTOCOL_VERSION = '2026-07-28';
export const EVENTS_EXTENSION = 'io.modelcontextprotocol/events';
export const SUBSCRIPTION_ID = 'io.modelcontextprotocol/subscriptionId';
export const EventMethod = {
  List: 'events/list',
  Poll: 'events/poll',
  Stream: 'events/stream',
  Subscribe: 'events/subscribe',
  Unsubscribe: 'events/unsubscribe',
} as const;
export type EventMethod = typeof EventMethod[keyof typeof EventMethod];
export const EventNotification = {
  Active: 'notifications/events/active',
  Event: 'notifications/events/event',
  Heartbeat: 'notifications/events/heartbeat',
  Error: 'notifications/events/error',
  Terminated: 'notifications/events/terminated',
  ListChanged: 'notifications/events/list_changed',
} as const;
export type EventNotification = typeof EventNotification[keyof typeof EventNotification];
export const EventProtocolErrorCode = {
  MethodNotFound: ProtocolErrorCode.MethodNotFound,
  InvalidParams: ProtocolErrorCode.InvalidParams,
  InternalError: ProtocolErrorCode.InternalError,
  NotFound: -32023,
  Forbidden: -32024,
  ResourceExhausted: -32025,
  Unsupported: -32026,
  CallbackEndpointError: -32027,
} as const;
export const clientInfo = {name: 'pi-mcp-events', version: createRequire(import.meta.url)('../package.json').version as string};
export const clientCapabilities = {extensions: {[EVENTS_EXTENSION]: {}}};
export const record = z.record(z.string(), z.unknown());
export const cursor = z.string().nullable().optional().transform(value => value ?? null);
export const deliveryMode = z.enum(['poll', 'push', 'webhook']);
export type DeliveryMode = z.infer<typeof deliveryMode>;
const jsonSchema = z.union([z.boolean(), record]);
const extendedTimestamp = z.iso.datetime({offset: true});
const timestamp = z.string().refine(value => extendedTimestamp.safeParse(normalizeTimestampOffset(value)).success, 'Invalid ISO 8601 timestamp');

export function normalizeTimestampOffset(value: string): string {
  return value.replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
}

export const descriptor = z.object({
  name: z.string().min(1),
  description: z.string().optional(),
  delivery: z.array(deliveryMode).min(1),
  inputSchema: jsonSchema,
  payloadSchema: jsonSchema,
  _meta: record.optional(),
});
export type EventDescriptor = z.infer<typeof descriptor>;
export const occurrence = z.object({
  eventId: z.string(),
  name: z.string(),
  timestamp,
  data: record,
  cursor,
  _meta: record.optional(),
});
export type EventOccurrence = z.infer<typeof occurrence>;
// The SDK validates and removes resultType before applying custom result schemas.
export const completeResult = z.object({});
export const catalogResult = completeResult.extend({events: z.array(descriptor), nextCursor: z.string().optional()});
export const pollResult = completeResult.extend({
  events: z.array(occurrence), cursor, truncated: z.boolean(),
  hasMore: z.boolean(), nextPollMs: z.number().int().nonnegative(),
});
export const subscribeResult = completeResult.extend({
  id: z.string().min(1), refreshBefore: timestamp.nullable(),
  cursor, truncated: z.boolean(),
  deliveryStatus: z.object({
    active: z.boolean(), lastDeliveryAt: timestamp.nullable().optional(),
    lastError: z.enum(['connection_refused', 'timeout', 'tls_error', 'http_4xx', 'http_5xx', 'challenge_failed']).nullable().optional(),
    failedSince: timestamp.nullable().optional(),
    throttled: z.boolean().optional(), retryAfterMs: z.number().int().nonnegative().optional(),
  }).optional(),
});
export type WebhookDeliveryStatus = NonNullable<z.infer<typeof subscribeResult>['deliveryStatus']>;
export const remoteError = z.object({code: z.number().int(), message: z.string(), data: record.optional()});
export const activeNotification = z.object({cursor, truncated: z.boolean()});
export const heartbeatNotification = z.object({cursor});
export const errorNotification = z.object({error: remoteError});

export class EventProtocolError extends Error {
  readonly code: number;
  readonly data?: unknown;
  constructor(error: {code: number; message: string; data?: unknown}, options?: ErrorOptions) {
    super(error.message, options);
    this.name = 'EventProtocolError';
    this.code = error.code;
    this.data = error.data;
  }
}

export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (typeof value === 'object' && value !== null) {
    return `{${Object.entries(value).sort(([a], [b]) => {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    })
      .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(',')}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error('Expected a JSON value');
  return encoded;
}
