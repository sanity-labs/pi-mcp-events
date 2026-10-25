export const EventsClientErrorCode = {
  InvalidOptions: 'INVALID_OPTIONS',
  InvalidCatalog: 'INVALID_CATALOG',
  InvalidArguments: 'INVALID_ARGUMENTS',
  InvalidEvent: 'INVALID_EVENT',
  EventNotFound: 'EVENT_NOT_FOUND',
  UnsupportedDeliveryMode: 'UNSUPPORTED_DELIVERY_MODE',
  AlreadyWatching: 'ALREADY_WATCHING',
  Closed: 'CLOSED',
  Stopped: 'STOPPED',
  LimitExceeded: 'LIMIT_EXCEEDED',
  ConnectionFailed: 'CONNECTION_FAILED',
  RequestFailed: 'REQUEST_FAILED',
  InvalidResponse: 'INVALID_RESPONSE',
  ConsumerRejected: 'CONSUMER_REJECTED',
  CheckpointFailed: 'CHECKPOINT_FAILED',
  RetryExhausted: 'RETRY_EXHAUSTED',
  SubscriptionTerminated: 'SUBSCRIPTION_TERMINATED',
} as const;
export type EventsClientErrorCode = typeof EventsClientErrorCode[keyof typeof EventsClientErrorCode];

export class EventsClientError extends Error {
  readonly code: EventsClientErrorCode;
  readonly details?: Readonly<Record<string, unknown>>;
  constructor(code: EventsClientErrorCode, message: string, options: ErrorOptions & {details?: Record<string, unknown>} = {}) {
    super(message, options);
    this.name = 'EventsClientError';
    this.code = code;
    this.details = options.details;
  }
}

export function limitExceeded(limit: string, max: number) {
  return new EventsClientError(EventsClientErrorCode.LimitExceeded, `MCP Events client limit exceeded: ${limit} (${max})`, {details: {limit, max}});
}
