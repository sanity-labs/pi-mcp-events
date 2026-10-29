import {EventsClientError, EventsClientErrorCode as Code} from './errors.ts';
import {EventProtocolError, EventProtocolErrorCode as ProtocolCode, type WebhookDeliveryStatus} from './protocol.ts';

const callbackFailures = {
  connection_refused: 'The webhook endpoint refused the connection. Check that the receiver is running and reachable.',
  timeout: 'The webhook endpoint timed out. Check receiver availability and response time.',
  tls_error: 'The webhook TLS connection failed. Check the endpoint certificate and TLS configuration.',
  http_4xx: 'The webhook endpoint rejected delivery. Check receiver routing and authentication.',
  http_5xx: 'The webhook endpoint returned a server error. Check the receiver before restarting.',
  challenge_failed: 'Webhook verification failed. Check the public receiver URL and challenge handling.',
} as const;

function callbackFailure(reason: unknown): string | undefined {
  if (typeof reason === 'string' && Object.hasOwn(callbackFailures, reason)) {
    return callbackFailures[reason as keyof typeof callbackFailures];
  }
}

function protocolDiagnostic(error: EventProtocolError): string {
  switch (error.code) {
    case ProtocolCode.InvalidParams: return 'The server rejected the subscription parameters. Check event arguments and webhook configuration.';
    case ProtocolCode.NotFound: return 'The server could not find the requested event or subscription. Refresh the catalog and check the configured event.';
    case ProtocolCode.Forbidden: return 'The server denied access. Check the configured credentials and event permissions.';
    case ProtocolCode.ResourceExhausted: return 'A server limit was reached. Check subscription and request quotas before restarting.';
    case ProtocolCode.Unsupported: return 'The server does not support a requested option. Check the advertised event schemas and delivery modes.';
    case ProtocolCode.CallbackEndpointError: {
      const data = error.data;
      const reason = typeof data === 'object' && data !== null && 'reason' in data ? data.reason : undefined;
      return callbackFailure(reason) ?? 'The webhook endpoint could not be verified or reached. Check receiver connectivity and configuration.';
    }
    case ProtocolCode.MethodNotFound: return 'The server does not support the requested method. Check its MCP Events and protocol support.';
    default: return 'The server reported an error. Check server health before restarting the watch.';
  }
}

function localDiagnostic(error: EventsClientError): string {
  switch (error.code) {
    case Code.EventNotFound: return 'The event is not in the current catalog. Check its configured name and refresh the catalog.';
    case Code.UnsupportedDeliveryMode: return 'The event does not support the configured delivery mode. Choose a mode it advertises.';
    case Code.UnsupportedEventSchema: return "The event schemas are unsupported or invalid. Check the server's event schemas.";
    case Code.InvalidArguments: return 'The event arguments do not match its schema. Check the configured arguments.';
    case Code.InvalidOptions: return 'The watch options are invalid. Check the event configuration.';
    case Code.InvalidResponse:
    case Code.InvalidEvent:
    case Code.InvalidCatalog: return 'The server returned invalid event data. Check its MCP Events implementation and schemas.';
    case Code.LimitExceeded: return 'A client limit was reached. Check event sizes and configured client limits.';
    case Code.ConsumerRejected: return 'The event could not be accepted. Check the event inbox and client limits before restarting.';
    case Code.CheckpointFailed: return 'Event progress could not be saved. Check event state storage before restarting.';
    default: return 'Event delivery failed. Check configuration, server access and local event storage.';
  }
}

export function failureDiagnostic(error: unknown): string {
  const wrappers: string[] = [];
  const seen = new Set<unknown>();
  while (error instanceof EventsClientError && !seen.has(error) && seen.size < 8) {
    seen.add(error);
    if (error.code === Code.RetryExhausted) wrappers.push('Retry limit reached.');
    else if (error.code === Code.SubscriptionTerminated) wrappers.push('The server ended the subscription.');
    else if (error.code !== Code.ConnectionFailed && error.code !== Code.RequestFailed) break;
    error = error.cause;
  }
  let reason: string;
  if (error instanceof EventProtocolError) {
    const code = Number.isSafeInteger(error.code) ? ` (MCP ${error.code})` : '';
    reason = `${protocolDiagnostic(error)}${code}`;
  } else if (error instanceof EventsClientError) reason = localDiagnostic(error);
  else reason = 'Check the server connection, credentials and MCP Events support before restarting.';
  return [...new Set(wrappers), reason].join(' ');
}

export function webhookDiagnostic(status: WebhookDeliveryStatus): string | undefined {
  const details: string[] = [];
  if (!status.active) details.push('Webhook delivery resumed after suspension.');
  const failure = callbackFailure(status.lastError);
  if (failure) details.push(`Last delivery failure: ${failure}`);
  if (status.throttled) details.push('The server is delaying webhook deliveries because of rate limits.');
  return details.length ? details.join(' ') : undefined;
}
