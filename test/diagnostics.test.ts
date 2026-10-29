import {expect, it} from 'vitest';
import {failureDiagnostic, webhookDiagnostic} from '../src/diagnostics.ts';
import {EventsClientError, EventsClientErrorCode as Code} from '../src/errors.ts';
import {EventProtocolError, EventProtocolErrorCode as ProtocolCode} from '../src/protocol.ts';

it.each([
  [ProtocolCode.InvalidParams, 'parameters', 'arguments'],
  [ProtocolCode.NotFound, 'could not find', 'catalog'],
  [ProtocolCode.Forbidden, 'denied access', 'credentials'],
  [ProtocolCode.ResourceExhausted, 'limit', 'quotas'],
  [ProtocolCode.Unsupported, 'requested option', 'delivery modes'],
  [ProtocolCode.MethodNotFound, 'method', 'protocol support'],
])('describes protocol error %i with an actionable reason', (code, reason, action) => {
  const text = failureDiagnostic(new EventProtocolError({code, message: 'private remote text', data: {secret: 'private remote data'}}));
  expect(text).toContain(reason);
  expect(text).toContain(action);
  expect(text).toContain(`MCP ${code}`);
  expect(text).not.toContain('private');
});

it.each([
  ['connection_refused', 'running and reachable'], ['timeout', 'response time'],
  ['tls_error', 'certificate'], ['http_4xx', 'routing and authentication'],
  ['http_5xx', 'receiver before restarting'], ['challenge_failed', 'challenge handling'],
])('only reads the recognized callback reason %s', (reason, action) => {
  const text = failureDiagnostic(new EventProtocolError({code: ProtocolCode.CallbackEndpointError, message: 'https://private.invalid/secret', data: {reason, body: 'private response'}}));
  expect(text).toContain(action);
  expect(text).not.toContain('private');
});

it.each([
  [Code.EventNotFound, 'configured name'], [Code.UnsupportedDeliveryMode, 'Choose a mode'],
  [Code.UnsupportedEventSchema, "server's event schemas"], [Code.InvalidArguments, 'configured arguments'],
  [Code.InvalidOptions, 'configuration'], [Code.InvalidResponse, 'implementation'],
  [Code.InvalidEvent, 'schemas'], [Code.InvalidCatalog, 'schemas'],
  [Code.LimitExceeded, 'client limits'], [Code.ConsumerRejected, 'inbox'],
  [Code.CheckpointFailed, 'storage'],
])('describes local error %s without its arbitrary details', (code, action) => {
  const text = failureDiagnostic(new EventsClientError(code, 'private message', {cause: new Error('private cause'), details: {path: 'private path'}}));
  expect(text).toContain(action);
  expect(text).not.toContain('private');
});

it('keeps retry and termination context together with known underlying reasons', () => {
  const error = new EventsClientError(Code.RetryExhausted, 'private', {cause:
    new EventsClientError(Code.SubscriptionTerminated, 'private', {cause:
      new EventProtocolError({code: ProtocolCode.Forbidden, message: 'private'})})});
  expect(failureDiagnostic(error)).toMatch(/Retry limit reached.*ended the subscription.*denied access/);
  const local = new EventsClientError(Code.RetryExhausted, 'private', {cause: new EventsClientError(Code.ConsumerRejected, 'private')});
  expect(failureDiagnostic(local)).toMatch(/Retry limit reached.*inbox/);
});

it('bounds cyclic causes and never walks arbitrary nested causes', () => {
  const error = new EventsClientError(Code.ConnectionFailed, 'private');
  error.cause = error;
  expect(failureDiagnostic(error)).not.toContain('private');
  const arbitrary = new Error('private', {cause: new EventProtocolError({code: ProtocolCode.Forbidden, message: 'private'})});
  expect(failureDiagnostic(arbitrary)).not.toContain('denied access');
  expect(failureDiagnostic(arbitrary)).not.toContain('private');
});

it('falls back safely for unknown codes, callback reasons, and data shapes', () => {
  for (const data of [null, 'private', {reason: 'https://private.invalid/secret'}, {reason: 'constructor'}, {reason: {secret: 'private'}}]) {
    expect(failureDiagnostic(new EventProtocolError({code: ProtocolCode.CallbackEndpointError, message: 'private', data}))).not.toContain('private');
  }
  expect(failureDiagnostic(new EventProtocolError({code: -32099, message: 'private'}))).toContain('MCP -32099');
  expect(failureDiagnostic(new Error('private'))).not.toContain('private');
});

it('keeps healthy refreshes quiet and describes suspension recovery and throttling', () => {
  expect(webhookDiagnostic({active: true, lastError: null})).toBeUndefined();
  expect(webhookDiagnostic({active: false, lastError: 'timeout'})).toMatch(/resumed after suspension.*timed out/);
  expect(webhookDiagnostic({active: true, throttled: true, retryAfterMs: 1000})).toContain('rate limits');
});
