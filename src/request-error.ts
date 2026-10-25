import {ProtocolError, SdkError, SdkErrorCode} from '@modelcontextprotocol/client';
import {EventsClientError, EventsClientErrorCode as ErrorCode} from './errors.ts';
import {EventProtocolError} from './protocol.ts';

export function normalizeRequestError(error: unknown) {
  if (error instanceof EventsClientError || error instanceof EventProtocolError) return error;
  if (error instanceof ProtocolError) return new EventProtocolError(error, {cause: error});
  if (error instanceof SdkError && error.code === SdkErrorCode.InvalidResult) {
    return new EventsClientError(ErrorCode.InvalidResponse, 'Invalid MCP Events response', {cause: error});
  }
  return new EventsClientError(ErrorCode.RequestFailed, 'MCP Events request failed', {cause: error});
}
