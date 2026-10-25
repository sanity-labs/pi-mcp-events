import type {JSONRPCMessage, JSONRPCRequest, Transport, TransportSendOptions} from '@modelcontextprotocol/client';
import {EVENTS_EXTENSION, PROTOCOL_VERSION, SUBSCRIPTION_ID} from '../src/protocol.ts';

export class TestTransport implements Transport {
  onmessage?: Transport['onmessage'];
  onclose?: Transport['onclose'];
  onerror?: Transport['onerror'];
  hasPerRequestStream = false;
  supportsEvents = true;
  listChanged = false;
  messages: Array<{message: JSONRPCMessage; options?: TransportSendOptions}> = [];
  handle: (message: JSONRPCRequest, options?: TransportSendOptions) => void | Promise<void> = () => {};
  async start() {}
  async close() { this.onclose?.(); }
  async send(message: JSONRPCMessage, options?: TransportSendOptions) {
    this.messages.push({message, options});
    if (!('method' in message) || !('id' in message)) return;
    if (message.method === 'server/discover') {
      this.respond(message, {
        supportedVersions: [PROTOCOL_VERSION],
        capabilities: this.supportsEvents ? {extensions: {[EVENTS_EXTENSION]: {listChanged: this.listChanged}}} : {},
      });
      return;
    }
    await this.handle(message, options);
  }
  respond(request: JSONRPCRequest, result: Record<string, unknown>) {
    this.onmessage?.({jsonrpc: '2.0', id: request.id, result: {resultType: 'complete', ...result}});
  }
  notify(request: JSONRPCRequest, kind: string, params: Record<string, unknown>) {
    this.onmessage?.({jsonrpc: '2.0', method: `notifications/events/${kind}`, params: {...params, _meta: {[SUBSCRIPTION_ID]: request.id}}});
  }
  fail(request: JSONRPCRequest, code: number, data?: Record<string, unknown>) {
    this.onmessage?.({jsonrpc: '2.0', id: request.id, error: {code, message: 'test error', data}});
  }
}

export const testDescriptor = {
  name: 'example.changed', delivery: ['poll', 'push', 'webhook'],
  inputSchema: {type: 'object', properties: {group: {type: 'string'}}},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'number'}}},
};
export const event = (id: string, cursor?: string | null) => ({
  eventId: id, name: testDescriptor.name, timestamp: new Date().toISOString(), data: {value: 1}, cursor,
});
