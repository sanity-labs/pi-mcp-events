import {randomUUID} from 'node:crypto';
import {
  Client, type JSONRPCMessage, type Transport, type TransportSendOptions,
  type StandardSchemaV1, type RequestOptions, StreamableHTTPClientTransport,
  type StreamableHTTPClientTransportOptions,
} from '@modelcontextprotocol/client';
import {
  EVENTS_EXTENSION, PROTOCOL_VERSION, SUBSCRIPTION_ID, clientCapabilities,
  clientInfo, record, remoteError, EventProtocolError,
} from './protocol.ts';

type Stream = {
  receive(message: JSONRPCMessage): void;
  end(error?: unknown): void;
};

export function nameHeader(name: string): string {
  if (name.length > 0 && name.trim() === name && /^[\x09\x20-\x7e]+$/.test(name)
    && !(name.startsWith('=?base64?') && name.endsWith('?='))) return name;
  return `=?base64?${Buffer.from(name).toString('base64')}?=`;
}

export function createHttpTransport(url: URL, options: StreamableHTTPClientTransportOptions = {}) {
  const baseFetch = options.fetch ?? fetch;
  return new StreamableHTTPClientTransport(url, {...options, fetch: async (input, init) => {
    if (typeof init?.body === 'string') {
      const message: unknown = JSON.parse(init.body);
      const parsed = record.safeParse(message);
      if (parsed.success && typeof parsed.data.method === 'string' && parsed.data.method.startsWith('events/')) {
        const headers = new Headers(init.headers);
        const params = record.safeParse(parsed.data.params);
        if (params.success && typeof params.data.name === 'string') headers.set('Mcp-Name', nameHeader(params.data.name));
        else headers.delete('Mcp-Name');
        init = {...init, headers};
      }
    }
    return baseFetch(input, init);
  }});
}

/** Intercepts only extension-owned stream IDs; ordinary SDK traffic passes through. */
class EventsTransport implements Transport {
  readonly raw: Transport;
  readonly streams = new Map<string, Stream>();
  onmessage?: Transport['onmessage'];
  onclose?: Transport['onclose'];
  onerror?: Transport['onerror'];
  private readonly disconnected: () => void;
  constructor(raw: Transport, disconnected: () => void) { this.raw = raw; this.disconnected = disconnected; }
  get hasPerRequestStream() { return this.raw.hasPerRequestStream; }
  get sessionId() { return this.raw.sessionId; }
  setProtocolVersion(version: string) { this.raw.setProtocolVersion?.(version); }
  setSupportedProtocolVersions(versions: string[]) { this.raw.setSupportedProtocolVersions?.(versions); }
  async start() {
    this.raw.onmessage = (message, extra) => {
      const correlation = 'method' in message
        ? message.params?._meta?.[SUBSCRIPTION_ID]
        : message.id;
      if (typeof correlation === 'string') {
        const stream = this.streams.get(correlation);
        if (stream) { stream.receive(message); return; }
        if (correlation.startsWith('pi-events:')) return;
      }
      this.onmessage?.(message, extra);
    };
    this.raw.onerror = error => this.onerror?.(error);
    this.raw.onclose = () => {
      this.disconnected();
      for (const stream of this.streams.values()) stream.end(new Error('MCP connection closed'));
      this.onclose?.();
    };
    await this.raw.start();
  }
  send(message: JSONRPCMessage, options?: TransportSendOptions) {
    if ('method' in message && message.method.startsWith('events/') && typeof message.params?.name === 'string') {
      options = {...options, headers: {...options?.headers, 'Mcp-Name': nameHeader(message.params.name)}};
    }
    return this.raw.send(message, options);
  }
  close() { return this.raw.close(); }
}

export class EventsWire {
  readonly client: Client;
  private transport: EventsTransport;
  private readonly factory?: () => Transport;
  private connected = false;
  private connectionAttempted = false;
  private extension?: Record<string, unknown>;
  private closed = false;
  private connecting?: Promise<Record<string, unknown>>;
  private connectionSignal?: AbortSignal;
  constructor(transport: Transport | (() => Transport)) {
    this.factory = typeof transport === 'function' ? transport : undefined;
    this.transport = this.wrap(typeof transport === 'function' ? transport() : transport);
    this.client = new Client(clientInfo, {
      capabilities: clientCapabilities,
      versionNegotiation: {mode: {pin: PROTOCOL_VERSION}},
    });
  }
  private wrap(transport: Transport) {
    return new EventsTransport(transport, () => { this.connected = false; });
  }
  connect(signal?: AbortSignal): Promise<Record<string, unknown>> {
    if (this.closed) return Promise.reject(new Error('MCP Events client is closed'));
    if (this.connecting) return this.connecting;
    if (this.connected) return Promise.resolve(this.extension!);
    this.connectionSignal = signal;
    const connect = async () => {
      try {
        if (this.connectionAttempted) {
          if (!this.factory) throw new Error('Reconnecting requires a transport factory');
          this.transport = this.wrap(this.factory());
        }
        this.connectionAttempted = true;
        await this.client.connect(this.transport, {signal});
        if (this.closed) throw new Error('MCP Events client is closed');
        const extension = this.client.getServerCapabilities()?.extensions?.[EVENTS_EXTENSION];
        if (!extension || typeof extension !== 'object') throw new Error('Server does not advertise MCP Events');
        this.extension = extension;
        this.connected = true;
        return extension;
      } catch (error) {
        await this.client.close();
        throw error;
      }
    };
    this.connecting = connect().finally(() => { this.connecting = undefined; });
    return this.connecting;
  }
  private async ensureConnected() {
    if (this.closed) throw new Error('MCP Events client is closed');
    if (this.connected) return;
    if (this.connecting) { await this.connecting; return; }
    if (!this.factory || this.closed) throw new Error('MCP Events client is not connected');
    this.connectionSignal?.throwIfAborted();
    await this.connect(this.connectionSignal);
  }
  async request<S extends StandardSchemaV1>(method: string, params: Record<string, unknown>, schema: S, options?: RequestOptions) {
    await this.ensureConnected();
    options?.signal?.throwIfAborted();
    return this.client.request({method, params}, schema, options);
  }

  async stream(
    method: 'events/stream' | 'subscriptions/listen',
    params: Record<string, unknown>,
    signal: AbortSignal,
    onNotification: (method: string, params: Record<string, unknown>) => Promise<void> | void,
  ): Promise<void> {
    await this.ensureConnected();
    signal.throwIfAborted();
    const id = `pi-events:${randomUUID()}`;
    const requestAbort = new AbortController();
    const done = Promise.withResolvers<void>();
    let ended = false;
    let queued = 0;
    let bytes = 0;
    let writes = Promise.resolve();
    const end = (error?: unknown) => {
      if (ended) return;
      ended = true;
      this.transport.streams.delete(id);
      requestAbort.abort();
      signal.removeEventListener('abort', abort);
      if (error === undefined) done.resolve();
      else done.reject(error);
    };
    const cancel = () => {
      requestAbort.abort();
      if (!this.transport.hasPerRequestStream) {
        void this.client.notification({method: 'notifications/cancelled', params: {requestId: id}}).catch(() => {});
      }
    };
    const abort = () => { cancel(); end(signal.reason); };
    this.transport.streams.set(id, {
      end,
      receive: message => {
        const size = Buffer.byteLength(JSON.stringify(message));
        if (++queued > 100 || (bytes += size) > 256 * 1024) {
          cancel();
          end(new Error('Event stream backlog exceeded its memory limit'));
          return;
        }
        writes = writes.then(async () => {
          if (ended) return;
          if ('error' in message) throw new EventProtocolError(remoteError.parse(message.error));
          if ('method' in message) {
            await onNotification(message.method, record.parse(message.params));
          } else {
            if (message.result.resultType !== 'complete') throw new Error('Invalid final stream result');
            end();
          }
        }).catch(error => { cancel(); end(error); }).finally(() => { queued--; bytes -= size; });
      },
    });
    signal.addEventListener('abort', abort, {once: true});
    const meta = {
      'io.modelcontextprotocol/protocolVersion': PROTOCOL_VERSION,
      'io.modelcontextprotocol/clientInfo': clientInfo,
      'io.modelcontextprotocol/clientCapabilities': clientCapabilities,
    };
    void this.transport.send({jsonrpc: '2.0', id, method, params: {...params, _meta: meta}}, {
      requestSignal: requestAbort.signal,
      onRequestStreamEnd: () => {
        void writes.then(() => end(new Error('Event stream disconnected')));
      },
    }).catch(end);
    try { await done.promise; }
    finally { await writes; }
  }

  async close() {
    this.closed = true;
    this.connected = false;
    for (const stream of this.transport.streams.values()) stream.end(new Error('MCP Events client closed'));
    await this.client.close();
  }
}
