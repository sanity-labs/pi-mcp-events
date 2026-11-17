import {randomUUID} from 'node:crypto';
import {encodeHeaderValue as nameHeader} from './argument-headers.ts';
import {boundedResponse, defaultResponseLimits, type ResponseLimits} from './http-limits.ts';
import {
  Client, ProtocolError, type JSONRPCMessage, type Transport, type TransportSendOptions,
  type StandardSchemaV1, type RequestOptions, StreamableHTTPClientTransport,
  type StreamableHTTPClientTransportOptions,
} from '@modelcontextprotocol/client';
import {
  EVENTS_EXTENSION, PROTOCOL_VERSION, SUBSCRIPTION_ID, clientCapabilities,
  clientInfo, record, remoteError, EventMethod, EventProtocolError, EventProtocolErrorCode,
} from './protocol.ts';

export {nameHeader};

type Stream = {
  receive(message: JSONRPCMessage): void;
  end(error?: unknown): void;
  disconnect(error: unknown): void;
};
type HttpState = ResponseLimits & {ceilings: Partial<ResponseLimits>; onLimit?(id: unknown, error: Error): void; headers?: HeaderProvider};
const httpStates = new WeakMap<Transport, HttpState>();
function applyResponseLimits(state: HttpState, limits?: ResponseLimits) {
  if (!limits) return;
  for (const key of ['maxResponseBytes', 'maxPollBytes'] as const) state[key] = Math.min(limits[key], state.ceilings[key] ?? Infinity);
}

type HeaderProvider = (name: string, args: unknown) => Record<string, string>;
const headerMethods = new Set<string>([EventMethod.Poll, EventMethod.Stream, EventMethod.Subscribe, EventMethod.Unsubscribe]);
function parameterHeaders(headers: Headers, method: string, params: Record<string, unknown> | undefined, provider?: HeaderProvider) {
  if (!provider) return;
  for (const name of [...headers.keys()]) if (name.toLowerCase().startsWith('mcp-param-')) headers.delete(name);
  if (headerMethods.has(method) && typeof params?.name === 'string') {
    for (const [name, value] of Object.entries(provider(params.name, params.arguments))) headers.set(name, value);
  }
}

export function createHttpTransport(url: URL, options: StreamableHTTPClientTransportOptions & Partial<ResponseLimits> = {}) {
  const baseFetch = options.fetch ?? fetch;
  const state: HttpState = {...defaultResponseLimits, ceilings: {}};
  for (const key of ['maxResponseBytes', 'maxPollBytes'] as const) {
    const value = options[key];
    if (value !== undefined) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error('Invalid HTTP response limit');
      state[key] = value;
      state.ceilings[key] = value;
    }
  }
  const transport = new StreamableHTTPClientTransport(url, {...options, fetch: async (input, init) => {
    let request: Record<string, unknown> | undefined;
    if (typeof init?.body === 'string') {
      const message: unknown = JSON.parse(init.body);
      const parsed = record.safeParse(message);
      if (parsed.success) request = parsed.data;
      if (parsed.success && typeof parsed.data.method === 'string') {
        const headers = new Headers(init.headers);
        const params = record.safeParse(parsed.data.params);
        parameterHeaders(headers, parsed.data.method, params.success ? params.data : undefined, state.headers);
        if (parsed.data.method.startsWith('events/')) {
          if (params.success && typeof params.data.name === 'string') headers.set('Mcp-Name', nameHeader(params.data.name));
          else headers.delete('Mcp-Name');
        }
        init = {...init, headers};
      }
    }
    const response = await baseFetch(input, init);
    const limit = request?.method === EventMethod.Poll ? Math.min(state.maxPollBytes, state.maxResponseBytes) : state.maxResponseBytes;
    const hasRequest = typeof request?.method === 'string' && (typeof request.id === 'string' || typeof request.id === 'number');
    const allowStreaming = init?.method?.toUpperCase() === 'GET' || hasRequest;
    return boundedResponse(response, limit, Math.min(256 * 1024, limit), error => state.onLimit?.(request?.id, error), allowStreaming);
  }});
  httpStates.set(transport, state);
  return transport;
}

/** Intercepts only extension-owned stream IDs; ordinary SDK traffic passes through. */
class EventsTransport implements Transport {
  readonly raw: Transport;
  readonly streams = new Map<string, Stream>();
  onmessage?: Transport['onmessage'];
  onclose?: Transport['onclose'];
  onerror?: Transport['onerror'];
  private closed = false;
  private readonly disconnected: () => void;
  constructor(raw: Transport, disconnected: () => void, private headers: () => HeaderProvider | undefined) { this.raw = raw; this.disconnected = disconnected; }
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
      if (this.closed) return;
      this.closed = true;
      this.disconnected();
      for (const stream of this.streams.values()) stream.disconnect(new Error('MCP connection closed'));
      this.onclose?.();
    };
    await this.raw.start();
  }
  async send(message: JSONRPCMessage, options?: TransportSendOptions) {
    if ('method' in message) {
      const headers = new Headers(options?.headers);
      if (this.hasPerRequestStream) parameterHeaders(headers, message.method, message.params, this.headers());
      options = {...options, headers: Object.fromEntries(headers)};
      if (message.method.startsWith('events/') && typeof message.params?.name === 'string') {
        options.headers = {...options.headers, 'Mcp-Name': nameHeader(message.params.name)};
      }
    }
    return this.raw.send(message, options);
  }
  close() { return this.raw.close(); }
}

export class EventsWire {
  readonly client: Client;
  private readonly streams = new Set<Stream>();
  private transport: EventsTransport;
  private readonly factory?: () => Transport;
  private connected = false;
  private connectionAttempted = false;
  private extension?: Record<string, unknown>;
  private closed = false;
  private connecting?: Promise<Record<string, unknown>>;
  private responseLimits?: ResponseLimits;
  private connectionSignal?: AbortSignal;
  private eventHeaders?: HeaderProvider;
  private refreshHeaders?: (signal?: AbortSignal) => Promise<unknown>;
  get usesHttpHeaders() { return this.transport.hasPerRequestStream === true; }
  setEventHeaders(provider: HeaderProvider, refresh: (signal?: AbortSignal) => Promise<unknown>) {
    this.eventHeaders = provider;
    this.refreshHeaders = refresh;
    const http = httpStates.get(this.transport.raw);
    if (http) http.headers = provider;
  }
  private async withHeaderRecovery<T>(method: string, signal: AbortSignal | undefined, operation: () => Promise<T>, canRetry = () => true): Promise<T> {
    try { return await operation(); }
    catch (error) {
      signal?.throwIfAborted();
      if (!this.usesHttpHeaders || !headerMethods.has(method) || !this.refreshHeaders || !canRetry()
        || !(error instanceof ProtocolError || error instanceof EventProtocolError) || error.code !== EventProtocolErrorCode.HeaderMismatch) throw error;
      await this.refreshHeaders(signal);
      signal?.throwIfAborted();
      return operation();
    }
  }
  constructor(transport: Transport | (() => Transport)) {
    this.factory = typeof transport === 'function' ? transport : undefined;
    this.transport = this.wrap(typeof transport === 'function' ? transport() : transport);
    this.client = new Client(clientInfo, {
      capabilities: clientCapabilities,
      versionNegotiation: {mode: {pin: PROTOCOL_VERSION}},
    });
  }
  private wrap(transport: Transport) {
    const wrapped = new EventsTransport(transport, () => {
      if (this.transport === wrapped) this.connected = false;
    }, () => this.eventHeaders);
    const http = httpStates.get(transport);
    if (http) {
      http.headers = this.eventHeaders;
      applyResponseLimits(http, this.responseLimits);
      http.onLimit = (id, error) => {
        if (typeof id === 'string') wrapped.streams.get(id)?.disconnect(error);
      };
    }
    return wrapped;
  }
  setResponseLimits(limits: ResponseLimits) {
    this.responseLimits = {maxResponseBytes: limits.maxResponseBytes, maxPollBytes: limits.maxPollBytes};
    const http = httpStates.get(this.transport.raw);
    if (http) applyResponseLimits(http, this.responseLimits);
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
    return this.withHeaderRecovery(method, options?.signal, () => this.client.request({method, params}, schema, options));
  }

  stream(
    method: typeof EventMethod.Stream | 'subscriptions/listen', params: Record<string, unknown>, signal: AbortSignal,
    onNotification: (method: string, params: Record<string, unknown>) => Promise<void> | void,
  ): Promise<void> {
    let notified = false;
    return this.withHeaderRecovery(method, signal, () => this.openStream(method, params, signal, (kind, value) => {
      notified = true;
      return onNotification(kind, value);
    }), () => !notified);
  }
  private async openStream(
    method: typeof EventMethod.Stream | 'subscriptions/listen',
    params: Record<string, unknown>,
    signal: AbortSignal,
    onNotification: (method: string, params: Record<string, unknown>) => Promise<void> | void,
  ): Promise<void> {
    await this.ensureConnected();
    signal.throwIfAborted();
    const transport = this.transport;
    const id = `pi-events:${randomUUID()}`;
    const requestAbort = new AbortController();
    const done = Promise.withResolvers<void>();
    let ended = false;
    let accepting = true;
    let queued = 0;
    let bytes = 0;
    let writes = Promise.resolve();
    const end = (error?: unknown) => {
      if (ended) return;
      ended = true;
      accepting = false;
      transport.streams.delete(id);
      this.streams.delete(stream);
      requestAbort.abort();
      signal.removeEventListener('abort', abort);
      if (error === undefined) done.resolve();
      else done.reject(error);
    };
    const disconnect = (error: unknown) => {
      if (!accepting) return;
      accepting = false;
      void writes.then(() => end(error));
    };
    const cancel = () => {
      requestAbort.abort();
      if (!transport.hasPerRequestStream && transport === this.transport) {
        void this.client.notification({method: 'notifications/cancelled', params: {requestId: id}}).catch(() => {});
      }
    };
    const abort = () => { cancel(); end(signal.reason); };
    const stream: Stream = {
      end, disconnect,
      receive: message => {
        if (!accepting) return;
        const size = Buffer.byteLength(JSON.stringify(message));
        if (queued >= 100 || bytes + size > 256 * 1024) {
          disconnect(new Error('Event stream backlog exceeded its memory limit'));
          cancel();
          return;
        }
        queued++;
        bytes += size;
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
    };
    this.streams.add(stream);
    transport.streams.set(id, stream);
    signal.addEventListener('abort', abort, {once: true});
    const meta = {
      'io.modelcontextprotocol/protocolVersion': PROTOCOL_VERSION,
      'io.modelcontextprotocol/clientInfo': clientInfo,
      'io.modelcontextprotocol/clientCapabilities': clientCapabilities,
    };
    void transport.send({jsonrpc: '2.0', id, method, params: {...params, _meta: meta}}, {
      requestSignal: requestAbort.signal,
      onRequestStreamEnd: () => {
        disconnect(new Error('Event stream disconnected'));
      },
    }).catch(disconnect);
    try { await done.promise; }
    finally { await writes; }
  }

  async close() {
    this.closed = true;
    this.connected = false;
    for (const stream of this.streams) stream.end(new Error('MCP Events client closed'));
    await this.client.close();
  }
}
