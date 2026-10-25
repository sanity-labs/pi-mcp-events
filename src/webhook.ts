import {isUtf8} from 'node:buffer';
import {randomBytes} from 'node:crypto';
import {createServer as createHttpServer, type IncomingMessage, type Server, type ServerResponse} from 'node:http';
import {createServer as createHttpsServer} from 'node:https';
import {Webhook} from 'standardwebhooks';
import {z} from 'zod';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import {cursor, occurrence, remoteError, type EventOccurrence} from './protocol.ts';

const control = z.discriminatedUnion('type', [
  z.object({type: z.literal('verification'), challenge: z.string().min(1)}),
  z.object({type: z.literal('gap'), cursor}),
  z.object({type: z.literal('terminated'), error: remoteError}),
]);
export type WebhookDelivery = EventOccurrence | Exclude<z.infer<typeof control>, {type: 'verification'}>;
export type WebhookRegistration = {
  readonly url: string;
  readonly secret: string;
  bind(id: string): void;
  close(): Promise<void>;
};
const optionsSchema = z.object({
  publicUrl: z.url(), host: z.string().min(1).default('127.0.0.1'), port: z.number().int().min(0).max(65535),
  tls: z.object({key: z.string().min(1), cert: z.string().min(1)}).optional(),
  maxBodyBytes: z.number().int().positive().default(256 * 1024),
  maxPendingDeliveries: z.number().int().positive().default(16),
  maxRegistrations: z.number().int().positive().default(32),
  maxTrackedDeliveries: z.number().int().positive().default(4096),
  deduplicationTtlMs: z.number().int().positive().default(15 * 60_000),
  requestTimeoutMs: z.number().int().positive().max(2 ** 31 - 1).default(10_000),
});
export type WebhookOptions = z.input<typeof optionsSchema>;
type Route = {
  verifier: Webhook;
  id?: string;
  closed: boolean;
  deliveries: Map<string, number>;
  accept(body: WebhookDelivery): Promise<void> | void;
  tail: Promise<void>;
  pending: number;
};
function singleHeader(request: IncomingMessage, name: string) {
  const values = request.headersDistinct[name];
  return values?.length === 1 ? values[0] : undefined;
}

export class WebhookReceiver {
  private readonly options: z.output<typeof optionsSchema>;
  private readonly routes = new Map<string, Route>();
  private server?: Server;
  private starting?: Promise<void>;
  private closing?: Promise<void>;
  private closed = false;

  constructor(options: WebhookOptions) {
    const parsed = optionsSchema.safeParse(options);
    if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid webhook receiver options', {cause: parsed.error});
    this.options = parsed.data;
    const url = new URL(this.options.publicUrl);
    if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
      throw new EventsClientError(ErrorCode.InvalidOptions, 'Webhook publicUrl must use HTTPS without credentials, query, or fragment');
    }
    if (!this.options.tls && !['127.0.0.1', '::1'].includes(this.options.host)) {
      throw new EventsClientError(ErrorCode.InvalidOptions, 'A receiver without TLS must bind to loopback behind an HTTPS proxy');
    }
  }
  private assertOpen() {
    if (this.closed) throw new EventsClientError(ErrorCode.Closed, 'Webhook receiver is closed');
  }
  start(): Promise<void> {
    this.assertOpen();
    if (this.starting) return this.starting;
    if (this.server) return Promise.resolve();
    const start = async () => {
      try {
        const listener = (request: IncomingMessage, response: ServerResponse) => {
          void this.handle(request, response).catch(() => {
            if (!response.headersSent) response.writeHead(503, {'connection': 'close'});
            response.end();
          });
        };
        const server = this.options.tls ? createHttpsServer(this.options.tls, listener) : createHttpServer(listener);
        this.server = server;
        server.requestTimeout = this.options.requestTimeoutMs;
        server.headersTimeout = this.options.requestTimeoutMs;
        await new Promise<void>((resolve, reject) => {
          server.once('error', reject);
          server.listen(this.options.port, this.options.host, () => { server.removeListener('error', reject); resolve(); });
        });
      } catch (error) {
        this.server = undefined;
        throw new EventsClientError(ErrorCode.ConnectionFailed, 'Webhook receiver could not listen', {cause: error});
      }
      this.assertOpen();
    };
    this.starting = start().finally(() => { this.starting = undefined; });
    return this.starting;
  }
  address() { return this.server?.address(); }
  register(accept: Route['accept']): WebhookRegistration {
    this.assertOpen();
    if (this.routes.size >= this.options.maxRegistrations) throw limitExceeded('maxRegistrations', this.options.maxRegistrations);
    const url = new URL(this.options.publicUrl);
    url.pathname = `${url.pathname.replace(/\/$/, '')}/${randomBytes(24).toString('hex')}`;
    const secret = `whsec_${randomBytes(32).toString('base64')}`;
    const route: Route = {verifier: new Webhook(secret), closed: false, deliveries: new Map(), accept, tail: Promise.resolve(), pending: 0};
    this.routes.set(url.pathname, route);
    return {
      url: url.href, secret,
      bind: id => {
        if (route.closed) throw new EventsClientError(ErrorCode.Closed, 'Webhook registration is closed');
        if (!id || (route.id !== undefined && route.id !== id)) throw new EventsClientError(ErrorCode.InvalidOptions, 'Webhook registration requires a stable subscription ID');
        route.id = id;
      },
      close: () => { route.closed = true; return route.tail.finally(() => { this.routes.delete(url.pathname); }); },
    };
  }
  private async handle(request: IncomingMessage, response: ServerResponse) {
    const reply = (status: number, body?: string) => {
      response.writeHead(status, {'content-type': 'application/json', 'cache-control': 'no-store', ...(status >= 400 ? {connection: 'close'} : {})});
      response.end(body);
    };
    if (request.method !== 'POST') { reply(405); return; }
    const route = this.routes.get(request.url ?? '');
    if (!route || route.closed) { reply(404); return; }
    const encoding = request.headers['content-encoding'];
    if (singleHeader(request, 'content-type')?.split(';')[0].trim().toLowerCase() !== 'application/json' || (encoding && encoding !== 'identity')) { reply(415); return; }
    const id = singleHeader(request, 'webhook-id');
    const timestamp = singleHeader(request, 'webhook-timestamp');
    const signature = singleHeader(request, 'webhook-signature');
    const subscription = singleHeader(request, 'x-mcp-subscription-id');
    if (!id || !/^[A-Za-z0-9_-]+$/.test(id) || !timestamp || !/^(0|[1-9]\d*)$/.test(timestamp) || !signature || !subscription) { reply(400); return; }
    if (route.pending >= this.options.maxPendingDeliveries) { reply(503); return; }
    route.pending++;
    try {
      const chunks: Buffer[] = [];
      let bytes = 0;
      for await (const chunk of request.iterator({destroyOnReturn: false})) {
        bytes += chunk.length;
        if (bytes > this.options.maxBodyBytes) { reply(413); return; }
        chunks.push(chunk);
      }
      const raw = Buffer.concat(chunks);
      if (!isUtf8(raw)) { reply(400); return; }
      try { route.verifier.verify(raw, {'webhook-id': id, 'webhook-timestamp': timestamp, 'webhook-signature': signature}, {jsonParse: false}); }
      catch { reply(401); return; }
      let body;
      try {
        const parsed: unknown = JSON.parse(raw.toString('utf8'));
        body = typeof parsed === 'object' && parsed !== null && 'type' in parsed ? control.parse(parsed) : occurrence.parse(parsed);
      } catch { reply(400); return; }
      if (route.closed) { reply(404); return; }
      if (route.id !== undefined && route.id !== subscription) { reply(403); return; }
      if ('type' in body && body.type === 'verification') { reply(200, JSON.stringify({challenge: body.challenge})); return; }
      if (!route.id) { reply(503); return; }
      const delivery = body;
      const operation = route.tail.then(async () => {
        if (route.closed) throw new EventsClientError(ErrorCode.Closed, 'Webhook registration is closed');
        const now = Date.now();
        for (const [key, receivedAt] of route.deliveries) {
          if (now - receivedAt >= this.options.deduplicationTtlMs) route.deliveries.delete(key);
        }
        if (route.deliveries.has(id)) return;
        await route.accept(delivery);
        route.deliveries.set(id, Date.now());
        if (route.deliveries.size > this.options.maxTrackedDeliveries) route.deliveries.delete(route.deliveries.keys().next().value!);
      });
      route.tail = operation.catch(() => {});
      await operation;
      reply(204);
    } finally { route.pending--; }
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    const routes = [...this.routes.values()];
    for (const route of routes) route.closed = true;
    this.routes.clear();
    this.closing = (async () => {
      await this.starting?.catch(() => {});
      const server = this.server;
      this.server = undefined;
      const stopped = server ? new Promise<void>((resolve, reject) => {
        server.close(error => error ? reject(error) : resolve());
        server.closeAllConnections();
      }) : Promise.resolve();
      await Promise.all([stopped, ...routes.map(route => route.tail)]);
    })();
    return this.closing;
  }
}
