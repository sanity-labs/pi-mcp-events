import {createHmac, randomBytes, timingSafeEqual} from 'node:crypto';
import {createServer as createHttpServer, type IncomingMessage, type Server, type ServerResponse} from 'node:http';
import {createServer as createHttpsServer} from 'node:https';
import {z} from 'zod';
import {cursor, occurrence, remoteError} from './protocol.ts';

const controlSchema = z.discriminatedUnion('type', [
  z.object({type: z.literal('verification'), challenge: z.string().min(1).max(4096)}),
  z.object({type: z.literal('gap'), cursor}),
  z.object({type: z.literal('terminated'), error: remoteError}),
]);
export type WebhookBody = z.infer<typeof controlSchema> | z.infer<typeof occurrence>;
export type WebhookRegistration = {
  url: string;
  secret: string;
  bind(id: string): void;
  close(): void;
};
type Route = {
  secret: string;
  id?: string;
  deliveries: Map<string, number>;
  accept(body: WebhookBody): Promise<void>;
  tail: Promise<void>;
  pending: number;
};
export type WebhookOptions = {
  publicUrl: string;
  host?: string;
  port: number;
  tls?: {key: string; cert: string};
};

function singleHeader(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return typeof value === 'string' ? value : undefined;
}

export function verifyWebhook(secret: string, id: string, timestamp: string, signatures: string, raw: Buffer, now = Date.now()): boolean {
  if (!/^[A-Za-z0-9_-]{1,256}$/.test(id) || !/^\d{1,16}$/.test(timestamp)) return false;
  if (Math.abs(now / 1000 - Number(timestamp)) > 300) return false;
  const expected = createHmac('sha256', Buffer.from(secret.slice(6), 'base64'))
    .update(`${id}.${timestamp}.`).update(raw).digest();
  return signatures.split(' ').some(signature => {
    if (!signature.startsWith('v1,')) return false;
    const actual = Buffer.from(signature.slice(3), 'base64');
    return actual.length === expected.length && timingSafeEqual(expected, actual);
  });
}

export class WebhookReceiver {
  private readonly options: WebhookOptions;
  private readonly routes = new Map<string, Route>();
  private server?: Server;
  constructor(options: WebhookOptions) {
    const url = new URL(options.publicUrl);
    if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
      throw new Error('Webhook publicUrl must be an HTTPS URL without credentials, query, or fragment');
    }
    const host = options.host ?? '127.0.0.1';
    if (!options.tls && host !== '127.0.0.1' && host !== '::1') {
      throw new Error('A receiver without TLS must bind to loopback behind an HTTPS reverse proxy');
    }
    this.options = {...options, host};
  }
  async start() {
    if (this.server) return;
    const listener = (request: IncomingMessage, response: ServerResponse) => {
      void this.handle(request, response).catch(() => {
        if (!response.headersSent) response.writeHead(503);
        response.end();
      });
    };
    const server = this.options.tls ? createHttpsServer(this.options.tls, listener) : createHttpServer(listener);
    server.requestTimeout = 10_000;
    server.headersTimeout = 10_000;
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      const fail = (error: Error) => { this.server = undefined; reject(error); };
      server.once('error', fail);
      server.listen(this.options.port, this.options.host, () => { server.removeListener('error', fail); resolve(); });
    });
  }
  address() { return this.server?.address(); }
  register(accept: Route['accept']): WebhookRegistration {
    const token = randomBytes(24).toString('hex');
    const url = new URL(this.options.publicUrl);
    url.pathname = `${url.pathname.replace(/\/$/, '')}/${token}`;
    const route: Route = {
      secret: `whsec_${randomBytes(32).toString('base64')}`, deliveries: new Map(), accept,
      tail: Promise.resolve(), pending: 0,
    };
    this.routes.set(url.pathname, route);
    return {
      url: url.href, secret: route.secret,
      bind: id => { route.id = id; },
      close: () => { this.routes.delete(url.pathname); },
    };
  }
  private async handle(request: IncomingMessage, response: ServerResponse) {
    const reply = (status: number, body?: string) => {
      response.writeHead(status, {'content-type': 'application/json', 'cache-control': 'no-store'});
      response.end(body);
    };
    if (request.method !== 'POST') { reply(405); return; }
    const route = this.routes.get(request.url ?? '');
    if (!route) { reply(404); return; }
    if (request.headers['content-type']?.split(';')[0].trim().toLowerCase() !== 'application/json') { reply(415); return; }
    const id = singleHeader(request, 'webhook-id');
    const timestamp = singleHeader(request, 'webhook-timestamp');
    const signature = singleHeader(request, 'webhook-signature');
    const subscription = singleHeader(request, 'x-mcp-subscription-id');
    if (!id || !timestamp || !signature || !subscription) { reply(400); return; }
    if (route.pending >= 16) { reply(503); return; }
    route.pending++;
    try {
      const chunks: Buffer[] = [];
      let bytes = 0;
      for await (const chunk of request) {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bytes += buffer.length;
        if (bytes > 256 * 1024) { reply(413); return; }
        chunks.push(buffer);
      }
      const raw = Buffer.concat(chunks);
      if (!verifyWebhook(route.secret, id, timestamp, signature, raw)) { reply(401); return; }
      let body: WebhookBody;
      try {
        const parsed: unknown = JSON.parse(raw.toString('utf8'));
        body = typeof parsed === 'object' && parsed !== null && 'type' in parsed
          ? controlSchema.parse(parsed)
          : occurrence.parse(parsed);
      }
      catch { reply(400); return; }
      if ('type' in body && body.type === 'verification') { reply(200, JSON.stringify({challenge: body.challenge})); return; }
      if (!route.id) { reply(503); return; }
      if (route.id !== subscription) { reply(403); return; }
      const operation = route.tail.then(async () => {
        const now = Date.now();
        for (const [key, receivedAt] of route.deliveries) {
          if (now - receivedAt > 15 * 60_000) route.deliveries.delete(key);
        }
        if (route.deliveries.has(id)) return;
        await route.accept(body);
        route.deliveries.set(id, now);
        if (route.deliveries.size > 4096) route.deliveries.delete(route.deliveries.keys().next().value!);
      });
      route.tail = operation.catch(() => {});
      await operation;
      reply(204);
    } finally { route.pending--; }
  }
  async close() {
    this.routes.clear();
    const server = this.server;
    this.server = undefined;
    if (!server) return;
    await new Promise<void>((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve());
      server.closeAllConnections();
    });
  }
}
