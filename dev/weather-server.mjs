import {createServer} from 'node:http';
import {randomInt, timingSafeEqual} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {parseArgs} from 'node:util';
import {pathToFileURL} from 'node:url';
import {createMcpHandler, Server, ProtocolError, ProtocolErrorCode} from '@modelcontextprotocol/server';
import {serveStdio, StdioServerTransport} from '@modelcontextprotocol/server/stdio';
import {toNodeHandler, localhostHostValidation, localhostOriginValidation} from '@modelcontextprotocol/node';
import {z} from 'zod';
import {catalog, createWeather, payloads, subscriptionArguments} from './weather.mjs';
import {createEventLog} from './event-log.mjs';
import {createWebhookDelivery, WebhookCallbackError, WebhookLimitError} from './webhook-delivery.mjs';
import {decodeHeaderValue} from '../src/argument-headers.ts';

const revision = '2026-07-28';
const subscriptionId = 'io.modelcontextprotocol/subscriptionId';
const watch = z.object({
  name: z.string(), arguments: subscriptionArguments.default({}), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(),
  maxEvents: z.number().int().positive().max(1000).optional(),
  heartbeatMs: z.number().int().positive().optional(),
});

export async function startWeatherServer({transport = 'http', port = 3015, intervalMs = 20_000, heartbeatMs = 1000, historyLimit = 1000, now = Date.now, onEvent = () => {},
  webhookToken, webhookCa, webhookAllowedUrls = [], webhookAllowedOrigins = [], webhookDeliveryOptions = {}} = {}) {
  if (!['http', 'stdio'].includes(transport)) throw new Error('Transport must be http or stdio');
  const webhooksEnabled = transport === 'http' && Boolean(webhookToken);
  if (!Number.isInteger(intervalMs) || (intervalMs !== 0 && (intervalMs < 10_000 || intervalMs > 2_147_483_647))) throw new Error('Interval must be 0 or an integer between 10000 and 2147483647 ms');
  if (!Number.isInteger(heartbeatMs) || heartbeatMs < 1 || heartbeatMs > 30_000) throw new Error('Heartbeat must be an integer between 1 and 30000 ms');
  if (!Number.isInteger(historyLimit) || historyLimit < 1) throw new Error('History limit must be a positive integer');
  const log = createEventLog(historyLimit);
  const weather = createWeather({now});
  const webhookUrlAllowlist = new Set(webhookAllowedUrls.map(value => new URL(value).toString()));
  const webhookOriginAllowlist = new Set(webhookAllowedOrigins.map(value => {
    const url = new URL(value);
    if (url.protocol !== 'https:' || !['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname)
      || url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
      throw new Error('Webhook test origins must be loopback HTTPS origins');
    }
    return url.origin;
  }));
  const webhookDelivery = createWebhookDelivery({
    ...webhookDeliveryOptions,
    now,
    ca: webhookCa,
    allowLocalAddresses: webhookUrlAllowlist.size > 0 || webhookOriginAllowlist.size > 0,
    allowUrl: () => false,
    matches: (subscription, event) => event.name === subscription.name
      && (!subscription.arguments.stationId || event.data.stationId === subscription.arguments.stationId),
    getCurrentCursor: input => log.cursor(JSON.stringify([input.name, input.arguments.stationId ?? null])),
    cursorForEvent: input => log.cursor(JSON.stringify([input.name, input.arguments.stationId ?? null])),
    replay: ({cursor, maxAgeMs, input}) => {
      const page = log.read({
        cursor, key: JSON.stringify([input.name, input.arguments.stationId ?? null]), maxEvents: historyLimit,
        maxAgeMs, matches: event => event.name === input.name
          && (!input.arguments.stationId || event.data.stationId === input.arguments.stationId),
      });
      return {events: page.events, cursor: page.startCursor, headCursor: page.cursor, truncated: page.truncated};
    },
  });
  const listeners = new Set();
  const key = params => JSON.stringify([params.name, params.arguments.stationId ?? null]);
  const matches = (params, event) => event.name === params.name && (!params.arguments.stationId || event.data.stationId === params.arguments.stationId);
  const read = params => {
    if (!Object.hasOwn(payloads, params.name)) throw new ProtocolError(-32023, 'Unknown weather event', {kind: 'event'});
    try {
      return log.read({
        ...params, key: key(params), matches: event => matches(params, event),
      });
    } catch (error) { throw new ProtocolError(ProtocolErrorCode.InvalidParams, error.message); }
  };
  const createServerInstance = () => {
    const server = new Server({name: 'weather-reference', version: '0.0.0'}, {
      supportedProtocolVersions: [revision], capabilities: {extensions: {'io.modelcontextprotocol/events': {}}},
    });
    server.setRequestHandler('events/list', {params: z.object({cursor: z.string().optional()}).default({})}, params => {
      if (params.cursor !== undefined) throw new ProtocolError(ProtocolErrorCode.InvalidParams, 'The weather catalog has one page');
      return {resultType: 'complete', events: catalog.map(event => ({
        ...event,
        delivery: webhooksEnabled ? event.delivery : event.delivery.filter(mode => mode !== 'webhook'),
      }))};
    });
    server.setRequestHandler('events/poll', {params: watch}, params => {
      const {events, cursor, truncated, hasMore} = read(params);
      return {resultType: 'complete', events, cursor, truncated, hasMore, nextPollMs: 1000};
    });
    if (webhooksEnabled) {
      server.setRequestHandler('events/subscribe', {params: z.object({
        name: z.string(), arguments: subscriptionArguments.default({}),
        delivery: z.object({mode: z.literal('webhook'), url: z.string(), secret: z.string()}),
        cursor: z.string().nullable().optional(), maxAgeMs: z.number().int().nonnegative().optional(),
        ttlMs: z.number().int().nonnegative().nullable().optional(),
      })}, async (params) => {
        if (!Object.hasOwn(payloads, params.name)) throw new ProtocolError(-32023, 'Unknown weather event', {kind: 'event'});
        let callbackUrl;
        try { callbackUrl = new URL(params.delivery.url).toString(); }
        catch { throw new ProtocolError(ProtocolErrorCode.InvalidParams, 'Invalid webhook URL'); }
        if ((webhookUrlAllowlist.size > 0 || webhookOriginAllowlist.size > 0)
          && !webhookUrlAllowlist.has(callbackUrl) && !webhookOriginAllowlist.has(new URL(callbackUrl).origin)) {
          throw new ProtocolError(ProtocolErrorCode.InvalidParams, 'Webhook URL is not in the configured test allowlist');
        }
        try {
          return {resultType: 'complete', ...await webhookDelivery.subscribe({
            principal: 'weather-reference-user', name: params.name, arguments: params.arguments,
            url: params.delivery.url, secret: params.delivery.secret,
            ttlMs: params.ttlMs === null ? undefined : params.ttlMs,
            cursor: params.cursor, maxAgeMs: params.maxAgeMs,
          })};
        } catch (error) {
          if (error instanceof WebhookCallbackError) throw new ProtocolError(-32027, error.message, {reason: error.reason});
          if (error instanceof WebhookLimitError) throw new ProtocolError(-32025, error.message, {limit: 'subscriptions'});
          throw new ProtocolError(ProtocolErrorCode.InvalidParams, error.message);
        }
      });
      server.setRequestHandler('events/unsubscribe', {params: z.object({
        name: z.string(), arguments: subscriptionArguments.default({}),
        delivery: z.object({mode: z.literal('webhook').optional(), url: z.string()}),
      })}, async params => {
        if (!Object.hasOwn(payloads, params.name)) throw new ProtocolError(-32023, 'Unknown weather event', {kind: 'event'});
        const removed = await webhookDelivery.unsubscribe({
          principal: 'weather-reference-user', name: params.name, arguments: params.arguments, url: params.delivery.url,
        });
        if (!removed) throw new ProtocolError(-32023, 'Unknown weather subscription', {kind: 'subscription'});
        return {resultType: 'complete'};
      });
    }
    server.setRequestHandler('events/stream', {params: watch}, async (params, ctx) => {
      const page = read({...params, maxEvents: historyLimit});
      const grantedHeartbeat = Math.min(params.heartbeatMs ?? heartbeatMs, heartbeatMs);
      const meta = {[subscriptionId]: ctx.mcpReq.id};
      const notify = (kind, value) => ctx.mcpReq.notify({method: `notifications/events/${kind}`, params: {...value, _meta: meta}});
      const finished = Promise.withResolvers();
      let cursor = page.startCursor;
      let writes = Promise.resolve();
      const enqueue = operation => {
        writes = writes.then(async () => { ctx.mcpReq.signal.throwIfAborted(); await operation(); });
        void writes.catch(finished.reject);
      };
      enqueue(async () => {
        await notify('active', {cursor: page.startCursor, truncated: page.truncated, heartbeatMs: grantedHeartbeat});
        for (const event of page.events) {
          ctx.mcpReq.signal.throwIfAborted();
          await notify('event', event);
        }
        cursor = page.cursor;
      });
      const receive = event => {
        const next = log.cursor(key(params));
        enqueue(async () => {
          if (matches(params, event)) await notify('event', {...event, cursor: next});
          cursor = next;
        });
      };
      listeners.add(receive);
      const heartbeat = setInterval(() => enqueue(() => notify('heartbeat', {cursor})), grantedHeartbeat);
      const abort = () => finished.resolve();
      ctx.mcpReq.signal.addEventListener('abort', abort, {once: true});
      if (ctx.mcpReq.signal.aborted) abort();
      try {
        await finished.promise;
      } catch (error) { if (!ctx.mcpReq.signal.aborted) throw error; }
      finally {
        clearInterval(heartbeat);
        listeners.delete(receive);
        ctx.mcpReq.signal.removeEventListener('abort', abort);
      }
      return {resultType: 'complete', _meta: meta};
    });
    return server;
  };
  const emit = events => events.map(({name, data}) => {
    const event = log.append(name, payloads[name].parse(data));
    onEvent(event);
    for (const receive of listeners) receive(event);
    webhookDelivery.emit(event);
    return event;
  });
  const advance = () => emit([...weather.advance(), ...weather.alerts()]);
  let timer;
  let alertTimer;
  const scheduleAlerts = (delayMs = 45_000) => {
    alertTimer = setTimeout(() => {
      emit(weather.alerts());
      scheduleAlerts();
    }, delayMs);
  };
  const schedule = () => {
    timer = setTimeout(() => {
      const readings = randomInt(1, 4);
      for (let i = 0; i < readings; i++) emit(weather.advance());
      schedule();
    }, randomInt(10_000, intervalMs + 1));
  };
  let handler;
  let http;
  let closing = false;
  let closed;
  const close = () => {
    if (closed) return closed;
    closing = true;
    clearTimeout(timer);
    clearTimeout(alertTimer);
    webhookDelivery.close();
    closed = (async () => {
      await webhookDelivery.waitForIdle();
      await handler?.close();
      if (transport === 'stdio') process.stdin.destroy();
      if (http?.listening) {
        http.closeAllConnections();
        await new Promise((resolve, reject) => http.close(error => error ? reject(error) : resolve()));
      }
    })();
    return closed;
  };
  try {
    if (transport === 'stdio') {
      const stdio = new StdioServerTransport(process.stdin, process.stdout, {maxBufferSize: 65_536});
      handler = serveStdio(createServerInstance, {legacy: 'reject', transport: stdio});
      const onclose = stdio.onclose;
      stdio.onclose = () => {
        onclose?.();
        void close().catch(error => { console.error(error); process.exitCode = 1; });
      };
    } else {
      handler = createMcpHandler(createServerInstance, {legacy: 'reject', maxRequestBodySize: 65_536});
      // The SDK checks core routing headers; Events also mirrors subscription arguments.
      const mcp = toNodeHandler({fetch: async request => {
        let body;
        if (request.method === 'POST') {
          try { body = await request.clone().json(); }
          catch { return handler.fetch(request); }
        }
        if (['events/subscribe', 'events/unsubscribe'].includes(body?.method)) {
          if (!webhookToken) return Response.json({jsonrpc: '2.0', id: body.id ?? null,
            error: {code: -32601, message: 'Method not found'},
          }, {status: 200});
          const authorization = request.headers.get('authorization') ?? '';
          const supplied = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
          const expected = webhookToken ?? '';
          const suppliedBytes = Buffer.from(supplied);
          const expectedBytes = Buffer.from(expected);
          const authenticated = expectedBytes.length > 0 && suppliedBytes.length === expectedBytes.length
            && timingSafeEqual(suppliedBytes, expectedBytes);
          if (!authenticated) return Response.json({jsonrpc: '2.0', id: body.id ?? null,
            error: {code: -32024, message: 'Authenticated principal required'},
          }, {status: 200});
        }
        if (['events/poll', 'events/stream', 'events/subscribe', 'events/unsubscribe'].includes(body?.method) && typeof body.params?.name === 'string') {
          const name = decodeHeaderValue(request.headers.get('Mcp-Name'));
          if (name !== body.params.name) return Response.json({jsonrpc: '2.0', id: body.id ?? null,
            error: {code: -32020, message: 'Mcp-Name must match params.name'},
          }, {status: 400});
          const station = body.params.arguments?.stationId;
          const header = request.headers.get('Mcp-Param-Station');
          if (station === undefined || station === null ? header !== null : decodeHeaderValue(header) !== station) {
            return Response.json({jsonrpc: '2.0', id: body.id ?? null,
              error: {code: -32020, message: 'Mcp-Param-Station must match arguments.stationId'},
            }, {status: 400});
          }
        }
        return handler.fetch(request, {parsedBody: body});
      }}, {maxRequestBodySize: 65_536});
      const hostAllowed = localhostHostValidation();
      const originAllowed = localhostOriginValidation();
      http = createServer((request, response) => {
        if (closing) { response.writeHead(503); response.end(); return; }
        if (!hostAllowed(request, response) || !originAllowed(request, response)) return;
        if (request.url === '/mcp') {
          void mcp(request, response).catch(() => { response.writeHead(500); response.end(); });
          return;
        }
        if (request.url === '/advance' && request.method === 'POST'
          && request.headers['content-type']?.split(';', 1)[0].trim().toLowerCase() === 'application/json') {
          request.resume();
          response.writeHead(200, {'content-type': 'application/json'});
          response.end(JSON.stringify(advance()));
          return;
        }
        response.writeHead(404); response.end();
      });
      await new Promise((resolve, reject) => { http.once('error', reject); http.listen(port, '127.0.0.1', resolve); });
    }
  } catch (error) {
    await close();
    throw error;
  }
  if (!closing && intervalMs > 0) { schedule(); scheduleAlerts(20_000); }
  return {
    url: http ? `http://127.0.0.1:${http.address().port}/mcp` : undefined, advance,
    waitForWebhookIdle: () => webhookDelivery.waitForIdle(), close,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const {values} = parseArgs({options: {transport: {type: 'string', default: 'http'}, port: {type: 'string', default: '3015'}, interval: {type: 'string', default: '20000'}}});
  const webhookCa = values.transport === 'http' && process.env.WEATHER_WEBHOOK_CA ? readFileSync(process.env.WEATHER_WEBHOOK_CA) : undefined;
  const webhookAllowedOrigins = values.transport === 'http' && process.env.WEATHER_WEBHOOK_LOCAL_ORIGIN ? [process.env.WEATHER_WEBHOOK_LOCAL_ORIGIN] : [];
  const log = values.transport === 'stdio' ? console.error : console.log;
  const server = await startWeatherServer({transport: values.transport, port: Number(values.port), intervalMs: Number(values.interval),
    webhookToken: values.transport === 'http' ? process.env.WEATHER_WEBHOOK_TOKEN : undefined, webhookCa, webhookAllowedOrigins,
    onEvent: event => {
      const {station, condition, temperatureC, windKph, rainfallMmPerHour, daylight, status, message, previousCondition} = event.data;
      let description = condition;
      if (previousCondition) description = `${previousCondition} → ${condition}`;
      if (status) description = `${status.toUpperCase()}: ${message}`;
      log(`\n${event.timestamp.slice(11, 19)}Z  ${station}  ${event.name}`);
      log(`  ${description}  |  ${daylight ? 'day' : 'night'}`);
      log(`  ${temperatureC} °C  |  wind ${windKph} km/h  |  rain ${rainfallMmPerHour} mm/h`);
      log(`  id: ${event.eventId}`);
    },
  });
  let cadence = 'Automatic readings disabled.';
  if (values.transport === 'http') cadence = 'POST /advance for the next reading.';
  if (Number(values.interval) !== 0) cadence = `Bursts of 1–3 readings, 10000–${values.interval} ms apart. Alerts every 45 seconds, starting after 20 seconds.`;
  log(`Island weather station: ${server.url ?? 'stdio'}\n${cadence} Weather changes over minutes. Ctrl-C to stop.`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { void server.close().catch(error => { console.error(error); process.exitCode = 1; }); });
}
