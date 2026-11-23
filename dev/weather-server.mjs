import {createServer} from 'node:http';
import {randomInt, randomUUID, timingSafeEqual} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {parseArgs} from 'node:util';
import {pathToFileURL} from 'node:url';
import {createMcpHandler, McpServer, ProtocolError} from '@modelcontextprotocol/server';
import {serveStdio, StdioServerTransport} from '@modelcontextprotocol/server/stdio';
import {toNodeHandler, localhostHostValidation, localhostOriginValidation} from '@modelcontextprotocol/node';
import {z} from 'zod';
import {createWeather, weatherEventTypes} from './weather.mjs';
import {createEventTypes} from './event-types.mjs';
import {createEventLog} from './event-log.mjs';
import {createWebhookDelivery, WebhookCallbackError, WebhookInputError, WebhookLimitError} from './webhook-delivery.mjs';
import {decodeHeaderValue} from '../src/argument-headers.ts';
import {canonical, EventProtocolErrorCode as code} from '../src/protocol.ts';

const revision = '2026-07-28';
const subscriptionId = 'io.modelcontextprotocol/subscriptionId';
const eventRequest = {
  name: z.string(), arguments: z.unknown().optional(), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(),
};
const watch = z.object({...eventRequest, maxEvents: z.number().int().positive().optional()});
const namedMethods = new Set(['events/poll', 'events/stream', 'events/subscribe', 'events/unsubscribe']);

export async function startWeatherServer({transport = 'http', port = 3015, intervalMs = 20_000, heartbeatMs = 1000, historyLimit = 1000, now = Date.now, onEvent = () => {},
  webhookToken, webhookCa, webhookAllowedUrls = [], webhookAllowedOrigins = [], webhookDeliveryOptions = {}, webhookStore,
  maxSubscriptions = 1024, checkIntervalMs = 1000, eventTypes = [], authorize = () => true, authenticate,
  registerTools, pollLeaseMs = 300_000, allowVerifiedWebhook = () => false} = {}) {
  if (!['http', 'stdio'].includes(transport)) throw new Error('Transport must be http or stdio');
  if (!Number.isInteger(intervalMs) || (intervalMs !== 0 && (intervalMs < 10_000 || intervalMs > 2_147_483_647))) throw new Error('Interval must be 0 or an integer between 10000 and 2147483647 ms');
  if (!Number.isInteger(heartbeatMs) || heartbeatMs < 1 || heartbeatMs > 30_000) throw new Error('Heartbeat must be an integer between 1 and 30000 ms');
  if (!Number.isInteger(historyLimit) || historyLimit < 1) throw new Error('History limit must be a positive integer');
  if (!Number.isSafeInteger(maxSubscriptions) || maxSubscriptions < 1) throw new Error('Subscription limit must be a positive integer');
  if (!Number.isSafeInteger(checkIntervalMs) || checkIntervalMs < 1) throw new Error('Check interval must be a positive integer');
  if (!Number.isSafeInteger(pollLeaseMs) || pollLeaseMs < 1 || pollLeaseMs > 2 ** 31 - 1) throw new Error('Invalid poll lease duration');
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
  const streams = new Set();
  const pollLeases = new Map();
  const usedSubscriptions = () => streams.size + pollLeases.size + webhookDelivery.busy;
  const principalLimit = Math.max(1, Math.floor(maxSubscriptions / 4));
  function assertCapacity(principal) {
    if (usedSubscriptions() >= maxSubscriptions) {
      throw new ProtocolError(code.ResourceExhausted, 'Subscription limit reached', {limit: 'subscriptions', max: maxSubscriptions});
    }
    if (!authenticate && !webhookToken) return;
    const used = [...streams, ...pollLeases.values()].filter(value => value.principal === principal).length
      + webhookDelivery.countFor(principal);
    if (used >= principalLimit) {
      throw new ProtocolError(code.ResourceExhausted, 'Principal subscription limit reached', {limit: 'principalSubscriptions', max: principalLimit});
    }
  }
  const forgetLease = (key, lease) => {
    clearTimeout(lease.timeout);
    if (pollLeases.get(key) !== lease) return;
    pollLeases.delete(key);
    void lease.read.finally(() => lease.definition.onEnd?.({subscriptionId: lease.id}));
  };
  const webhooksEnabled = transport === 'http' && Boolean(webhookToken || authenticate);
  const principal = ctx => ctx.http?.authInfo?.extra?.principal;
  const types = createEventTypes({log, webhooksEnabled, authorize, async onEnding(name, error, onlyPrincipal) {
    for (const [key, lease] of pollLeases) {
      if (lease.name === name && (onlyPrincipal === undefined || lease.principal === onlyPrincipal)) forgetLease(key, lease);
    }
    const affected = [...streams].filter(stream => stream.name === name
      && (onlyPrincipal === undefined || stream.principal === onlyPrincipal));
    await Promise.all([...affected.map(stream => stream.terminate(error)),
      webhookDelivery.terminate(name, error, {principal: onlyPrincipal})]);
  }});
  for (const [name, definition] of [...eventTypes, ...weatherEventTypes]) types.register(name, definition);

  const webhookDelivery = createWebhookDelivery({
    ...webhookDeliveryOptions,
    now: webhookDeliveryOptions.now ?? Date.now,
    ca: webhookCa,
    allowLocalAddresses: url => webhookUrlAllowlist.has(url.href) || webhookOriginAllowlist.has(url.origin),
    allowUrl: allowVerifiedWebhook,
    matches: types.matches,
    shouldEmit: input => !types.get(input.name).definition.check,
    getCurrentCursor: types.head,
    cursorForEvent: types.head,
    replay: async ({cursor, maxAgeMs, input}) => {
      const page = await types.read({...input, cursor, startNow: (cursor ?? null) === null, maxEvents: historyLimit, maxAgeMs});
      return {events: page.events, cursor: page.startCursor, headCursor: page.cursor, truncated: page.truncated};
    },
    hasCheck: name => Boolean(types.get(name).definition.check),
    check: types.read,
    onCheckClose: (name, input) => {
      const close = types.find(name)?.definition.onEnd;
      if (close) return () => close(input);
    },
    authorize: types.assertAccess,
    assertCapacity,
    checkIntervalMs,
    store: webhookStore,
  });

  const webhookInput = (params, ctx) => {
    const input = types.validate(params, 'webhook', principal(ctx));
    if (input.principal === undefined) throw new ProtocolError(code.Forbidden, 'Authenticated principal required');
    const {delivery, ...subscription} = input;
    return {...subscription, url: delivery.url, secret: delivery.secret};
  };

  function describeError(error) {
    if (error instanceof ProtocolError) return {code: error.code, message: error.message, data: error.data};
    return {code: code.InternalError, message: 'Event source unavailable'};
  }

  async function readPoll(input) {
    const definition = types.get(input.name).definition;
    if (!definition.check || definition.replay !== false) return types.read(input);
    const key = canonical([input.principal ?? null, input.name, input.arguments]);
    let lease = pollLeases.get(key);
    const initial = !lease;
    if (!lease) {
      assertCapacity(input.principal);
      lease = {name: input.name, principal: input.principal, id: randomUUID(), read: Promise.resolve(), definition};
      pollLeases.set(key, lease);
    }
    clearTimeout(lease.timeout);
    lease.timeout = setTimeout(() => forgetLease(key, lease), pollLeaseMs).unref();
    const page = lease.read.then(() => types.read({...input, cursor: null, startNow: initial, subscriptionId: lease.id}));
    lease.read = page.catch(() => {});
    return page;
  }

  function createServerInstance() {
    const server = new McpServer({name: 'weather-reference', version: '0.0.0'}, {
      supportedProtocolVersions: [revision], capabilities: {extensions: {'io.modelcontextprotocol/events': {}}},
    });
    const core = server.server;
    core.setRequestHandler('events/list', {params: z.object({cursor: z.string().optional()}).default({})}, params => {
      if (params.cursor !== undefined) throw new ProtocolError(code.InvalidParams, 'The weather catalog has one page');
      return {resultType: 'complete', events: types.list()};
    });
    core.setRequestHandler('events/poll', {params: watch}, async (params, ctx) => {
      const input = types.validate(params, 'poll', principal(ctx));
      const {events, cursor, truncated, hasMore} = await readPoll({...input, startNow: (input.cursor ?? null) === null,
        maxEvents: Math.min(params.maxEvents ?? historyLimit, historyLimit)});
      return {resultType: 'complete', events, cursor, truncated, hasMore, nextPollMs: Math.min(1000, Math.ceil(pollLeaseMs / 2))};
    });
    core.setRequestHandler('events/subscribe', {params: z.object({
      ...eventRequest,
      delivery: z.object({mode: z.literal('webhook'), url: z.string(), secret: z.string()}),
      ttlMs: z.number().int().nonnegative().nullable().optional(),
    })}, async (params, ctx) => {
      const input = webhookInput(params, ctx);
      try {
        return {resultType: 'complete', ...await webhookDelivery.subscribe(input)};
      } catch (error) {
        if (error instanceof ProtocolError) throw error;
        if (error instanceof WebhookCallbackError) throw new ProtocolError(code.CallbackEndpointError, error.message, {reason: error.reason});
        if (error instanceof WebhookLimitError) throw new ProtocolError(code.ResourceExhausted, error.message, error.data);
        if (error instanceof WebhookInputError) throw new ProtocolError(code.InvalidParams, error.message);
        throw error;
      }
    });
    core.setRequestHandler('events/unsubscribe', {params: z.object({
      name: eventRequest.name, arguments: eventRequest.arguments,
      delivery: z.object({mode: z.literal('webhook').optional(), url: z.string()}),
    })}, async (params, ctx) => {
      const input = webhookInput(params, ctx);
      let removed;
      try { removed = await webhookDelivery.unsubscribe(input); }
      catch (error) {
        if (error instanceof WebhookInputError) throw new ProtocolError(code.InvalidParams, error.message);
        throw error;
      }
      if (!removed) throw new ProtocolError(code.NotFound, 'Unknown webhook subscription', {kind: 'subscription'});
      return {resultType: 'complete'};
    });
    core.setRequestHandler('events/stream', {params: z.object({
      ...eventRequest, heartbeatMs: z.number().int().positive().optional(),
    })}, async (params, ctx) => {
      const input = types.validate(params, 'push', principal(ctx));
      assertCapacity(input.principal);
      const definition = types.get(input.name).definition;
      const grantedHeartbeat = Math.min(params.heartbeatMs ?? heartbeatMs, heartbeatMs);
      const meta = {[subscriptionId]: ctx.mcpReq.id};
      const signal = ctx.mcpReq.signal;
      const finished = Promise.withResolvers();
      let cursor = input.cursor ?? null;
      let heartbeat;
      let sourceTimer;
      let writes = Promise.resolve();
      let queued = 0;
      let ending = false;
      let checking = false;
      const scheduleHeartbeat = () => {
        clearTimeout(heartbeat);
        if (!ending) heartbeat = setTimeout(() => enqueue(() => notify('heartbeat', {cursor})), grantedHeartbeat);
      };
      const notify = async (kind, value) => {
        await ctx.mcpReq.notify({method: `notifications/events/${kind}`, params: {...value, _meta: meta}});
        scheduleHeartbeat();
      };
      const enqueue = operation => {
        if (++queued > historyLimit + 64) {
          ending = true;
          --queued;
          finished.resolve();
          return writes;
        }
        const next = writes.then(async () => { signal.throwIfAborted(); await operation(); });
        writes = next.catch(finished.reject).finally(() => { --queued; });
        return writes;
      };
      const closeStream = () => {
        ending = true;
        finished.resolve();
      };
      const stream = {
        ...input,
        terminate(error) {
          if (ending) return writes;
          ending = true;
          return enqueue(async () => {
            await notify('terminated', {error: describeError(error)});
            finished.resolve();
          });
        },
        receive(event) {
          if (ending || definition.check) return;
          const next = types.head(input);
          enqueue(async () => {
            try { types.assertAccess(input); }
            catch (error) { void stream.terminate(error); return; }
            if (types.matches(input, event)) await notify('event', {...event, cursor: next});
            cursor = next;
          });
        },
      };
      const readPage = async initial => {
        let page;
        try {
          types.assertAccess(input);
          page = await types.read({...input, cursor, startNow: initial && cursor === null,
            maxAgeMs: initial ? input.maxAgeMs : undefined, maxEvents: historyLimit, subscriptionId: sourceId});
        } catch (error) {
          if (initial) throw error;
          if (error instanceof ProtocolError && [code.Forbidden, code.NotFound, code.Unsupported].includes(error.code)) {
            void stream.terminate(error);
          } else {
            await notify('error', {error: describeError(error)});
          }
          return false;
        }
        if (ending) return false;
        if (initial || page.truncated) {
          await notify('active', {cursor: page.startCursor, truncated: page.truncated, heartbeatMs: grantedHeartbeat});
        }
        for (const event of page.events) {
          signal.throwIfAborted();
          if (ending) return false;
          await notify('event', event);
        }
        cursor = page.cursor;
        return page.hasMore;
      };
      const sourceId = randomUUID();
      const scheduleSource = (ms = checkIntervalMs) => {
        if (ending || checking) return;
        checking = true;
        sourceTimer = setTimeout(() => enqueue(async () => {
          const hasMore = await readPage(false);
          checking = false;
          scheduleSource(hasMore ? 0 : checkIntervalMs);
        }), ms);
      };
      streams.add(stream);
      signal.addEventListener('abort', closeStream, {once: true});
      if (signal.aborted) closeStream();
      try {
        const initial = enqueue(async () => {
          const hasMore = await readPage(true);
          if (definition.check) scheduleSource(hasMore ? 0 : checkIntervalMs);
        });
        await Promise.race([initial, finished.promise]);
        await finished.promise;
      } catch (error) {
        if (!signal.aborted) throw error;
      } finally {
        clearTimeout(heartbeat);
        clearTimeout(sourceTimer);
        streams.delete(stream);
        definition.onEnd?.({...input, subscriptionId: sourceId});
        signal.removeEventListener('abort', closeStream);
      }
      return {resultType: 'complete', _meta: meta};
    });
    registerTools?.(server);
    return core;
  }

  function emit(events) {
    return events.map(({name, data}) => {
      const {definition} = types.get(name);
      const payload = definition.payloadSchema.parse(data);
      const event = definition.replay === false
        ? {name, data: payload, eventId: randomUUID(), timestamp: new Date().toISOString()}
        : log.append(name, payload);
      onEvent(event);
      for (const stream of streams) stream.receive(event);
      webhookDelivery.emit(event);
      return event;
    });
  }
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
    for (const [key, lease] of pollLeases) forgetLease(key, lease);
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
        if (namedMethods.has(body?.method) && typeof body.params?.name === 'string') {
          const name = decodeHeaderValue(request.headers.get('Mcp-Name'));
          if (name !== body.params.name || types.headerMismatch(name, body.params.arguments, request.headers)) {
            return Response.json({jsonrpc: '2.0', id: body.id ?? null,
              error: {code: code.HeaderMismatch, message: 'Event routing headers must match the request arguments'},
            }, {status: 400});
          }
        }
        const authorization = request.headers.get('authorization') ?? '';
        const supplied = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
        let authenticated = authenticate?.(supplied);
        if (webhookToken && supplied) {
          const suppliedBytes = Buffer.from(supplied);
          const expectedBytes = Buffer.from(webhookToken);
          if (suppliedBytes.length === expectedBytes.length && timingSafeEqual(suppliedBytes, expectedBytes)) authenticated = 'weather-reference-user';
        }
        const authInfo = typeof authenticated !== 'string' || authenticated.length === 0 ? undefined : {
          token: supplied, scopes: [], clientId: 'weather-reference', extra: {principal: authenticated},
        };
        return handler.fetch(request, {parsedBody: body, authInfo});
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
    emit: (name, data) => emit([{name, data}])[0],
    eventTypes: types, webhookDelivery,
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
