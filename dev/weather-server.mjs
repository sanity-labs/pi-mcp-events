import {createServer} from 'node:http';
import {randomInt} from 'node:crypto';
import {parseArgs} from 'node:util';
import {pathToFileURL} from 'node:url';
import {createMcpHandler, Server, ProtocolError, ProtocolErrorCode} from '@modelcontextprotocol/server';
import {toNodeHandler, localhostHostValidation, localhostOriginValidation} from '@modelcontextprotocol/node';
import {z} from 'zod';
import {catalog, createWeather, payloads, subscriptionArguments} from './weather.mjs';
import {createEventLog} from './event-log.mjs';

// SEP-3415 at b0efd116faca84c9caad109ccf0778da4dc49579; polling and streaming only.
const revision = '2026-07-28';
const subscriptionId = 'io.modelcontextprotocol/subscriptionId';
const watch = z.object({
  name: z.string(), arguments: subscriptionArguments.default({}), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(),
  maxEvents: z.number().int().positive().max(1000).optional(),
});

export async function startWeatherServer({port = 3015, intervalMs = 20_000, heartbeatMs = 1000, historyLimit = 1000, now = Date.now, onEvent = () => {}} = {}) {
  if (!Number.isInteger(intervalMs) || (intervalMs !== 0 && (intervalMs < 10_000 || intervalMs > 2_147_483_647))) throw new Error('Interval must be 0 or an integer between 10000 and 2147483647 ms');
  if (!Number.isInteger(heartbeatMs) || heartbeatMs < 1 || heartbeatMs > 30_000) throw new Error('Heartbeat must be an integer between 1 and 30000 ms');
  if (!Number.isInteger(historyLimit) || historyLimit < 1) throw new Error('History limit must be a positive integer');
  const log = createEventLog(historyLimit);
  const weather = createWeather({now});
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
  const handler = createMcpHandler(() => {
    const server = new Server({name: 'weather-reference', version: '0.0.0'}, {
      supportedProtocolVersions: [revision], capabilities: {extensions: {'io.modelcontextprotocol/events': {}}},
    });
    server.setRequestHandler('events/list', {params: z.object({cursor: z.string().optional()}).default({})}, params => {
      if (params.cursor !== undefined) throw new ProtocolError(ProtocolErrorCode.InvalidParams, 'The weather catalog has one page');
      return {resultType: 'complete', events: catalog};
    });
    server.setRequestHandler('events/poll', {params: watch}, params => {
      const {events, cursor, truncated, hasMore} = read(params);
      return {resultType: 'complete', events, cursor, truncated, hasMore, nextPollMs: 1000};
    });
    server.setRequestHandler('events/stream', {params: watch}, async (params, ctx) => {
      const page = read({...params, maxEvents: historyLimit});
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
        await notify('active', {cursor: page.startCursor, truncated: page.truncated});
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
      const heartbeat = setInterval(() => enqueue(() => notify('heartbeat', {cursor})), heartbeatMs);
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
  }, {legacy: 'reject', maxRequestBodySize: 65_536});
  const emit = events => events.map(({name, data}) => {
    const event = log.append(name, payloads[name].parse(data));
    onEvent(event);
    for (const receive of listeners) receive(event);
    return event;
  });
  const advance = () => emit([...weather.advance(), ...weather.alerts()]);
  // The SDK checks core routing headers; the draft adds Mcp-Name to Events methods.
  const mcp = toNodeHandler({fetch: async request => {
    let body;
    if (request.method === 'POST') {
      try { body = await request.clone().json(); }
      catch { return handler.fetch(request); }
    }
    if (['events/poll', 'events/stream', 'events/subscribe', 'events/unsubscribe'].includes(body?.method) && typeof body.params?.name === 'string') {
      const header = request.headers.get('Mcp-Name');
      let name = header;
      if (header?.startsWith('=?base64?') && header.endsWith('?=')) {
        const encoded = header.slice(9, -2);
        const decoded = Buffer.from(encoded, 'base64');
        name = decoded.toString('base64') === encoded ? decoded.toString('utf8') : null;
      }
      if (name !== body.params.name) return Response.json({jsonrpc: '2.0', id: body.id ?? null,
        error: {code: -32020, message: 'Mcp-Name must match params.name'},
      }, {status: 400});
    }
    return handler.fetch(request, {parsedBody: body});
  }}, {maxRequestBodySize: 65_536});
  const hostAllowed = localhostHostValidation();
  const originAllowed = localhostOriginValidation();
  const http = createServer((request, response) => {
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
  try {
    await new Promise((resolve, reject) => { http.once('error', reject); http.listen(port, '127.0.0.1', resolve); });
  } catch (error) { await handler.close(); throw error; }
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
  if (intervalMs > 0) { schedule(); scheduleAlerts(20_000); }
  return {
    url: `http://127.0.0.1:${http.address().port}/mcp`, advance,
    async close() {
      clearTimeout(timer);
      clearTimeout(alertTimer);
      await handler.close();
      http.closeAllConnections();
      await new Promise((resolve, reject) => http.close(error => error ? reject(error) : resolve()));
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const {values} = parseArgs({options: {port: {type: 'string', default: '3015'}, interval: {type: 'string', default: '20000'}}});
  const server = await startWeatherServer({port: Number(values.port), intervalMs: Number(values.interval),
    onEvent: event => {
      const {station, condition, temperatureC, windKph, rainfallMmPerHour, daylight, status, message, previousCondition} = event.data;
      let description = condition;
      if (previousCondition) description = `${previousCondition} → ${condition}`;
      if (status) description = `${status.toUpperCase()}: ${message}`;
      console.log(`\n${event.timestamp.slice(11, 19)}Z  ${station}  ${event.name}`);
      console.log(`  ${description}  |  ${daylight ? 'day' : 'night'}`);
      console.log(`  ${temperatureC} °C  |  wind ${windKph} km/h  |  rain ${rainfallMmPerHour} mm/h`);
      console.log(`  id: ${event.eventId}`);
    },
  });
  const cadence = Number(values.interval) === 0 ? 'POST /advance for the next reading.' : `Bursts of 1–3 readings, 10000–${values.interval} ms apart. Alerts every 45 seconds, starting after 20 seconds.`;
  console.log(`Island weather station: ${server.url}\n${cadence} Weather changes over minutes. Ctrl-C to stop.`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { void server.close().catch(error => { console.error(error); process.exitCode = 1; }); });
}
