import {randomBytes} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {ProtocolError} from '@modelcontextprotocol/server';
import {z} from 'zod';
import {startWeatherServer} from '../../dev/weather-server.mjs';

const station = 'weather.station.tick';
const pulse = 'weather.pulse';
const storedOrigin = 'https://conformance.invalid';
const primaryPrincipal = 'local-conformance';
const maxSubscriptions = 16;
const empty = z.object({});
const named = z.object({name: z.string()});
const identified = z.object({id: z.string()});
const tenant = z.object({principal: z.string(), name: z.string(), url: z.string()});

export async function startWeatherConformanceServer({port = 0} = {}) {
  const records = new Map();
  const store = {
    async get(id) { return records.get(id); },
    async put(record) { records.set(record.id, structuredClone(record)); },
    async delete(id) { return records.delete(id); },
    async *listByName(name) {
      for (const record of records.values()) if (record.name === name) yield record;
    },
  };
  const allowedOrigins = new Set();
  const failed = new Set();
  const gapped = new Set();
  const minted = new Map();
  let generation = 0;
  let mintedCount = 0;
  let head = 0;
  const readings = [];
  const lastBeat = new Map();
  const types = [
    [station, {
      description: 'A weather station tick with replay from the upstream log.',
      inputSchema: z.object({label: z.string().optional()}),
      payloadSchema: z.object({tick: z.number().int()}),
      check({maxEvents, maxAgeMs}, cursor) {
        const position = Number(cursor);
        const fresh = {events: [], cursor: String(head)};
        if (cursor === null) return fresh;
        if (failed.delete(station)) throw new Error('Weather station unavailable');
        if (gapped.delete(station) || !Number.isSafeInteger(position) || position > head || position < (readings[0]?.data.tick ?? head + 1) - 1) {
          return {...fresh, truncated: true};
        }
        const recent = readings.filter(reading => reading.data.tick > position);
        const remaining = maxAgeMs === undefined ? recent : recent.filter(reading => Date.parse(reading.timestamp) >= Date.now() - maxAgeMs);
        const events = remaining.slice(0, maxEvents);
        return {
          events, cursor: String(events.at(-1)?.data.tick ?? head), hasMore: events.length < remaining.length,
          truncated: remaining.length < recent.length,
        };
      },
    }],
    [pulse, {
      description: 'A live signal from a weather station with no replay.',
      inputSchema: empty, payloadSchema: z.object({beat: z.number().int()}),
      delivery: ['push', 'webhook'], replay: false,
      check({subscriptionId}) {
        const previous = lastBeat.get(subscriptionId);
        lastBeat.set(subscriptionId, head);
        const events = previous === undefined || previous === head
          ? [] : [{eventId: `pulse-${head}`, timestamp: new Date().toISOString(), data: {beat: head}}];
        return {events, cursor: null, truncated: gapped.delete(pulse)};
      },
      onEnd({subscriptionId}) { lastBeat.delete(subscriptionId); },
    }],
    ['weather.rain.gauge', {
      description: 'Rain gauge readings available via polling.',
      inputSchema: empty, payloadSchema: z.object({millimeters: z.number()}),
      delivery: ['poll'],
    }],
  ];
  const server = await startWeatherServer({
    port, intervalMs: 0, eventTypes: types, maxSubscriptions, checkIntervalMs: 250,
    authenticate: () => primaryPrincipal,
    allowVerifiedWebhook: url => new URL(url).origin === storedOrigin,
    webhookStore: store,
    webhookDeliveryOptions: {trustLoopbackHttp: origin => allowedOrigins.has(origin), retryBaseMs: 500},
    registerTools(mcp) {
      const tool = (name, description, inputSchema, run) => {
        mcp.registerTool(`events_conformance_${name}`, {description, inputSchema}, async args => {
          try { return {content: [{type: 'text', text: await run(args) ?? 'ok'}]}; }
          catch (error) { return {isError: true, content: [{type: 'text', text: error.message}]}; }
        });
      };
      tool('allow_callback_origin', 'Allow an exact loopback origin for the local conformance receiver.',
        z.object({origin: z.string()}), ({origin}) => {
          const url = new URL(origin);
          if (url.origin !== origin || url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port) {
            throw new Error('Expected a loopback HTTP origin with a port');
          }
          allowedOrigins.add(origin);
        });
      tool('catalog', 'Change a test event type.', z.object({op: z.enum(['add', 'remove', 'retype', 'extend']), name: z.string().optional()}),
        async ({op, name}) => {
          if (op === 'add') {
            const name = `weather.test.${++mintedCount}`;
            minted.set(name, 1);
            server.eventTypes.register(name, {description: 'Test weather event.', inputSchema: empty, payloadSchema: z.object({value: z.literal(1)})});
            return `ok: ${name}`;
          }
          if (!minted.has(name)) throw new Error('Expected a test event type');
          if (op === 'remove') {
            await server.eventTypes.remove(name);
            minted.delete(name);
          } else if (op === 'retype') {
            const next = minted.get(name) + 1;
            await server.eventTypes.update(name, {payloadSchema: z.object({value: z.literal(next)})}, {breaking: true});
            minted.set(name, next);
          } else {
            await server.eventTypes.update(name, {inputSchema: z.object({site: z.string().optional()})});
          }
        });
      tool('generation', 'Read the current reference server generation.', empty, () => String(generation));
      tool('quota', 'Read the subscription quota.', empty, () => JSON.stringify({name: station, max: maxSubscriptions / 4}));
      tool('restart', 'Clear process-local webhook state and retain the shared store.', empty, () => {
        server.webhookDelivery.reset();
        return String(++generation);
      });
      tool('subscribe_as', 'Create a placeholder subscription for another test principal.', tenant, async ({name, url, principal}) => {
        if (new URL(url).origin !== storedOrigin) throw new Error('Expected the conformance placeholder callback');
        const input = server.eventTypes.validate({name, arguments: {}}, 'webhook', principal);
        return (await server.webhookDelivery.subscribe({...input, url, secret: `whsec_${randomBytes(32).toString('base64')}`})).id;
      });
      tool('subscription_exists', 'Check whether a test principal has a live webhook subscription.', tenant, ({name, url, principal}) => {
        return String([...records.values()].some(record => record.name === name && record.url === url
          && record.principal === principal && record.expiresAt > Date.now()));
      });
      tool('subscription_state', 'Read the selected webhook subscription state.', identified, ({id}) => {
        return (records.get(id)?.expiresAt ?? 0) > Date.now() ? 'active' : 'absent';
      });
      tool('terminate', 'Revoke access to a test event type.', named, ({name}) => server.eventTypes.terminate(name));
      tool('webhook_gap', 'Signal an upstream gap to the selected webhook subscription.', identified, ({id}) => {
        const cursor = String(head);
        if (!server.webhookDelivery.gap(id, cursor)) throw new Error('Unknown webhook subscription');
        return cursor;
      });
      tool('webhook_terminate', 'Revoke the selected webhook subscription.', identified, async ({id}) => {
        const record = records.get(id);
        if (!record) throw new Error('Unknown webhook subscription');
        await server.webhookDelivery.terminate(record.name, new ProtocolError(-32024, 'Weather station access revoked'), {id});
      });
      tool('yield_error', 'Make one upstream read fail.', named, ({name}) => {
        if (name !== station) throw new Error('Expected the checked weather station type');
        failed.add(name);
      });
      tool('yield_gap', 'Move a checked feed past one replay gap.', named, ({name}) => {
        if (name !== station && name !== pulse) throw new Error('Unknown test source');
        gapped.add(name);
      });
    },
  });
  const timer = setInterval(() => {
    head++;
    readings.push({eventId: `station-${head}`, timestamp: new Date().toISOString(), data: {tick: head}, cursor: String(head)});
    if (readings.length > 200) readings.shift();
    server.emit('weather.rain.gauge', {millimeters: head % 12});
  }, 1000).unref();
  return {...server, close() { clearInterval(timer); return server.close(); }};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const server = await startWeatherConformanceServer();
  console.log(server.url);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => void server.close());
}
