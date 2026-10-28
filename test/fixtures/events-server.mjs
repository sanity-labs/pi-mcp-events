import assert from 'node:assert/strict';
import {createHmac, randomBytes, randomUUID} from 'node:crypto';
import {createServer, request as httpRequest} from 'node:http';
import {createServer as createHttpsServer, request as httpsRequest} from 'node:https';
import {createServer as createTcpServer} from 'node:net';
import {createEventLog} from '../../dev/event-log.mjs';

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return server.address().port;
}
async function close(server) {
  server.closeAllConnections?.();
  await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
}
const schema = {
  inputSchema: {type: 'object', required: ['channel'], properties: {channel: {type: 'string'}}, additionalProperties: false},
  payloadSchema: {type: 'object', required: ['channel', 'value'], properties: {channel: {type: 'string'}, value: {type: 'number'}}, additionalProperties: false},
};

// Synthetic test server only. It accepts callbacks solely through its own loopback TLS proxy.
export async function startFixture(tls) {
  const reservation = createTcpServer();
  const callbackPort = await listen(reservation);
  await close(reservation);
  const proxy = createHttpsServer(tls, (request, response) => {
    const upstream = httpRequest({hostname: '127.0.0.1', port: callbackPort, path: request.url, method: request.method, headers: request.headers}, incoming => {
      response.writeHead(incoming.statusCode, incoming.headers);
      incoming.pipe(response);
    });
    upstream.on('error', () => {response.writeHead(502); response.end();});
    request.pipe(upstream);
  });
  const proxyPort = await listen(proxy);
  const callbackOrigin = `https://127.0.0.1:${proxyPort}`;
  const token = randomBytes(32).toString('hex');
  const streams = new Set();
  const webhooks = new Map();
  const history = createEventLog();
  const requests = [];
  let verifications = 0;
  let deliveries = 0;
  const send = (response, value) => {
    response.writeHead(200, {'content-type': 'application/json'});
    response.end(JSON.stringify(value));
  };
  const signed = async (subscription, body) => {
    assert.equal(new URL(subscription.url).origin, callbackOrigin);
    const raw = JSON.stringify(body);
    const id = `msg_${randomUUID()}`;
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = createHmac('sha256', Buffer.from(subscription.secret.slice(6), 'base64')).update(`${id}.${timestamp}.${raw}`).digest('base64');
    return new Promise((resolve, reject) => {
      const request = httpsRequest(subscription.url, {method: 'POST', ca: tls.cert, headers: {
        'content-type': 'application/json', 'webhook-id': id, 'webhook-timestamp': timestamp,
        'webhook-signature': `v1,${signature}`, 'x-mcp-subscription-id': subscription.id,
      }}, response => {
        let text = '';
        response.on('data', chunk => {text += chunk;});
        response.on('end', () => {
          if (response.statusCode < 200 || response.statusCode >= 300) reject(new Error(`Callback returned ${response.statusCode}`));
          else resolve(text ? JSON.parse(text) : undefined);
        });
      });
      request.on('error', reject);
      request.setTimeout(5000, () => request.destroy(new Error('Callback timed out')));
      request.end(raw);
    });
  };
  function notify(stream, kind, params) {
    stream.response.write(`data: ${JSON.stringify({jsonrpc: '2.0', method: `notifications/events/${kind}`, params: {...params, _meta: {'io.modelcontextprotocol/subscriptionId': stream.id}}})}\n\n`);
  }
  const server = createServer((request, response) => {
    void handle(request, response).catch(error => {
      if (!response.headersSent) response.writeHead(500, {'content-type': 'application/json'});
      response.end(JSON.stringify({error: error.message}));
    });
  });
  async function handle(request, response) {
    if (request.headers.authorization !== `Bearer ${token}`) {response.writeHead(401); response.end(); return;}
    if (request.url === '/status') {send(response, {streams: streams.size, webhooks: webhooks.size}); return;}
    let raw = '';
    for await (const chunk of request) {
      raw += chunk;
      if (Buffer.byteLength(raw) > 65536) throw new Error('Fixture request too large');
    }
    const body = JSON.parse(raw);
    if (request.url === '/emit') {
      const event = history.append('example.changed', body);
      for (const stream of streams) if (stream.channel === body.channel) notify(stream, 'event', {...event, cursor: history.cursor(stream.key)});
      for (const [key, subscription] of webhooks) {
        if (subscription.expiresAt <= Date.now()) {webhooks.delete(key); continue;}
        if (subscription.channel !== body.channel) continue;
        await signed(subscription, {...event, name: 'example.live_changed', cursor: null});
        deliveries++;
      }
      send(response, {eventId: event.eventId});
      return;
    }
    assert.equal(request.url, '/mcp');
    const params = body.params ?? {};
    requests.push({method: body.method, params});
    const reply = result => send(response, {jsonrpc: '2.0', id: body.id, result: {resultType: 'complete', ...result}});
    const channel = params.arguments?.channel;
    switch (body.method) {
      case 'server/discover':
        reply({supportedVersions: ['2026-07-28'], capabilities: {extensions: {'io.modelcontextprotocol/events': {}}}});
        return;
      case 'events/list':
        reply({events: [{name: 'example.changed', delivery: ['poll', 'push'], ...schema}, {name: 'example.live_changed', delivery: ['webhook'], ...schema}]});
        return;
      case 'events/poll': {
        const {events, cursor, truncated, hasMore} = history.read({
          cursor: params.cursor, key: JSON.stringify([params.name, channel]), maxEvents: params.maxEvents ?? 50,
          matches: event => event.data.channel === channel,
        });
        reply({events, cursor, truncated, hasMore, nextPollMs: 1000});
        return;
      }
      case 'events/stream': {
        response.writeHead(200, {'content-type': 'text/event-stream', 'cache-control': 'no-cache'});
        const key = JSON.stringify([params.name, channel]);
        const page = history.read({cursor: params.cursor, key, matches: event => event.data.channel === channel});
        const stream = {id: body.id, response, channel, key};
        streams.add(stream);
        notify(stream, 'active', {cursor: page.startCursor, truncated: page.truncated});
        for (const event of page.events) notify(stream, 'event', event);
        const heartbeat = setInterval(() => notify(stream, 'heartbeat', {cursor: history.cursor(key)}), 1000);
        response.on('close', () => {clearInterval(heartbeat); streams.delete(stream);});
        return;
      }
      case 'events/subscribe': {
        const key = JSON.stringify([params.name, params.arguments, params.delivery.url]);
        let subscription = webhooks.get(key);
        if (!subscription) {
          subscription = {id: randomUUID(), channel, url: params.delivery.url, secret: params.delivery.secret};
          const challenge = randomUUID();
          assert.deepEqual(await signed(subscription, {type: 'verification', challenge}), {challenge});
          verifications++;
        }
        subscription.expiresAt = Date.now() + 60_000;
        webhooks.set(key, subscription);
        reply({id: subscription.id, refreshBefore: new Date(subscription.expiresAt).toISOString(), cursor: null, truncated: false});
        return;
      }
      case 'events/unsubscribe':
        webhooks.delete(JSON.stringify([params.name, params.arguments, params.delivery.url]));
        reply({});
        return;
      default:
        send(response, {jsonrpc: '2.0', id: body.id, error: {code: -32601, message: 'Unknown fixture method'}});
    }
  }
  try {
    const port = await listen(server);
    const baseUrl = `http://127.0.0.1:${port}`;
    return {
      connection: {baseUrl, url: `${baseUrl}/mcp`, token, callbackOrigin, callbackPort, callbackTls: false, webhookEventName: 'example.live_changed', webhookReplay: false},
      requests,
      disconnect() { for (const stream of streams) stream.response.end(); },
      terminateStreams() {
        for (const stream of streams) {
          notify(stream, 'terminated', {error: {code: -32024, message: 'Fixture subscription ended'}});
          stream.response.end();
        }
      },
      stats: () => ({streams: streams.size, webhooks: webhooks.size, verifications, deliveries}),
      async close() {for (const stream of streams) stream.response.end(); await close(server); await close(proxy);},
    };
  } catch (error) {await close(proxy); throw error;}
}
