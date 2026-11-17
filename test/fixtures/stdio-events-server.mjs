import {appendFileSync, existsSync, unlinkSync} from 'node:fs';
import {createInterface} from 'node:readline';
import {readCompleteJsonLines} from './jsonl.mjs';

const historyPath = process.argv[2];
const requestLogPath = process.argv[3];
const exitPath = process.argv[4];
const streams = new Map();
const input = createInterface({input: process.stdin});

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function readHistory() {
  try {
    return readCompleteJsonLines(historyPath);
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

setInterval(() => {
  if (existsSync(exitPath)) {
    unlinkSync(exitPath);
    appendFileSync(requestLogPath, `${JSON.stringify({kind: 'exit'})}\n`);
    process.exit(1);
  }
}, 10);

function notify(stream, method, params) {
  send({jsonrpc: '2.0', method: `notifications/events/${method}`, params: {
    ...params,
    _meta: {'io.modelcontextprotocol/subscriptionId': stream.id},
  }});
}

function openStream(request) {
  const params = request.params ?? {};
  appendFileSync(requestLogPath, `${JSON.stringify({kind: 'stream', id: request.id, channel: params.arguments?.channel, cursor: params.cursor ?? null})}\n`);
  const history = readHistory();
  const latest = Number(history.at(-1)?.cursor ?? 0);
  const cursor = params.cursor === null || params.cursor === undefined ? latest : Number(params.cursor);
  const stream = {
    id: request.id,
    channel: params.arguments?.channel,
    cursor,
    timer: undefined,
  };
  streams.set(request.id, stream);
  notify(stream, 'active', {cursor: String(cursor), truncated: false, heartbeatMs: 50});
  const deliver = () => {
    if (!streams.has(stream.id)) return;
    const records = readHistory();
    const pending = records.filter(event => Number(event.cursor) > stream.cursor);
    for (const event of pending) {
      if (event.data.channel === stream.channel) notify(stream, 'event', event);
      stream.cursor = Number(event.cursor);
    }
    const current = Number(records.at(-1)?.cursor ?? stream.cursor);
    if (current > stream.cursor) stream.cursor = current;
    notify(stream, 'heartbeat', {cursor: String(stream.cursor)});
  };
  stream.timer = setInterval(deliver, 25);
  deliver();
}

input.on('line', line => {
  let message;
  try { message = JSON.parse(line); }
  catch { process.exitCode = 1; return; }
  if (message.method === 'initialize') {
    send({jsonrpc: '2.0', id: message.id, result: {
      protocolVersion: message.params?.protocolVersion ?? '2026-07-28',
      capabilities: {},
      serverInfo: {name: 'stdio-events-fixture', version: '1.0.0'},
    }});
    return;
  }
  if (message.method === 'notifications/initialized') return;
  if (message.method === 'server/discover') {
    send({jsonrpc: '2.0', id: message.id, result: {
      resultType: 'complete', supportedVersions: ['2026-07-28'],
      capabilities: {extensions: {'io.modelcontextprotocol/events': {}}},
    }});
    return;
  }
  if (message.method === 'events/list') {
    send({jsonrpc: '2.0', id: message.id, result: {resultType: 'complete', events: [{
      name: 'example.changed', delivery: ['push'],
      inputSchema: {type: 'object', required: ['channel'], properties: {channel: {type: 'string'}}, additionalProperties: false},
      payloadSchema: {type: 'object', required: ['channel', 'value'], properties: {
        channel: {type: 'string'}, value: {type: 'number'},
      }, additionalProperties: false},
    }]}});
    return;
  }
  if (message.method === 'events/stream') {
    openStream(message);
    return;
  }
  if (message.method === 'notifications/cancelled') {
    appendFileSync(requestLogPath, `${JSON.stringify({kind: 'cancel', requestId: message.params?.requestId})}\n`);
    const stream = streams.get(message.params?.requestId);
    if (stream) {
      clearInterval(stream.timer);
      streams.delete(stream.id);
    }
  }
});

process.on('SIGTERM', () => {
  for (const stream of streams.values()) clearInterval(stream.timer);
  streams.clear();
  process.exit(0);
});
