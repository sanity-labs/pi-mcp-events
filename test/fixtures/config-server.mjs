import {createInterface} from 'node:readline';

const input = createInterface({input: process.stdin});
input.on('line', line => {
  const message = JSON.parse(line);
  if (!('id' in message)) return;
  let result;
  if (message.method === 'server/discover') {
    result = {supportedVersions: ['2026-07-28'], capabilities: {extensions: {'io.modelcontextprotocol/events': {}}}};
  } else {
    result = {events: [{
      name: 'test.changed', delivery: ['poll'], inputSchema: {}, payloadSchema: {},
      description: JSON.stringify({cwd: process.cwd(), args: process.argv.slice(2), token: process.env.PI_EVENTS_TEST_TOKEN, literal: process.env.PI_EVENTS_LITERAL, inherited: process.env.PI_EVENTS_UNLISTED}),
    }]};
  }
  process.stdout.write(`${JSON.stringify({jsonrpc: '2.0', id: message.id, result: {resultType: 'complete', ...result}})}\n`);
});
