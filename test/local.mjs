import {readFileSync, writeFileSync} from 'node:fs';
import {resolve, dirname, join} from 'node:path';

const connectionPath = resolve(process.env.LOCAL_EVENTS_CONNECTION_FILE ?? '.local/mellon-connection.json');
const connection = JSON.parse(readFileSync(connectionPath, 'utf8'));
if (new URL(connection.url).hostname !== '127.0.0.1' || new URL(connection.baseUrl).hostname !== '127.0.0.1') throw new Error('This helper only connects to local Mellon');
const modes = ['poll', 'push', 'webhook'];
const [action, requestedMode = 'all', input = '42'] = process.argv.slice(2);
if (action === 'configure') {
  const directory = dirname(connectionPath);
  const path = join(directory, 'pi-events.json');
  writeFileSync(path, JSON.stringify({
    stateDir: './manual-state',
    servers: {mellon: {transport: 'http', url: connection.url, headers: {Authorization: `Bearer ${connection.token}`}}},
    webhook: {publicUrl: `${connection.callbackOrigin}/hooks`, host: '127.0.0.1', port: Number(new URL(connection.callbackOrigin).port), keyFile: './tls/localhost.key', certFile: './tls/localhost.crt'},
    watches: modes.map(mode => ({id: mode, server: 'mellon', name: 'example.changed', mode, arguments: {channel: `manual-${mode}`}})),
  }, null, 2), {mode: 0o600});
  console.log(`Local Pi configuration written to ${path}`);
} else if (action === 'emit') {
  const selected = requestedMode === 'all' ? modes : [requestedMode];
  const value = Number(input);
  if (!Number.isFinite(value) || selected.some(mode => !modes.includes(mode))) throw new Error('Use emit [all|poll|push|webhook] [number]');
  for (const mode of selected) {
    const response = await fetch(`${connection.baseUrl}/emit`, {method: 'POST', headers: {'Content-Type': 'application/json', Authorization: `Bearer ${connection.token}`}, body: JSON.stringify({channel: `manual-${mode}`, value})});
    if (!response.ok) throw new Error(`Local event emission failed: ${response.status}`);
    console.log(`${mode}: emitted ${value}`);
  }
} else throw new Error('Use configure or emit [all|poll|push|webhook] [number]');
