import {readFileSync} from 'node:fs';
import {resolve, dirname} from 'node:path';
import {z} from 'zod';
import type {Transport} from '@modelcontextprotocol/client';
import {getDefaultEnvironment, StdioClientTransport} from '@modelcontextprotocol/client/stdio';
import {canonical, deliveryMode} from './protocol.ts';
import {WebhookReceiver} from './webhook.ts';
import {createHttpTransport} from './wire.ts';

const envValue = z.union([z.string(), z.object({env: z.string().min(1), prefix: z.string().optional()})]);
const serverSchema = z.discriminatedUnion('transport', [
  z.object({transport: z.literal('http'), url: z.url(), headers: z.record(z.string(), envValue).optional()}),
  z.object({transport: z.literal('stdio'), command: z.string().min(1), args: z.array(z.string()).optional(), cwd: z.string().optional(), env: z.record(z.string(), envValue).optional()}),
]);
const configSchema = z.object({
  stateDir: z.string().optional(),
  servers: z.record(z.string(), serverSchema),
  webhook: z.object({publicUrl: z.url(), host: z.string().optional(), port: z.number().int().min(1).max(65535), keyFile: z.string().optional(), certFile: z.string().optional()}).optional(),
  watches: z.array(z.object({
    id: z.string().min(1), server: z.string().min(1), name: z.string().min(1), mode: deliveryMode,
    arguments: z.record(z.string(), z.unknown()).default({}), maxAgeMs: z.number().int().nonnegative().optional(),
    maxEvents: z.number().int().positive().optional(), ttlMs: z.number().int().positive().nullable().optional(),
  })).default([]),
});
export type EventsConfig = z.infer<typeof configSchema>;
function environment(values?: Record<string, z.infer<typeof envValue>>) {
  return Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) => {
    if (typeof value === 'string') return [key, value];
    const resolved = process.env[value.env];
    if (resolved === undefined) throw new Error(`Missing environment variable ${value.env}`);
    return [key, `${value.prefix ?? ''}${resolved}`];
  }));
}
export function readConfig(path: string) {
  const config = configSchema.parse(JSON.parse(readFileSync(path, 'utf8')));
  if (config.watches.length > 512) throw new Error('At most 512 watches can be configured. Remove unused watches.');
  const ids = new Set<string>();
  const subscriptions = new Set<string>();
  const perServer = new Map<string, number>();
  for (const watch of config.watches) {
    if (ids.has(watch.id)) throw new Error('Duplicate configured watch ID');
    if (!config.servers[watch.server]) throw new Error('Watch refers to an unknown server');
    const subscription = canonical([watch.server, watch.name, watch.arguments, watch.mode]);
    if (subscriptions.has(subscription)) throw new Error('Duplicate configured subscription. Keep one watch per server, event, arguments, and delivery mode.');
    if (watch.mode === 'webhook' && !config.webhook) throw new Error('Webhook watches require a webhook receiver. Add the webhook configuration.');
    const count = (perServer.get(watch.server) ?? 0) + 1;
    if (count > 32) throw new Error('At most 32 watches can run per server. Remove unused watches.');
    perServer.set(watch.server, count);
    subscriptions.add(subscription);
    ids.add(watch.id);
  }
  if (config.webhook && Boolean(config.webhook.keyFile) !== Boolean(config.webhook.certFile)) throw new Error('Webhook TLS requires both keyFile and certFile');
  return config;
}
export function createTransport(server: z.infer<typeof serverSchema>, baseDirectory: string): Transport {
  if (server.transport === 'http') return createHttpTransport(new URL(server.url), {requestInit: {headers: environment(server.headers)}});
  return new StdioClientTransport({command: server.command, args: server.args, cwd: server.cwd ? resolve(baseDirectory, server.cwd) : baseDirectory, env: {...getDefaultEnvironment(), ...environment(server.env)}, stderr: 'ignore'});
}
export function createReceiver(config: EventsConfig, configPath: string) {
  const settings = config.webhook;
  if (!settings) return undefined;
  const tls = settings.keyFile && settings.certFile ? {
    key: readFileSync(resolve(dirname(configPath), settings.keyFile), 'utf8'),
    cert: readFileSync(resolve(dirname(configPath), settings.certFile), 'utf8'),
  } : undefined;
  return new WebhookReceiver({...settings, tls});
}
