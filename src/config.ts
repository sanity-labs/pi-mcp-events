import {readFileSync} from 'node:fs';
import {dirname, resolve} from 'node:path';
import type {Transport} from '@modelcontextprotocol/client';
import {getDefaultEnvironment, StdioClientTransport} from '@modelcontextprotocol/client/stdio';
import {z} from 'zod';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import {clientSettingsSchema, watchOptionsSchema} from './options.ts';
import {canonical} from './protocol.ts';
import {WebhookReceiver, webhookOptionsSchema, validateWebhookAddress} from './webhook.ts';
import {createHttpTransport} from './wire.ts';

const environmentValue = z.union([
  z.string(), z.strictObject({env: z.string().min(1), prefix: z.string().optional()}),
]);
const values = z.record(z.string().min(1), environmentValue);
const serverSchema = z.discriminatedUnion('transport', [
  z.strictObject({
    transport: z.literal('http'),
    url: z.url({protocol: /^https?$/}).refine(value => {
      if (!URL.canParse(value)) return false;
      const url = new URL(value);
      return !url.username && !url.password && !url.hash;
    }, 'HTTP server URL must not contain credentials or a fragment'),
    headers: values.optional(),
  }),
  z.strictObject({
    transport: z.literal('stdio'), command: z.string().min(1), args: z.array(z.string()).optional(),
    cwd: z.string().min(1).optional(), env: values.optional(),
  }),
]);
const configSchema = z.strictObject({
  stateDir: z.string().min(1).optional(),
  client: clientSettingsSchema.strict().prefault({}),
  servers: z.record(z.string().min(1), serverSchema),
  webhook: webhookOptionsSchema.omit({tls: true}).extend({
    keyFile: z.string().min(1).optional(), certFile: z.string().min(1).optional(),
  }).strict().optional(),
  watches: z.array(watchOptionsSchema.omit({cursor: true}).extend({
    id: z.string().min(1), server: z.string().min(1),
    arguments: z.record(z.string(), z.json()).default({}),
  }).strict()).default([]),
});
export type EventsConfig = z.output<typeof configSchema>;
export type ServerConfig = z.output<typeof serverSchema>;

function invalid(message: string, details?: Record<string, unknown>): never {
  throw new EventsClientError(ErrorCode.InvalidConfiguration, message, {details});
}
export function parseConfig(input: unknown): EventsConfig {
  const result = configSchema.safeParse(input);
  if (!result.success) {
    invalid('Invalid MCP Events configuration', {issues: result.error.issues.map(({path, code, message}) => ({path, code, message}))});
  }
  const config = result.data;
  if (config.webhook) {
    const {keyFile, certFile} = config.webhook;
    if (Boolean(keyFile) !== Boolean(certFile)) invalid('Webhook TLS requires both keyFile and certFile');
    try { validateWebhookAddress(config.webhook, keyFile !== undefined); }
    catch (error) {
      if (error instanceof EventsClientError) invalid(error.message);
      throw error;
    }
  }
  const ids = new Set<string>();
  const subscriptions = new Set<string>();
  const perServer = new Map<string, number>();
  let webhooks = 0;
  for (const watch of config.watches) {
    if (ids.has(watch.id)) invalid('Duplicate configured watch ID', {watch: watch.id});
    if (!Object.hasOwn(config.servers, watch.server)) invalid('Watch refers to an unknown server', {watch: watch.id, server: watch.server});
    const subscription = canonical([watch.server, watch.name, watch.arguments, watch.mode]);
    if (subscriptions.has(subscription)) invalid('Duplicate configured subscription', {watch: watch.id});
    if (watch.mode === 'webhook') {
      if (!config.webhook) invalid('Webhook watches require a webhook receiver', {watch: watch.id});
      if (++webhooks > config.webhook.maxRegistrations) throw limitExceeded('maxRegistrations', config.webhook.maxRegistrations);
    }
    const count = (perServer.get(watch.server) ?? 0) + 1;
    if (count > config.client.maxSubscriptions) throw limitExceeded('maxSubscriptions', config.client.maxSubscriptions);
    perServer.set(watch.server, count);
    subscriptions.add(subscription);
    ids.add(watch.id);
  }
  return config;
}
function readText(path: string) {
  try { return readFileSync(path, 'utf8'); }
  catch (error) { throw new EventsClientError(ErrorCode.ConfigurationReadFailed, 'Could not read MCP Events configuration file', {cause: error, details: {path}}); }
}
export function readConfig(path: string): EventsConfig {
  const text = readText(path);
  let value: unknown;
  try { value = JSON.parse(text); }
  catch { invalid('MCP Events configuration must be valid JSON', {path}); }
  return parseConfig(value);
}
function environment(input?: Record<string, z.output<typeof environmentValue>>) {
  return Object.fromEntries(Object.entries(input ?? {}).map(([key, value]) => {
    if (typeof value === 'string') return [key, value];
    const resolved = process.env[value.env];
    if (resolved === undefined) throw new EventsClientError(ErrorCode.MissingEnvironmentVariable, 'MCP Events environment variable is not set', {details: {variable: value.env}});
    return [key, `${value.prefix ?? ''}${resolved}`];
  }));
}
export function createTransport(server: ServerConfig, baseDirectory: string): Transport {
  if (server.transport === 'http') {
    const values = environment(server.headers);
    let headers: Headers;
    try { headers = new Headers(values); }
    catch { invalid('Invalid HTTP headers in MCP Events configuration'); }
    return createHttpTransport(new URL(server.url), {requestInit: {headers}});
  }
  return new StdioClientTransport({
    command: server.command, args: server.args, cwd: resolve(baseDirectory, server.cwd ?? '.'),
    env: {...getDefaultEnvironment(), ...environment(server.env)}, stderr: 'ignore',
  });
}
export function createReceiver(config: EventsConfig, configPath: string): WebhookReceiver | undefined {
  const settings = config.webhook;
  if (!settings) return undefined;
  const {keyFile, certFile, ...options} = settings;
  const tls = keyFile && certFile ? {
    key: readText(resolve(dirname(configPath), keyFile)),
    cert: readText(resolve(dirname(configPath), certFile)),
  } : undefined;
  return new WebhookReceiver({...options, tls});
}
