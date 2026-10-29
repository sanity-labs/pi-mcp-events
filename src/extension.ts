import {createHash} from 'node:crypto';
import {existsSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {getAgentDir, type ExtensionAPI, type ExtensionContext} from '@earendil-works/pi-coding-agent';
import {EventsClient, type Watch, type WatchOptions} from './client.ts';
import {createReceiver, createTransport, readConfig} from './config.ts';
import {ConversationDelivery} from './conversation.ts';
import {failureDiagnostic, webhookDiagnostic} from './diagnostics.ts';
import {EventsClientError, EventsClientErrorCode} from './errors.ts';
import {MESSAGE_TYPE, OBSERVATION_EVENT, STATUS_MESSAGE_TYPE} from './pi-contract.ts';
import {renderEventMessage} from './message-renderer.ts';
import {canonical} from './protocol.ts';
import {SessionState} from './session-state.ts';
import type {WebhookReceiver} from './webhook.ts';
import {EventsWire} from './wire.ts';

const hash = (value: string) => createHash('sha256').update(value).digest('hex');
type SessionWatch = Pick<WatchOptions, 'name' | 'mode'> & {
  server: string; watch?: Watch; start?: () => Promise<void>; starting?: Promise<void>;
  failure?: 'connection failed' | 'could not start' | 'unsupported schema' | 'failed';
  diagnostic?: string; sourceDiagnostic?: string; webhookDiagnostic?: string;
};
type ActiveSession = {
  closed: boolean; clients: Map<string, {client: EventsClient; ready: Promise<void>; catalogFailed?: boolean}>; watches: Map<string, SessionWatch>;
  stopping?: Promise<void>;
  state?: SessionState; delivery?: ConversationDelivery; receiver?: WebhookReceiver; path?: string;
};

export default function mcpEvents(pi: ExtensionAPI) {
  pi.registerMessageRenderer(MESSAGE_TYPE, renderEventMessage);
  let active: ActiveSession | undefined;
  const report = (content: string) => {
    try { pi.sendMessage({customType: STATUS_MESSAGE_TYPE, display: true, content}, {triggerTurn: false}); }
    catch { /* Reporting must not interrupt ordinary Pi use. */ }
  };
  const closeConnections = (session: ActiveSession): Promise<void> => {
    if (session.stopping) return session.stopping;
    const clients = [...session.clients.values()];
    session.clients.clear();
    const receiver = session.receiver;
    session.receiver = undefined;
    session.stopping = (async () => {
      await Promise.allSettled(clients.map(({client}) => client.close()));
      try { await receiver?.close(); }
      catch { /* Finite webhook grants expire if eager cleanup cannot complete. */ }
    })().finally(() => { session.stopping = undefined; });
    return session.stopping;
  };
  const stop = async () => {
    const session = active;
    active = undefined;
    if (!session) return;
    session.closed = true;
    session.delivery?.close();
    await closeConnections(session);
  };
  const start = async (ctx: ExtensionContext) => {
    await stop();
    const configPath = resolve(process.env.PI_MCP_EVENTS_CONFIG ?? join(ctx.cwd, '.pi', 'mcp-events.json'));
    if (!existsSync(configPath)) return;
    const session: ActiveSession = {closed: false, clients: new Map(), watches: new Map()};
    active = session;
    try {
      const config = readConfig(configPath);
      for (const {id, server, name, mode} of config.watches) session.watches.set(id, {server, name, mode});
      const persistent = ctx.sessionManager.getSessionFile() !== undefined;
      if (persistent) {
        const directory = config.stateDir ? resolve(dirname(configPath), config.stateDir) : join(getAgentDir(), 'mcp-events');
        session.path = join(directory, `${hash(`${configPath}\0${ctx.sessionManager.getSessionId()}`)}.json`);
      }
      const state = new SessionState(session.path, {onWarning: () => session.delivery?.schedule()});
      session.state = state;
      const delivery = new ConversationDelivery(pi, ctx, state, session.path);
      session.delivery = delivery;
      delivery.schedule();
      session.receiver = createReceiver(config, configPath);
      const identity = (watch: typeof config.watches[number]) => canonical([
        watch.id, watch.server, hash(canonical(config.servers[watch.server])), watch.mode, watch.name, watch.arguments,
        watch.mode === 'webhook' ? config.webhook?.publicUrl : null,
      ]);
      state.retainCursors(config.watches.map(identity));
      state.retainWebhookIdentities(config.watches.filter(watch => watch.mode === 'webhook').map(identity));
      const connection = (server: string) => {
        const existing = session.clients.get(server);
        if (existing) return existing;
        session.receiver ??= createReceiver(config, configPath);
        const client = new EventsClient(new EventsWire(() => createTransport(config.servers[server]!, dirname(configPath))), {...config.client, webhookReceiver: session.receiver});
        const ready = client.connect().then(() => {
          void client.catalogUpdates?.closed.catch(() => {
            const connection = session.clients.get(server);
            if (session.closed || connection?.client !== client) return;
            connection.catalogFailed = true;
            report(`MCP Events catalog updates stopped for ${server}. Watch definitions may be outdated. Use /reload to reconnect.`);
          });
        }).catch(async error => {
          if (session.clients.get(server)?.client === client) session.clients.delete(server);
          await client.close().catch(() => {});
          if (!session.closed) report(`MCP Events could not connect to ${server}. ${failureDiagnostic(error)}`);
          throw error;
        });
        const created = {client, ready};
        session.clients.set(server, created);
        return created;
      };
      for (const configured of config.watches) {
        const key = identity(configured);
        const entry = session.watches.get(configured.id)!;
        const launch = async () => {
          await entry.watch?.closed.catch(() => {});
          await session.stopping;
          if (session.closed) return;
          const restarting = entry.watch !== undefined || entry.failure !== undefined;
          entry.failure = undefined;
          entry.diagnostic = undefined;
          entry.sourceDiagnostic = undefined;
          entry.webhookDiagnostic = undefined;
          let client: EventsClient;
          try {
            const connected = connection(configured.server);
            await connected.ready;
            client = connected.client;
            if (restarting && !session.closed) await client.refresh();
          } catch (error) {
            entry.failure = 'connection failed';
            entry.diagnostic = failureDiagnostic(error);
            return;
          }
          if (session.closed) return;
          try {
            const webhookIdentity = configured.mode === 'webhook' ? state.webhookIdentity(key) : undefined;
            const watch = client.watch({...configured, webhookIdentity, cursor: state.getCursor(key),
              ttlMs: state.mode !== 'persistent' && configured.ttlMs === null ? undefined : configured.ttlMs,
            }, observation => {
              if (session.closed) throw new Error('Pi session changed');
              if (observation.type === 'status') {
                if (observation.status === 'source-error') entry.sourceDiagnostic = failureDiagnostic(observation.error);
                if (observation.status === 'webhook-delivery') entry.webhookDiagnostic = webhookDiagnostic(observation.deliveryStatus);
              } else {
                state.accept({server: configured.server, watch: configured.id, observation});
                if (observation.type === 'event') entry.sourceDiagnostic = undefined;
                delivery.schedule();
              }
              try { pi.events.emit(OBSERVATION_EVENT, {server: configured.server, watch: configured.id, observation}); }
              catch { /* Other extensions must not prevent conversation delivery. */ }
            }, cursor => {
              if (session.closed) throw new Error('Pi session changed');
              state.setCursor(key, cursor);
            });
            entry.watch = watch;
            void watch.closed.catch(error => {
              if (session.closed || entry.watch !== watch) return;
              entry.diagnostic = failureDiagnostic(error);
              if (error instanceof EventsClientError && error.code === EventsClientErrorCode.UnsupportedEventSchema) {
                entry.failure = 'unsupported schema';
                report(`MCP Events watch "${configured.id}" stopped because its event schemas are unsupported or invalid. Check the server's event schemas.`);
                return;
              }
              entry.failure = 'failed';
              report(`MCP Events watch "${configured.id}" stopped. ${entry.diagnostic}`);
            });
          } catch (error) {
            entry.failure = 'could not start';
            entry.diagnostic = failureDiagnostic(error);
            if (error instanceof EventsClientError && error.code === EventsClientErrorCode.UnsupportedEventSchema) {
              entry.failure = 'unsupported schema';
              report(`MCP Events could not start watch "${configured.id}" because its event schemas are unsupported or invalid. Check the server's event schemas.`);
              return;
            }
            report(`MCP Events could not start watch "${configured.id}". ${entry.diagnostic}`);
          }
        };
        entry.start = () => {
          if (entry.starting) return entry.starting;
          if (entry.watch && entry.watch.status !== 'stopped') return Promise.resolve();
          entry.starting = launch().finally(() => { entry.starting = undefined; });
          return entry.starting;
        };
      }
      await Promise.all([...session.watches.values()].map(entry => entry.start!()));
    } catch {
      if (session.closed) return;
      await stop();
      report('MCP Events could not start. Check PI_MCP_EVENTS_CONFIG and the event configuration file.');
    }
  };
  pi.on('session_start', (_event, ctx) => start(ctx));
  pi.on('session_shutdown', stop);
  pi.registerCommand('mcp-events', {
    description: 'Manage event subscriptions: status, start <watch-id>, stop [watch-id]',
    handler: async (args, ctx) => {
      const [action = 'status', id] = args.trim().split(/\s+/);
      if (action === 'start') {
        if (!id) { ctx.ui.notify('/mcp-events start <watch-id>', 'info'); return; }
        const entry = active?.watches.get(id);
        if (!entry?.start) { ctx.ui.notify(`MCP Events watch "${id}" was not found. Use /mcp-events status to list watches.`, 'warning'); return; }
        if (entry.watch && entry.watch.status !== 'stopped') { report(`MCP Events watch "${id}" is already running.`); return; }
        await entry.start();
        if (entry.failure) { ctx.ui.notify(`MCP Events watch "${id}" could not start. Use /mcp-events status for details.`, 'warning'); return; }
        report(`MCP Events watch "${id}" is starting.`);
        return;
      }
      if (action === 'stop') {
        if (id) {
          const entry = active?.watches.get(id);
          if (!entry) { ctx.ui.notify(`MCP Events watch "${id}" was not found. Use /mcp-events status to list watches.`, 'warning'); return; }
          await entry.starting;
          await entry.watch?.stop();
        }
        else if (active) {
          const session = active;
          await Promise.all([...session.watches.values()].map(entry => entry.starting));
          await closeConnections(session);
        }
        report(id ? `MCP Events watch "${id}" stopped.` : 'MCP Events subscriptions stopped.');
        return;
      }
      if (action && action !== 'status') { ctx.ui.notify('/mcp-events status\n/mcp-events start <watch-id>\n/mcp-events stop [watch-id]', 'info'); return; }
      const session = active;
      const running = [...session?.watches.values() ?? []].filter(entry => entry.starting || (entry.watch && entry.watch.status !== 'stopped')).length;
      const watches = [...session?.watches ?? []].map(([id, entry]) =>
        [`${id}: ${entry.starting ? 'starting' : entry.failure ?? entry.watch?.status ?? 'stopped'} — ${entry.server} / ${entry.name} (${entry.mode})`,
          ...[entry.diagnostic, entry.sourceDiagnostic, entry.webhookDiagnostic].filter(Boolean).map(text => `  ${text}`)].join('\n'));
      const catalogWarnings = [...session?.clients ?? []].filter(([, connection]) => connection.catalogFailed)
        .map(([server]) => `Catalog updates stopped for ${server}. Watch definitions may be outdated. Use /reload to reconnect.`);
      const schemaWarnings = [...session?.clients ?? []].flatMap(([server, {client}]) => client.catalogIssues
        .map(({name}) => `Unavailable event: ${server} / ${name} (unsupported or invalid schema).`));
      report(session?.state
        ? [`MCP Events: ${session.state.mode}; ${running} running watch(es); ${session.state.size} pending observation(s).`,
          `Event state: ${session.path ?? 'memory only'}`, ...watches, ...catalogWarnings, ...schemaWarnings, session.state.warning?.message].filter(Boolean).join('\n')
        : 'MCP Events has no active session.');
    },
  });
}
