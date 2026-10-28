import {createHash} from 'node:crypto';
import {existsSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {getAgentDir, type ExtensionAPI, type ExtensionContext} from '@earendil-works/pi-coding-agent';
import {EventsClient, type Watch, type WatchOptions} from './client.ts';
import {createReceiver, createTransport, readConfig} from './config.ts';
import {ConversationDelivery} from './conversation.ts';
import {MESSAGE_TYPE, OBSERVATION_EVENT, STATUS_MESSAGE_TYPE} from './pi-contract.ts';
import {renderEventMessage} from './message-renderer.ts';
import {canonical} from './protocol.ts';
import {SessionState} from './session-state.ts';
import type {WebhookReceiver} from './webhook.ts';
import {EventsWire} from './wire.ts';

const hash = (value: string) => createHash('sha256').update(value).digest('hex');
type SessionWatch = Pick<WatchOptions, 'name' | 'mode'> & {
  server: string; watch?: Watch; failure?: 'connection failed' | 'could not start' | 'failed';
};
type ActiveSession = {
  closed: boolean; clients: EventsClient[]; watches: Map<string, SessionWatch>;
  state?: SessionState; delivery?: ConversationDelivery; receiver?: WebhookReceiver; path?: string;
};

export default function mcpEvents(pi: ExtensionAPI) {
  pi.registerMessageRenderer(MESSAGE_TYPE, renderEventMessage);
  let active: ActiveSession | undefined;
  const report = (content: string) => {
    try { pi.sendMessage({customType: STATUS_MESSAGE_TYPE, display: true, content}, {triggerTurn: false}); }
    catch { /* Reporting must not interrupt ordinary Pi use. */ }
  };
  const stop = async () => {
    const session = active;
    active = undefined;
    if (!session) return;
    session.closed = true;
    session.delivery?.close();
    await Promise.allSettled(session.clients.map(client => client.close()));
    try { await session.receiver?.close(); }
    catch { /* Clients already attempted unsubscribe; shutdown must continue. */ }
  };
  const start = async (ctx: ExtensionContext) => {
    await stop();
    const configPath = resolve(process.env.PI_MCP_EVENTS_CONFIG ?? join(ctx.cwd, '.pi', 'mcp-events.json'));
    if (!existsSync(configPath)) return;
    const session: ActiveSession = {closed: false, clients: [], watches: new Map()};
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
      for (const [server, settings] of Object.entries(config.servers)) {
        if (session.closed) return;
        const client = new EventsClient(new EventsWire(() => createTransport(settings, dirname(configPath))), {...config.client, webhookReceiver: session.receiver});
        session.clients.push(client);
        try { await client.connect(); }
        catch {
          for (const entry of session.watches.values()) if (entry.server === server) entry.failure = 'connection failed';
          if (!session.closed) report(`MCP Events could not connect to ${server}. Check its configuration, credentials, and protocol support.`);
          await client.close().catch(() => {});
          continue;
        }
        for (const configured of config.watches.filter(watch => watch.server === server)) {
          if (session.closed) return;
          const key = identity(configured);
          const entry = session.watches.get(configured.id)!;
          try {
            const watch = client.watch({...configured, cursor: state.getCursor(key),
              ttlMs: state.mode !== 'persistent' && configured.ttlMs === null ? undefined : configured.ttlMs,
            }, observation => {
              if (session.closed) throw new Error('Pi session changed');
              state.accept({server, watch: configured.id, observation});
              delivery.schedule();
              try { pi.events.emit(OBSERVATION_EVENT, {server, watch: configured.id, observation}); }
              catch { /* Other extensions must not prevent conversation delivery. */ }
            }, cursor => {
              if (session.closed) throw new Error('Pi session changed');
              state.setCursor(key, cursor);
            });
            entry.watch = watch;
            void watch.closed.catch(() => {
              entry.failure = 'failed';
              if (!session.closed) report(`MCP Events watch ${configured.id} stopped. Check its configuration and server access.`);
            });
          } catch {
            entry.failure = 'could not start';
            report(`MCP Events could not start watch ${configured.id}. Check its event name, arguments, and delivery mode.`);
          }
        }
      }
    } catch {
      if (session.closed) return;
      await stop();
      report('MCP Events could not start. Check PI_MCP_EVENTS_CONFIG and the event configuration file.');
    }
  };
  pi.on('session_start', (_event, ctx) => start(ctx));
  pi.on('session_shutdown', stop);
  pi.registerCommand('mcp-events', {
    description: 'Show event subscription status or stop subscriptions: status, stop [watch-id]',
    handler: async (args, ctx) => {
      const [action = 'status', id] = args.trim().split(/\s+/);
      if (action === 'stop') {
        if (id) {
          const entry = active?.watches.get(id);
          if (!entry) { ctx.ui.notify(`MCP Events watch ${id} was not found. Use /mcp-events status to list watches.`, 'warning'); return; }
          await entry.watch?.stop();
        }
        else await stop();
        ctx.ui.notify('MCP Events subscriptions stopped.', 'info');
        return;
      }
      if (action && action !== 'status') { ctx.ui.notify('/mcp-events status\n/mcp-events stop [watch-id]', 'info'); return; }
      const session = active;
      const running = [...session?.watches.values() ?? []].filter(entry => entry.watch && entry.watch.status !== 'stopped').length;
      const watches = [...session?.watches ?? []].map(([id, entry]) =>
        `${id}: ${entry.failure ?? entry.watch?.status ?? 'starting'} — ${entry.server} / ${entry.name} (${entry.mode})`);
      report(session?.state
        ? [`MCP Events: ${session.state.mode}; ${running} running watch(es); ${session.state.size} pending observation(s).`,
          `Event state: ${session.path ?? 'memory only'}`, ...watches, session.state.warning?.message].filter(Boolean).join('\n')
        : 'MCP Events has no active session.');
    },
  });
}
