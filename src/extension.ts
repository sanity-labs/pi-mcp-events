import {createHash} from 'node:crypto';
import {existsSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {getAgentDir, type ExtensionAPI, type ExtensionContext} from '@earendil-works/pi-coding-agent';
import {EventsClient, type Watch} from './client.ts';
import {createReceiver, createTransport, readConfig} from './config.ts';
import {ConversationDelivery} from './conversation.ts';
import {OBSERVATION_EVENT, STATUS_MESSAGE_TYPE} from './pi-contract.ts';
import {canonical} from './protocol.ts';
import {SessionState} from './session-state.ts';
import type {WebhookReceiver} from './webhook.ts';
import {EventsWire} from './wire.ts';

const hash = (value: string) => createHash('sha256').update(value).digest('hex');
type ActiveSession = {
  closed: boolean; clients: EventsClient[]; watches: Map<string, Watch>;
  state?: SessionState; delivery?: ConversationDelivery; receiver?: WebhookReceiver; path?: string;
};

export default function mcpEvents(pi: ExtensionAPI) {
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
          if (!session.closed) report(`MCP Events could not connect to ${server}. Check its configuration, credentials, and protocol support.`);
          await client.close().catch(() => {});
          continue;
        }
        for (const configured of config.watches.filter(watch => watch.server === server)) {
          if (session.closed) return;
          const key = identity(configured);
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
            session.watches.set(configured.id, watch);
            void watch.closed.catch(() => {
              if (!session.closed) report(`MCP Events watch ${configured.id} stopped. Check its configuration and server access.`);
            }).finally(() => session.watches.delete(configured.id));
          } catch {
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
        if (id) await active?.watches.get(id)?.stop();
        else await stop();
        ctx.ui.notify('MCP Events subscriptions stopped.', 'info');
        return;
      }
      if (action && action !== 'status') { ctx.ui.notify('/mcp-events status\n/mcp-events stop [watch-id]', 'info'); return; }
      const session = active;
      report(session?.state
        ? `MCP Events: ${session.state.mode}; ${session.watches.size} active watch(es); ${session.state.size} pending observation(s).\nEvent state: ${session.path ?? 'memory only'}${session.state.warning ? `\n${session.state.warning.message}` : ''}`
        : 'MCP Events has no active session.');
    },
  });
}
