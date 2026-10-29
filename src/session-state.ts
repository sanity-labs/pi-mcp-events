import {WebhookIdentities} from './webhook-identities.ts';
import {EventsClientError, EventsClientErrorCode as ErrorCode} from './errors.ts';
import {Checkpoints, Inbox, type CheckpointOptions, type InboxItem, type InboxOptions} from './inbox.ts';

export type PersistenceWarning = Readonly<{
  code: 'PERSISTENCE_UNAVAILABLE';
  operation: 'read' | 'write';
  message: string;
  error: EventsClientError;
}>;
export type SessionStateOptions = {
  inbox?: InboxOptions;
  checkpoints?: CheckpointOptions;
  onWarning?: (warning: PersistenceWarning) => void | Promise<void>;
};

/** Best-effort persistence with one active writer. Omit the path to use only memory. */
export class SessionState {
  private inbox: Inbox;
  private checkpoints: Checkpoints;
  private identities: WebhookIdentities;
  private persistence: 'memory' | 'persistent' | 'degraded';
  private persistenceWarning?: PersistenceWarning;
  private readonly onWarning?: SessionStateOptions['onWarning'];

  constructor(path?: string, options: SessionStateOptions = {}) {
    this.inbox = new Inbox(undefined, options.inbox);
    this.checkpoints = new Checkpoints(undefined, options.checkpoints);
    this.identities = new WebhookIdentities(undefined, options.checkpoints);
    this.onWarning = options.onWarning;
    this.persistence = path === undefined ? 'memory' : 'persistent';
    if (path === undefined) return;
    let readError: EventsClientError | undefined;
    const restore = (operation: () => void) => {
      try { operation(); }
      catch (error) {
        if (!(error instanceof EventsClientError) || ![
          ErrorCode.InvalidState, ErrorCode.StateReadFailed, ErrorCode.LimitExceeded,
        ].some(code => code === error.code)) throw error;
        readError ??= error;
      }
    };
    restore(() => {
      this.inbox = new Inbox(path, options.inbox);
      this.checkpoints = new Checkpoints(`${path}.cursors.json`, options.checkpoints);
    });
    restore(() => { this.identities = new WebhookIdentities(`${path}.webhooks.json`, options.checkpoints); });
    if (readError) {
      this.degrade(readError, 'read');
      this.notify();
    }
  }

  get mode() { return this.persistence; }
  get warning() { return this.persistenceWarning; }
  get size() { return this.inbox.size; }
  snapshot(): readonly InboxItem[] { return this.inbox.snapshot(); }
  accept(item: InboxItem) { this.mutate(() => this.inbox.accept(item)); }
  acknowledge(count: number) { this.mutate(() => this.inbox.acknowledge(count)); }
  getCursor(key: string) { return this.checkpoints.get(key); }
  setCursor(key: string, cursor: string | null) { this.mutate(() => this.checkpoints.set(key, cursor)); }
  retainCursors(keys: Iterable<string>) {
    const active = [...keys];
    this.mutate(() => this.checkpoints.retain(active));
  }

  webhookIdentity(key: string) {
    this.mutate(() => this.identities.ensure(key));
    return this.identities.get(key)!;
  }
  retainWebhookIdentities(keys: Iterable<string>) {
    const active = [...keys];
    this.mutate(() => this.identities.retain(active));
  }

  private mutate(operation: () => void) {
    try { operation(); }
    catch (error) {
      if (!(error instanceof EventsClientError) || error.code !== ErrorCode.StateWriteFailed) throw error;
      this.degrade(error, 'write');
      // Replacement already applied the mutation, even when directory sync failed.
      try { if (error.details?.replaced !== true) operation(); }
      finally { this.notify(); }
    }
  }

  private degrade(error: EventsClientError, operation: PersistenceWarning['operation']) {
    this.inbox = this.inbox.toMemory();
    this.checkpoints = this.checkpoints.toMemory();
    this.identities = this.identities.toMemory();
    this.persistence = 'degraded';
    const reason = operation === 'read'
      ? 'MCP Events could not restore saved state. Some pending events or resume progress may be unavailable.'
      : 'MCP Events could not save state.';
    this.persistenceWarning = Object.freeze({
      code: 'PERSISTENCE_UNAVAILABLE', operation, error,
      message: `${reason} Event delivery will continue in memory for this session. New progress will not be saved; restarting may lose pending events or repeat earlier ones. Check the event state files and storage access before the next session.`,
    });
  }

  private notify() {
    try { void Promise.resolve(this.onWarning?.(this.persistenceWarning!)).catch(() => {}); }
    catch { /* The warning remains available when the conversation cannot accept it. */ }
  }
}
