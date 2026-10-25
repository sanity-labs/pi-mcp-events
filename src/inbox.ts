import {resolve} from 'node:path';
import {z} from 'zod';
import type {Observation} from './client.ts';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import {EventProtocolError, occurrence, remoteError, subscribeResult} from './protocol.ts';
import {decodeState, encodeState, readState, writeState} from './state-file.ts';

export type InboxItem = {server: string; watch: string; observation: Observation};
export type InboxOptions = {maxItems?: number; maxBytes?: number};
export type CheckpointOptions = {maxEntries?: number; maxBytes?: number};
const fileOptions = z.object({
  path: z.string().min(1).transform(path => resolve(path)).optional(),
  maxBytes: z.number().int().positive().default(256 * 1024),
});
const inboxOptions = fileOptions.extend({maxItems: z.number().int().positive().default(100)});
const checkpointOptions = fileOptions.extend({maxEntries: z.number().int().positive().default(512)});
const observation: z.ZodType<Observation> = z.discriminatedUnion('type', [
  z.object({type: z.literal('event'), event: occurrence.extend({data: z.record(z.string(), z.json()), _meta: z.record(z.string(), z.json()).optional()})}),
  z.object({type: z.literal('gap'), cursor: z.string().nullable()}),
  z.discriminatedUnion('status', [
    z.object({type: z.literal('status'), status: z.enum(['retrying', 'stopped'])}),
    z.object({type: z.literal('status'), status: z.literal('source-error'), error: remoteError.extend({data: z.record(z.string(), z.json()).optional()}).transform(error => new EventProtocolError(error))}),
    z.object({type: z.literal('status'), status: z.literal('webhook-delivery'), deliveryStatus: subscribeResult.shape.deliveryStatus.unwrap()}),
  ]),
]);
const inboxItem = z.object({server: z.string().min(1), watch: z.string().min(1), observation});
const inboxState = z.strictObject({version: z.literal(1), items: z.array(inboxItem)});
const checkpointState = z.strictObject({version: z.literal(1), cursors: z.array(z.tuple([z.string().min(1), z.string()]))});
function settings<S extends z.ZodType>(schema: S, value: unknown): z.output<S> {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid MCP Events storage options');
  return parsed.data;
}

/** Use one active writer per file. Omit the path for an in-memory inbox. */
export class Inbox {
  private readonly options: z.output<typeof inboxOptions>;
  private items: InboxItem[];
  private writeError?: EventsClientError;
  constructor(path?: string, options: InboxOptions = {}) {
    this.options = settings(inboxOptions, {...options, path});
    this.items = readState(this.options.path, {version: 1, items: []}, inboxState, this.options.maxBytes).items;
    this.check(this.items);
    encodeState({version: 1, items: this.items}, this.options.maxBytes);
  }
  get size() { return this.items.length; }
  private check(items: readonly InboxItem[]) {
    if (items.length > this.options.maxItems) throw limitExceeded('maxItems', this.options.maxItems);
  }
  private save(items: InboxItem[]) {
    this.check(items);
    const text = encodeState({version: 1, items}, this.options.maxBytes);
    const detached = decodeState(text, inboxState).items;
    try { writeState(this.options.path, text); }
    catch (error) {
      if (error instanceof EventsClientError && error.details?.replaced === true) {
        this.items = detached;
        this.writeError = error;
      }
      throw error;
    }
    this.items = detached;
  }
  accept(item: InboxItem) {
    if (this.writeError) throw this.writeError;
    let parsed: InboxItem;
    try { parsed = inboxItem.parse(item); }
    catch { throw new EventsClientError(ErrorCode.InvalidState, 'Invalid MCP Events inbox observation'); }
    this.save([...this.items, parsed]);
  }
  snapshot(): readonly InboxItem[] {
    return decodeState(encodeState({version: 1, items: this.items}, this.options.maxBytes), inboxState).items;
  }
  acknowledge(count: number) {
    if (this.writeError) throw this.writeError;
    if (!Number.isInteger(count) || count < 0 || count > this.items.length) {
      throw new EventsClientError(ErrorCode.InvalidOptions, 'Inbox acknowledgement must be a valid prefix count');
    }
    if (count > 0) this.save(this.items.slice(count));
  }
}

/** Use one active writer per file. Omit the path for in-memory checkpoints. */
export class Checkpoints {
  private readonly options: z.output<typeof checkpointOptions>;
  private values: Map<string, string>;
  private writeError?: EventsClientError;
  constructor(path?: string, options: CheckpointOptions = {}) {
    this.options = settings(checkpointOptions, {...options, path});
    const state = readState(this.options.path, {version: 1, cursors: []}, checkpointState, this.options.maxBytes);
    this.values = new Map(state.cursors);
    if (this.values.size !== state.cursors.length) throw new EventsClientError(ErrorCode.InvalidState, 'Duplicate stored event checkpoint');
    this.check(this.values);
    encodeState({version: 1, cursors: [...this.values]}, this.options.maxBytes);
  }
  get(key: string): string | null { return this.values.get(key) ?? null; }
  private check(values: ReadonlyMap<string, string>) {
    if (values.size > this.options.maxEntries) throw limitExceeded('maxEntries', this.options.maxEntries);
  }
  private save(values: Map<string, string>) {
    this.check(values);
    const text = encodeState({version: 1, cursors: [...values]}, this.options.maxBytes);
    try { writeState(this.options.path, text); }
    catch (error) {
      if (error instanceof EventsClientError && error.details?.replaced === true) {
        this.values = values;
        this.writeError = error;
      }
      throw error;
    }
    this.values = values;
  }
  set(key: string, cursor: string | null) {
    if (this.writeError) throw this.writeError;
    if (typeof key !== 'string' || !key || (typeof cursor !== 'string' && cursor !== null)) {
      throw new EventsClientError(ErrorCode.InvalidOptions, 'A checkpoint requires a key and a string or null cursor');
    }
    if (this.get(key) === cursor) return;
    const values = new Map(this.values);
    if (cursor === null) values.delete(key);
    else values.set(key, cursor);
    this.save(values);
  }
  retain(keys: Iterable<string>) {
    if (this.writeError) throw this.writeError;
    const active = new Set(keys);
    const values = new Map([...this.values].filter(([key]) => active.has(key)));
    if (values.size !== this.values.size) this.save(values);
  }
}
