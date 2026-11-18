import {z} from 'zod';
import {deliveryMode} from './protocol.ts';
import {encodeState, readState, writeState} from './state-file.ts';
import {EventsClientError, EventsClientErrorCode, limitExceeded} from './errors.ts';

export const watchDefinition = z.strictObject({
  id: z.string().min(1), server: z.string().min(1), name: z.string().min(1),
  arguments: z.record(z.string(), z.json()), mode: deliveryMode,
  replaces: z.string().min(1).optional(),
  configuredId: z.string().min(1).optional(),
  maxEvents: z.number().int().positive().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(),
  ttlMs: z.number().int().positive().nullable().optional(),
  expiresAt: z.number().int().nonnegative().max(8.64e15).optional(),
  phase: z.enum(['active', 'catching-up', 'stopped', 'expired']),
  warning: z.string().optional(),
}).refine(value => value.replaces !== value.id, 'A watch cannot replace itself');
export type WatchDefinition = z.infer<typeof watchDefinition>;
const schema = z.strictObject({version: z.literal(1), watches: z.array(watchDefinition).max(512), overrides: z.record(z.string(), z.string()).default({})});
const maxBytes = 256 * 1024;

export class WatchDefinitions {
  private values: Map<string, WatchDefinition>;
  private overrides: Record<string, string>;
  private writeError?: EventsClientError;
  constructor(private path?: string) {
    const state = readState(path, {version: 1, watches: [], overrides: {}}, schema, maxBytes);
    this.overrides = state.overrides;
    this.values = new Map(state.watches.map(value => [value.id, value]));
    if (Object.entries(this.overrides).some(([id, target]) => this.values.get(target)?.configuredId !== id)) {
      throw new EventsClientError(EventsClientErrorCode.InvalidState, 'Invalid configured watch override');
    }
    if (this.values.size !== state.watches.length) throw new EventsClientError(EventsClientErrorCode.InvalidState, 'Duplicate saved watch ID');
  }
  configuredOverrides() { return {...this.overrides}; }
  list() { return structuredClone([...this.values.values()]); }
  toMemory() {
    const memory = new WatchDefinitions();
    memory.values = new Map(this.values);
    memory.overrides = {...this.overrides};
    return memory;
  }
  remove(id: string) {
    const values = new Map(this.values);
    values.delete(id);
    const overrides = Object.fromEntries(Object.entries(this.overrides).filter(([, value]) => value !== id));
    this.commit(values, overrides);
  }
  set(input: WatchDefinition, configuredId?: string, maxEntries = 512) {
    if (this.writeError) throw this.writeError;
    const limit = Math.max(0, Math.min(512, maxEntries));
    const value = watchDefinition.parse(input);
    const values = new Map(this.values).set(value.id, value);
    const overrides = configuredId === undefined ? {...this.overrides} : {...this.overrides, [configuredId]: value.id};
    const protectedIds = new Set([value.id, ...Object.values(overrides), ...[...values.values()].flatMap(watch => watch.replaces ? [watch.replaces] : [])]);
    const removable = [...values.values()].filter(watch => ['stopped', 'expired'].includes(watch.phase) && !protectedIds.has(watch.id));
    while (true) {
      try {
        if (values.size > limit) throw limitExceeded('maxEntries', limit);
        encodeState({version: 1, watches: [...values.values()], overrides}, maxBytes);
        break;
      } catch (error) {
        if (!(error instanceof EventsClientError) || error.code !== EventsClientErrorCode.LimitExceeded || !removable.length) throw error;
        values.delete(removable.shift()!.id);
      }
    }
    this.commit(values, overrides);
  }
  private commit(values: Map<string, WatchDefinition>, overrides: Record<string, string>) {
    if (this.writeError) throw this.writeError;
    const text = encodeState({version: 1, watches: [...values.values()], overrides}, maxBytes);
    try { writeState(this.path, text); }
    catch (error) {
      if (error instanceof EventsClientError && error.details?.replaced === true) {
        this.values = values;
        this.overrides = overrides;
        this.writeError = error;
      }
      throw error;
    }
    this.values = values;
    this.overrides = overrides;
  }
}
