import {z} from 'zod';
import {deliveryMode} from './protocol.ts';
import {encodeState, readState, writeState} from './state-file.ts';
import {EventsClientError, EventsClientErrorCode, limitExceeded} from './errors.ts';

export const watchDefinition = z.strictObject({
  id: z.string().min(1).max(128), server: z.string().min(1), name: z.string().min(1),
  arguments: z.record(z.string(), z.json()), mode: deliveryMode,
  replaces: z.string().min(1).optional(),
  expiresAt: z.number().int().nonnegative().max(8.64e15).optional(),
  phase: z.enum(['active', 'catching-up', 'stopped', 'expired']),
  warning: z.string().optional(),
}).refine(value => value.replaces !== value.id, 'A watch cannot replace itself');
export type WatchDefinition = z.infer<typeof watchDefinition>;
const schema = z.strictObject({version: z.literal(1), watches: z.array(watchDefinition).max(512)});
const maxBytes = 256 * 1024;

export class WatchDefinitions {
  private values: Map<string, WatchDefinition>;
  private writeError?: EventsClientError;
  constructor(private path?: string) {
    const state = readState(path, {version: 1, watches: []}, schema, maxBytes);
    this.values = new Map(state.watches.map(value => [value.id, value]));
    if (this.values.size !== state.watches.length) throw new EventsClientError(EventsClientErrorCode.InvalidState, 'Duplicate saved watch ID');
  }
  list() { return structuredClone([...this.values.values()]); }
  toMemory() {
    const memory = new WatchDefinitions();
    memory.values = new Map(this.values);
    return memory;
  }
  set(input: WatchDefinition) {
    if (this.writeError) throw this.writeError;
    const value = watchDefinition.parse(input);
    const values = new Map(this.values).set(value.id, value);
    if (values.size > 512) throw limitExceeded('maxEntries', 512);
    const text = encodeState({version: 1, watches: [...values.values()]}, maxBytes);
    try { writeState(this.path, text); }
    catch (error) {
      if (error instanceof EventsClientError && error.details?.replaced === true) {
        this.values = values;
        this.writeError = error;
      }
      throw error;
    }
    this.values = values;
  }
}
