import {randomBytes} from 'node:crypto';
import {resolve} from 'node:path';
import {z} from 'zod';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import type {CheckpointOptions} from './inbox.ts';
import {encodeState, readState, writeState} from './state-file.ts';

export const webhookIdentitySchema = z.strictObject({
  routeId: z.string().regex(/^[a-f0-9]{48}$/),
  secret: z.string().regex(/^whsec_[A-Za-z0-9+/]+={0,2}$/).refine(value => {
    const encoded = value.slice(6);
    const bytes = Buffer.from(encoded, 'base64');
    return bytes.length >= 24 && bytes.length <= 64 && bytes.toString('base64') === encoded;
  }),
});
export type WebhookIdentity = z.infer<typeof webhookIdentitySchema>;
export function createWebhookIdentity(): WebhookIdentity {
  return {routeId: randomBytes(24).toString('hex'), secret: `whsec_${randomBytes(32).toString('base64')}`};
}
const optionsSchema = z.object({
  path: z.string().min(1).transform(path => resolve(path)).optional(),
  maxEntries: z.number().int().positive().default(512),
  maxBytes: z.number().int().positive().default(256 * 1024),
});
const stateSchema = z.strictObject({version: z.literal(1), identities: z.array(z.tuple([z.string().min(1), webhookIdentitySchema]))});

export class WebhookIdentities {
  private readonly options: z.output<typeof optionsSchema>;
  private values: Map<string, WebhookIdentity>;
  private writeError?: EventsClientError;
  constructor(path?: string, options: CheckpointOptions = {}) {
    const parsed = optionsSchema.safeParse({...options, path});
    if (!parsed.success) throw new EventsClientError(ErrorCode.InvalidOptions, 'Invalid webhook identity storage options');
    this.options = parsed.data;
    const state = readState(this.options.path, {version: 1, identities: []}, stateSchema, this.options.maxBytes);
    this.values = new Map(state.identities);
    if (this.values.size !== state.identities.length) throw new EventsClientError(ErrorCode.InvalidState, 'Duplicate stored webhook identity');
    this.check(this.values);
    encodeState({version: 1, identities: [...this.values]}, this.options.maxBytes);
  }
  get(key: string): WebhookIdentity | undefined {
    const identity = this.values.get(key);
    return identity && {...identity};
  }
  toMemory(): WebhookIdentities {
    const identities = new WebhookIdentities(undefined, this.options);
    identities.values = new Map(this.values);
    return identities;
  }
  private check(values: ReadonlyMap<string, WebhookIdentity>) {
    if (values.size > this.options.maxEntries) throw limitExceeded('maxEntries', this.options.maxEntries);
  }
  private save(values: Map<string, WebhookIdentity>) {
    this.check(values);
    const text = encodeState({version: 1, identities: [...values]}, this.options.maxBytes);
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
  ensure(key: string) {
    if (this.writeError) throw this.writeError;
    if (!key) throw new EventsClientError(ErrorCode.InvalidOptions, 'A webhook identity requires a key');
    if (this.values.has(key)) return;
    this.save(new Map([...this.values, [key, createWebhookIdentity()]]));
  }
  retain(keys: Iterable<string>) {
    if (this.writeError) throw this.writeError;
    const active = new Set(keys);
    const values = new Map([...this.values].filter(([key]) => active.has(key)));
    if (values.size !== this.values.size) this.save(values);
  }
}
