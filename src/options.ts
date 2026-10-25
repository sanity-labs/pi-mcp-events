import {z} from 'zod';
import {deliveryMode} from './protocol.ts';

export const watchOptionsSchema = z.object({
  name: z.string().min(1), arguments: z.record(z.string(), z.unknown()),
  mode: deliveryMode, ttlMs: z.number().int().positive().nullable().optional(), cursor: z.string().nullable().optional(),
  maxAgeMs: z.number().int().nonnegative().optional(), maxEvents: z.number().int().positive().default(50),
});
export const clientSettingsSchema = z.object({
  webhookRefreshMs: z.number().int().positive().default(5 * 60_000),
  webhookUnsubscribeTimeoutMs: z.number().int().positive().max(2 ** 31 - 1).default(5000),
  catalogAckTimeoutMs: z.number().int().positive().default(30_000),
  heartbeatMs: z.number().int().positive().max(Number.MAX_SAFE_INTEGER / 2).default(30_000),
  pollFloorMs: z.number().int().positive().default(1000),
  retryMs: z.number().int().positive().max(30_000).default(1000),
  maxRetries: z.number().int().nonnegative().default(10),
  maxSubscriptions: z.number().int().positive().default(32),
  maxCatalogEntries: z.number().int().positive().default(1000),
  maxCatalogPages: z.number().int().positive().default(100),
  maxPollBytes: z.number().int().positive().default(256 * 1024),
  maxTrackedEventIds: z.number().int().positive().default(4096),
});
