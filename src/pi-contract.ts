import type {InboxItem} from './inbox.ts';

export const OBSERVATION_EVENT = 'mcp-events:observation';
export const MESSAGE_TYPE = 'mcp-events';
export const STATUS_MESSAGE_TYPE = 'mcp-events-status';
export type PiEventBatch = {events: readonly InboxItem[]; batchId: string};
