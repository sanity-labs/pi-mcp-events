import type {Observation} from './client.ts';

export const OBSERVATION_EVENT = 'mcp-events:observation';
export const MESSAGE_TYPE = 'mcp-events';
export type PiEventObservation = {server: string; watch: string; observation: Observation};
export type PiEventBatch = {events: readonly PiEventObservation[]; batchId: string};
