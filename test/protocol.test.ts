import {describe, expect, it} from 'vitest';
import {canonical, descriptor, occurrence, pollResult} from '../src/protocol.ts';
import {EventContract} from '../src/validation.ts';

const contract = {
  name: 'example.changed', delivery: ['poll' as const],
  inputSchema: {type: 'object', required: ['channel'], properties: {channel: {type: 'string'}}},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'number'}}},
};

describe('pinned event contracts', () => {
  it('normalizes absent cursors and preserves array order in subscription identity', () => {
    expect(pollResult.parse({events: [], truncated: false, hasMore: false, nextPollMs: 1000}).cursor).toBeNull();
    expect(canonical({b: [2, 1], a: {y: 2, x: 1}})).toBe(canonical({a: {x: 1, y: 2}, b: [2, 1]}));
    expect(canonical([1, 2])).not.toBe(canonical([2, 1]));
  });

  it('validates advertised delivery modes, arguments, names, and payloads', () => {
    expect(() => descriptor.parse({...contract, delivery: ['unknown']})).toThrow();
    const schema = new EventContract(contract);
    expect(() => schema.arguments({channel: 'test'})).not.toThrow();
    expect(() => schema.arguments({channel: 42})).toThrow('inputSchema');
    const event = occurrence.parse({eventId: 'e1', name: contract.name, timestamp: new Date().toISOString(), data: {value: 1}});
    schema.event(event);
    expect(() => schema.event({...event, name: 'another.event'})).toThrow('name');
    expect(() => schema.event({...event, data: {value: 'invalid'}})).toThrow('payloadSchema');
  });

  it.each(['http://json-schema.org/draft-07/schema#', 'https://json-schema.org/draft/2019-09/schema', 'https://json-schema.org/draft/2020-12/schema'])('supports schema dialect %s', dialect => {
    const schema = new EventContract({...contract, payloadSchema: {...contract.payloadSchema, $schema: dialect}});
    expect(() => schema.event({eventId: 'e', name: contract.name, timestamp: new Date().toISOString(), data: {value: 'invalid'}, cursor: null})).toThrow('payloadSchema');
  });
});
