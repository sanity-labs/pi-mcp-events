import {describe, expect, it} from 'vitest';
import {canonical, descriptor, occurrence, pollResult, subscribeResult} from '../src/protocol.ts';
import {EventContract} from '../src/validation.ts';

const contract = {
  name: 'example.changed', delivery: ['poll' as const],
  inputSchema: {type: 'object', required: ['channel'], properties: {channel: {type: 'string'}}},
  payloadSchema: {type: 'object', required: ['value'], properties: {value: {type: 'number'}}},
};

describe('received timestamps', () => {
  const event = (timestamp: string) => ({eventId: 'e', name: contract.name, timestamp, data: {value: 1}});
  const grant = (timestamp: string) => ({id: 'sub', refreshBefore: timestamp, truncated: false,
    deliveryStatus: {active: true, lastDeliveryAt: timestamp, failedSince: timestamp}});

  it.each(['Z', '+00:00', '-04:30', '+0000', '-0000', '+0530', '-0430'])('preserves timestamps with offset %s', offset => {
    const timestamp = `2024-02-29T12:34:56.123${offset}`;
    expect(occurrence.parse(event(timestamp)).timestamp).toBe(timestamp);
    expect(subscribeResult.parse(grant(timestamp))).toMatchObject(grant(timestamp));
  });

  it.each([
    '2026-02-29T12:34:56+0000', '2026-04-31T12:34:56+0000',
    '2026-10-29T24:00:00+0000', '2026-10-29T12:60:00+0000', '2026-10-29T12:34:60+0000',
    '2026-10-29T12:34:56+2400', '2026-10-29T12:34:56-0060',
    '2026-10-29T12:34:56', '2026-10-29T12:34:56+00',
    '2026-10-29T12:34:56+0000junk', '2026-10-29T12:34:56+0000\n', '2026-10-29T12:34:56+0000 ',
  ])('rejects invalid or ambiguous timestamp %j in every timestamp field', timestamp => {
    expect(occurrence.safeParse(event(timestamp)).success).toBe(false);
    const valid = '2026-10-29T12:34:56Z';
    expect(subscribeResult.safeParse({...grant(valid), refreshBefore: timestamp}).success).toBe(false);
    for (const field of ['lastDeliveryAt', 'failedSince']) {
      expect(subscribeResult.safeParse({...grant(valid), deliveryStatus: {active: true, [field]: timestamp}}).success).toBe(false);
    }
  });
});

describe('pinned event contracts', () => {
  it.each([true, false])('applies boolean input and payload schemas: %s', accepts => {
    const advertised = descriptor.parse({...contract, inputSchema: accepts, payloadSchema: accepts});
    const schema = new EventContract(advertised);
    const event = occurrence.parse({eventId: 'e', name: contract.name, timestamp: new Date().toISOString(), data: {anything: [1, 'two']}});
    if (accepts) {
      expect(() => schema.arguments({anything: true})).not.toThrow();
      expect(() => schema.event(event)).not.toThrow();
    } else {
      expect(() => schema.arguments({})).toThrow('inputSchema');
      expect(() => schema.event(event)).toThrow('payloadSchema');
    }
  });
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
