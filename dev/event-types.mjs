import {ProtocolError} from '@modelcontextprotocol/server';
import {z} from 'zod';
import {argumentHeaderDeclarations, argumentHeaders, decodeHeaderValue} from '../src/argument-headers.ts';
import {canonical, EventProtocolErrorCode as code} from '../src/protocol.ts';

const allModes = ['poll', 'push', 'webhook'];
const emptyArguments = z.strictObject({});

export function createEventTypes({log, webhooksEnabled, authorize, onEnding}) {
  const types = new Map();
  const denied = new Map();
  let revision = 0;

  function compile(name, definition) {
    const args = definition.inputSchema ?? emptyArguments;
    const inputSchema = z.toJSONSchema(args);
    const modes = definition.delivery ?? allModes;
    const delivery = modes.filter(mode => (mode !== 'webhook' || webhooksEnabled)
      && (mode !== 'poll' || definition.replay !== false || definition.check));
    if (!modes.length || modes.some(mode => !allModes.includes(mode))) throw new Error('Expected supported Events delivery modes');
    return {
      definition, args, revision: ++revision, headers: argumentHeaderDeclarations(inputSchema),
      descriptor: {
        name, description: definition.description, delivery, inputSchema, payloadSchema: z.toJSONSchema(definition.payloadSchema),
      },
    };
  }

  const schemaChanged = feature => new ProtocolError(code.Unsupported, 'Event schema changed', {feature, reason: 'schema_changed'});

  function get(name) {
    const type = types.get(name);
    if (!type) throw new ProtocolError(code.NotFound, 'Unknown event type', {kind: 'event'});
    return type;
  }

  function assertAccess(input) {
    const type = get(input.name);
    if (input.eventRevision !== undefined && input.eventRevision !== type.revision) {
      throw schemaChanged(type.breakingFeature ?? 'payloadSchema');
    }
    const error = denied.get(input.name)?.get(input.principal) ?? denied.get(input.name)?.get(null);
    if (error) throw error;
    if (!authorize(input)) throw new ProtocolError(code.Forbidden, 'Event access denied');
  }

  function validate(input, mode, principal) {
    const type = get(input.name);
    if (!type.descriptor.delivery.includes(mode)) {
      throw new ProtocolError(code.Unsupported, 'Event type does not offer this delivery mode', {feature: 'deliveryMode', value: mode});
    }
    const args = type.args.safeParse(input.arguments === undefined ? {} : input.arguments);
    if (!args.success) throw new ProtocolError(code.InvalidParams, 'Arguments do not match the event input schema');
    const request = {...input, arguments: args.data, mode, principal, eventRevision: type.revision};
    assertAccess(request);
    return request;
  }

  const key = input => canonical([input.name, input.arguments, get(input.name).revision]);
  const matches = (input, event) => {
    const type = types.get(input.name);
    return input.name === event.name && !!type && (type.definition.match?.(event, input.arguments) ?? true);
  };
  const head = input => {
    const definition = get(input.name).definition;
    if (definition.replay === false || definition.check) return null;
    return log.cursor(key(input));
  };

  function occurrence(name, event, cursor) {
    const definition = get(name).definition;
    return {
      name, eventId: event.eventId, timestamp: event.timestamp ?? new Date().toISOString(),
      data: definition.payloadSchema.parse(event.data), cursor,
    };
  }

  async function read(input) {
    const type = get(input.name);
    const {definition} = type;
    if (definition.check) {
      const startCursor = input.cursor ?? null;
      const page = await definition.check(input, startCursor);
      const cursor = definition.replay === false ? null : page.cursor ?? null;
      const batch = input.startNow ? [] : page.events;
      const events = batch.map((event, index) => {
        const position = event.cursor ?? (index === batch.length - 1 && !page.truncated ? cursor : startCursor);
        return occurrence(input.name, event, cursor === null ? null : position);
      });
      let replayCursor = startCursor;
      if (page.truncated || startCursor === null) replayCursor = page.startCursor ?? (events.length ? startCursor : cursor);
      return {events, startCursor: replayCursor, cursor, hasMore: !!page.hasMore,
        truncated: cursor !== null && !!page.truncated};
    }
    if (definition.replay === false) return {events: [], startCursor: null, cursor: null, hasMore: false, truncated: false};
    try { return log.read({...input, key: key(input), matches: event => matches(input, event)}); }
    catch (error) { throw new ProtocolError(code.InvalidParams, error.message); }
  }

  return {
    list: () => [...types.values()].map(type => type.descriptor).filter(type => type.delivery.length),
    get, validate, assertAccess, matches, head, read,
    find: name => types.get(name),
    headerMismatch(name, args, headers) {
      const type = types.get(name);
      if (!type || !type.args.safeParse(args === undefined ? {} : args).success) return false;
      const expected = argumentHeaders(type.headers, args);
      return type.headers.some(({name: suffix, type: kind}) => {
        const header = `Mcp-Param-${suffix}`;
        const actual = headers.get(header);
        const wanted = expected[header];
        if (wanted === undefined) return actual !== null;
        const decoded = decodeHeaderValue(actual);
        const value = decodeHeaderValue(wanted);
        if (kind === 'integer' && decoded && /^-?\d+(?:\.0+)?$/.test(decoded)) return Number(decoded) !== Number(value);
        return decoded !== value;
      });
    },
    register(name, definition) {
      const type = compile(name, definition);
      if (types.has(name)) throw new Error(`Event type already registered: ${name}`);
      types.set(name, type);
    },
    async update(name, changes, {breaking = false} = {}) {
      const type = get(name);
      const updated = compile(name, {...type.definition, ...changes});
      if (breaking) updated.breakingFeature = changes.payloadSchema ? 'payloadSchema' : 'inputSchema';
      else updated.revision = type.revision;
      types.set(name, updated);
      if (breaking) {
        log.discard(name);
        await onEnding(name, schemaChanged(updated.breakingFeature));
      }
    },
    async remove(name) {
      get(name);
      types.delete(name);
      denied.delete(name);
      log.discard(name);
      await onEnding(name, new ProtocolError(code.NotFound, 'Event type was removed', {kind: 'event'}));
    },
    async terminate(name, {principal} = {}) {
      get(name);
      const error = new ProtocolError(code.Forbidden, 'Event access revoked');
      if (!denied.has(name)) denied.set(name, new Map());
      denied.get(name).set(principal ?? null, error);
      await onEnding(name, error, principal);
    },
  };
}
