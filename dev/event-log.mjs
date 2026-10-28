import {randomUUID} from 'node:crypto';

export function createEventLog(limit = 1000) {
  const instance = randomUUID();
  const entries = [];
  let position = 0;
  const cursorAt = (key, at) => Buffer.from(JSON.stringify([instance, key, at])).toString('base64url');
  return {
    cursor: key => cursorAt(key, position),
    append(name, data) {
      const event = {eventId: randomUUID(), name, timestamp: new Date().toISOString(), data: structuredClone(data)};
      entries.push({position: ++position, event});
      if (entries.length > limit) entries.shift();
      return event;
    },
    read({cursor, key, matches, maxEvents = limit, maxAgeMs}) {
      let after = position;
      let truncated = false;
      if (cursor !== null && cursor !== undefined) {
        let decoded;
        try { decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString()); }
        catch { throw new Error('Invalid event cursor'); }
        if (!Array.isArray(decoded) || decoded.length !== 3 || typeof decoded[0] !== 'string'
          || decoded[1] !== key || !Number.isSafeInteger(decoded[2]) || decoded[2] < 0) throw new Error('Invalid event cursor');
        after = decoded[2];
        const floor = (entries[0]?.position ?? position + 1) - 1;
        if (decoded[0] !== instance || after < floor) { after = floor; truncated = true; }
        else if (after > position) throw new Error('Invalid event cursor');
        if (maxAgeMs !== undefined) {
          const expired = entries.filter(entry => Date.parse(entry.event.timestamp) < Date.now() - maxAgeMs).at(-1)?.position;
          if (expired !== undefined && expired > after) { after = expired; truncated = true; }
        }
      }
      const startCursor = cursorAt(key, after);
      const pending = entries.filter(entry => entry.position > after && matches(entry.event));
      const selected = pending.slice(0, maxEvents);
      const hasMore = selected.length < pending.length;
      return {
        events: selected.map(entry => ({...structuredClone(entry.event), cursor: cursorAt(key, entry.position)})),
        startCursor, cursor: cursorAt(key, hasMore ? selected.at(-1).position : position), truncated, hasMore,
      };
    },
  };
}
