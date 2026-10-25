import {createReadStream} from 'node:fs';
import {open} from 'node:fs/promises';
import {createInterface} from 'node:readline';
import type {ExtensionContext} from '@earendil-works/pi-coding-agent';

export async function hasSessionBatch(manager: ExtensionContext['sessionManager'], batchId: string): Promise<boolean> {
  const path = manager.getSessionFile();
  if (path) return hasDurableBatch(path, batchId);
  return manager.getEntries().some(entry => {
    if (entry.type !== 'custom_message' || entry.customType !== 'mcp-events') return false;
    const details = entry.details;
    return typeof details === 'object' && details !== null && 'batchId' in details && details.batchId === batchId;
  });
}

/** Pi emits message_end before it writes the session. Confirm the disk record first. */
export async function hasDurableBatch(path: string | undefined, batchId: string): Promise<boolean> {
  if (!path) return false;
  const stream = createReadStream(path, {encoding: 'utf8'});
  const lines = createInterface({input: stream, crlfDelay: Infinity});
  try {
    for await (const line of lines) {
      if (!line.includes(batchId)) continue;
      const entry = JSON.parse(line) as {type?: string; customType?: string; details?: {batchId?: string}};
      if (entry.type !== 'custom_message' || entry.customType !== 'mcp-events' || entry.details?.batchId !== batchId) continue;
      const file = await open(path, 'r');
      try { await file.sync(); } finally { await file.close(); }
      return true;
    }
  } catch { return false; }
  finally { lines.close(); stream.destroy(); }
  return false;
}
