import {isUtf8} from 'node:buffer';
import {randomUUID} from 'node:crypto';
import {closeSync, fsyncSync, mkdirSync, openSync, readSync, renameSync, unlinkSync, writeFileSync} from 'node:fs';
import {dirname} from 'node:path';
import type {z} from 'zod';
import {EventsClientError, EventsClientErrorCode as ErrorCode, limitExceeded} from './errors.ts';
import {EventProtocolError} from './protocol.ts';

export function encodeState(value: unknown, maxBytes: number): string {
  let text: string;
  try {
    text = JSON.stringify(value, (_key, item) => item instanceof EventProtocolError
      ? {code: item.code, message: item.message, data: item.data} : item);
  } catch { throw new EventsClientError(ErrorCode.InvalidState, 'MCP Events state must contain JSON values'); }
  if (typeof text !== 'string') throw new EventsClientError(ErrorCode.InvalidState, 'MCP Events state must contain JSON values');
  if (Buffer.byteLength(text) > maxBytes) throw limitExceeded('maxBytes', maxBytes);
  return text;
}
export function decodeState<T>(text: string, schema: z.ZodType<T>): T {
  try { return schema.parse(JSON.parse(text)); }
  catch { throw new EventsClientError(ErrorCode.InvalidState, 'Invalid or unsupported MCP Events state'); }
}
export function readState<T>(path: string | undefined, empty: T, schema: z.ZodType<T>, maxBytes: number): T {
  if (path === undefined) return empty;
  let fd: number;
  try { fd = openSync(path, 'r'); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return empty;
    throw new EventsClientError(ErrorCode.StateReadFailed, 'Could not read MCP Events state', {cause: error, details: {path}});
  }
  try {
    const buffer = Buffer.alloc(Math.min(8192, maxBytes + 1));
    const chunks: Buffer[] = [];
    let bytes = 0;
    while (bytes <= maxBytes) {
      const read = readSync(fd, buffer, 0, Math.min(buffer.length, maxBytes + 1 - bytes), null);
      if (read === 0) break;
      bytes += read;
      if (bytes > maxBytes) throw limitExceeded('maxBytes', maxBytes);
      chunks.push(Buffer.from(buffer.subarray(0, read)));
    }
    const raw = Buffer.concat(chunks);
    if (!isUtf8(raw)) throw new EventsClientError(ErrorCode.InvalidState, 'MCP Events state must be UTF-8 JSON');
    return decodeState(raw.toString('utf8'), schema);
  } catch (error) {
    if (error instanceof EventsClientError) throw error;
    throw new EventsClientError(ErrorCode.StateReadFailed, 'Could not read MCP Events state', {cause: error, details: {path}});
  } finally { closeSync(fd); }
}
export function writeState(path: string | undefined, text: string) {
  if (path === undefined) return;
  const directory = dirname(path);
  const temporary = `${path}.${randomUUID()}.tmp`;
  let created = false;
  let replaced = false;
  try {
    mkdirSync(directory, {recursive: true, mode: 0o700});
    const fd = openSync(temporary, 'wx', 0o600);
    created = true;
    try { writeFileSync(fd, text); fsyncSync(fd); }
    finally { closeSync(fd); }
    renameSync(temporary, path);
    created = false;
    replaced = true;
    // Windows does not support opening a directory for fsync.
    if (process.platform !== 'win32') {
      const directoryFd = openSync(directory, 'r');
      try { fsyncSync(directoryFd); } finally { closeSync(directoryFd); }
    }
  } catch (error) {
    const message = replaced
      ? 'MCP Events state was replaced but could not be synced; reopen the store before writing again'
      : 'Could not save MCP Events state';
    throw new EventsClientError(ErrorCode.StateWriteFailed, message, {cause: error, details: {path, replaced}});
  } finally {
    if (created) {
      try { unlinkSync(temporary); }
      catch { /* Preserve the write failure if temporary-file cleanup also fails. */ }
    }
  }
}
