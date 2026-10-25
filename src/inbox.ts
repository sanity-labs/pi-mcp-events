import {closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync} from 'node:fs';
import {dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import type {Observation} from './client.ts';
import {z} from 'zod';

export type InboxItem = {server: string; watch: string; observation: Observation};

export function atomicJson(path: string, value: unknown) {
  const directory = dirname(path);
  mkdirSync(directory, {recursive: true, mode: 0o700});
  const temporary = `${path}.${randomUUID()}.tmp`;
  try {
    const fd = openSync(temporary, 'wx', 0o600);
    try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); }
    finally { closeSync(fd); }
    renameSync(temporary, path);
    const directoryFd = openSync(directory, 'r');
    try { fsyncSync(directoryFd); } finally { closeSync(directoryFd); }
  } finally { if (existsSync(temporary)) unlinkSync(temporary); }
}

export class Checkpoints {
  private readonly path: string | undefined;
  private values: Record<string, string>;
  constructor(path?: string) {
    this.path = path;
    this.values = path && existsSync(path) ? z.record(z.string(), z.string()).parse(JSON.parse(readFileSync(path, 'utf8'))) : {};
  }
  get(key: string) { return this.values[key] ?? null; }
  retain(keys: Iterable<string>) {
    const active = new Set(keys);
    const values = Object.fromEntries(Object.entries(this.values).filter(([key]) => active.has(key)));
    if (Object.keys(values).length === Object.keys(this.values).length) return;
    if (this.path) atomicJson(this.path, values);
    this.values = values;
  }
  set(key: string, cursor: string | null) {
    const values = {...this.values};
    if (cursor === null) delete values[key];
    else values[key] = cursor;
    if (Object.keys(values).length > 512) throw new Error('Too many stored event checkpoints');
    if (this.path) atomicJson(this.path, values);
    this.values = values;
  }
}

export class Inbox {
  private readonly path: string | undefined;
  private items: InboxItem[];
  constructor(path?: string) {
    this.path = path;
    this.items = path && existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) as InboxItem[] : [];
    if (!Array.isArray(this.items)) throw new Error('Invalid MCP Events inbox');
    this.check(this.items);
  }
  get size() { return this.items.length; }
  private check(items: InboxItem[]) {
    if (items.length > 100 || Buffer.byteLength(JSON.stringify(items)) > 256 * 1024) throw new Error('MCP Events inbox is full');
  }
  private save(items: InboxItem[]) {
    if (this.path) atomicJson(this.path, items);
    this.items = items;
  }
  accept(item: InboxItem) {
    const next = [...this.items, item];
    this.check(next);
    this.save(next);
  }
  snapshot(): readonly InboxItem[] { return [...this.items]; }
  acknowledge(count: number) { this.save(this.items.slice(count)); }
}
