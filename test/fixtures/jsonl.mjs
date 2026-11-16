import {existsSync, readFileSync} from 'node:fs';

export function readCompleteJsonLines(path) {
  if (!existsSync(path)) return [];
  const lines = readFileSync(path, 'utf8').split('\n');
  lines.pop();
  return lines.map(line => JSON.parse(line));
}
