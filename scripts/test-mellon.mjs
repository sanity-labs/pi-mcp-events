import {spawnSync} from 'node:child_process';

if (!process.env.LOCAL_EVENTS_CONNECTION_FILE) {
  throw new Error('Set LOCAL_EVENTS_CONNECTION_FILE to the running Mellon fixture connection file. See docs/development.md.');
}
for (const [command, ...args] of [
  ['npm', 'run', 'build'],
  ['npm', 'exec', '--', 'vitest', 'run', '--config', 'test/mellon.config.ts'],
  [process.execPath, 'test/pi-local.mjs'],
  [process.execPath, 'test/pi-local.mjs', '--no-session'],
]) {
  const result = spawnSync(command, args, {stdio: 'inherit'});
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
