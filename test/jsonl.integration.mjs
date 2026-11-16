import assert from 'node:assert/strict';
import {appendFileSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {test} from 'node:test';
import {readCompleteJsonLines} from './fixtures/jsonl.mjs';

test('reads complete JSON lines while another process appends a record', t => {
  const directory = mkdtempSync(join(tmpdir(), 'pi-events-jsonl-'));
  t.after(() => rmSync(directory, {recursive: true, force: true}));
  const path = join(directory, 'events.jsonl');
  assert.deepEqual(readCompleteJsonLines(path), []);
  writeFileSync(path, '{"value":1}\n{"value":');
  assert.deepEqual(readCompleteJsonLines(path), [{value: 1}]);
  appendFileSync(path, '2}\n');
  assert.deepEqual(readCompleteJsonLines(path), [{value: 1}, {value: 2}]);
  appendFileSync(path, 'invalid\n');
  assert.throws(() => readCompleteJsonLines(path), SyntaxError);
});
