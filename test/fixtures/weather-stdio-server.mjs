import {appendFileSync} from 'node:fs';
import {startWeatherServer} from '../../dev/weather-server.mjs';
import {readCompleteJsonLines} from './jsonl.mjs';

const [controlPath, receiptPath, historyLimit = '1000'] = process.argv.slice(2);
const receipt = value => appendFileSync(receiptPath, `${JSON.stringify({...value, pid: process.pid})}\n`);
let consumed = readCompleteJsonLines(controlPath).length;
let seconds = 0;
const server = await startWeatherServer({transport: 'stdio', intervalMs: 0, heartbeatMs: 100,
  historyLimit: Number(historyLimit), now: () => Date.UTC(2026, 10, 17, 12) + seconds * 1000});
receipt({type: 'started', cwd: process.cwd(), url: server.url ?? null});
const timer = setInterval(() => {
  for (const command of readCompleteJsonLines(controlPath).slice(consumed)) {
    consumed++;
    if (command.type === 'crash') {
      receipt({id: command.id});
      process.exit(1);
    }
    seconds = command.seconds;
    receipt({id: command.id, events: [...server.advance(), ...server.advance()]});
  }
}, 10);
process.stdin.once('end', () => clearInterval(timer));
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => {
  clearInterval(timer);
  void server.close().catch(error => { console.error(error); process.exitCode = 1; });
});
process.once('exit', () => receipt({type: 'exited'}));
