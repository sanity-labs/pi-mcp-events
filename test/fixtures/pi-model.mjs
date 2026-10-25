import {appendFileSync, existsSync} from 'node:fs';
import {setTimeout as delay} from 'node:timers/promises';
import {fauxAssistantMessage, fauxProvider, fauxToolCall} from '@earendil-works/pi-ai';

export default function testModel(pi) {
  const unsubscribe = pi.events.on('mcp-events:observation', item => appendFileSync(process.env.EVENTS_TEST_OBSERVATIONS, JSON.stringify(item) + '\n'));
  pi.on('session_shutdown', unsubscribe);
  const faux = fauxProvider({provider: 'events-test', api: 'events-test', models: [{id: 'local'}], tokensPerSecond: 100000});
  const respond = async (context, options) => {
    appendFileSync(process.env.EVENTS_TEST_CONTEXTS, JSON.stringify(context) + '\n');
    const last = context.messages.at(-1);
    if (last?.role === 'user' && JSON.stringify(last.content).includes('HOLD_TEST')) {
      while (!existsSync(process.env.EVENTS_TEST_RELEASE) && !options?.signal?.aborted) await delay(10);
    }
    if (last?.role === 'user' && JSON.stringify(last.content).includes('READ_TEST')) {
      return fauxAssistantMessage(fauxToolCall('read', {path: 'ordinary.txt'}), {stopReason: 'toolUse'});
    }
    return fauxAssistantMessage('Local model received the conversation.');
  };
  faux.setResponses(Array.from({length: 100}, () => respond));
  pi.registerProvider(faux.provider);
  pi.registerCommand('test-reload', {description: 'Reload this test session', handler: (_args, ctx) => ctx.reload()});
  pi.registerCommand('test-shutdown', {description: 'Shut down this test session', handler: (_args, ctx) => ctx.shutdown()});
}
