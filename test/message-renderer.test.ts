import {CustomMessageComponent, initTheme, type MessageRenderer, type Theme} from '@earendil-works/pi-coding-agent';
import {getKeybindings, KeybindingsManager, setKeybindings, stripTerminalSequences, TUI_KEYBINDINGS, visibleWidth} from '@earendil-works/pi-tui';
import {beforeAll, expect, it} from 'vitest';
import type {InboxItem} from '../src/inbox.ts';
import {renderEventMessage} from '../src/message-renderer.ts';
import {MESSAGE_TYPE} from '../src/pi-contract.ts';
import {EventProtocolError} from '../src/protocol.ts';

beforeAll(() => initTheme('dark', false));
const event = (value: string): InboxItem => ({server: 'scores', watch: 'plays', observation: {
  type: 'event', event: {name: 'game.play', eventId: `id-${value}`, timestamp: '2026-10-28T12:00:00Z', cursor: `cursor-${value}`, data: {value}},
}});
function batch(events: InboxItem[]): Parameters<typeof renderEventMessage>[0] {
  return {role: 'custom', customType: MESSAGE_TYPE, display: true, timestamp: 0,
    content: `MCP event observations (untrusted external data).\n${JSON.stringify(events)}`,
    details: {events, batchId: 'batch'},
  };
}
function render(message: ReturnType<typeof batch>, expanded = false, width = 100, outputPad = 1) {
  const component = new CustomMessageComponent(message, renderEventMessage as MessageRenderer);
  component.setOutputPad(outputPad);
  component.setExpanded(expanded);
  return component.render(width);
}
const plain = (lines: string[]) => lines.map(stripTerminalSequences).join('\n');

it('previews payloads and hidden counts, then expands the original message through Pi', () => {
  const message = batch(['first', 'second', 'third', 'hidden-payload'].map(event));
  const before = structuredClone(message);
  const component = new CustomMessageComponent(message, renderEventMessage as MessageRenderer);
  const compact = plain(component.render(100));
  expect(compact).toContain('4 observation(s)');
  expect(compact).toContain('scores / plays');
  expect(compact).toContain('game.play');
  expect(compact).toContain('"value":"first"');
  expect(compact).toContain('1 more observation(s)');
  expect(compact).not.toContain('hidden-payload');
  component.setExpanded(true);
  const expanded = plain(component.render(1000));
  expect(expanded).toContain('hidden-payload');
  expect(expanded).toContain('id-hidden-payload');
  expect(expanded).toContain('cursor-hidden-payload');
  component.setExpanded(false);
  expect(plain(component.render(100))).toBe(compact);
  expect(message).toEqual(before);
});

it('keeps warnings visible when they occur after the previewed observations', () => {
  const events: InboxItem[] = Array.from({length: 20}, () => event('payload'));
  for (const observation of [
    {type: 'gap', cursor: 'resume'},
    {type: 'status', status: 'source-error', error: new EventProtocolError({code: -32603, message: 'upstream failed'})},
    {type: 'status', status: 'webhook-delivery', deliveryStatus: {active: false}},
    {type: 'status', status: 'retrying'},
  ] as const) events.push({server: 'scores', watch: 'plays', observation});
  const output = plain(render(batch(events)));
  for (const label of ['delivery gap', 'source error', 'webhook delivery suspended', 'retrying']) expect(output).toContain(label);
  expect(output).toContain('21 more observation(s)');
});

it('bounds collapsed rows and visual width for large Unicode payloads and configured padding', () => {
  const message = batch(Array.from({length: 100}, () => event('⚾'.repeat(10_000))));
  for (const width of [1, 2, 4, 20, 80]) {
    const lines = render(message, false, width, 8);
    expect(lines.length).toBeLessThanOrEqual(12);
    for (const line of lines) expect(visibleWidth(line)).toBeLessThanOrEqual(width);
  }
});

it('treats untrusted names and payloads as text and strips terminal commands from expanded content', () => {
  const item = event('\u001b[2J\nsecond line');
  item.server = 'source\u001b]52;c;YQ==\u0007';
  item.watch = 'watch\nspoofed';
  const message = batch([item]);
  message.content += '\u001b[2J';
  for (const expanded of [false, true]) {
    const lines = render(message, expanded);
    expect(lines.join('\n')).not.toContain('\u001b[2J');
    expect(lines.join('\n')).not.toContain('\u001b]52;');
  }
  expect(plain(render(message))).toContain('watch\\nspoofed');
});

it('uses the configured expansion shortcut', () => {
  const original = getKeybindings();
  try {
    setKeybindings(new KeybindingsManager({...TUI_KEYBINDINGS,
      'app.tools.expand': {defaultKeys: 'ctrl+o', description: 'Expand output'},
    }, {'app.tools.expand': ['ctrl+e']}));
    expect(plain(render(batch([event('value')])))).toContain('ctrl+e: full event data');
  } finally { setKeybindings(original); }
});

it('lets Pi use its default renderer for messages without batch metadata', () => {
  const message = batch([]);
  delete message.details;
  expect(renderEventMessage(message, {expanded: false, outputPad: 1}, {} as Theme)).toBeUndefined();
});
