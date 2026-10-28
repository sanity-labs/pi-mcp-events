import {keyText, type MessageRenderer} from '@earendil-works/pi-coding-agent';
import {Box, Text, stripTerminalSequences, truncateToWidth} from '@earendil-works/pi-tui';
import type {Observation} from './client.ts';
import type {PiEventBatch} from './pi-contract.ts';

const label = (value: string) => stripTerminalSequences(JSON.stringify(value)).slice(1, -1);

function warning(observation: Observation): string | undefined {
  if (observation.type === 'gap') return 'delivery gap';
  if (observation.type !== 'status') return;
  if (observation.status === 'source-error') return 'source error';
  if (observation.status === 'webhook-delivery') {
    if (!observation.deliveryStatus.active) return 'webhook delivery suspended';
    return;
  }
  return observation.status;
}

export const renderEventMessage: MessageRenderer<PiEventBatch> = (message, {expanded, outputPad}, theme) => {
  const events = message.details?.events;
  if (!Array.isArray(events)) return;
  const box = new Box(outputPad, 1, text => theme.bg('customMessageBg', text));
  const component = {
    render(width: number) {
      box.setPaddingX(Math.min(outputPad, Math.max(0, Math.floor((width - 1) / 2))));
      return box.render(width);
    },
    invalidate() { box.invalidate(); },
  };
  if (expanded) {
    const content = typeof message.content === 'string' ? message.content
      : message.content.filter(block => block.type === 'text').map(block => block.text).join('\n');
    box.addChild(new Text(stripTerminalSequences(content), 0, 0));
    return component;
  }
  const lines = [theme.fg('customMessageLabel', `MCP Events · ${events.length} observation(s) · untrusted data`)];
  const warnings = new Map<string, number>();
  for (const {observation} of events) {
    const kind = warning(observation);
    if (kind) warnings.set(kind, (warnings.get(kind) ?? 0) + 1);
  }
  for (const [kind, count] of warnings) lines.push(theme.fg('warning', `${count} × ${kind}`));
  const previews = events.slice(0, 3);
  for (const {server, watch, observation} of previews) {
    const source = `${label(server)} / ${label(watch)}`;
    if (observation.type === 'event') {
      lines.push(`${source} · ${label(observation.event.name)}`);
      lines.push(`  ${stripTerminalSequences(JSON.stringify(observation.event.data))}`);
    } else if (observation.type === 'gap') lines.push(`${source} · delivery gap`);
    else lines.push(`${source} · ${warning(observation) ?? 'webhook delivery active'}`);
  }
  if (events.length > previews.length) lines.push(theme.fg('dim', `… ${events.length - previews.length} more observation(s)`));
  const expand = keyText('app.tools.expand');
  const hint = expand ? `${expand}: full event data` : 'Expand tool output for full event data';
  lines.push(theme.fg('dim', hint));
  box.addChild({
    render: width => lines.map(line => truncateToWidth(line, width)),
    invalidate() {},
  });
  return component;
};
