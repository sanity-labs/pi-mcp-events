import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type {ExtensionAPI, ExtensionContext} from '@earendil-works/pi-coding-agent';
import {afterEach, expect, it, vi} from 'vitest';
import {ConversationDelivery} from '../src/conversation.ts';
import {SessionState} from '../src/session-state.ts';
import {MESSAGE_TYPE, STATUS_MESSAGE_TYPE} from '../src/pi-contract.ts';

const cleanup: Array<() => void> = [];
afterEach(() => { for (const close of cleanup.splice(0).reverse()) close(); vi.useRealTimers(); });
function setup(state = new SessionState(), path?: string) {
  vi.useFakeTimers();
  const entries: unknown[] = []; let idle = true;
  const sendMessage = vi.fn(); const notify = vi.fn();
  const api = {sendMessage} as unknown as ExtensionAPI;
  const ctx = {isIdle: () => idle, ui: {notify}, sessionManager: {getEntries: () => entries}} as unknown as ExtensionContext;
  const delivery = new ConversationDelivery(api, ctx, state, path);
  cleanup.push(() => delivery.close());
  return {state, delivery, sendMessage, notify, setIdle(value: boolean) {idle = value;},
    append(message: unknown) {entries.push({...(message as object), type: 'custom_message'});}};
}
const item = (cursor: string) => ({server: 's', watch: 'w', observation: {type: 'gap' as const, cursor}});

it('waits for an idle conversation and an accepted batch before acknowledging its prefix', async () => {
  const h = setup(); h.setIdle(false); h.state.accept(item('one')); h.delivery.schedule();
  await vi.advanceTimersByTimeAsync(300); expect(h.sendMessage).not.toHaveBeenCalled();
  h.setIdle(true); await vi.advanceTimersByTimeAsync(100);
  const first = h.sendMessage.mock.calls[0]![0];
  expect(first).toMatchObject({customType: MESSAGE_TYPE, display: true, details: {events: [item('one')]}});
  expect(h.sendMessage.mock.calls[0]![1]).toEqual({triggerTurn: true, deliverAs: 'followUp'});
  expect(first.content).toContain('untrusted external data');
  h.state.accept(item('two')); await vi.advanceTimersByTimeAsync(300);
  expect(h.sendMessage).toHaveBeenCalledTimes(1); expect(h.state.size).toBe(2);
  h.append(first); await vi.advanceTimersByTimeAsync(100);
  const second = h.sendMessage.mock.calls[1]![0]; expect(second.details.events).toEqual([item('two')]);
  h.append(second); await vi.advanceTimersByTimeAsync(100); expect(h.state.size).toBe(0);
});

it('retains a failed batch, retries delivery, and cancels pending work on close', async () => {
  const h = setup(); h.state.accept(item('one'));
  h.sendMessage.mockImplementationOnce(() => {throw new Error('temporarily unavailable');});
  h.delivery.schedule(); await vi.advanceTimersByTimeAsync(100);
  expect(h.state.snapshot()).toEqual([item('one')]); expect(h.notify).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(100); expect(h.sendMessage).toHaveBeenCalledTimes(2);
  h.delivery.close(); h.state.accept(item('two')); h.delivery.schedule();
  await vi.advanceTimersByTimeAsync(500); expect(h.sendMessage).toHaveBeenCalledTimes(2);
});

it('displays one persistence warning without starting a model turn or exposing the OS error', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'pi-warning-')); cleanup.push(() => rmSync(directory, {recursive: true, force: true}));
  const blocked = join(directory, 'blocked'); writeFileSync(blocked, 'not a directory');
  const path = join(blocked, 'state.json'); const h = setup(new SessionState(path), path);
  h.delivery.schedule(); await vi.advanceTimersByTimeAsync(300);
  expect(h.sendMessage).toHaveBeenCalledTimes(1);
  expect(h.sendMessage.mock.calls[0]![0]).toMatchObject({customType: STATUS_MESSAGE_TYPE, display: true, details: {code: 'PERSISTENCE_UNAVAILABLE', path}});
  expect(h.sendMessage.mock.calls[0]![0].content).toContain('Pi conversation storage is unchanged');
  expect(h.sendMessage.mock.calls[0]![1]).toEqual({triggerTurn: false});
  h.state.accept(item('one')); h.delivery.schedule(); await vi.advanceTimersByTimeAsync(100);
  expect(h.sendMessage).toHaveBeenCalledTimes(2);
  expect(h.sendMessage.mock.calls[1]![0].customType).toBe(MESSAGE_TYPE);
});
