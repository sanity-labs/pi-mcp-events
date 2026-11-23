import {createHash, createHmac, randomBytes, timingSafeEqual} from 'node:crypto';
import {lookup} from 'node:dns/promises';
import {BlockList, isIP} from 'node:net';
import {request as httpRequest} from 'node:http';
import {request as httpsRequest} from 'node:https';
import {setTimeout as delay} from 'node:timers/promises';
import {canonical, EventProtocolErrorCode as code} from '../src/protocol.ts';

const DEFAULT_TTL_MS = 60 * 60 * 1000;
const MAX_TTL_MS = 60 * 60 * 1000;
const MAX_BODY_BYTES = 256 * 1024;
const MAX_RESPONSE_BYTES = 8 * 1024;
const ROTATION_GRACE_MS = 300_000;
const SUSPENSION_WINDOW_MS = 3_600_000;

// Allocation references:
// https://www.iana.org/assignments/iana-ipv4-special-registry/
// https://www.iana.org/assignments/iana-ipv6-special-registry/
const blockedAddresses = new BlockList();
for (const cidr of [
  '0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
  '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
  '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '224.0.0.0/4', '240.0.0.0/4',
  '::/128', '::1/128', '::ffff:0:0:0/96', '64:ff9b::/96', '64:ff9b:1::/48', '100::/64', '2001::/23',
  '2001:db8::/32', '2002::/16', '2620:4f:8000::/48', '3fff::/20', '5f00::/16', 'fc00::/7', 'fe80::/10', 'fec0::/10', 'ff00::/8',
]) {
  const [address, prefix] = cidr.split('/');
  blockedAddresses.addSubnet(address, Number(prefix), isIP(address) === 4 ? 'ipv4' : 'ipv6');
}

export class WebhookCallbackError extends Error {
  constructor(reason) {
    super('Webhook endpoint verification failed');
    this.reason = reason;
  }
}

class WebhookPayloadLimitError extends Error {}

export class WebhookInputError extends Error {}

export class WebhookLimitError extends Error {
  constructor(data = {limit: 'subscriptions'}) {
    super('Webhook limit reached');
    this.data = data;
  }
}


export function isPublicAddress(address) {
  const family = isIP(address);
  if (family === 0) return false;
  if (family === 4) return !blockedAddresses.check(address, 'ipv4');
  // Retain the previous treatment of legacy dotted IPv4-compatible addresses.
  const compatible = address.match(/^::(\d+\.\d+\.\d+\.\d+)(?:%[^%]+)?$/);
  if (compatible) return !blockedAddresses.check(compatible[1], 'ipv4');
  return !blockedAddresses.check(address, 'ipv6');
}

function validateSecret(secret) {
  if (typeof secret !== 'string' || !secret.startsWith('whsec_')) throw new WebhookInputError('Invalid webhook secret');
  const encoded = secret.slice(6);
  const bytes = Buffer.from(encoded, 'base64');
  if (bytes.length < 24 || bytes.length > 64 || bytes.toString('base64') !== encoded) throw new WebhookInputError('Invalid webhook secret');
  return bytes;
}

function validateCallbackUrl(value, trustLoopbackHttp) {
  let url;
  try { url = new URL(value); } catch { throw new WebhookInputError('Invalid webhook URL'); }
  const allowedHttp = url.protocol === 'http:' && url.hostname === '127.0.0.1' && trustLoopbackHttp(url.origin);
  if ((url.protocol !== 'https:' && !allowedHttp) || url.username || url.password || url.hash || url.hostname.length === 0) {
    throw new WebhookInputError('Webhook URL must be an HTTPS URL without credentials or fragment');
  }
  if (url.port && (Number(url.port) < 1 || Number(url.port) > 65535)) throw new WebhookInputError('Invalid webhook port');
  return url;
}

function isLoopback(hostname) {
  const address = hostname.replace(/^\[|\]$/g, '');
  return address === '::1' || (isIP(address) === 4 && address.startsWith('127.'));
}

async function resolvePublicAddresses(url, {allowLocalAddresses, resolveHost}) {
  const hostname = url.hostname.replace(/^\[|\]$/g, '');
  if (allowLocalAddresses && (hostname === 'localhost' || hostname.endsWith('.localhost') || isLoopback(hostname))) {
    if (isIP(hostname)) return [{address: hostname, family: isIP(hostname)}];
    return [{address: '127.0.0.1', family: 4}];
  }
  let records;
  if (isIP(hostname)) records = [{address: hostname, family: isIP(hostname)}];
  else records = await resolveHost(hostname);
  if (!records.length || records.some(record => !isPublicAddress(record.address))) throw new Error('Webhook destination is not publicly routable');
  return records;
}

function signature(secret, messageId, timestamp, body) {
  return `v1,${createHmac('sha256', secret).update(`${messageId}.${timestamp}.`).update(body).digest('base64')}`;
}

function requestCallback({url, addresses, headers, body, timeoutMs, ca, signal}) {
  return new Promise((resolve, reject) => {
    const send = url.protocol === 'https:' ? httpsRequest : httpRequest;
    const request = send(url, {
      method: 'POST', headers, timeout: timeoutMs, ca, signal,
      lookup(_hostname, options, callback) {
        if (options.all) { callback(null, addresses); return; }
        const {address, family} = addresses[0];
        callback(null, address, family);
      },
    }, response => {
      const chunks = [];
      let size = 0;
      response.on('data', chunk => {
        size += chunk.length;
        if (size > MAX_RESPONSE_BYTES) {
          const error = Object.assign(new Error('Webhook response too large'), {code: 'ERESPONSETOOLARGE'});
          reject(error);
          response.destroy(error);
        }
        else chunks.push(chunk);
      });
      response.on('error', reject);
      response.on('end', () => resolve({status: response.statusCode ?? 0, body: Buffer.concat(chunks)}));
    });
    request.on('timeout', () => request.destroy(Object.assign(new Error('Webhook request timed out'), {code: 'ETIMEDOUT'})));
    request.on('error', reject);
    request.end(body);
  });
}

function constantTimeStringEqual(left, right) {
  const a = Buffer.from(String(left));
  const b = Buffer.from(String(right));
  return a.length === b.length && timingSafeEqual(a, b);
}

export function createWebhookDelivery({
  now = Date.now,
  resolveHost = host => lookup(host, {all: true, verbatim: true}),
  post,
  ca,
  allowLocalAddresses = false,
  trustLoopbackHttp = () => false,
  allowUrl = () => false,
  matches = () => true,
  shouldEmit = () => true,
  authorize = () => {},
  hasCheck = () => false,
  check = () => undefined,
  onCheckClose = () => undefined,
  checkIntervalMs = 1000,
  assertCapacity = () => {},
  store,
  getCurrentCursor = () => null,
  cursorForEvent = (_subscription, event) => event.cursor ?? null,
  replay = ({cursor}) => ({events: [], cursor, truncated: false}),
  defaultTtlMs = DEFAULT_TTL_MS,
  maxTtlMs = MAX_TTL_MS,
  maxSubscriptions = 100,
  maxPendingPerSubscription = 100,
  maxPendingTotal = 1000,
  maxAttempts = 5,
  suspensionMinAttempts = 100,
  retryBaseMs = 250,
  retryMaxMs = 30_000,
  timeoutMs = 5_000,
} = {}) {
  const callbackUrl = value => validateCallbackUrl(value, trustLoopbackHttp);
  const subscriptions = new Map();
  const opening = new Map();
  const verifiedEndpoints = new Map();
  const subscriptionLocks = new Map();
  const verificationLocks = new Map();
  const verificationHosts = new Map();
  const failedVerifications = new Map();
  const semaphoreWaiters = [];
  const jobs = new Set();
  const closing = new AbortController();
  let pendingTotal = 0;
  let activeWorkers = 0;
  let closed = false;

  const withLock = async (locks, key, operation) => {
    const previous = locks.get(key) ?? Promise.resolve();
    let release;
    const current = new Promise(resolve => { release = resolve; });
    const tail = previous.then(() => current);
    locks.set(key, tail);
    await previous;
    try { return await operation(); }
    finally {
      release();
      if (locks.get(key) === tail) locks.delete(key);
    }
  };

  const keyFor = ({principal, name, arguments: args, url}) => canonical([principal, url, name, args]);
  const idFor = key => `sub_${createHash('sha256').update(key).digest('hex').slice(0, 24)}`;
  const remove = subscription => {
    if (subscriptions.get(subscription.key) !== subscription) return;
    subscriptions.delete(subscription.key);
    clearTimeout(subscription.expiryTimer);
    clearTimeout(subscription.checkTimer);
    clearTimeout(subscription.refreshTimer);
    subscription.controller.abort();
    subscription.paused?.resolve();
    subscription.endCheck?.();
  };
  const scheduleExpiry = subscription => {
    clearTimeout(subscription.expiryTimer);
    const remaining = Math.max(0, subscription.expiresAt - now());
    subscription.expiryTimer = setTimeout(() => {
      if (subscription.expiresAt > now()) {
        scheduleExpiry(subscription);
      } else if (store) {
        track(syncStoredGrant(subscription));
      } else {
        remove(subscription);
      }
    }, Math.min(remaining, 2 ** 31 - 1));
    subscription.expiryTimer.unref();
  };
  const sweep = () => {
    const time = now();
    for (const subscription of subscriptions.values()) {
      if (subscription.expiresAt <= time && !store) {
        remove(subscription);
      }
    }
    for (const [key, expiry] of verifiedEndpoints) if (expiry <= time) verifiedEndpoints.delete(key);
    for (const [key, failure] of failedVerifications) if (failure.until <= time) failedVerifications.delete(key);
    for (const [host, usage] of verificationHosts) {
      if (!usage.pending && usage.until <= time) verificationHosts.delete(host);
    }
  };
  const acquireWorker = async () => {
    if (activeWorkers < 8) { activeWorkers++; return; }
    await new Promise(resolve => semaphoreWaiters.push(resolve));
  };
  const releaseWorker = () => {
    const next = semaphoreWaiters.shift();
    if (next) next();
    else activeWorkers--;
  };
  const safeCursor = subscription => {
    return subscription.cursor;
  };
  const advanceWatermark = subscription => {
    while (subscription.pending[0]?.done) {
      const item = subscription.pending.shift();
      if (item.lost) subscription.watermarkBlocked = true;
      if (item.event?.type === 'gap' && item.accepted) {
        subscription.cursor = item.cursor ?? subscription.cursor;
        subscription.watermarkBlocked = false;
      } else if (!subscription.watermarkBlocked) {
        subscription.cursor = item.cursor ?? subscription.cursor;
      }
    }
  };
  const signedPost = async (subscription, bodyValue, messageId) => {
    const body = Buffer.from(JSON.stringify(bodyValue));
    if (body.length > MAX_BODY_BYTES) throw new WebhookPayloadLimitError();
    const timestamp = Math.floor(now() / 1000).toString();
    const signatures = [signature(subscription.secret, messageId, timestamp, body)];
    const previous = subscription.previousSecret;
    if (previous?.expiresAt > now()) signatures.push(signature(validateSecret(previous.secret), messageId, timestamp, body));
    const headers = {
      'content-type': 'application/json',
      'webhook-id': messageId,
      'webhook-timestamp': timestamp,
      'webhook-signature': signatures.join(' '),
      'x-mcp-subscription-id': subscription.id,
      'content-length': String(body.length),
    };
    const url = callbackUrl(subscription.url);
    const configuredLocal = typeof allowLocalAddresses === 'function' ? allowLocalAddresses(url) : allowLocalAddresses;
    const allowLocal = configuredLocal || url.protocol === 'http:';
    const signal = subscription.controller?.signal ?? closing.signal;
    const timeout = AbortSignal.timeout(timeoutMs);
    const deadline = AbortSignal.any([timeout, signal]);
    const sender = post ?? requestCallback;
    const stopped = Promise.withResolvers();
    const abort = () => stopped.reject(deadline.reason);
    deadline.addEventListener('abort', abort, {once: true});
    if (deadline.aborted) abort();
    try {
      const addresses = await Promise.race([stopped.promise, resolvePublicAddresses(url, {allowLocalAddresses: allowLocal, resolveHost})]);
      deadline.throwIfAborted();
      return await sender({url, addresses, address: addresses[0], headers, body, timeoutMs, ca, signal: deadline});
    } catch (error) {
      if (timeout.aborted && !signal.aborted) throw Object.assign(new Error('Webhook request timed out'), {code: 'ETIMEDOUT'});
      throw error;
    } finally { deadline.removeEventListener('abort', abort); }
  };
  const wake = subscription => {
    const paused = subscription.paused;
    if (!paused) return;
    subscription.paused = undefined;
    subscription.recent = [];
    clearTimeout(subscription.refreshTimer);
    paused.resolve();
  };
  const observe = (subscription, failed) => {
    const at = now();
    const recent = subscription.recent;
    recent.push({at, failed});
    while (recent[0] && recent[0].at <= at - SUSPENSION_WINDOW_MS) recent.shift();
    if (!failed || recent.length < suspensionMinAttempts || subscription.paused) return;
    const failures = recent.filter(item => item.failed).length;
    if (failures * 100 <= recent.length * 95) return;
    subscription.paused = Promise.withResolvers();
    if (store) scheduleRefreshCheck(subscription);
  };
  const sendFailure = (subscription, category, retryable) => {
    subscription.lastError = category;
    if (!retryable) return;
    subscription.failedSince ??= new Date(now()).toISOString();
    observe(subscription, true);
  };
  const acceptDelivery = subscription => {
    subscription.lastDeliveryAt = new Date(now()).toISOString();
    subscription.lastError = null;
    subscription.failedSince = undefined;
    observe(subscription, false);
  };
  const markLost = (subscription, item) => {
    if (item.event.type === 'gap') {
      subscription.refreshGap = {cursor: item.cursor};
      return;
    }
    item.lost = true;
    if (!subscription.gapPending) subscription.gapCursor = subscription.latestCursor;
    subscription.gapPending = true;
  };
  const canSend = subscription => !closed && !subscription.controller.signal.aborted && subscription.expiresAt > now();

  const dispatch = async (subscription, item) => {
    try {
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        while (subscription.paused && canSend(subscription)) await subscription.paused.promise;
        if (!canSend(subscription)) break;
        await acquireWorker();
        try {
          if (!canSend(subscription)) break;
          if (subscription.paused) { attempt--; continue; }
          try { authorize(sourceContext(subscription)); }
          catch (error) { track(endSubscription(subscription, error)); break; }
          if (store) {
            const stored = await store.get(subscription.id);
            if (!stored || stored.expiresAt <= now()) { remove(subscription); break; }
            subscription.secret = validateSecret(stored.secret);
            subscription.previousSecret = stored.previousSecret;
          }
          const payload = {...item.event, cursor: item.event.type === 'gap' ? item.cursor : safeCursor(subscription)};
          const response = await signedPost(subscription, payload, item.messageId);
          if (response.status >= 200 && response.status < 300) {
            item.done = true;
            item.accepted = true;
            acceptDelivery(subscription);
            advanceWatermark(subscription);
            return;
          }
          const retryable = response.status !== 410 && response.status !== 413;
          sendFailure(subscription, response.status >= 500 ? 'http_5xx' : 'http_4xx', retryable);
          if (!retryable) {
            item.done = true;
            if (response.status === 413 || item.event.type === 'gap') markLost(subscription, item);
            advanceWatermark(subscription);
            return;
          }
        } catch (error) {
          if (subscription.controller.signal.aborted) break;
          if (error instanceof WebhookPayloadLimitError) {
            item.done = true;
            markLost(subscription, item);
            advanceWatermark(subscription);
            return;
          }
          let reason = 'connection_refused';
          if (error.code === 'ETIMEDOUT') reason = 'timeout';
          else if (typeof error.code === 'string' && /CERT|TLS|SSL/.test(error.code)) reason = 'tls_error';
          sendFailure(subscription, reason, true);
        } finally { releaseWorker(); }
        if (attempt < maxAttempts) {
          try { await delay(Math.min(retryBaseMs * (2 ** (attempt - 1)), retryMaxMs), undefined, {signal: subscription.controller.signal}); }
          catch (error) { if (!subscription.controller.signal.aborted) throw error; }
        }
      }
      item.done = true;
      if (canSend(subscription)) markLost(subscription, item);
      advanceWatermark(subscription);
    } finally { pendingTotal--; }
  };
  const enqueue = (subscription, event) => {
    const item = {event: structuredClone(event), cursor: event.cursor ?? subscription.latestCursor, messageId: `msg_${randomBytes(18).toString('hex')}`, done: false};
    const liveCount = subscription.pending.filter(value => !value.done).length;
    if (subscription.gapPending || liveCount >= maxPendingPerSubscription || pendingTotal >= maxPendingTotal) {
      subscription.gapPending = true;
      subscription.gapCursor = item.cursor;
      return false;
    }
    subscription.pending.push(item);
    subscription.latestCursor = item.cursor;
    pendingTotal++;
    const job = dispatch(subscription, item).finally(() => {
      jobs.delete(job);
      for (const current of subscriptions.values()) flushGap(current);
    });
    jobs.add(job);
    return true;
  };
  const flushGap = subscription => {
    if (!subscription.gapPending || subscription.paused || !canSend(subscription)
      || subscription.pending.length || pendingTotal >= maxPendingTotal) return;
    subscription.gapPending = false;
    subscription.watermarkBlocked = true;
    const cursor = subscription.gapCursor;
    const item = {
      event: {type: 'gap', cursor},
      cursor,
      messageId: `msg_gap_${randomBytes(12).toString('hex')}`,
      done: false,
    };
    subscription.pending.push(item);
    pendingTotal++;
    const job = dispatch(subscription, item).finally(() => {
      jobs.delete(job);
      for (const current of subscriptions.values()) flushGap(current);
    });
    jobs.add(job);
  };
  const track = operation => {
    const job = operation.catch(() => {}).finally(() => jobs.delete(job));
    jobs.add(job);
    return job;
  };
  const scheduleRefreshCheck = subscription => {
    clearTimeout(subscription.refreshTimer);
    subscription.refreshTimer = setTimeout(() => track(syncStoredGrant(subscription)), 1000).unref();
  };
  const syncStoredGrant = async subscription => {
    if (closed || subscriptions.get(subscription.key) !== subscription) return;
    try {
      const record = await store.get(subscription.id);
      if (closed || subscriptions.get(subscription.key) !== subscription) return;
      if (!record || record.expiresAt <= now()) {
        remove(subscription);
        return;
      }
      if (record.expiresAt > subscription.expiresAt) {
        subscription.expiresAt = record.expiresAt;
        subscription.previousSecret = record.previousSecret;
        subscription.secret = validateSecret(record.secret);
        subscription.encodedSecret = record.secret;
        scheduleExpiry(subscription);
        wake(subscription);
        flushGap(subscription);
      }
      if (subscription.paused) scheduleRefreshCheck(subscription);
    } catch {
      if (!closed && subscriptions.get(subscription.key) === subscription) scheduleRefreshCheck(subscription);
    }
  };
  const ensureVerified = async (principal, url, secret, id) => {
    const endpointKey = canonical([principal, url]);
    const host = new URL(url).hostname;
    return withLock(verificationLocks, endpointKey, async () => {
      if ((verifiedEndpoints.get(endpointKey) ?? 0) > now()) return;
      const failure = failedVerifications.get(endpointKey);
      if (failure && failure.until > now()) throw failure.error;
      let usage = verificationHosts.get(host);
      if (!usage || (usage.until <= now() && !usage.pending)) {
        usage = {started: 0, pending: 0, until: now() + 60_000, tail: Promise.resolve()};
        verificationHosts.set(host, usage);
      }
      if (usage.started >= 10) {
        throw new WebhookLimitError({limit: 'verification', retryAfterMs: Math.max(1, usage.until - now())});
      }
      usage.started++;
      usage.pending++;
      const verification = usage.tail.then(async () => {
        try {
          const challenge = randomBytes(24).toString('base64url');
          const response = await signedPost({url, id, secret}, {type: 'verification', challenge}, `msg_verification_${randomBytes(12).toString('hex')}`);
          let echoed;
          try { echoed = JSON.parse(response.body.toString()).challenge; } catch { echoed = undefined; }
          if (response.status < 200 || response.status >= 300 || typeof echoed !== 'string' || !constantTimeStringEqual(echoed, challenge)) {
            throw new WebhookCallbackError('challenge_failed');
          }
          if (verifiedEndpoints.size >= 1000) verifiedEndpoints.delete(verifiedEndpoints.keys().next().value);
          verifiedEndpoints.set(endpointKey, now() + maxTtlMs);
        } catch (error) {
          let result = error;
          if (!(result instanceof WebhookCallbackError)) {
            let reason = 'connection_refused';
            if (error.code === 'ETIMEDOUT') reason = 'timeout';
            else if (typeof error.code === 'string' && /CERT|TLS|SSL/.test(error.code)) reason = 'tls_error';
            result = new WebhookCallbackError(reason);
          }
          failedVerifications.set(endpointKey, {error: result, until: now() + 60_000});
          throw result;
        } finally { usage.pending--; }
      });
      usage.tail = verification.catch(() => {});
      return verification;
    });
  };

  const sourceContext = ({name, principal, arguments: args, id, latestCursor, eventRevision}) => ({
    name, principal, arguments: args, mode: 'webhook', subscriptionId: id, cursor: latestCursor, eventRevision,
  });
  const load = ({key, secret, ...record}, position) => {
    const subscription = {
      ...record, key, secret: validateSecret(secret), encodedSecret: secret, cursor: position, latestCursor: position, pending: [], gapPending: false,
      mode: 'webhook', recent: [], controller: new AbortController(),
    };
    subscription.endCheck = onCheckClose(record.name, sourceContext(subscription));
    subscriptions.set(key, subscription);
    scheduleExpiry(subscription);
    return subscription;
  };

  const endSubscription = async (subscription, error) => {
    const ended = await withLock(subscriptionLocks, subscription.key, async () => {
      if (subscriptions.get(subscription.key) !== subscription) return false;
      remove(subscription);
      return !store || await store.delete(subscription.id);
    });
    if (!ended) return;
    const envelope = {type: 'terminated', error: {code: error.code ?? code.Forbidden, message: error.message, data: error.data}};
    const messageId = `msg_terminated_${randomBytes(12).toString('hex')}`;
    const endpoint = {...subscription, controller: undefined};
    for (let attempt = 1; attempt <= maxAttempts && !closed; attempt++) {
      try {
        const result = await signedPost(endpoint, envelope, messageId);
        if ((result.status >= 200 && result.status < 300) || [410, 413].includes(result.status)) return;
      } catch { /* Delivery can resume when the endpoint recovers. */ }
      if (attempt < maxAttempts) {
        try { await delay(Math.min(retryBaseMs * (2 ** (attempt - 1)), retryMaxMs), undefined, {signal: closing.signal}); }
        catch { return; }
      }
    }
  };

  const advanceObservedCursor = (subscription, cursor) => {
    const skipped = {cursor, done: true};
    if (subscription.pending.at(-1)?.done) subscription.pending[subscription.pending.length - 1] = skipped;
    else subscription.pending.push(skipped);
    subscription.latestCursor = skipped.cursor;
    advanceWatermark(subscription);
  };

  const startCheck = subscription => {
    const poll = async () => {
      let hasMore;
      try {
        const input = sourceContext(subscription);
        authorize(input);
        const page = await check(input);
        if (subscription.controller.signal.aborted) return;
        if (page.truncated) {
          subscription.gapPending = true;
          subscription.gapCursor = page.startCursor;
          flushGap(subscription);
        }
        for (const event of page.events) enqueue(subscription, event);
        if (subscription.gapPending) subscription.gapCursor = page.cursor;
        else advanceObservedCursor(subscription, page.cursor);
        hasMore = page.hasMore;
      } catch (error) {
        if ([code.Forbidden, code.NotFound, code.Unsupported].includes(error.code)) { await endSubscription(subscription, error); return; }
      }
      if (!subscription.controller.signal.aborted) subscription.checkTimer = setTimeout(() => track(poll()), hasMore ? 0 : checkIntervalMs).unref();
    };
    subscription.checkTimer = setTimeout(() => track(poll()), checkIntervalMs).unref();
  };

  const validateInput = input => {
    if (!input || typeof input !== 'object' || typeof input.principal !== 'string' || !input.principal.trim()) throw new Error('Authenticated principal required');
    if (typeof input.name !== 'string' || !input.name.trim()) throw new Error('Event name required');
    if (!input.arguments || typeof input.arguments !== 'object' || Array.isArray(input.arguments)) throw new Error('Subscription arguments must be an object');
    if (typeof input.url !== 'string') throw new WebhookInputError('Invalid webhook URL');
    callbackUrl(input.url);
    validateSecret(input.secret);
    const requestedTtl = input.ttlMs === null || input.ttlMs === undefined ? defaultTtlMs : input.ttlMs;
    if (!Number.isSafeInteger(requestedTtl) || requestedTtl < 0) throw new WebhookInputError('Invalid webhook TTL');
    return {ttlMs: Math.min(Math.max(requestedTtl, 1000), maxTtlMs)};
  };

  const result = (subscription, truncated = false) => ({
    id: subscription.id, refreshBefore: new Date(subscription.expiresAt).toISOString(),
    cursor: safeCursor(subscription), truncated,
  });
  const emitTo = (subscription, event) => {
    if (!shouldEmit(subscription)) return false;
    const item = {...event, cursor: cursorForEvent(subscription, event)};
    if (subscription.gapPending) {
      subscription.gapCursor = item.cursor;
      return false;
    }
    if (!matches(subscription, event)) {
      advanceObservedCursor(subscription, item.cursor ?? subscription.latestCursor);
      return false;
    }
    return enqueue(subscription, item);
  };

  return {
    async subscribe(input) {
      if (closed) throw new Error('Webhook delivery is closed');
      sweep();
      const {ttlMs} = validateInput(input);
      const url = callbackUrl(input.url).href;
      const key = keyFor({...input, url});
      const id = idFor(key);
      return withLock(subscriptionLocks, key, async () => {
        if (closed) throw new Error('Webhook delivery is closed');
        sweep();
        const secret = validateSecret(input.secret);
        const stored = await store?.get(id);
        const verified = stored && stored.expiresAt > now();
        let subscription = subscriptions.get(key);
        if (!subscription) {
          if (subscriptions.size + opening.size >= maxSubscriptions) throw new WebhookLimitError();
          assertCapacity(input.principal);
          opening.set(key, input.principal);
        }
        try {
          if (!verified && !allowUrl(url, input)) await ensureVerified(input.principal, url, secret, id);
          if (closed) throw new Error('Webhook delivery is closed');
          const checkAccess = () => authorize({
            name: input.name, arguments: input.arguments, principal: input.principal, mode: 'webhook', eventRevision: input.eventRevision,
          });
          checkAccess();
          const record = {
            id, key, principal: input.principal, name: input.name, arguments: structuredClone(input.arguments),
            url, secret: input.secret, expiresAt: now() + ttlMs, eventRevision: input.eventRevision,
          };
          const oldSecret = verified ? stored.secret : subscription?.encodedSecret;
          if (oldSecret && oldSecret !== input.secret) {
            record.previousSecret = {secret: oldSecret, expiresAt: now() + ROTATION_GRACE_MS};
          } else {
            const previous = stored?.previousSecret ?? subscription?.previousSecret;
            if (previous?.expiresAt > now()) record.previousSecret = previous;
          }
          if (subscription) {
            await store?.put(record);
            checkAccess();
            subscription.secret = secret;
            subscription.encodedSecret = record.secret;
            subscription.previousSecret = record.previousSecret;
            subscription.expiresAt = record.expiresAt;
            scheduleExpiry(subscription);
            const deliveryStatus = {
              active: !subscription.paused, lastDeliveryAt: subscription.lastDeliveryAt ?? null, lastError: subscription.lastError ?? null,
              ...(subscription.failedSince === undefined ? {} : {failedSince: subscription.failedSince}),
            };
            const gap = subscription.refreshGap;
            if (gap) {
              subscription.cursor = gap.cursor;
              subscription.refreshGap = undefined;
              subscription.watermarkBlocked = false;
              advanceWatermark(subscription);
            }
            wake(subscription);
            flushGap(subscription);
            return {...result(subscription, !!gap), deliveryStatus};
          }

          const currentCursor = getCurrentCursor(input);
          const isChecked = hasCheck(input.name);
          let recovered;
          if (isChecked || (input.cursor !== null && input.cursor !== undefined)) {
            recovered = await replay({cursor: input.cursor, maxAgeMs: input.maxAgeMs, input: {
              name: input.name, principal: input.principal, arguments: input.arguments, mode: 'webhook', subscriptionId: id,
            }});
          } else {
            recovered = {events: [], cursor: currentCursor, headCursor: currentCursor, truncated: false};
          }
          if (closed) throw new Error('Webhook delivery is closed');
          checkAccess();
          await store?.put(record);
          try {
            if (closed) throw new Error('Webhook delivery is closed');
            checkAccess();
          } catch (error) {
            await store?.delete(id);
            throw error;
          }
          subscription = load(record, recovered.cursor ?? null);
          for (const event of recovered.events ?? []) enqueue(subscription, event);
          advanceObservedCursor(subscription, recovered.headCursor ?? recovered.cursor ?? null);
          if (isChecked) startCheck(subscription);
          return result(subscription, Boolean(recovered.truncated));
        } finally {
          opening.delete(key);
        }
      });
    },
    async unsubscribe(input) {
      const key = keyFor({...input, url: callbackUrl(input.url).href});
      return withLock(subscriptionLocks, key, async () => {
        const subscription = subscriptions.get(key);
        const removed = store ? await store.delete(idFor(key)) : subscription !== undefined;
        if (subscription) remove(subscription);
        return removed;
      });
    },
    async terminate(name, error, {principal, id} = {}) {
      for await (const record of store?.listByName(name) ?? []) {
        if (!subscriptions.has(record.key) && record.expiresAt > now()) load(record, null);
      }
      const selected = [...subscriptions.values()].filter(subscription => subscription.name === name
        && (principal === undefined || subscription.principal === principal)
        && (id === undefined || subscription.id === id));
      await Promise.all(selected.map(subscription => endSubscription(subscription, error)));
    },
    gap(id, cursor) {
      const subscription = [...subscriptions.values()].find(value => value.id === id);
      if (!subscription) return false;
      subscription.gapPending = true;
      subscription.gapCursor = cursor ?? subscription.latestCursor;
      flushGap(subscription);
      return true;
    },
    subscriptions(name) {
      sweep();
      return [...subscriptions.values()].filter(value => name === undefined || value.name === name)
        .map(({id, name, principal, url}) => ({id, name, principal, url}));
    },
    emit(event) {
      if (closed) throw new Error('Webhook delivery is closed');
      if (!event || typeof event.name !== 'string' || typeof event.eventId !== 'string' || typeof event.timestamp !== 'string' || !event.data || typeof event.data !== 'object') throw new Error('Invalid event');
      sweep();
      const offered = store ? new Set(subscriptions.keys()) : undefined;
      let accepted = 0;
      for (const subscription of subscriptions.values()) if (emitTo(subscription, event)) accepted++;
      if (store) track((async () => {
        for await (const record of store.listByName(event.name)) {
          if (closed) return;
          if (offered.has(record.key) || record.expiresAt <= now() || !shouldEmit(record)) continue;
          await withLock(subscriptionLocks, record.key, async () => {
            const existing = subscriptions.get(record.key);
            if (existing) {
              emitTo(existing, event);
              return;
            }
            const live = await store.get(record.id);
            if (!live || live.expiresAt <= now() || closed) return;
            const subscription = load(live, null);
            emitTo(subscription, event);
          });
        }
      })());
      return accepted;
    },
    reset() {
      for (const subscription of subscriptions.values()) remove(subscription);
      verifiedEndpoints.clear();
    },
    async waitForIdle() {
      while (jobs.size) await Promise.allSettled([...jobs]);
    },
    close() {
      closed = true;
      closing.abort();
      for (const subscription of subscriptions.values()) remove(subscription);
    },
    get size() { sweep(); return subscriptions.size; },
    get busy() { sweep(); return subscriptions.size + opening.size; },
    countFor(principal) {
      sweep();
      return [...subscriptions.values()].filter(item => item.principal === principal).length
        + [...opening.values()].filter(value => value === principal).length;
    },
  };
}
