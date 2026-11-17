import {createHash, createHmac, randomBytes, timingSafeEqual} from 'node:crypto';
import {lookup} from 'node:dns/promises';
import {BlockList, isIP} from 'node:net';
import https from 'node:https';
import {setTimeout as delay} from 'node:timers/promises';

const DEFAULT_TTL_MS = 60 * 60 * 1000;
const MAX_TTL_MS = 60 * 60 * 1000;
const MAX_BODY_BYTES = 256 * 1024;
const MAX_RESPONSE_BYTES = 8 * 1024;

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

export class WebhookLimitError extends Error {
  constructor() { super('Webhook subscription limit reached'); }
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
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
  if (typeof secret !== 'string' || !secret.startsWith('whsec_')) throw new Error('Invalid webhook secret');
  const encoded = secret.slice(6);
  const bytes = Buffer.from(encoded, 'base64');
  if (bytes.length < 24 || bytes.length > 64 || bytes.toString('base64') !== encoded) throw new Error('Invalid webhook secret');
  return bytes;
}

function validateCallbackUrl(value) {
  let url;
  try { url = new URL(value); } catch { throw new Error('Invalid webhook URL'); }
  if (url.protocol !== 'https:' || url.username || url.password || url.hash || url.hostname.length === 0) {
    throw new Error('Webhook URL must be an HTTPS URL without credentials or fragment');
  }
  if (url.port && (Number(url.port) < 1 || Number(url.port) > 65535)) throw new Error('Invalid webhook port');
  return url;
}

function isLoopback(hostname) {
  const address = hostname.replace(/^\[|\]$/g, '');
  return address === '::1' || (isIP(address) === 4 && address.startsWith('127.'));
}

async function resolvePublicAddress(url, {allowLocalAddresses, resolveHost}) {
  const hostname = url.hostname.replace(/^\[|\]$/g, '');
  if (allowLocalAddresses && (hostname === 'localhost' || hostname.endsWith('.localhost') || isLoopback(hostname))) {
    return isIP(hostname) ? {address: hostname, family: isIP(hostname)} : {address: '127.0.0.1', family: 4};
  }
  let records;
  if (isIP(hostname)) records = [{address: hostname, family: isIP(hostname)}];
  else records = await resolveHost(hostname);
  if (!records.length || records.some(record => !isPublicAddress(record.address))) throw new Error('Webhook destination is not publicly routable');
  return records[0];
}

function signature(secret, messageId, timestamp, body) {
  return `v1,${createHmac('sha256', secret).update(`${messageId}.${timestamp}.`).update(body).digest('base64')}`;
}

function requestHttps({url, address, headers, body, timeoutMs, ca, signal}) {
  return new Promise((resolve, reject) => {
    const request = https.request(url, {
      method: 'POST', headers, timeout: timeoutMs, ca, signal,
      lookup(_hostname, _options, callback) { callback(null, address.address, address.family); },
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
  allowUrl = () => false,
  matches = () => true,
  getCurrentCursor = () => null,
  cursorForEvent = (_subscription, event) => event.cursor ?? null,
  replay = ({cursor}) => ({events: [], cursor, truncated: false}),
  defaultTtlMs = DEFAULT_TTL_MS,
  maxTtlMs = MAX_TTL_MS,
  maxSubscriptions = 100,
  maxPendingPerSubscription = 100,
  maxPendingTotal = 1000,
  maxAttempts = 5,
  retryBaseMs = 250,
  retryMaxMs = 30_000,
  timeoutMs = 5_000,
} = {}) {
  const subscriptions = new Map();
  const verifiedEndpoints = new Map();
  const subscriptionLocks = new Map();
  const verificationLocks = new Map();
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
    subscription.controller.abort();
  };
  const scheduleExpiry = subscription => {
    clearTimeout(subscription.expiryTimer);
    const remaining = Math.max(0, subscription.expiresAt - now());
    subscription.expiryTimer = setTimeout(() => {
      if (subscription.expiresAt <= now()) remove(subscription);
      else scheduleExpiry(subscription);
    }, Math.min(remaining, 2 ** 31 - 1));
    subscription.expiryTimer.unref();
  };
  const sweep = () => {
    const time = now();
    for (const subscription of subscriptions.values()) {
      if (subscription.expiresAt <= time) {
        remove(subscription);
      }
    }
    for (const [key, expiry] of verifiedEndpoints) if (expiry <= time) verifiedEndpoints.delete(key);
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
    if (body.length > MAX_BODY_BYTES) throw new Error('Webhook body too large');
    const timestamp = Math.floor(now() / 1000).toString();
    const headers = {
      'content-type': 'application/json',
      'webhook-id': messageId,
      'webhook-timestamp': timestamp,
      'webhook-signature': signature(subscription.secret, messageId, timestamp, body),
      'x-mcp-subscription-id': subscription.id,
      'content-length': String(body.length),
    };
    const url = validateCallbackUrl(subscription.url);
    const address = await resolvePublicAddress(url, {allowLocalAddresses, resolveHost});
    const sender = post ?? requestHttps;
    return sender({url, address, headers, body, timeoutMs, ca, signal: subscription.controller?.signal ?? closing.signal});
  };
  const dispatch = async (subscription, item) => {
    await acquireWorker();
    try {
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        if (closed || subscription.controller.signal.aborted || subscription.expiresAt <= now()) break;
        try {
          const payload = {...item.event, cursor: item.event.type === 'gap' ? item.cursor : safeCursor(subscription)};
          const response = await signedPost(subscription, payload, item.messageId);
          if (response.status >= 200 && response.status < 300) {
            item.done = true;
            item.accepted = true;
            advanceWatermark(subscription);
            return;
          }
          if (response.status === 410 || response.status === 413) {
            item.done = true;
            if (response.status === 413 && item.event.type !== 'gap') {
              item.lost = true;
              if (!subscription.gapPending) subscription.gapCursor = subscription.latestCursor;
              subscription.gapPending = true;
            }
            advanceWatermark(subscription);
            return;
          }
        } catch { /* Retry connection failures within the delivery limit. */ }
        if (attempt < maxAttempts) {
          try { await delay(Math.min(retryBaseMs * (2 ** (attempt - 1)), retryMaxMs), undefined, {signal: subscription.controller.signal}); }
          catch (error) { if (!subscription.controller.signal.aborted) throw error; }
        }
      }
      item.done = true;
      if (!closed && !subscription.controller.signal.aborted && item.event.type !== 'gap') {
        item.lost = true;
        if (!subscription.gapPending) subscription.gapCursor = subscription.latestCursor;
        subscription.gapPending = true;
      }
      advanceWatermark(subscription);
    } finally {
      pendingTotal--;
      releaseWorker();
    }
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
    if (!subscription.gapPending || closed || subscription.expiresAt <= now()
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
  const ensureVerified = async (principal, url, secret, id) => {
    const endpointKey = canonical([principal, url]);
    return withLock(verificationLocks, endpointKey, async () => {
      if ((verifiedEndpoints.get(endpointKey) ?? 0) > now()) return;
      const challenge = randomBytes(24).toString('base64url');
      const probe = {url, id, secret};
      let response;
      try {
        response = await signedPost(probe, {type: 'verification', challenge}, `msg_verification_${randomBytes(12).toString('hex')}`);
      } catch (error) {
        if (error.code === 'ETIMEDOUT') throw new WebhookCallbackError('timeout');
        if (typeof error.code === 'string' && /CERT|TLS|SSL/.test(error.code)) throw new WebhookCallbackError('tls_error');
        throw new WebhookCallbackError('connection_refused');
      }
      let echoed;
      try { echoed = JSON.parse(response.body.toString('utf8')).challenge; } catch { echoed = undefined; }
      if (response.status < 200 || response.status >= 300 || !constantTimeStringEqual(echoed, challenge)) throw new WebhookCallbackError('challenge_failed');
      if (verifiedEndpoints.size >= 1000) verifiedEndpoints.delete(verifiedEndpoints.keys().next().value);
      verifiedEndpoints.set(endpointKey, now() + maxTtlMs);
    });
  };
  const validateInput = input => {
    if (!input || typeof input !== 'object' || typeof input.principal !== 'string' || !input.principal.trim()) throw new Error('Authenticated principal required');
    if (typeof input.name !== 'string' || !input.name.trim()) throw new Error('Event name required');
    if (!input.arguments || typeof input.arguments !== 'object' || Array.isArray(input.arguments)) throw new Error('Subscription arguments must be an object');
    if (typeof input.url !== 'string') throw new Error('Invalid webhook URL');
    validateCallbackUrl(input.url);
    validateSecret(input.secret);
    const requestedTtl = input.ttlMs === null || input.ttlMs === undefined ? defaultTtlMs : input.ttlMs;
    if (!Number.isSafeInteger(requestedTtl) || requestedTtl < 0) throw new Error('Invalid webhook TTL');
    return {ttlMs: Math.min(Math.max(requestedTtl, 1000), maxTtlMs)};
  };

  return {
    async subscribe(input) {
      if (closed) throw new Error('Webhook delivery is closed');
      sweep();
      const {ttlMs} = validateInput(input);
      const url = new URL(input.url).toString();
      const key = keyFor({...input, url});
      return withLock(subscriptionLocks, key, async () => {
        if (closed) throw new Error('Webhook delivery is closed');
        sweep();
        let subscription = subscriptions.get(key);
        const secret = validateSecret(input.secret);
        if (!subscription && subscriptions.size >= maxSubscriptions) throw new WebhookLimitError();
        if (!allowUrl(url, input)) await ensureVerified(input.principal, url, secret, idFor(key));
        if (closed) throw new Error('Webhook delivery is closed');
        sweep();
        subscription = subscriptions.get(key);
        if (!subscription && subscriptions.size >= maxSubscriptions) throw new WebhookLimitError();
        if (!subscription) {
          const currentCursor = input.cursor === null || input.cursor === undefined
            ? getCurrentCursor(input)
            : undefined;
          const recovered = currentCursor === undefined ? replay({cursor: input.cursor, maxAgeMs: input.maxAgeMs, input}) : {events: [], cursor: currentCursor, truncated: false};
          subscription = {
            id: idFor(key), key, principal: input.principal, name: input.name,
            arguments: structuredClone(input.arguments), url, secret,
            expiresAt: now() + ttlMs, cursor: recovered.cursor ?? null,
            latestCursor: recovered.headCursor ?? recovered.cursor ?? null, pending: [], gapPending: false,
            controller: new AbortController(),
          };
          subscriptions.set(key, subscription);
          scheduleExpiry(subscription);
          for (const event of recovered.events ?? []) if (matches(input, event)) enqueue(subscription, event);
          return {id: subscription.id, refreshBefore: new Date(subscription.expiresAt).toISOString(), cursor: safeCursor(subscription), truncated: Boolean(recovered.truncated)};
        }
        subscription.secret = secret;
        subscription.expiresAt = now() + ttlMs;
        scheduleExpiry(subscription);
        return {id: subscription.id, refreshBefore: new Date(subscription.expiresAt).toISOString(), cursor: safeCursor(subscription), truncated: false};
      });
    },
    async unsubscribe(input) {
      if (!input || typeof input.principal !== 'string' || !input.principal.trim()) throw new Error('Authenticated principal required');
      if (typeof input.name !== 'string' || typeof input.url !== 'string' || !input.arguments || typeof input.arguments !== 'object') throw new Error('Invalid webhook subscription key');
      const key = keyFor({...input, url: new URL(input.url).toString()});
      return withLock(subscriptionLocks, key, () => {
        const subscription = subscriptions.get(key);
        if (!subscription) return false;
        remove(subscription);
        return true;
      });
    },
    emit(event) {
      if (closed) throw new Error('Webhook delivery is closed');
      if (!event || typeof event.name !== 'string' || typeof event.eventId !== 'string' || typeof event.timestamp !== 'string' || !event.data || typeof event.data !== 'object') throw new Error('Invalid event');
      sweep();
      let accepted = 0;
      for (const subscription of subscriptions.values()) {
        const item = {...structuredClone(event), cursor: cursorForEvent(subscription, event)};
        if (subscription.gapPending) {
          subscription.gapCursor = item.cursor;
          continue;
        }
        if (!matches(subscription, event)) {
          const skipped = {cursor: item.cursor ?? subscription.latestCursor, done: true};
          if (subscription.pending.at(-1)?.done) subscription.pending[subscription.pending.length - 1] = skipped;
          else subscription.pending.push(skipped);
          subscription.latestCursor = skipped.cursor;
          advanceWatermark(subscription);
          continue;
        }
        if (enqueue(subscription, item)) accepted++;
      }
      return accepted;
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
  };
}
