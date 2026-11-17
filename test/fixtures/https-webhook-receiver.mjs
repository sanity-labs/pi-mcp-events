import {execFileSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer} from 'node:https';
import {randomBytes} from 'node:crypto';
import {Webhook} from 'standardwebhooks';

const defaultResponse = () => ({status: 204});

function responseFor(value) {
  const result = value ?? {status: 204};
  if (!Number.isInteger(result.status) || result.status < 100 || result.status > 599) {
    throw new Error('Webhook response status must be an HTTP status code');
  }
  return result;
}

async function readBody(request, limit) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error('Webhook request body is too large'), {status: 413});
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function send(response, result) {
  const headers = {'cache-control': 'no-store', ...result.headers};
  if (result.body === undefined || result.body === null) {
    response.writeHead(result.status, headers);
    response.end();
    return;
  }
  if (Buffer.isBuffer(result.body) || typeof result.body === 'string') {
    response.writeHead(result.status, headers);
    response.end(result.body);
    return;
  }
  response.writeHead(result.status, {'content-type': 'application/json', ...headers});
  response.end(JSON.stringify(result.body));
}

/**
 * Starts a loopback-only HTTPS callback receiver for webhook integration tests.
 * The returned `ca` is the generated self-signed certificate; clients should
 * trust it explicitly through their request's `ca` option.
 */
export async function startHttpsWebhookReceiver({
  secret = `whsec_${randomBytes(32).toString('base64')}`,
  response = defaultResponse,
  challengeResponse = challenge => ({status: 200, body: {challenge}}),
  maxBodyBytes = 262_144,
} = {}) {
  if (!Number.isInteger(maxBodyBytes) || maxBodyBytes < 1) throw new Error('maxBodyBytes must be a positive integer');
  const verifier = new Webhook(secret);
  const directory = mkdtempSync(join(tmpdir(), 'pi-events-webhook-'));
  const keyPath = join(directory, 'key.pem');
  const certPath = join(directory, 'cert.pem');
  try {
    execFileSync('openssl', [
      'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyPath,
      '-out', certPath, '-days', '1', '-subj', '/CN=127.0.0.1',
      '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost',
    ], {stdio: 'ignore'});
  } catch (error) {
    rmSync(directory, {recursive: true, force: true});
    throw error;
  }
  const key = readFileSync(keyPath);
  const cert = readFileSync(certPath);
  const requests = [];
  const server = createServer({key, cert}, (request, responseStream) => {
    void (async () => {
      let rawBody;
      try {
        rawBody = await readBody(request, maxBodyBytes);
      } catch (error) {
        request.resume();
        send(responseStream, {status: error.status ?? 400});
        return;
      }

      const record = {
        method: request.method,
        url: request.url,
        headers: {...request.headers},
        rawBody,
        body: undefined,
        verified: false,
        verificationError: undefined,
      };
      try {
        verifier.verify(rawBody, request.headers);
        record.verified = true;
      } catch (error) {
        record.verificationError = error.message;
      }
      try { record.body = JSON.parse(rawBody.toString('utf8')); }
      catch { /* Keep malformed payloads available as rawBody for assertions. */ }
      requests.push(record);

      if (!record.verified) {
        send(responseStream, {status: 401});
        return;
      }
      const challenge = record.body?.type === 'verification';
      const selected = challenge && challengeResponse !== false
        ? await challengeResponse(record.body.challenge, record)
        : await response(record);
      send(responseStream, responseFor(selected));
    })().catch(() => {
      if (!responseStream.headersSent) send(responseStream, {status: 500});
      else responseStream.destroy();
    });
  });

  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
  } catch (error) {
    rmSync(directory, {recursive: true, force: true});
    throw error;
  }

  const address = server.address();
  if (!address || typeof address === 'string') {
    await new Promise(resolve => server.close(resolve));
    rmSync(directory, {recursive: true, force: true});
    throw new Error('HTTPS webhook receiver did not bind to a TCP port');
  }
  let closed = false;
  return {
    url: `https://127.0.0.1:${address.port}/webhook`,
    secret,
    ca: cert,
    requests,
    clearRequests() { requests.length = 0; },
    async close() {
      if (closed) return;
      closed = true;
      server.closeAllConnections();
      await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
      rmSync(directory, {recursive: true, force: true});
    },
  };
}
