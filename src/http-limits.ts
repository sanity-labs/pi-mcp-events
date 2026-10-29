import {limitExceeded} from './errors.ts';

export type ResponseLimits = {maxResponseBytes: number; maxPollBytes: number};
export const defaultResponseLimits: ResponseLimits = {maxResponseBytes: 1024 * 1024, maxPollBytes: 256 * 1024};

export function boundedResponse(response: Response, limit: number, streamLimit: number, onLimit: (error: Error) => void, allowStreaming = true): Response {
  if (!response.body) return response;
  const streaming = allowStreaming && response.ok && response.status !== 202
    && response.headers.get('content-type')?.split(';')[0].trim().toLowerCase() === 'text/event-stream';
  const maxBytes = streaming ? streamLimit : limit;
  let bytes = 0;
  let lineBytes = 0;
  let afterCR = false;
  let frameEnded = false;
  const count = () => {
    if (++bytes > maxBytes) throw limitExceeded('maxStreamFrameBytes', maxBytes);
  };
  const scan = (chunk: Uint8Array) => {
    if (!streaming) {
      bytes += chunk.byteLength;
      if (bytes > maxBytes) throw limitExceeded('maxResponseBytes', maxBytes);
      return;
    }
    for (const byte of chunk) {
      if (afterCR) {
        afterCR = false;
        if (byte === 10) {
          count();
          if (frameEnded) { bytes = 0; frameEnded = false; }
          continue;
        }
        if (frameEnded) { bytes = 0; frameEnded = false; }
      }
      count();
      if (byte === 13) {
        afterCR = true;
        frameEnded = lineBytes === 0;
        lineBytes = 0;
      } else if (byte === 10) {
        if (lineBytes === 0) bytes = 0;
        lineBytes = 0;
      } else lineBytes++;
    }
  };
  const body = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      try { scan(chunk); }
      catch (error) { onLimit(error as Error); throw error; }
      controller.enqueue(chunk);
    },
  }));
  const bounded = new Response(body, {status: response.status, statusText: response.statusText, headers: response.headers});
  for (const key of ['url', 'redirected', 'type'] as const) Object.defineProperty(bounded, key, {value: response[key]});
  return bounded;
}
