/** Typed helpers for hiloop's hand-authored Server-Sent Events routes. */

import type { Client } from "./client/types.gen";
import type { ErrorBody } from "./types.gen";

export type SdkResult<T> =
  | { data: T; error: undefined; request?: Request; response?: Response }
  | { data: undefined; error: ErrorBody; request?: Request; response?: Response };

/** One canonical telemetry event plus its SSE resume metadata. */
export interface RunTailEvent {
  data: Record<string, unknown>;
  id?: string;
  event?: string;
  retry?: number;
}

/** The terminal disposition of a streamed execution. */
export interface ExecExit {
  exitCode: number;
  signal: number;
}

/** A stdout chunk, stderr chunk, or terminal exit from an execution. */
export interface ExecOutputEvent {
  stdout?: Uint8Array;
  stderr?: Uint8Array;
  exit?: ExecExit;
}

/** A gRPC status delivered after an execution stream has opened. */
export interface StreamError {
  code: number;
  message: string;
}

interface DecodedSseEvent {
  data: unknown;
  id?: string;
  event?: string;
  retry?: number;
}

interface SseFields {
  data: string[];
  id?: string;
  event?: string;
  retry?: number;
}

function parseSseLine(line: string, fields: SseFields): void {
  if (!line || line.startsWith(":")) return;
  const separator = line.indexOf(":");
  const field = separator === -1 ? line : line.slice(0, separator);
  const raw = separator === -1 ? "" : line.slice(separator + 1);
  const value = raw.startsWith(" ") ? raw.slice(1) : raw;
  if (field === "data") fields.data.push(value);
  else if (field === "id") fields.id = value;
  else if (field === "event") fields.event = value;
  else if (field === "retry") {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isNaN(parsed)) fields.retry = parsed;
  }
}

function parseSseData(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

class SseDecoder {
  private buffer = "";
  private pendingCr = false;

  feed(chunk: string, final = false): DecodedSseEvent[] {
    let input = chunk;
    let normalized = "";
    if (this.pendingCr && (input || final)) {
      normalized = "\n";
      if (input.startsWith("\n")) input = input.slice(1);
      this.pendingCr = false;
    }
    if (!final && input.endsWith("\r")) {
      input = input.slice(0, -1);
      this.pendingCr = true;
    }
    this.buffer += normalized + input.replace(/\r\n?/g, "\n");
    const blocks = this.buffer.split("\n\n");
    this.buffer = blocks.pop() ?? "";
    return blocks.flatMap((block) => {
      const event = this.decodeBlock(block);
      return event ? [event] : [];
    });
  }

  private decodeBlock(block: string): DecodedSseEvent | undefined {
    const fields: SseFields = { data: [] };
    for (const line of block.split("\n")) parseSseLine(line, fields);
    if (fields.data.length === 0) return undefined;
    return {
      data: parseSseData(fields.data.join("\n")),
      id: fields.id,
      event: fields.event,
      retry: fields.retry,
    };
  }
}

async function* decodeSse(body: ReadableStream<Uint8Array>): AsyncGenerator<DecodedSseEvent> {
  const reader = body.getReader();
  const text = new TextDecoder();
  const decoder = new SseDecoder();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      for (const event of decoder.feed(text.decode(value, { stream: true }))) yield event;
    }
    for (const event of decoder.feed(text.decode(), true)) yield event;
  } finally {
    reader.releaseLock();
  }
}

async function openSse(
  client: Client,
  options: {
    url: string;
    path?: Record<string, unknown>;
    query?: Record<string, unknown>;
    signal?: AbortSignal;
  },
): Promise<SdkResult<ReadableStream<Uint8Array>>> {
  const result = await client.get<{ 200: ReadableStream<Uint8Array> }, { default: ErrorBody }>({
    ...options,
    headers: { Accept: "text/event-stream" },
    parseAs: "stream",
    security: [{ type: "http", scheme: "bearer" }],
  });
  if (result.error !== undefined) return result;
  if (!result.data) throw new Error("SSE response has no body");
  return {
    data: result.data,
    error: undefined,
    request: result.request,
    response: result.response,
  };
}

function decodeBase64(value: unknown): Uint8Array {
  if (typeof value !== "string") throw new Error("execution chunk is not base64 text");
  const binary = globalThis.atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function decodeExecutionEvent(data: unknown): ExecOutputEvent | StreamError | undefined {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("execution stream frame is not a JSON object");
  }
  const value = data as Record<string, unknown>;
  if (Object.keys(value).length === 0) return undefined;
  if (typeof value.code === "number") {
    return { code: value.code, message: typeof value.message === "string" ? value.message : "" };
  }
  if ("stdoutChunk" in value) return { stdout: decodeBase64(value.stdoutChunk) };
  if ("stderrChunk" in value) return { stderr: decodeBase64(value.stderrChunk) };
  if (value.exit && typeof value.exit === "object" && !Array.isArray(value.exit)) {
    const exit = value.exit as Record<string, unknown>;
    return { exit: { exitCode: Number(exit.exitCode ?? 0), signal: Number(exit.signal ?? 0) } };
  }
  throw new Error("execution stream frame has no stdoutChunk, stderrChunk, exit, or status");
}

/** Open an execution stream, preserving typed HTTP errors at connection time. */
export async function streamExecution(options: {
  client: Client;
  executionId: string;
  signal?: AbortSignal;
}): Promise<SdkResult<AsyncGenerator<ExecOutputEvent | StreamError>>> {
  const opened = await openSse(options.client, {
    url: "/v1/executions/{execution_id}:stream",
    path: { execution_id: options.executionId },
    signal: options.signal,
  });
  if (opened.error !== undefined) return opened;
  const body = opened.data;

  async function* events(): AsyncGenerator<ExecOutputEvent | StreamError> {
    for await (const frame of decodeSse(body)) {
      const event = decodeExecutionEvent(frame.data);
      if (event) yield event;
    }
  }

  return { data: events(), error: undefined, request: opened.request, response: opened.response };
}

/** Tail canonical telemetry events for one run, including resumable SSE cursor ids. */
export async function tailRun(options: {
  client: Client;
  runId: string;
  lineagePath?: string;
  signal?: string;
  cursor?: string;
  abortSignal?: AbortSignal;
}): Promise<SdkResult<AsyncGenerator<RunTailEvent>>> {
  const opened = await openSse(options.client, {
    url: "/v1/telemetry/tail",
    query: {
      run_id: options.runId,
      lineage_path: options.lineagePath,
      signal: options.signal,
      cursor: options.cursor,
    },
    signal: options.abortSignal,
  });
  if (opened.error !== undefined) return opened;
  const body = opened.data;

  async function* events(): AsyncGenerator<RunTailEvent> {
    for await (const frame of decodeSse(body)) {
      if (!frame.data || typeof frame.data !== "object" || Array.isArray(frame.data)) {
        throw new Error("run tail frame is not a JSON object");
      }
      yield {
        data: frame.data as Record<string, unknown>,
        id: frame.id,
        event: frame.event,
        retry: frame.retry,
      };
    }
  }

  return { data: events(), error: undefined, request: opened.request, response: opened.response };
}
