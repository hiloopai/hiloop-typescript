/** Ergonomic sandbox lifecycle helpers over the generated hiloop client. */

import type { Client } from "./client/types.gen";
import {
  runtimeServiceCreateSandbox,
  runtimeServiceCreateSnapshot,
  runtimeServiceExecuteSandbox,
  runtimeServiceExposePort,
  runtimeServiceFileFromArtifact,
  runtimeServiceFileToArtifact,
  runtimeServiceForkSandbox,
  runtimeServiceGetOperation,
  runtimeServiceGetSandbox,
  runtimeServiceListExposedPorts,
  runtimeServiceStartExecution,
  runtimeServiceUnexposePort,
} from "./sdk.gen";
import { type ExecOutputEvent, type SdkResult, type StreamError, streamExecution } from "./sse";
import type {
  CommandSpec,
  CreateSandboxRequest,
  CreateSnapshotRequest,
  ErrorBody,
  ExecuteResult,
  ExposePortRequest,
  ExposePortResponse,
  FileFromArtifactResult,
  FileToArtifactResult,
  ForkSandboxRequest,
  ListExposedPortsResponse,
  Operation,
  Sandbox as SandboxModel,
  SnapshotResult,
  UnexposePortResponse,
} from "./types.gen";

const TERMINAL_OPERATION_STATES = new Set(["succeeded", "failed", "cancelled"]);

export interface WaitOptions {
  timeoutMs?: number;
  pollIntervalMs?: number;
  idempotencyKey?: string;
}

/** A submitted sandbox operation reached a failed or cancelled terminal state. */
export class OperationFailed extends Error {
  readonly code?: string;

  constructor(readonly operation: Operation) {
    super(
      `operation ${operation.id ?? "unknown"} ended ${operation.state ?? "unknown"}: ${operation.error?.message ?? operation.error?.code ?? "unknown error"}`,
    );
    this.name = "OperationFailed";
    this.code = operation.error?.code;
  }
}

function required<T>(value: T | null | undefined, field: string): T {
  if (value === undefined || value === null)
    throw new Error(`API response is missing required ${field}`);
  return value;
}

function passError<T>(result: {
  data: undefined;
  error: ErrorBody;
  request?: Request;
  response?: Response;
}): SdkResult<T> {
  return result;
}

function success<T>(data: T, source: { request?: Request; response?: Response }): SdkResult<T> {
  return { data, error: undefined, request: source.request, response: source.response };
}

function idempotencyKey(value?: string): string {
  return value ?? globalThis.crypto.randomUUID();
}

const sleep = (milliseconds: number) =>
  milliseconds === 0
    ? Promise.resolve()
    : new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

async function waitOperation(
  client: Client,
  operationId: string,
  options: WaitOptions,
): Promise<SdkResult<Operation>> {
  const timeoutMs = options.timeoutMs ?? 300_000;
  const pollIntervalMs = options.pollIntervalMs ?? 250;
  const deadline = Date.now() + timeoutMs;
  while (true) {
    const result = await runtimeServiceGetOperation({ client, path: { id: operationId } });
    if (result.error !== undefined) return passError(result);
    const operation = required(result.data.operation, "operation");
    if (operation.state && TERMINAL_OPERATION_STATES.has(operation.state)) {
      if (operation.state !== "succeeded") throw new OperationFailed(operation);
      return success(operation, result);
    }
    if (Date.now() >= deadline) {
      throw new Error(`operation ${operationId} did not finish within ${timeoutMs}ms`);
    }
    await sleep(pollIntervalMs);
  }
}

function resultOf<T>(operation: Operation, field: keyof NonNullable<Operation["result"]>): T {
  const result = required(operation.result, "operation.result");
  return required(result[field] as T | undefined, `operation.result.${field}`);
}

/** A connected sandbox with lifecycle, execution, file, snapshot, and port helpers. */
export class Sandbox {
  private constructor(
    readonly client: Client,
    readonly model: SandboxModel,
  ) {}

  /** The sandbox's canonical id. */
  get id(): string {
    return required(this.model.id, "sandbox.id");
  }

  /** Create a sandbox, wait for its operation, and return the ready sandbox. */
  static async create(options: {
    client: Client;
    body: CreateSandboxRequest;
    timeoutMs?: number;
    pollIntervalMs?: number;
    idempotencyKey?: string;
  }): Promise<SdkResult<Sandbox>> {
    const created = await runtimeServiceCreateSandbox({
      client: options.client,
      body: options.body,
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
    });
    if (created.error !== undefined) return passError(created);
    const operation = required(created.data.operation, "create operation");
    const settled = await waitOperation(
      options.client,
      required(operation.id, "operation.id"),
      options,
    );
    if (settled.error !== undefined) return settled;
    const sandbox = required(created.data.sandbox, "sandbox");
    return Sandbox.connect({ client: options.client, id: required(sandbox.id, "sandbox.id") });
  }

  /** Connect to an existing sandbox by id. */
  static async connect(options: { client: Client; id: string }): Promise<SdkResult<Sandbox>> {
    const result = await runtimeServiceGetSandbox({
      client: options.client,
      path: { id: options.id },
    });
    if (result.error !== undefined) return passError(result);
    return success(new Sandbox(options.client, required(result.data.sandbox, "sandbox")), result);
  }

  /** Run a buffered command and wait for its typed execution result. */
  async exec(
    command: CommandSpec,
    options: WaitOptions & { stdin?: string } = {},
  ): Promise<SdkResult<ExecuteResult>> {
    const submitted = await runtimeServiceExecuteSandbox({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { sandbox_id: this.id, command, stdin: options.stdin },
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
    });
    if (submitted.error !== undefined) return passError(submitted);
    const operation = required(submitted.data.operation, "execute operation");
    const settled = await waitOperation(
      this.client,
      required(operation.id, "operation.id"),
      options,
    );
    if (settled.error !== undefined) return settled;
    return success(resultOf<ExecuteResult>(settled.data, "execute"), settled);
  }

  /** Start an execution and stream typed stdout, stderr, exit, and status events. */
  async execStream(
    command: CommandSpec,
    options: { stdin?: string; pty?: boolean; idempotencyKey?: string; signal?: AbortSignal } = {},
  ): Promise<SdkResult<AsyncGenerator<ExecOutputEvent | StreamError>>> {
    const started = await runtimeServiceStartExecution({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { sandbox_id: this.id, command, stdin: options.stdin, pty: options.pty },
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
      signal: options.signal,
    });
    if (started.error !== undefined) return passError(started);
    const execution = required(started.data.execution, "execution");
    return streamExecution({
      client: this.client,
      executionId: required(execution.id, "execution.id"),
      signal: options.signal,
    });
  }

  /** Fork this sandbox, wait for the child, and return it connected. */
  async fork(body: ForkSandboxRequest, options: WaitOptions = {}): Promise<SdkResult<Sandbox>> {
    const submitted = await runtimeServiceForkSandbox({
      client: this.client,
      path: { source_sandbox_id: this.id },
      body: { ...body, source_sandbox_id: this.id },
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
    });
    if (submitted.error !== undefined) return passError(submitted);
    const operation = required(submitted.data.operation, "fork operation");
    const settled = await waitOperation(
      this.client,
      required(operation.id, "operation.id"),
      options,
    );
    if (settled.error !== undefined) return settled;
    const sandbox = required(submitted.data.sandbox, "sandbox");
    return Sandbox.connect({ client: this.client, id: required(sandbox.id, "sandbox.id") });
  }

  /** Capture a snapshot and wait for its typed result. */
  async snapshot(
    body: CreateSnapshotRequest = {},
    options: WaitOptions = {},
  ): Promise<SdkResult<SnapshotResult>> {
    const submitted = await runtimeServiceCreateSnapshot({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { ...body, sandbox_id: this.id },
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
    });
    if (submitted.error !== undefined) return passError(submitted);
    const operation = required(submitted.data.operation, "snapshot operation");
    const settled = await waitOperation(
      this.client,
      required(operation.id, "operation.id"),
      options,
    );
    if (settled.error !== undefined) return settled;
    return success(resultOf<SnapshotResult>(settled.data, "snapshot"), settled);
  }

  /** Archive a sandbox file and return its artifact result. */
  async fileToArtifact(
    path: string,
    options: WaitOptions & { mediaType?: string } = {},
  ): Promise<SdkResult<FileToArtifactResult>> {
    const submitted = await runtimeServiceFileToArtifact({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { sandbox_id: this.id, path, media_type: options.mediaType },
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
    });
    if (submitted.error !== undefined) return passError(submitted);
    const operation = required(submitted.data.operation, "file export operation");
    const settled = await waitOperation(
      this.client,
      required(operation.id, "operation.id"),
      options,
    );
    if (settled.error !== undefined) return settled;
    return success(resultOf<FileToArtifactResult>(settled.data, "file_to_artifact"), settled);
  }

  /** Materialize an artifact into this sandbox and return the move result. */
  async fileFromArtifact(
    artifactId: string,
    path: string,
    options: WaitOptions = {},
  ): Promise<SdkResult<FileFromArtifactResult>> {
    const submitted = await runtimeServiceFileFromArtifact({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { sandbox_id: this.id, artifact_id: artifactId, path },
      headers: { "idempotency-key": idempotencyKey(options.idempotencyKey) },
    });
    if (submitted.error !== undefined) return passError(submitted);
    const operation = required(submitted.data.operation, "file import operation");
    const settled = await waitOperation(
      this.client,
      required(operation.id, "operation.id"),
      options,
    );
    if (settled.error !== undefined) return settled;
    return success(resultOf<FileFromArtifactResult>(settled.data, "file_from_artifact"), settled);
  }

  /** Expose one guest port through the preview edge. */
  async exposePort(
    port: number,
    options: { authMode?: ExposePortRequest["auth_mode"] } = {},
  ): Promise<SdkResult<ExposePortResponse>> {
    return runtimeServiceExposePort({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { sandbox_id: this.id, port, auth_mode: options.authMode },
    });
  }

  /** Revoke one exposed guest port. */
  async unexposePort(port: number): Promise<SdkResult<UnexposePortResponse>> {
    return runtimeServiceUnexposePort({
      client: this.client,
      path: { sandbox_id: this.id },
      body: { sandbox_id: this.id, port },
    });
  }

  /** List this sandbox's active port exposures. */
  async listExposedPorts(): Promise<SdkResult<ListExposedPortsResponse>> {
    return runtimeServiceListExposedPorts({
      client: this.client,
      path: { sandbox_id: this.id },
    });
  }
}
