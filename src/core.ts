import { AsyncLocalStorage } from "node:async_hooks";
import { env } from "node:process";
import { openAdapter, type DatabaseAdapter } from "./adapters.js";
import {
  credentialSource,
  injectPassword,
  validateCredentialRef,
  validatePasswordReferenceTarget,
} from "./connection.js";
import {
  asStateQLError,
  CredentialResolutionError,
  isTransientDatabaseError,
  StateQLError,
} from "./errors.js";
import {
  AdapterExecutionError,
  createAdapterContext,
  stoppedStateQLError,
  type AdapterContext,
} from "./execution.js";
import type {
  ConnectionRecord,
  ResultRecord,
  SessionRecord,
  StateStore,
  TransactionRecord,
} from "./store.js";
import type {
  CommandExecutionContext,
  CommandOrigin,
  CredentialAccess,
  CredentialOperation,
  CredentialRequest,
  CredentialResolver,
  CredentialSource,
  ExecutionOptions,
  Failure,
  HistoryCategory,
  Response,
  ResultData,
  StateConfidence,
  Success,
  Warning,
} from "./types.js";
import { compactRows, redact } from "./util.js";

export interface ActionResult<T> {
  data: T;
  handle?: string;
  executed?: boolean;
  cached?: boolean;
  warnings?: Warning[];
  stateVersion?: string;
  confidence?: StateConfidence;
  session?: SessionRecord;
}

export interface CoreSettings {
  previewRows: number;
  cacheTtlSeconds: number;
  resultTtlSeconds: number;
  maxCellCharacters: number;
  maxResultRows: number;
  maxResultBytes: number;
  timeoutMs: number;
  credentialTimeoutMs: number;
}

type NativeDriver = "mongodb" | "redis";
type SqlCommand = "query" | "exec" | "plan";

const NATIVE_METHODS: Record<NativeDriver, Record<SqlCommand, string>> = {
  mongodb: { query: "mongoQuery", exec: "mongoExec", plan: "mongoPlan" },
  redis: { query: "redisQuery", exec: "redisExec", plan: "redisPlan" },
};

/** State and checks shared by every command; one instance per StateQL. */
export class Core {
  readonly commandContexts = new AsyncLocalStorage<CommandExecutionContext>();

  constructor(
    readonly store: StateStore,
    readonly settings: CoreSettings,
    readonly sessionName: string,
    readonly actorId: string,
    readonly now: () => Date,
    readonly signal: AbortSignal | undefined,
    private readonly credentialResolver: CredentialResolver | undefined,
  ) {}

  withCommandContext<T>(supplied: CommandExecutionContext, action: () => T): T {
    return this.commandContexts.run(
      mergeCommandExecutionContext(this.commandContexts.getStore(), supplied),
      action,
    );
  }

  executionContext(options: ExecutionOptions): AdapterContext {
    return createAdapterContext(
      executionTimeout(options.timeoutMs ?? this.settings.timeoutMs),
      combineAbortSignals(
        options.signal,
        this.commandContexts.getStore()?.signal,
        this.signal,
      ),
    );
  }

  expiresAt(milliseconds: number): string {
    return new Date(this.now().getTime() + milliseconds).toISOString();
  }

  async openAdapter(
    connection: ConnectionRecord,
    context: AdapterContext,
    source: string,
  ): Promise<DatabaseAdapter> {
    try {
      return await openAdapter(connection, context, source);
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      throw new StateQLError(
        "CONNECTION_FAILED",
        safeCredentialErrorMessage(error, source),
        { retryable: true },
      );
    }
  }

  /** Resolves read credentials, then runs the action on a fresh adapter. */
  async withAdapter<T>(
    session: SessionRecord,
    connection: ConnectionRecord,
    operation: CredentialOperation,
    context: AdapterContext,
    action: (adapter: DatabaseAdapter) => Promise<T>,
  ): Promise<T> {
    const source = await this.resolveConnectionSource(connection, session, operation, "read", context);
    return this.runOnAdapter(connection, context, source, action);
  }

  /** Opens the adapter, maps driver errors to StateQL errors, and always closes it. */
  async runOnAdapter<T>(
    connection: ConnectionRecord,
    context: AdapterContext,
    source: string,
    action: (adapter: DatabaseAdapter) => Promise<T>,
  ): Promise<T> {
    const adapter = await this.openAdapter(connection, context, source);
    try {
      return await action(adapter);
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, true);
      throw new StateQLError(
        "QUERY_FAILED",
        safeCredentialErrorMessage(error, source),
        { retryable: isTransientDatabaseError(error), executed: true },
      );
    } finally {
      await closeAdapterQuietly(adapter);
    }
  }

  async run<T>(
    command: string,
    action: (session: SessionRecord) => Promise<ActionResult<T>>,
    historySql?: string | (() => string | undefined),
    historyTarget?: string,
  ): Promise<Response<T>> {
    const started = performance.now();
    const duration = () => Math.round((performance.now() - started) * 1000) / 1000;
    const commandContext = this.commandContexts.getStore();
    const entry = {
      actorId: this.actorId,
      origin: commandContext?.origin ?? "legacy",
      category: historyCategory(command),
      internal: commandContext?.internal ?? false,
      command,
      target: historyTarget,
    };
    const session = this.store.ensureSession(this.sessionName);
    if (!this.store.isSessionMember(session.id, this.actorId)) {
      const error = new StateQLError(
        "PERMISSION_DENIED",
        `Actor "${this.actorId}" is not attached to session "${session.name}".`,
      );
      return {
        ok: false,
        command_id: this.store.randomId("cmd"),
        session_id: session.id,
        error: error.details,
        meta: { duration_ms: duration() },
      } satisfies Failure;
    }
    const actorName = this.store.getActor(session.id, this.actorId)!.actor_name;
    try {
      const result = await action(session);
      const sqlText = typeof historySql === "function" ? historySql() : historySql;
      const history = this.store.addHistory({
        ...entry,
        sessionId: session.id,
        actorName,
        ...(result.handle ? { handle: result.handle } : {}),
        ...(sqlText !== undefined ? { sql: sqlText } : {}),
        executed: result.executed ?? false,
        cached: result.cached ?? false,
        success: true,
      });
      return {
        ok: true,
        command_id: history.id,
        session_id: (result.session ?? session).id,
        data: result.data,
        warnings: result.warnings ?? [],
        meta: {
          duration_ms: duration(),
          ...(result.stateVersion ? { state_version: result.stateVersion } : {}),
          ...(result.confidence ? { state_confidence: result.confidence } : {}),
        },
      } satisfies Success<T>;
    } catch (error) {
      const stateqlError = asStateQLError(error);
      const sqlText = typeof historySql === "function" ? historySql() : historySql;
      const history = this.store.addHistory({
        ...entry,
        sessionId: session.id,
        actorName,
        ...(sqlText !== undefined ? { sql: sqlText } : {}),
        executed: stateqlError.details.executed,
        cached: false,
        success: false,
        errorCode: stateqlError.details.code,
      });
      return {
        ok: false,
        command_id: history.id,
        session_id: session.id,
        error: stateqlError.details,
        meta: { duration_ms: duration() },
      } satisfies Failure;
    }
  }

  /** Runs a command on a stored result that has not expired. */
  withResult<T>(
    command: string,
    idOrAlias: string,
    action: (result: ResultRecord, session: SessionRecord) => Promise<ActionResult<T>>,
  ): Promise<Response<T>> {
    return this.run(command, async (session) => {
      const result = this.requireResult(idOrAlias, session);
      if (Date.parse(result.expires_at) <= this.now().getTime()) {
        throw new StateQLError(
          "RESULT_EXPIRED",
          `Result "${result.id}" has expired.`,
          { retryable: true, suggestedAction: "Run the original query again." },
        );
      }
      return action(result, session);
    });
  }

  async resolveConnectionSource(
    connection: ConnectionRecord,
    session: SessionRecord,
    operation: CredentialOperation,
    access: CredentialAccess,
    context: AdapterContext,
  ): Promise<string> {
    const references = [connection.secret_env, connection.credential_ref, connection.password_ref]
      .filter((value) => value !== null);
    if (references.length > 1) {
      throw new StateQLError("STATE_CORRUPTED", "Connection has ambiguous credential references.");
    }
    const details = {
      connection: {
        id: connection.id,
        name: connection.name,
        driver: connection.driver,
        database: connection.database_name,
        readOnly: Boolean(connection.read_only),
      },
    };
    if (connection.password_ref !== null) {
      validateCredentialRef(connection.password_ref);
      const driver = validatePasswordReferenceTarget(connection.source);
      if (driver !== connection.driver) {
        throw new StateQLError("STATE_CORRUPTED", "Password-reference target driver does not match the stored connection.");
      }
      const password = await this.resolveCredential(
        connection.password_ref,
        "password_ref",
        session,
        operation,
        access,
        context,
        details,
        connection.source,
      );
      return injectPassword(connection.source, password).source;
    }
    const reference = connection.secret_env ?? connection.credential_ref;
    if (!reference) return connection.source;
    const source: Exclude<CredentialSource, "password_ref"> = connection.secret_env
      ? "secret_env"
      : "credential_ref";
    const value = await this.resolveCredential(reference, source, session, operation, access, context, details);
    return credentialSource(value, connection.driver, source).source;
  }

  async resolveCredential(
    reference: string,
    source: CredentialSource,
    session: SessionRecord,
    operation: CredentialOperation,
    access: CredentialAccess,
    context: AdapterContext,
    details: Pick<CredentialRequest, "profile" | "requestedReadOnly" | "connection"> = {},
    passwordTarget?: string,
  ): Promise<string> {
    const resolver = this.credentialResolver;
    const credentialContext = createAdapterContext(
      this.settings.credentialTimeoutMs,
      context.signal,
    );
    let value: string | undefined;
    if (!resolver) {
      if (credentialContext.signal?.aborted) {
        throw credentialStateQLError(reference, new CredentialResolutionError("cancelled"));
      }
      if (credentialContext.deadline <= Date.now()) {
        throw credentialStateQLError(reference, new CredentialResolutionError("timeout"));
      }
      if (source === "secret_env") value = env[reference];
    } else {
      const baseRequest = {
        reference,
        actorId: this.actorId,
        session: { id: session.id, name: session.name },
        operation,
        access,
        ...(context.signal ? { signal: context.signal } : {}),
        ...details,
      };
      const request: CredentialRequest = source === "password_ref"
        ? { ...baseRequest, source, target: passwordTarget! }
        : { ...baseRequest, source };
      try {
        value = await resolveCredentialBeforeDeadline(resolver, request, credentialContext);
      } catch (error) {
        throw credentialStateQLError(reference, error);
      }
    }
    const missing = value === undefined || (source !== "password_ref" && value === "");
    if (missing) {
      throw credentialStateQLError(reference, new CredentialResolutionError("unavailable"));
    }
    // The database deadline starts after credential resolution, which may wait for a person.
    context.deadline = Date.now() + (context.timeoutMs ?? this.settings.timeoutMs);
    return value!;
  }

  requireConnection(session: SessionRecord): ConnectionRecord {
    const connection = this.store.activeConnection(session);
    if (!connection) {
      throw new StateQLError(
        "CONNECTION_NOT_FOUND",
        "No active connection.",
        { suggestedAction: "Run stql connect first." },
      );
    }
    return connection;
  }

  requireSqlConnection(session: SessionRecord, command: SqlCommand): ConnectionRecord {
    const connection = this.requireConnection(session);
    rejectSqlOnNativeConnection(connection, command);
    return connection;
  }

  requireNativeConnection(
    session: SessionRecord,
    driver: NativeDriver,
    command: SqlCommand,
  ): ConnectionRecord {
    const connection = this.requireConnection(session);
    if (connection.driver !== driver) {
      throw new StateQLError(
        "INVALID_COMMAND",
        `${NATIVE_METHODS[driver][command]} requires an active ${driverDisplayName(driver)} connection.`,
      );
    }
    return connection;
  }

  requireWritable(connection: ConnectionRecord): void {
    if (!connection.read_only) return;
    throw new StateQLError(
      "READ_ONLY_CONNECTION",
      "Connection is read-only.",
      { suggestedAction: "Reconnect with --read-write." },
    );
  }

  requireResult(idOrAlias: string, session: SessionRecord): ResultRecord {
    const result = this.store.getResult(idOrAlias, session.id);
    if (!result) {
      throw new StateQLError("RESULT_NOT_FOUND", `Result "${idOrAlias}" was not found.`);
    }
    return result;
  }

  requireActiveTransaction(session: SessionRecord, id?: string): TransactionRecord {
    const transactionId = id ?? session.active_transaction_id;
    if (!transactionId) {
      throw new StateQLError("TRANSACTION_NOT_FOUND", "No active transaction.");
    }
    const transaction = this.store.getTransaction(transactionId);
    const active = transaction !== undefined &&
      transaction.session_id === session.id &&
      transaction.state === "active" &&
      session.active_transaction_id === transaction.id;
    if (!active) {
      throw new StateQLError(
        "TRANSACTION_NOT_FOUND",
        `Active transaction "${transactionId}" was not found.`,
      );
    }
    if (transaction.owner_actor_id !== this.actorId) {
      throw new StateQLError("PERMISSION_DENIED", "Only the transaction owner may control it.");
    }
    return transaction;
  }

  rejectDuringStagedTransaction(session: SessionRecord, operation: string): void {
    if (!session.active_transaction_id) return;
    throw new StateQLError(
      "TRANSACTION_FAILED",
      `${operation} cannot run while a staged transaction is active.`,
      { suggestedAction: "Commit or roll back the transaction first." },
    );
  }

  throwMembershipDenied(session: SessionRecord): never {
    throw new StateQLError(
      "PERMISSION_DENIED",
      `Actor "${this.actorId}" is not attached to session "${session.name}".`,
    );
  }

  resultData(
    result: ResultRecord,
    cached: boolean,
    previewRows = this.settings.previewRows,
  ): ResultData {
    const rows = this.store.resultRows(result);
    const preview = compactRows(rows.slice(0, previewRows), this.settings.maxCellCharacters);
    const alias = result.alias ?? this.store.generatedAlias(result.id);
    return {
      result_id: result.id,
      alias,
      display_alias: alias,
      rows: result.row_count,
      columns: this.store.resultColumns(result),
      preview,
      preview_count: preview.length,
      truncated: preview.length < result.row_count,
      cached,
      ...(cached ? { duplicate_of: result.id } : {}),
      ...(result.sql.startsWith("Redis native ") ? { next_cursor: redisResultCursor(result.parameters) } : {}),
      state_version: result.state_version,
      storage: {
        mode: "materialized",
        expires_at: result.expires_at,
      },
    };
  }

  cacheValid(result: ResultRecord, stateVersion: string, stateSignature: string): boolean {
    const now = this.now().getTime();
    return Date.parse(result.expires_at) > now &&
      Date.parse(result.created_at) + this.settings.cacheTtlSeconds * 1000 > now &&
      result.state_version === stateVersion &&
      result.state_signature === stateSignature;
  }
}

export function adapterOfKind<K extends DatabaseAdapter["kind"]>(
  adapter: DatabaseAdapter,
  kind: K,
): Extract<DatabaseAdapter, { kind: K }> {
  if (adapter.kind !== kind) {
    throw new StateQLError("STATE_CORRUPTED", "Adapter does not match the statement driver.");
  }
  return adapter as Extract<DatabaseAdapter, { kind: K }>;
}

export function rejectSqlOnNativeConnection(connection: ConnectionRecord, command: SqlCommand): void {
  if (connection.driver !== "mongodb" && connection.driver !== "redis") return;
  const method = NATIVE_METHODS[connection.driver][command];
  const name = driverDisplayName(connection.driver);
  throw new StateQLError(
    "INVALID_COMMAND",
    `SQL is not supported for ${name} connections; use ${method} instead.`,
    { suggestedAction: `Use ${method} with a native ${name} command.` },
  );
}

export function driverDisplayName(driver: ConnectionRecord["driver"]): string {
  switch (driver) {
    case "mongodb": return "MongoDB";
    case "redis": return "Redis";
    case "postgres": return "PostgreSQL";
    case "mysql": return "MySQL";
    case "sqlite": return "SQLite";
  }
}

const COMMAND_ORIGINS = new Set<CommandOrigin>(["legacy", "user", "model", "system", "api"]);

export function parseCommandOrigin(value: unknown): CommandOrigin {
  if (typeof value === "string" && COMMAND_ORIGINS.has(value as CommandOrigin)) {
    return value as CommandOrigin;
  }
  throw new StateQLError("INVALID_COMMAND", `Unknown command origin "${String(value)}".`);
}

export function mergeCommandExecutionContext(
  inherited: CommandExecutionContext | undefined,
  supplied: CommandExecutionContext,
): CommandExecutionContext {
  if (!supplied || typeof supplied !== "object") {
    throw new StateQLError("INVALID_COMMAND", "Command execution context must be an object.");
  }
  if (supplied.signal !== undefined && !(supplied.signal instanceof AbortSignal)) {
    throw new StateQLError("INVALID_COMMAND", "Command execution context signal must be an AbortSignal.");
  }
  if (supplied.internal !== undefined && typeof supplied.internal !== "boolean") {
    throw new StateQLError("INVALID_COMMAND", "Command execution context internal must be boolean.");
  }
  return {
    signal: combineAbortSignals(inherited?.signal, supplied.signal),
    origin: supplied.origin === undefined ? inherited?.origin : parseCommandOrigin(supplied.origin),
    internal: supplied.internal ?? inherited?.internal,
  };
}

function combineAbortSignals(...signals: Array<AbortSignal | undefined>): AbortSignal | undefined {
  const present = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  if (present.length === 0) return undefined;
  if (present.length === 1) return present[0];
  return AbortSignal.any(present);
}

export async function closeAdapterQuietly(adapter: DatabaseAdapter): Promise<void> {
  try {
    await adapter.close();
  } catch {
    // Preserve the operation result or primary sanitized error.
  }
}

async function resolveCredentialBeforeDeadline(
  resolver: CredentialResolver,
  request: CredentialRequest,
  context: AdapterContext,
): Promise<string | undefined> {
  if (context.signal?.aborted) throw new CredentialResolutionError("cancelled");
  const remaining = context.deadline - Date.now();
  if (remaining <= 0) throw new CredentialResolutionError("timeout");

  return new Promise<string | undefined>((resolve, reject) => {
    let settled = false;
    const finish = (action: (value?: string) => void, value?: string): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      context.signal?.removeEventListener("abort", abort);
      action(value);
    };
    const abort = () => finish(() => reject(new CredentialResolutionError("cancelled")));
    const timer = setTimeout(
      () => finish(() => reject(new CredentialResolutionError("timeout"))),
      remaining,
    );
    context.signal?.addEventListener("abort", abort, { once: true });

    Promise.resolve()
      .then(() => resolver(request))
      .then(
        (value) => finish(resolve, value),
        (error: unknown) => finish(() => reject(error)),
      );
  });
}

function credentialStateQLError(reference: string, error: unknown): StateQLError {
  if (error instanceof CredentialResolutionError) {
    switch (error.reason) {
      case "unavailable":
        return new StateQLError(
          "CREDENTIAL_UNAVAILABLE",
          `Credential reference "${reference}" is unavailable.`,
          { retryable: true },
        );
      case "denied":
        return new StateQLError("PERMISSION_DENIED", `Credential access for "${reference}" was denied.`);
      case "cancelled":
        return new StateQLError("OPERATION_CANCELLED", "Credential resolution was cancelled.", { retryable: true });
      case "timeout":
        return new StateQLError(
          "DEADLINE_EXCEEDED",
          "Credential resolution exceeded the credential deadline.",
          { retryable: true },
        );
    }
  }
  return new StateQLError(
    "CREDENTIAL_RESOLUTION_FAILED",
    `Credential reference "${reference}" could not be resolved.`,
    { retryable: true },
  );
}

export function safeCredentialErrorMessage(error: unknown, source: string): string {
  let message = errorMessage(error).split(source).join("[credential redacted]");
  const authority = /^[a-z][a-z\d+.-]*:\/\/([^/?#]*)/i.exec(source)?.[1];
  if (authority) {
    const at = authority.lastIndexOf("@");
    const colon = at < 0 ? -1 : authority.slice(0, at).indexOf(":");
    const password = colon < 0 ? "" : authority.slice(colon + 1, at);
    const secrets = new Set([password]);
    try { secrets.add(decodeURIComponent(password)); } catch { /* malformed values stay encoded */ }
    for (const secret of secrets) {
      if (secret) message = message.split(secret).join("[credential redacted]");
    }
  }
  return redact(message).replace(/\b([a-z][a-z0-9+.-]*:\/\/)[^\s/@]+(?::[^\s/@]*)?@/giu, "$1***@");
}

export function executionTimeout(value: number, name = "timeoutMs"): number {
  const timeout = positiveInteger(value, name);
  if (timeout > 2_147_483_647) {
    throw new StateQLError("INVALID_COMMAND", `${name} cannot exceed 2147483647 milliseconds.`);
  }
  return timeout;
}

export function nonNegativeInteger(value: number, name: string): number {
  if (Number.isInteger(value) && value >= 0) return value;
  throw new StateQLError("INVALID_COMMAND", `${name} must be a non-negative integer.`);
}

export function positiveInteger(value: number, name: string): number {
  if (Number.isInteger(value) && value > 0) return value;
  throw new StateQLError("INVALID_COMMAND", `${name} must be a positive integer.`);
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function redisResultCursor(parameters: string): string | null {
  try {
    const value = JSON.parse(parameters) as unknown;
    const cursor = Array.isArray(value) ? value[1] : null;
    return typeof cursor === "string" ? cursor : null;
  } catch {
    return null;
  }
}

export function historyCategory(command: string): HistoryCategory {
  if (["query", "exec", "plan", "apply", "mongo.query", "mongo.exec", "mongo.plan", "redis.query", "redis.exec", "redis.plan", "filter"].includes(command)) return "statement";
  if (command.startsWith("inspect.") || ["objects.list", "object.describe", "table.read"].includes(command)) return "introspection";
  return "management";
}

export function parseHistoryCategory(value: unknown): HistoryCategory {
  if (value === "statement" || value === "introspection" || value === "management") return value;
  throw new StateQLError("INVALID_COMMAND", `Unknown history category "${String(value)}".`);
}
