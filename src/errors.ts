import type { StateQLErrorShape } from "./types.js";

export type CredentialResolutionFailure =
  | "unavailable"
  | "denied"
  | "cancelled"
  | "timeout";

export class CredentialResolutionError extends Error {
  constructor(readonly reason: CredentialResolutionFailure) {
    super(`Credential resolution ${reason}.`);
    this.name = "CredentialResolutionError";
  }
}

export class StateQLError extends Error {
  readonly details: StateQLErrorShape;

  constructor(
    code: string,
    message: string,
    options: {
      retryable?: boolean;
      executed?: boolean;
      suggestedAction?: string;
      extra?: Record<string, unknown>;
    } = {},
  ) {
    super(message);
    this.name = "StateQLError";
    this.details = {
      code,
      message,
      retryable: options.retryable ?? false,
      executed: options.executed ?? false,
      ...(options.suggestedAction
        ? { suggested_action: options.suggestedAction }
        : {}),
      ...options.extra,
    };
  }
}

export function exitCodeFor(code: string): number {
  if (code === "INVALID_COMMAND" || code === "INVALID_SQL") return 2;
  if (code.startsWith("CONNECTION_") || code.startsWith("CREDENTIAL_")) return 3;
  if (
    code === "QUERY_FAILED" ||
    code === "DEADLINE_EXCEEDED" ||
    code === "OPERATION_CANCELLED"
  ) return 4;
  if (
    code === "READ_ONLY_CONNECTION" ||
    code === "UNBOUNDED_MUTATION" ||
    code === "DESTRUCTIVE_OPERATION_BLOCKED"
  ) {
    return 5;
  }
  if (
    code === "POTENTIAL_DUPLICATE_WRITE" ||
    code === "OUTCOME_UNKNOWN" ||
    code === "IDEMPOTENCY_CONFLICT"
  ) return 6;
  if (code.startsWith("STALE_") || code === "RESULT_EXPIRED") return 7;
  if (code === "PERMISSION_DENIED") return 8;
  if (code === "UNSUPPORTED_DRIVER") return 9;
  return 1;
}

const TRANSIENT_NETWORK_CODES = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "EPIPE",
  "EAI_AGAIN",
  "PROTOCOL_CONNECTION_LOST",
]);
// SQLSTATE classes: connection, transaction rollback, resources, operator intervention, system.
const TRANSIENT_SQLSTATE_CLASSES = new Set(["08", "40", "53", "57", "58"]);
// MySQL: too many connections, lock wait timeout, deadlock.
const TRANSIENT_MYSQL_ERRNOS = new Set([1040, 1205, 1213]);
const TRANSIENT_ERROR_NAMES = new Set([
  "MongoNetworkError",
  "MongoNetworkTimeoutError",
  "MongoServerSelectionError",
  "SocketClosedUnexpectedlyError",
  "ConnectionTimeoutError",
]);
const TRANSIENT_SQLITE_MESSAGE = /database (table )?is locked|SQLITE_BUSY/i;
const SQL_SYNTAX_MESSAGE = /syntax error|incomplete input|unrecognized token/i;

/** True when the database rejected the SQL text itself, not its tables or data. */
export function isSqlSyntaxError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const details = error as { code?: unknown; errno?: unknown };
  if (details.code === "42601" || details.errno === 1064) return true;
  return SQL_SYNTAX_MESSAGE.test(error.message);
}

// Permanent failures such as a missing table must not invite retries; unknown errors are not retryable.
export function isTransientDatabaseError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const details = error as {
    code?: unknown;
    sqlState?: unknown;
    errno?: unknown;
    errorLabels?: unknown;
  };
  if (typeof details.code === "string" && TRANSIENT_NETWORK_CODES.has(details.code)) return true;
  if (TRANSIENT_ERROR_NAMES.has(error.name)) return true;
  if (typeof details.errno === "number" && TRANSIENT_MYSQL_ERRNOS.has(details.errno)) return true;
  if (Array.isArray(details.errorLabels) && details.errorLabels.includes("TransientTransactionError")) {
    return true;
  }
  const sqlState = typeof details.sqlState === "string" ? details.sqlState : details.code;
  if (typeof sqlState === "string" && /^[0-9A-Z]{5}$/.test(sqlState)) {
    return TRANSIENT_SQLSTATE_CLASSES.has(sqlState.slice(0, 2));
  }
  return TRANSIENT_SQLITE_MESSAGE.test(error.message);
}

export function asStateQLError(error: unknown): StateQLError {
  if (error instanceof StateQLError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new StateQLError("INTERNAL_ERROR", message);
}
