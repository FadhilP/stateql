import type { StateQLErrorShape } from "./types.js";

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
  if (code.startsWith("CONNECTION_")) return 3;
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

export function asStateQLError(error: unknown): StateQLError {
  if (error instanceof StateQLError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new StateQLError("INTERNAL_ERROR", message);
}
