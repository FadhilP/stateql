import { StateQLError } from "./errors.js";

const MAX_TIMER_MS = 2_147_483_647;

export interface AdapterContext {
  deadline: number;
  timeoutMs?: number;
  signal?: AbortSignal;
}

export class AdapterExecutionError extends Error {
  constructor(
    message: string,
    readonly reason: "timeout" | "aborted",
    readonly outcomeUnknown: boolean,
  ) {
    super(message);
    this.name = "AdapterExecutionError";
  }
}

export class BatchWriteError extends Error {
  constructor(
    message: string,
    readonly outcomeUnknown: boolean,
  ) {
    super(message);
    this.name = "BatchWriteError";
  }
}

export class AdapterWriteError extends Error {
  constructor(
    message: string,
    readonly outcomeUnknown: boolean,
  ) {
    super(message);
    this.name = "AdapterWriteError";
  }
}

export function createAdapterContext(
  timeoutMs: number,
  signal?: AbortSignal,
): AdapterContext {
  return {
    deadline: Date.now() + timeoutMs,
    timeoutMs,
    ...(signal ? { signal } : {}),
  };
}

export function remainingMilliseconds(context: AdapterContext): number {
  return Math.max(1, Math.min(MAX_TIMER_MS, Math.ceil(context.deadline - Date.now())));
}

export function isStopped(context: AdapterContext): boolean {
  return Boolean(context.signal?.aborted) || context.deadline <= Date.now();
}

export function throwIfStopped(
  context: AdapterContext,
  outcomeUnknown: boolean,
): void {
  if (isStopped(context)) throw stoppedError(context, outcomeUnknown);
}

export function stoppedError(
  context: AdapterContext,
  outcomeUnknown: boolean,
): AdapterExecutionError {
  const aborted = context.signal?.aborted ?? false;
  return new AdapterExecutionError(
    aborted ? "Database operation cancelled." : "Database operation timed out.",
    aborted ? "aborted" : "timeout",
    outcomeUnknown,
  );
}

/** Settles with a stopped error at the deadline or on abort, and calls onStop to release the driver. */
export function withContext<T>(
  promise: Promise<T>,
  context: AdapterContext,
  onStop: () => void,
  outcomeUnknown: boolean,
): Promise<T> {
  const stopQuietly = (): void => {
    try {
      onStop();
    } catch {
      // Preserve the deadline or cancellation error.
    }
  };
  if (isStopped(context)) {
    stopQuietly();
    return Promise.reject(stoppedError(context, outcomeUnknown));
  }
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (action: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      context.signal?.removeEventListener("abort", stop);
      action();
    };
    const stop = (): void => {
      finish(() => {
        stopQuietly();
        reject(stoppedError(context, outcomeUnknown));
      });
    };
    const timer = setTimeout(stop, remainingMilliseconds(context));
    context.signal?.addEventListener("abort", stop, { once: true });
    promise.then(
      (value) => (isStopped(context) ? stop() : finish(() => resolve(value))),
      (error) => (isStopped(context) ? stop() : finish(() => reject(error))),
    );
  });
}

export function stoppedStateQLError(
  error: AdapterExecutionError,
  executed: boolean,
): StateQLError {
  return new StateQLError(
    error.reason === "timeout" ? "DEADLINE_EXCEEDED" : "OPERATION_CANCELLED",
    error.message,
    { retryable: true, executed },
  );
}
