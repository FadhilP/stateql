import { version } from "./connection.js";
import {
  adapterOfKind,
  closeAdapterQuietly,
  safeCredentialErrorMessage,
  type ActionResult,
  type Core,
} from "./core.js";
import { StateQLError } from "./errors.js";
import {
  AdapterExecutionError,
  BatchWriteError,
  stoppedStateQLError,
} from "./execution.js";
import { transactionData } from "./response-data.js";
import type { ConnectionRecord, SessionRecord, StateStore } from "./store.js";
import type {
  CommitTransactionData,
  ExecutionOptions,
  MongoWriteOutcome,
  RollbackTransactionData,
  TransactionData,
} from "./types.js";
import { storedMongoOperation } from "./writes.js";

const COMMIT_UNKNOWN_ACTION = "Inspect database state before issuing any replacement write.";

export function beginTransaction(
  core: Core,
  session: SessionRecord,
  isolation: string | undefined,
): ActionResult<TransactionData> {
  const connection = core.requireConnection(session);
  if (connection.driver === "redis") {
    throw new StateQLError(
      "UNSUPPORTED_DRIVER",
      "Redis does not support staged StateQL transactions; use redisPlan/apply for one guarded mutation.",
    );
  }
  if (connection.read_only) {
    throw new StateQLError(
      "READ_ONLY_CONNECTION",
      "Cannot begin a write transaction on a read-only connection.",
    );
  }
  if (session.active_transaction_id) {
    throw new StateQLError(
      "TRANSACTION_FAILED",
      `Transaction "${session.active_transaction_id}" is already active.`,
    );
  }
  const transaction = core.store.createTransaction({
    sessionId: session.id,
    actorId: core.actorId,
    connectionId: connection.id,
    isolation: normalizeIsolation(
      isolation ?? (connection.driver === "mongodb" ? "snapshot" : "serializable"),
      connection.driver,
    ),
  });
  if (!transaction) {
    throw new StateQLError("TRANSACTION_FAILED", "Another actor acquired the active transaction.");
  }
  return {
    data: transactionData(transaction, 0),
    handle: transaction.id,
    executed: true,
    stateVersion: transaction.start_version,
  };
}

export function transactionStatus(
  core: Core,
  session: SessionRecord,
  id: string | undefined,
): ActionResult<TransactionData> {
  const transactionId = id ?? session.active_transaction_id;
  if (!transactionId) {
    throw new StateQLError("TRANSACTION_NOT_FOUND", "No active transaction.");
  }
  const transaction = core.store.getTransaction(transactionId);
  if (!transaction || transaction.session_id !== session.id) {
    throw new StateQLError("TRANSACTION_NOT_FOUND", `Transaction "${transactionId}" was not found.`);
  }
  return {
    data: transactionData(transaction, core.store.transactionOperations(transaction.id).length),
    handle: transaction.id,
    stateVersion: transaction.start_version,
  };
}

/** Runs every staged write in one database transaction, then records the outcome. */
export async function commitTransaction(
  core: Core,
  session: SessionRecord,
  id: string | undefined,
  options: ExecutionOptions,
): Promise<ActionResult<CommitTransactionData>> {
  const transaction = core.requireActiveTransaction(session, id);
  const connection = core.store.getConnection(transaction.connection_id);
  if (!connection) {
    throw new StateQLError("CONNECTION_NOT_FOUND", "Transaction connection was not found.");
  }
  if (version(connection) !== transaction.start_version) {
    throw new StateQLError(
      "TRANSACTION_FAILED",
      "Connection state changed after the transaction began.",
      { suggestedAction: "Roll back and begin a new transaction." },
    );
  }
  // Validate durable payloads before opening a database adapter or changing the transaction state.
  const operations = core.store.validatedTransactionOperations(transaction.id);
  if (operations.some((operation) => operation.connection_id !== connection.id)) {
    throw new StateQLError("TRANSACTION_FAILED", "Transaction contains writes staged for another connection.");
  }
  const mongoCommands = connection.driver === "mongodb"
    ? operations.map(storedMongoOperation)
    : undefined;
  const sqlHasMongoWrites = !mongoCommands &&
    operations.some((operation) => operation.statement_type.startsWith("mongo."));
  if (sqlHasMongoWrites) {
    throw new StateQLError("TRANSACTION_FAILED", "Transaction contains native MongoDB writes for a SQL connection.");
  }
  const context = core.executionContext(options);
  const source = await core.resolveConnectionSource(connection, session, "transaction.commit", "write", context);
  const adapter = await core.openAdapter(connection, context, source);
  const markUnknown = () => markTransactionOutcomeUnknown(core.store, transaction.id, session.id, core.actorId);
  const outcomeUnknown = (message: string) => new StateQLError("OUTCOME_UNKNOWN", message, {
    executed: true,
    suggestedAction: COMMIT_UNKNOWN_ACTION,
  });
  try {
    if (!core.store.claimTransactionForCommit(transaction.id, session.id, core.actorId, operations)) {
      throw new StateQLError("TRANSACTION_FAILED", "Transaction is no longer active.");
    }

    let results: Array<{ affectedRows: number; outcome?: MongoWriteOutcome }>;
    try {
      results = mongoCommands
        ? await adapterOfKind(adapter, "mongodb").writeBatch(mongoCommands, transaction.isolation_level)
        : await adapterOfKind(adapter, "sql").writeBatch(operations, transaction.isolation_level);
    } catch (error) {
      const definite =
        (error instanceof BatchWriteError || error instanceof AdapterExecutionError) &&
        !error.outcomeUnknown;
      if (!definite) {
        markUnknown();
        throw outcomeUnknown(safeCredentialErrorMessage(error, source));
      }
      core.store.finishTransaction(transaction.id, session.id, core.actorId, "failed");
      if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, false);
      throw new StateQLError(
        "TRANSACTION_FAILED",
        safeCredentialErrorMessage(error, source),
        { retryable: true },
      );
    }

    if (results.length !== operations.length) {
      markUnknown();
      throw outcomeUnknown("Database returned an incomplete transaction result.");
    }

    let stateVersion: string;
    try {
      stateVersion = core.store.commitTransactionMetadata({
        transactionId: transaction.id,
        sessionId: session.id,
        actorId: core.actorId,
        connectionId: connection.id,
        operations: operations.map((operation, index) => ({
          id: operation.id,
          affectedRows: results[index]!.affectedRows,
          ...(results[index]!.outcome ? { outcome: results[index]!.outcome } : {}),
        })),
      });
    } catch (error) {
      markUnknown();
      throw outcomeUnknown(safeCredentialErrorMessage(error, source));
    }

    return {
      data: {
        transaction_id: transaction.id,
        state: "committed",
        statements_executed: operations.length,
        affected_rows: results.reduce((total, result) => total + result.affectedRows, 0),
        state_version: stateVersion,
      },
      handle: transaction.id,
      executed: true,
      stateVersion,
      confidence: adapter.confidence,
    };
  } finally {
    await closeAdapterQuietly(adapter);
  }
}

export function rollbackTransaction(
  core: Core,
  session: SessionRecord,
  id: string | undefined,
): ActionResult<RollbackTransactionData> {
  const transaction = core.requireActiveTransaction(session, id);
  const count = core.store.transactionOperations(transaction.id).length;
  if (!core.store.finishTransaction(transaction.id, session.id, core.actorId, "rolled_back")) {
    throw new StateQLError("TRANSACTION_FAILED", "Transaction is no longer active.");
  }
  return {
    data: {
      transaction_id: transaction.id,
      state: "rolled_back",
      discarded_statements: count,
    },
    handle: transaction.id,
    executed: true,
    stateVersion: transaction.start_version,
  };
}

function markTransactionOutcomeUnknown(
  store: StateStore,
  transactionId: string,
  sessionId: string,
  actorId: string,
): void {
  try {
    store.markTransactionOutcomeUnknown(transactionId, sessionId, actorId);
  } catch {
    // A stale committing transaction is recovered as unknown after five minutes.
  }
}

function normalizeIsolation(isolation: string, driver: ConnectionRecord["driver"]): string {
  const normalized = isolation.trim().toLowerCase().replace(/[_-]+/g, " ").replace(/\s+/g, " ");
  if (driver === "redis") {
    throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support staged SQL-style transactions.");
  }
  if (driver === "mongodb") {
    if (normalized === "snapshot") return normalized;
    throw new StateQLError("INVALID_COMMAND", `MongoDB does not support isolation level "${normalized}".`);
  }
  const supported = new Set(["serializable", "repeatable read", "read committed", "read uncommitted"]);
  if (!supported.has(normalized)) {
    throw new StateQLError("INVALID_COMMAND", `Unsupported isolation level "${isolation}".`);
  }
  if (driver === "sqlite" && normalized !== "serializable") {
    throw new StateQLError("INVALID_COMMAND", `SQLite does not support isolation level "${normalized}".`);
  }
  return normalized;
}
