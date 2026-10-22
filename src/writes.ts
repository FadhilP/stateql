import type { BatchWriteOperation, DatabaseAdapter } from "./adapters.js";
import { databaseIdentity, version } from "./connection.js";
import {
  adapterOfKind,
  closeAdapterQuietly,
  errorMessage,
  safeCredentialErrorMessage,
  type ActionResult,
  type Core,
} from "./core.js";
import { StateQLError } from "./errors.js";
import {
  AdapterExecutionError,
  AdapterWriteError,
  BatchWriteError,
  stoppedStateQLError,
  type AdapterContext,
} from "./execution.js";
import {
  analyzeMongoWriteSafety,
  deserializeMongoWriteCommand,
  serializeMongoCommand,
  validateMongoWriteCommand,
} from "./mongodb.js";
import {
  invalidCommand,
  mongoDescriptor,
  redisDescriptor,
  sqlParametersLength,
} from "./reads.js";
import {
  serializeRedisCommand,
  validateRedisWriteCommand,
  type RedisPrecondition,
} from "./redis.js";
import { operationData } from "./response-data.js";
import { analyzeSql } from "./sql.js";
import type { ConnectionRecord, OperationRecord, SessionRecord } from "./store.js";
import type { compileTableUpdate, TableUpdate } from "./table-editor.js";
import type {
  ExecData,
  MongoWriteCommand,
  MongoWriteOutcome,
  SqlParameters,
} from "./types.js";
import { hash } from "./util.js";

const REPLAY_ACTION = "Inspect database state, then use --replay only if another execution is safe.";
const FINALIZE_ACTION = "Inspect database state before issuing any replacement write.";

interface WriteOutput {
  affectedRows: number;
  outcome?: MongoWriteOutcome;
}

/** A validated write for one driver; runWrite does deduplication, staging, and outcome tracking. */
export interface WriteStatement {
  descriptor: string;
  parameters: SqlParameters;
  statementType: string;
  /** Duplicate-write protection compares this across versions; keep its input stable. */
  fingerprint: string;
  stagingError?: string;
  conflictMessage: string;
  unknownAction: string;
  execute(adapter: DatabaseAdapter, operation: OperationRecord): Promise<WriteOutput>;
}

export interface WriteSafety {
  allowUnbounded?: boolean;
  allowDestructive?: boolean;
  expectedRows?: 1;
}

export interface WriteOptions {
  replay?: boolean;
  idempotencyKey?: string;
}

/** A claimed plan that this write finishes; its source was resolved with write access. */
export interface PlanClaim {
  planId: string;
  claimToken: string;
  source: string;
}

type CompiledTableUpdate = ReturnType<typeof compileTableUpdate>;

export function prepareSqlWrite(
  connection: ConnectionRecord,
  sql: string,
  parameters: SqlParameters,
  safety: WriteSafety,
): WriteStatement {
  const analysis = analyzeSql(sql, connection.driver);
  if (analysis.read) {
    throw new StateQLError(
      "INVALID_SQL",
      "exec accepts write statements only; use query for reads.",
    );
  }
  rejectUnsafeWrite(
    analysis.unboundedMutation,
    analysis.destructive,
    safety,
    "Mutation has no WHERE clause.",
    "Destructive operation requires an explicit override.",
  );
  const autocommitHasInputs = safety.expectedRows !== undefined || sqlParametersLength(parameters) > 0;
  if (analysis.requiresAutocommit && autocommitHasInputs) {
    throw new StateQLError(
      "INVALID_SQL",
      "Autocommit maintenance statements do not accept StateQL parameters or row-count preconditions.",
    );
  }
  const statementName = analysis.statementType.toUpperCase();
  return {
    descriptor: sql,
    parameters,
    statementType: analysis.statementType,
    fingerprint: hash({
      sql: analysis.normalized,
      parameters,
      database: databaseIdentity(connection),
    }),
    ...(analysis.requiresAutocommit
      ? { stagingError: `${statementName} cannot be staged in a transaction.` }
      : {}),
    conflictMessage: "The row changed or no longer matches. Reload before editing.",
    unknownAction: REPLAY_ACTION,
    async execute(adapter) {
      const sqlAdapter = adapterOfKind(adapter, "sql");
      if (!analysis.requiresAutocommit) return sqlAdapter.write(sql, parameters, safety.expectedRows);
      if (!sqlAdapter.writeAutocommit) {
        throw new AdapterWriteError(`${statementName} requires adapter autocommit execution.`, false);
      }
      return sqlAdapter.writeAutocommit(sql, parameters);
    },
  };
}

export function prepareMongoWrite(
  connection: ConnectionRecord,
  command: unknown,
  safety: WriteSafety,
): WriteStatement {
  const value = invalidCommand(() => validateMongoWriteCommand(command));
  const unsafe = analyzeMongoWriteSafety(value);
  rejectUnsafeWrite(
    unsafe.unbounded,
    unsafe.destructive,
    safety,
    "MongoDB mutation has an empty filter.",
    "Destructive MongoDB operation requires an explicit override.",
  );
  const serialized = serializeMongoCommand(value);
  return {
    descriptor: mongoDescriptor(value.operation),
    parameters: [serialized],
    statementType: `mongo.${value.operation}`,
    fingerprint: hash({ command: serialized, database: databaseIdentity(connection) }),
    ...(value.operation === "createIndex" || value.operation === "dropIndex"
      ? { stagingError: "MongoDB index changes cannot be staged in a transaction." }
      : {}),
    conflictMessage: "The document changed or was removed. Reload before editing.",
    unknownAction: REPLAY_ACTION,
    execute: (adapter) => adapterOfKind(adapter, "mongodb").write(value, safety.expectedRows),
  };
}

export function prepareRedisWrite(
  connection: ConnectionRecord,
  command: unknown,
  precondition?: RedisPrecondition,
): WriteStatement {
  const value = invalidCommand(() => validateRedisWriteCommand(command));
  const serialized = serializeRedisCommand(value);
  return {
    descriptor: redisDescriptor(value.command),
    parameters: [serialized],
    statementType: `redis.${value.command.toLowerCase()}`,
    fingerprint: hash({ command: serialized, database: databaseIdentity(connection) }),
    stagingError: "Redis writes cannot be staged in a transaction.",
    conflictMessage: "The Redis key changed. Reload and plan again.",
    unknownAction: "Inspect Redis state before issuing a replacement write.",
    execute: (adapter) => adapterOfKind(adapter, "redis").write(value, precondition),
  };
}

export function prepareTableBatch(
  connection: ConnectionRecord,
  planId: string,
  updates: TableUpdate[],
  compiled: CompiledTableUpdate[],
): WriteStatement {
  if (connection.driver === "redis") {
    throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support table edit batches.");
  }
  const mongoCommands = connection.driver === "mongodb"
    ? compiled.map((item) => {
        if (!item.mongo) throw new StateQLError("STALE_PLAN", "Table edit batch contains mixed drivers.");
        return item.mongo;
      })
    : undefined;
  return {
    descriptor: `Conditional table update batch (${updates.length} rows)`,
    parameters: [JSON.stringify({ version: 1, updates })],
    statementType: "table.updates",
    fingerprint: hash({ plan: planId, updates }),
    stagingError: "Table edit batches cannot be staged in a transaction.",
    conflictMessage: "At least one row changed; the entire edit batch was rolled back.",
    unknownAction: "Inspect every edited row before retrying.",
    async execute(adapter, operation) {
      const results = mongoCommands
        ? await adapterOfKind(adapter, "mongodb").writeBatch(mongoCommands, "snapshot", true)
        : await adapterOfKind(adapter, "sql").writeBatch(
            compiled.map((item, index): BatchWriteOperation => ({
              ...operation,
              id: `${operation.id}:${index}`,
              sql: item.sql,
              parameters: JSON.stringify(item.params),
              statement_type: "update",
              expectedRows: 1,
            })),
            "serializable",
          );
      return { affectedRows: results.reduce((sum, item) => sum + item.affectedRows, 0) };
    },
  };
}

/** Reserves the write, stages it or runs it once, and records a definite or unknown outcome. */
export async function runWrite(
  core: Core,
  session: SessionRecord,
  connection: ConnectionRecord,
  statement: WriteStatement,
  options: WriteOptions,
  context: AdapterContext,
  claim?: PlanClaim,
): Promise<ActionResult<ExecData>> {
  if (options.idempotencyKey !== undefined && !options.idempotencyKey.trim()) {
    throw new StateQLError("INVALID_COMMAND", "Idempotency key cannot be empty.");
  }
  const transactionId = session.active_transaction_id ?? undefined;
  if (transactionId) requireStagingTransaction(core, session, connection, statement, transactionId);

  const reservation = core.store.reserveOperation({
    sessionId: session.id,
    actorId: core.actorId,
    connectionId: connection.id,
    fingerprint: statement.fingerprint,
    sql: statement.descriptor,
    parameters: statement.parameters,
    statementType: statement.statementType,
    status: transactionId ? "pending" : "executing",
    transactionId,
    replay: options.replay ?? false,
    idempotencyKey: options.idempotencyKey,
    stateVersionBefore: version(connection),
  });
  if (reservation.denied) throw reservationDenied(core, session, reservation.denied);
  const previous = reservation.previous;
  const keyReused = previous !== undefined &&
    options.idempotencyKey !== undefined &&
    !options.replay &&
    previous.fingerprint !== statement.fingerprint;
  if (keyReused) {
    throw new StateQLError(
      "IDEMPOTENCY_CONFLICT",
      "Idempotency key was already used for a different write.",
      { extra: { previous_operation_id: previous.id } },
    );
  }
  if (!reservation.operation) return duplicateWrite(previous!, options.idempotencyKey);

  const operation = reservation.operation;
  if (transactionId) {
    return {
      data: operationData(operation),
      handle: operation.id,
      executed: false,
      stateVersion: version(connection),
    };
  }

  let adapter: DatabaseAdapter;
  let source: string;
  try {
    source = claim?.source ??
      await core.resolveConnectionSource(connection, session, "exec", "write", context);
    adapter = await core.openAdapter(connection, context, source);
  } catch (error) {
    core.store.failOperation(operation.id);
    if (error instanceof StateQLError) throw error;
    throw new StateQLError("CONNECTION_FAILED", "Database connection failed.", { retryable: true });
  }

  try {
    const write = await statement.execute(adapter, operation);
    const finalized = finishWrite(core, connection, operation, write, claim);
    return {
      data: {
        ...operationData(finalized.operation),
        duplicate: Boolean(previous),
        duplicate_override: Boolean(previous),
      },
      handle: finalized.operation.id,
      executed: true,
      stateVersion: finalized.stateVersion,
      confidence: adapter.confidence,
    };
  } catch (error) {
    throw writeFailure(core, operation, statement, error, source);
  } finally {
    await closeAdapterQuietly(adapter);
  }
}

function rejectUnsafeWrite(
  unbounded: boolean,
  destructive: boolean,
  safety: WriteSafety,
  unboundedMessage: string,
  destructiveMessage: string,
): void {
  if (unbounded && !safety.allowUnbounded) {
    throw new StateQLError("UNBOUNDED_MUTATION", unboundedMessage, {
      extra: { override_flag: "--allow-unbounded" },
    });
  }
  if (destructive && !safety.allowDestructive) {
    throw new StateQLError("DESTRUCTIVE_OPERATION_BLOCKED", destructiveMessage, {
      extra: { override_flag: "--allow-destructive" },
    });
  }
}

function requireStagingTransaction(
  core: Core,
  session: SessionRecord,
  connection: ConnectionRecord,
  statement: WriteStatement,
  transactionId: string,
): void {
  if (statement.stagingError) {
    throw new StateQLError("TRANSACTION_FAILED", statement.stagingError, {
      suggestedAction: "Roll back or commit the staged transaction, then run this statement separately.",
    });
  }
  const transaction = core.store.getTransaction(transactionId);
  const matches = transaction !== undefined &&
    transaction.session_id === session.id &&
    transaction.state === "active" &&
    transaction.connection_id === connection.id;
  if (!matches) {
    throw new StateQLError(
      "TRANSACTION_FAILED",
      "Active transaction does not match the active connection.",
    );
  }
  if (transaction.owner_actor_id !== core.actorId) {
    throw new StateQLError("PERMISSION_DENIED", "Only the transaction owner may stage writes.");
  }
}

function reservationDenied(
  core: Core,
  session: SessionRecord,
  denied: "membership" | "transaction",
): StateQLError {
  if (denied === "membership") {
    return new StateQLError(
      "PERMISSION_DENIED",
      "Actor membership changed before the write was reserved.",
    );
  }
  const active = core.store.getSession(session.id)?.active_transaction_id;
  const transaction = active ? core.store.getTransaction(active) : undefined;
  if (transaction && transaction.owner_actor_id !== core.actorId) {
    return new StateQLError("PERMISSION_DENIED", "Only the transaction owner may stage writes.");
  }
  return new StateQLError(
    "TRANSACTION_FAILED",
    "The active transaction changed before the write was reserved.",
  );
}

function duplicateWrite(
  previous: OperationRecord,
  idempotencyKey: string | undefined,
): ActionResult<ExecData> {
  if (previous.status === "executing" || previous.status === "outcome_unknown") {
    throw new StateQLError(
      "OUTCOME_UNKNOWN",
      "A matching write has an unknown outcome.",
      {
        executed: true,
        suggestedAction: REPLAY_ACTION,
        extra: { previous_operation_id: previous.id },
      },
    );
  }
  if (!idempotencyKey) {
    throw new StateQLError(
      "POTENTIAL_DUPLICATE_WRITE",
      "An equivalent operation was previously applied.",
      { extra: { previous_operation_id: previous.id, replay_required: true } },
    );
  }
  return {
    data: {
      ...operationData(previous),
      duplicate: true,
      duplicate_of: previous.id,
      idempotency_key: idempotencyKey,
    },
    handle: previous.id,
    cached: true,
    stateVersion: previous.state_version_after ?? previous.state_version_before,
  };
}

function finishWrite(
  core: Core,
  connection: ConnectionRecord,
  operation: OperationRecord,
  write: WriteOutput,
  claim: PlanClaim | undefined,
): { operation: OperationRecord; stateVersion: string } {
  try {
    if (claim) {
      return core.store.finishPlannedOperation({
        planId: claim.planId,
        claimToken: claim.claimToken,
        operationId: operation.id,
        connectionId: connection.id,
        affectedRows: write.affectedRows,
        outcome: write.outcome,
      });
    }
    const stateVersion = core.store.bumpVersion(connection.id);
    return {
      operation: core.store.finishOperation(operation.id, write.affectedRows, stateVersion, write.outcome),
      stateVersion,
    };
  } catch (error) {
    core.store.markOperationOutcomeUnknown(operation.id);
    throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), {
      executed: true,
      suggestedAction: FINALIZE_ACTION,
    });
  }
}

function writeFailure(
  core: Core,
  operation: OperationRecord,
  statement: WriteStatement,
  error: unknown,
  source: string,
): StateQLError {
  if (error instanceof StateQLError) return error;
  const definite =
    (error instanceof AdapterExecutionError ||
      error instanceof AdapterWriteError ||
      error instanceof BatchWriteError) &&
    !error.outcomeUnknown;
  if (!definite) {
    core.store.markOperationOutcomeUnknown(operation.id);
    return new StateQLError(
      "OUTCOME_UNKNOWN",
      safeCredentialErrorMessage(error, source),
      { executed: true, suggestedAction: statement.unknownAction },
    );
  }
  core.store.failOperation(operation.id);
  if (error.message.startsWith("ROW_CONFLICT:")) {
    return new StateQLError("ROW_CONFLICT", statement.conflictMessage);
  }
  if (error instanceof AdapterExecutionError) return stoppedStateQLError(error, false);
  return new StateQLError(
    "QUERY_FAILED",
    safeCredentialErrorMessage(error, source),
    { executed: true },
  );
}

export function storedMongoOperation(operation: OperationRecord): MongoWriteCommand {
  if (!operation.statement_type.startsWith("mongo.")) {
    throw new StateQLError(
      "TRANSACTION_FAILED",
      "MongoDB transaction contains a mixed or corrupt native payload.",
    );
  }
  return storedMongoWrite(
    operation.parameters,
    operation.statement_type,
    `operation "${operation.id}"`,
    "TRANSACTION_FAILED",
  );
}

export function storedMongoWrite(
  parameters: string,
  statementType: string,
  label: string,
  errorCode: string,
): MongoWriteCommand {
  try {
    const payload = JSON.parse(parameters) as unknown;
    if (!Array.isArray(payload) || payload.length !== 1 || typeof payload[0] !== "string") {
      throw new Error("payload must contain one EJSON command string");
    }
    const command = deserializeMongoWriteCommand(payload[0]);
    if (statementType !== `mongo.${command.operation}`) {
      throw new Error("operation does not match its statement type");
    }
    return command;
  } catch {
    throw new StateQLError(errorCode, `Stored MongoDB ${label} payload is invalid.`);
  }
}
