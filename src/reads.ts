import type { DatabaseAdapter } from "./adapters.js";
import { version } from "./connection.js";
import {
  adapterOfKind,
  errorMessage,
  nonNegativeInteger,
  type ActionResult,
  type Core,
} from "./core.js";
import { StateQLError } from "./errors.js";
import { serializeMongoCommand, validateMongoReadCommand } from "./mongodb.js";
import { serializeRedisCommand, validateRedisReadCommand } from "./redis.js";
import { paginationWarnings } from "./response-data.js";
import { analyzeSql } from "./sql.js";
import type { ConnectionRecord, SessionRecord } from "./store.js";
import type {
  Column,
  ExecutionOptions,
  MongoReadCommand,
  ResultData,
  Row,
  SqlParameters,
  Warning,
} from "./types.js";
import { hash } from "./util.js";

interface ReadOutput {
  rows: Row[];
  columns: Column[];
  parameters?: SqlParameters;
}

/** A validated read for one driver; runRead does the caching, limits, and storage. */
export interface ReadStatement {
  descriptor: string;
  identity: Record<string, unknown>;
  parameters: SqlParameters;
  cacheable: boolean;
  warnings: Warning[];
  limitHint: string;
  execute(adapter: DatabaseAdapter, fetchLimit: number): Promise<ReadOutput>;
}

export interface ReadOptions extends ExecutionOptions {
  cache?: "auto" | "bypass" | "require";
  previewRows?: number;
}

export function prepareSqlRead(
  connection: ConnectionRecord,
  sql: string,
  parameters: SqlParameters,
): ReadStatement {
  const analysis = analyzeSql(sql, connection.driver);
  if (!analysis.read) {
    throw new StateQLError(
      "INVALID_SQL",
      "query accepts read statements only; use exec for writes.",
    );
  }
  if (analysis.requiresAutocommit && sqlParametersLength(parameters) > 0) {
    throw new StateQLError(
      "INVALID_SQL",
      "Autocommit diagnostic statements do not accept StateQL parameters.",
    );
  }
  return {
    descriptor: sql,
    identity: { sql: analysis.normalized, parameters },
    parameters,
    cacheable: analysis.cacheable,
    warnings: paginationWarnings(analysis.ordered),
    limitHint: "Add a narrower WHERE clause or LIMIT.",
    async execute(adapter, fetchLimit) {
      const sqlAdapter = adapterOfKind(adapter, "sql");
      const executionSql = analysis.wrapForLimit
        ? boundedReadSql(analysis.limitSql ?? sql, fetchLimit)
        : sql;
      if (!analysis.requiresAutocommit) return sqlAdapter.read(executionSql, parameters);
      if (!sqlAdapter.readAutocommit) {
        throw new StateQLError(
          "UNSUPPORTED_DRIVER",
          `${analysis.statementType.toUpperCase()} requires adapter autocommit reads.`,
        );
      }
      return sqlAdapter.readAutocommit(executionSql, parameters);
    },
  };
}

export function prepareMongoRead(command: unknown): ReadStatement {
  const value = invalidCommand(() => validateMongoReadCommand(command));
  const serialized = serializeMongoCommand(value);
  return {
    descriptor: mongoDescriptor(value.operation),
    identity: { command: serialized },
    parameters: [serialized],
    cacheable: true,
    warnings: mongoPaginationWarnings(value),
    limitHint: "Use a narrower filter or limit.",
    execute: (adapter, fetchLimit) => adapterOfKind(adapter, "mongodb").read(value, fetchLimit),
  };
}

export function prepareRedisRead(command: unknown): ReadStatement {
  const value = invalidCommand(() => validateRedisReadCommand(command));
  const serialized = serializeRedisCommand(value);
  return {
    descriptor: redisDescriptor(value.command),
    identity: { command: serialized },
    parameters: [serialized],
    cacheable: true,
    warnings: [],
    limitHint: "Read fewer keys or members.",
    async execute(adapter) {
      const result = await adapterOfKind(adapter, "redis").read(value);
      return { ...result, parameters: [serialized, result.nextCursor ?? null] };
    },
  };
}

/** Serves a valid cached result, or runs the read and stores the bounded result. */
export async function runRead(
  core: Core,
  session: SessionRecord,
  connection: ConnectionRecord,
  statement: ReadStatement,
  options: ReadOptions,
): Promise<ActionResult<ResultData>> {
  const cacheMode = options.cache ?? "auto";
  if (!statement.cacheable && cacheMode === "require") {
    throw cacheMiss("This statement is not cacheable.");
  }
  const { maxResultRows, maxResultBytes, resultTtlSeconds } = core.settings;
  const previewRows = options.previewRows ?? core.settings.previewRows;
  const context = core.executionContext(options);
  return core.withAdapter(session, connection, "query", context, async (adapter) => {
    const stateVersion = version(connection);
    const stateSignature = await adapter.signature();
    const fingerprint = hash({
      ...statement.identity,
      driver: connection.driver,
      connection: connection.id,
      database: connection.database_name,
      transaction: session.active_transaction_id,
      stateVersion,
    });
    const cached = statement.cacheable && cacheMode !== "bypass"
      ? core.store.findResult(fingerprint)
      : undefined;
    if (
      cached &&
      cached.row_count <= maxResultRows &&
      core.cacheValid(cached, stateVersion, stateSignature)
    ) {
      return {
        data: core.resultData(cached, true, previewRows),
        handle: cached.id,
        cached: true,
        warnings: statement.warnings,
        stateVersion,
        confidence: cached.state_confidence,
      };
    }
    if (cacheMode === "require") throw cacheMiss("No valid cached result exists.");

    const result = await statement.execute(adapter, maxResultRows + 1);
    if (result.rows.length > maxResultRows) {
      throw new StateQLError(
        "OUTPUT_LIMIT_EXCEEDED",
        `Query exceeds the ${maxResultRows}-row materialization limit.`,
        { suggestedAction: statement.limitHint },
      );
    }
    const parameters = result.parameters ?? statement.parameters;
    const resultBytes =
      Buffer.byteLength(JSON.stringify(parameters), "utf8") +
      Buffer.byteLength(JSON.stringify(result.rows), "utf8") +
      Buffer.byteLength(JSON.stringify(result.columns), "utf8");
    if (resultBytes > maxResultBytes) {
      throw new StateQLError(
        "OUTPUT_LIMIT_EXCEEDED",
        `Query exceeds the ${maxResultBytes}-byte materialization limit.`,
        { suggestedAction: statement.limitHint },
      );
    }
    const saved = core.store.saveResult({
      sessionId: session.id,
      connectionId: connection.id,
      fingerprint,
      sql: statement.descriptor,
      parameters,
      rows: result.rows,
      columns: result.columns,
      stateVersion,
      stateSignature,
      stateConfidence: adapter.confidence,
      expiresAt: core.expiresAt(resultTtlSeconds * 1000),
    });
    return {
      data: core.resultData(saved, false, previewRows),
      handle: saved.id,
      executed: true,
      warnings: statement.warnings,
      stateVersion,
      confidence: adapter.confidence,
    };
  });
}

export function queryPreviewRows(value: number): number {
  const previewRows = nonNegativeInteger(value, "previewRows");
  if (previewRows <= 200) return previewRows;
  throw new StateQLError(
    "OUTPUT_LIMIT_EXCEEDED",
    "previewRows cannot exceed 200 rows.",
  );
}

export function sqlParametersLength(parameters: SqlParameters): number {
  return Array.isArray(parameters)
    ? parameters.length
    : Object.keys(parameters).length;
}

export function mongoDescriptor(operation: string): string {
  return `MongoDB native ${operation}`;
}

export function redisDescriptor(command: string): string {
  return `Redis native ${command}`;
}

/** Driver validators throw plain errors; callers see them as INVALID_COMMAND. */
export function invalidCommand<T>(validate: () => T): T {
  try {
    return validate();
  } catch (error) {
    throw new StateQLError("INVALID_COMMAND", errorMessage(error));
  }
}

function cacheMiss(message: string): StateQLError {
  return new StateQLError("CACHE_MISS", message, {
    retryable: true,
    suggestedAction: "Run with cache auto or cache bypass.",
  });
}

function boundedReadSql(sql: string, limit: number): string {
  const statement = sql.trim().replace(/;\s*$/, "");
  return `SELECT * FROM (${statement}) AS _stateql_bounded LIMIT ${limit}`;
}

function mongoPaginationWarnings(command: MongoReadCommand): Warning[] {
  const ordered = command.operation === "find"
    ? command.options?.sort !== undefined
    : command.pipeline.some((stage) =>
        Object.hasOwn(stage, "$sort")
      );
  return ordered
    ? []
    : [{
        code: "NON_DETERMINISTIC_PAGINATION",
        message: "MongoDB result has no explicit sort.",
      }];
}
