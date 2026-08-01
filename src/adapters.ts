import { fork, type ChildProcess } from "node:child_process";
import {
  createConnection as createMySqlConnection,
  type Connection as MySqlCoreConnection,
  type ExecuteValues,
  type FieldPacket as MySqlFieldPacket,
  type QueryResult as MySqlQueryResult,
  type ResultSetHeader as MySqlResultSetHeader,
  type RowDataPacket as MySqlRowDataPacket,
} from "mysql2";
import type { Connection as MySqlConnection } from "mysql2/promise";
import { Client, types as pgTypes, type QueryResult } from "pg";
import type { Column, Row, SqlParameters, StateConfidence } from "./types.js";
import type { ConnectionRecord, OperationRecord } from "./store.js";
import { isSqlParameters, parseJson, toJsonSafe } from "./util.js";

export interface ReadResult {
  rows: Row[];
  columns: Column[];
}

export interface WriteResult {
  affectedRows: number;
}

export interface AdapterContext {
  deadline: number;
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

export interface Adapter {
  readonly confidence: StateConfidence;
  read(sql: string, params: SqlParameters): Promise<ReadResult>;
  write(sql: string, params: SqlParameters): Promise<WriteResult>;
  writeBatch(
    operations: OperationRecord[],
    isolation: string,
  ): Promise<WriteResult[]>;
  signature(): Promise<string>;
  inspect(kind: string, table?: string): Promise<unknown>;
  close(): Promise<void>;
}

export function createAdapterContext(
  timeoutMs: number,
  signal?: AbortSignal,
): AdapterContext {
  return {
    deadline: Date.now() + timeoutMs,
    ...(signal ? { signal } : {}),
  };
}

export async function createAdapter(
  connection: ConnectionRecord,
  context: AdapterContext,
  input: { source: string },
): Promise<Adapter> {
  const { source } = input;
  if (connection.driver === "sqlite") {
    return new SQLiteAdapter(source, Boolean(connection.read_only), context);
  }
  if (connection.driver === "postgres") {
    return new PostgresAdapter(source, Boolean(connection.read_only), context);
  }
  return new MySqlAdapter(source, Boolean(connection.read_only), context);
}

interface SQLiteResponse {
  id: number;
  result?: unknown;
  error?: {
    message: string;
    outcomeUnknown?: boolean;
  };
}

interface PendingSQLiteCall {
  batch: boolean;
  resolve(value: unknown): void;
  reject(error: unknown): void;
}

class SQLiteAdapter implements Adapter {
  readonly confidence = "database_reported" as const;
  private readonly child: ChildProcess;
  private readonly pending = new Map<number, PendingSQLiteCall>();
  private nextId = 1;
  private closed = false;
  private closePromise?: Promise<void>;

  constructor(
    private readonly source: string,
    private readonly readOnly: boolean,
    private readonly context: AdapterContext,
  ) {
    this.child = fork(new URL("./sqlite-process.js", import.meta.url), [], {
      execArgv: [],
      stdio: ["ignore", "ignore", "ignore", "ipc"],
      serialization: "advanced",
    });
    this.child.on("message", (message: SQLiteResponse) => {
      const call = this.pending.get(message.id);
      if (!call) return;
      if (message.error) {
        call.reject(
          call.batch
            ? new BatchWriteError(
                message.error.message,
                message.error.outcomeUnknown ?? true,
              )
            : message.error.outcomeUnknown === undefined
              ? new Error(message.error.message)
              : new AdapterWriteError(
                  message.error.message,
                  message.error.outcomeUnknown,
                ),
        );
      } else {
        call.resolve(message.result);
      }
    });
    this.child.on("error", (error) => this.failPending(error));
    this.child.on("exit", (code, signal) => {
      if (!this.closed) {
        this.failPending(
          new Error(
            `SQLite execution process exited unexpectedly (${signal ?? code ?? "unknown"}).`,
          ),
        );
      }
    });
  }

  async read(sql: string, params: SqlParameters): Promise<ReadResult> {
    return this.call<ReadResult>("read", [sql, params], false, false);
  }

  async write(sql: string, params: SqlParameters): Promise<WriteResult> {
    return this.call<WriteResult>("write", [sql, params], true, false);
  }

  async writeBatch(
    operations: OperationRecord[],
    isolation: string,
  ): Promise<WriteResult[]> {
    return this.call<WriteResult[]>(
      "writeBatch",
      [operations, isolation],
      true,
      true,
    );
  }

  async signature(): Promise<string> {
    return this.call<string>("signature", [], false, false);
  }

  async inspect(kind: string, table?: string): Promise<unknown> {
    return this.call("inspect", [kind, table], false, false);
  }

  async close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.closePromise = new Promise<void>((resolve) => {
      const done = (): void => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        try {
          this.child.kill("SIGKILL");
        } catch {
          // Process already exited.
        }
      }, 1_000);
      timer.unref();
      this.child.once("exit", done);
      try {
        this.child.disconnect();
      } catch {
        try {
          this.child.kill("SIGKILL");
        } catch {
          // Process already exited.
        }
      }
    });
    return this.closePromise;
  }

  private async call<T>(
    operation: "read" | "write" | "writeBatch" | "signature" | "inspect",
    args: unknown[],
    outcomeUnknown: boolean,
    batch: boolean,
  ): Promise<T> {
    throwIfStopped(this.context, false);
    if (this.closed) throw new Error("SQLite adapter is closed.");
    const id = this.nextId++;
    const result = new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { batch, resolve, reject });
      try {
        this.child.send(
          {
            id,
            source: this.source,
            readOnly: this.readOnly,
            operation,
            args,
            // Let the client deadline terminate the worker before SQLite's
            // lock timeout races it and reports a known, non-executed failure.
            busyTimeoutMs: Math.min(
              5_000,
              remainingMilliseconds(this.context) + 250,
            ),
          },
          (error) => {
            if (error) reject(error);
          },
        );
      } catch (error) {
        reject(error);
      }
    });
    try {
      return (await withContext(
        result,
        this.context,
        () => this.terminate(),
        outcomeUnknown,
      )) as T;
    } finally {
      this.pending.delete(id);
    }
  }

  private terminate(): void {
    if (this.closed) return;
    this.closed = true;
    try {
      this.child.kill("SIGKILL");
    } catch {
      // Process already exited.
    }
  }

  private failPending(error: unknown): void {
    for (const call of this.pending.values()) call.reject(error);
    this.pending.clear();
  }
}

const STRICT_POSTGRES_SSL_MODE_ALIASES = new Set(["prefer", "require", "verify-ca"]);

export function normalizePostgresConnectionString(source: string): string {
  try {
    const url = new URL(source);
    const parameters = [...url.searchParams.entries()];
    const libpqCompat = parameters.filter(([key]) => key === "uselibpqcompat").at(-1)?.[1];
    const sslMode = parameters.filter(([key]) => key === "sslmode").at(-1)?.[1];
    if (libpqCompat === "true" || !sslMode || !STRICT_POSTGRES_SSL_MODE_ALIASES.has(sslMode)) return source;
    url.searchParams.delete("sslmode");
    url.searchParams.append("sslmode", "verify-full");
    return url.toString();
  } catch {
    return source;
  }
}

class PostgresAdapter implements Adapter {
  readonly confidence = "ttl_based" as const;
  private readonly client: Client;
  private connected = false;
  private ending: Promise<void> | undefined;

  constructor(
    source: string,
    private readonly readOnly: boolean,
    private readonly context: AdapterContext,
  ) {
    const timeout = Math.min(
      2_147_483_647,
      remainingMilliseconds(context),
    );
    this.client = new Client({
      connectionString: normalizePostgresConnectionString(source),
      connectionTimeoutMillis: timeout,
      statement_timeout: timeout,
    });
  }

  async read(sql: string, params: SqlParameters): Promise<ReadResult> {
    await this.connect();
    await this.query("BEGIN READ ONLY", [], false);
    try {
      await this.setLocalDeadline();
      const result = await this.query(sql, postgresParams(params), false);
      await this.query("COMMIT", [], false);
      return {
        rows: toJsonSafe(result.rows as Row[]),
        columns: result.fields.map((field) => ({
          name: field.name,
          type:
            pgTypes.getTypeParser(field.dataTypeID).name ||
            `oid_${field.dataTypeID}`,
        })),
      };
    } catch (error) {
      await this.rollbackQuietly();
      throw error;
    }
  }

  async write(sql: string, params: SqlParameters): Promise<WriteResult> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    try {
      await this.connect();
      await this.query("BEGIN", [], false);
    } catch (error) {
      if (error instanceof AdapterExecutionError) throw error;
      throw new AdapterWriteError(errorText(error), false);
    }
    let committing = false;
    try {
      await this.setLocalDeadline();
      const result = await this.query(sql, postgresParams(params), true);
      committing = true;
      await this.query("COMMIT", [], true);
      return { affectedRows: result.rowCount ?? 0 };
    } catch (error) {
      const rolledBack = await this.rollbackQuietly();
      if (!committing && rolledBack) {
        if (error instanceof AdapterExecutionError) {
          throw new AdapterExecutionError(error.message, error.reason, false);
        }
        throw new AdapterWriteError(errorText(error), false);
      }
      if (error instanceof AdapterExecutionError) throw error;
      throw new AdapterWriteError(errorText(error), true);
    }
  }

  async writeBatch(
    operations: OperationRecord[],
    isolation: string,
  ): Promise<WriteResult[]> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    await this.connect();
    const level = isolation.toUpperCase();
    if (!POSTGRES_ISOLATION_LEVELS.has(level)) {
      throw new Error(`Unsupported PostgreSQL isolation level "${isolation}".`);
    }
    try {
      await this.query(`BEGIN ISOLATION LEVEL ${level}`, [], false);
    } catch (error) {
      throw new BatchWriteError(errorText(error), false);
    }
    const results: WriteResult[] = [];
    try {
      for (const operation of operations) {
        await this.setLocalDeadline();
        const result = await this.query(
          operation.sql,
          postgresParams(parseJson<SqlParameters>(
            operation.parameters,
            `operation "${operation.id}" parameters`,
            isSqlParameters,
          )),
          true,
        );
        results.push({ affectedRows: result.rowCount ?? 0 });
      }
    } catch (error) {
      if (await this.rollbackQuietly()) {
        throw new BatchWriteError(errorText(error), false);
      }
      throw new BatchWriteError(errorText(error), true);
    }
    try {
      await this.query("COMMIT", [], true);
      return results;
    } catch (error) {
      await this.rollbackQuietly();
      throw new BatchWriteError(errorText(error), true);
    }
  }

  async signature(): Promise<string> {
    throwIfStopped(this.context, false);
    return "ttl";
  }

  async inspect(kind: string, table?: string): Promise<unknown> {
    await this.connect();
    await this.query("BEGIN READ ONLY", [], false);
    try {
      const result = await this.inspectTransaction(kind, table);
      await this.query("COMMIT", [], false);
      return result;
    } catch (error) {
      await this.rollbackQuietly();
      throw error;
    }
  }

  async close(): Promise<void> {
    if (!this.ending) {
      this.ending = this.client.end().catch(() => undefined);
    }
    await this.ending;
  }

  private async inspectTransaction(kind: string, table?: string): Promise<unknown> {
    if (kind === "schema") {
      await this.setLocalDeadline();
      const result = await this.query(
        `SELECT table_schema AS schema, table_name AS name, table_type AS type
         FROM information_schema.tables
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
         ORDER BY table_schema, table_name`,
        [],
        false,
      );
      return { tables: result.rows };
    }
    if (!table) throw new Error(`Table is required for inspect ${kind}.`);
    const [schema, name] = table.includes(".")
      ? table.split(".", 2)
      : ["public", table];
    await this.setLocalDeadline();
    const columns = await this.query(
      `SELECT column_name AS name, data_type AS type,
              is_nullable = 'YES' AS nullable
       FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [schema, name],
      false,
    );
    if (columns.rows.length === 0) {
      throw new Error(`Table "${table}" was not found.`);
    }
    if (kind === "columns") return { table: name, schema, columns: columns.rows };

    await this.setLocalDeadline();
    const indexes = await this.query(
      `SELECT indexname AS name, indexdef AS definition
       FROM pg_indexes WHERE schemaname = $1 AND tablename = $2
       ORDER BY indexname`,
      [schema, name],
      false,
    );
    await this.setLocalDeadline();
    const constraints = await this.query(
      `SELECT constraint_name AS name, constraint_type AS type
       FROM information_schema.table_constraints
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY constraint_name`,
      [schema, name],
      false,
    );
    if (kind === "indexes") return { table: name, schema, indexes: indexes.rows };
    if (kind === "constraints") {
      return { table: name, schema, constraints: constraints.rows };
    }
    if (kind !== "table") throw new Error(`Unknown inspection kind "${kind}".`);
    return {
      table: name,
      schema,
      columns: columns.rows,
      indexes: indexes.rows.length,
      constraints: constraints.rows.length,
    };
  }

  private async connect(): Promise<void> {
    if (this.connected) return;
    throwIfStopped(this.context, false);
    await withContext(
      this.client.connect(),
      this.context,
      () => this.stop(),
      false,
    );
    this.connected = true;
    if (this.readOnly) {
      await this.query("SET default_transaction_read_only = on", [], false);
    }
  }

  private async setLocalDeadline(): Promise<void> {
    await this.query(
      `SET LOCAL statement_timeout = ${remainingMilliseconds(this.context)}`,
      [],
      false,
    );
  }

  private async query(
    sql: string,
    params: unknown[],
    outcomeUnknown: boolean,
  ): Promise<QueryResult> {
    throwIfStopped(this.context, outcomeUnknown);
    try {
      return await withContext(
        this.client.query(sql, params),
        this.context,
        () => this.stop(),
        outcomeUnknown,
      );
    } catch (error) {
      if (error instanceof AdapterExecutionError) throw error;
      if (postgresErrorCode(error) === "57014") {
        throw stoppedError(this.context, outcomeUnknown);
      }
      throw error;
    }
  }

  private async rollbackQuietly(): Promise<boolean> {
    if (this.ending) return false;
    try {
      await this.query("ROLLBACK", [], false);
      return true;
    } catch {
      return false;
    }
  }

  private stop(): void {
    if (!this.ending) {
      this.ending = this.client.end().catch(() => undefined);
    }
  }
}

class MySqlAdapter implements Adapter {
  readonly confidence = "ttl_based" as const;
  private rawClient: MySqlCoreConnection | undefined;
  private client: MySqlConnection | undefined;
  private connected = false;
  private closed = false;

  constructor(
    private readonly source: string,
    private readonly readOnly: boolean,
    private readonly context: AdapterContext,
  ) {}

  async read(sql: string, params: SqlParameters): Promise<ReadResult> {
    await this.query("START TRANSACTION READ ONLY", [], false, false);
    try {
      const [result, fields] = await this.query(
        sql,
        mysqlParams(params),
        false,
        true,
      );
      await this.query("COMMIT", [], false, false);
      return {
        rows: toJsonSafe(mysqlRows(result)),
        columns: fields.map((field) => ({
          name: field.name,
          type: mysqlFieldType(field),
        })),
      };
    } catch (error) {
      await this.rollbackQuietly();
      throw error;
    }
  }

  async write(sql: string, params: SqlParameters): Promise<WriteResult> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    let values: ExecuteValues[];
    try {
      values = mysqlParams(params);
    } catch (error) {
      throw new AdapterWriteError(errorText(error), false);
    }
    try {
      await this.query("START TRANSACTION", [], false, false);
    } catch (error) {
      if (error instanceof AdapterExecutionError) throw error;
      throw new AdapterWriteError(errorText(error), false);
    }
    try {
      const [result] = await this.query(sql, values, true, true);
      await this.query("COMMIT", [], true, false);
      return { affectedRows: mysqlAffectedRows(result) };
    } catch (error) {
      await this.rollbackQuietly();
      if (error instanceof AdapterExecutionError) throw error;
      throw new AdapterWriteError(errorText(error), true);
    }
  }

  async writeBatch(
    operations: OperationRecord[],
    isolation: string,
  ): Promise<WriteResult[]> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    const unsupported = operations.find(
      (operation) =>
        !MYSQL_TRANSACTIONAL_STATEMENTS.has(operation.statement_type),
    );
    if (unsupported) {
      throw new BatchWriteError(
        `MySQL transactions cannot atomically include ${unsupported.statement_type.toUpperCase()} statements.`,
        false,
      );
    }
    const level = isolation.toUpperCase();
    if (!MYSQL_ISOLATION_LEVELS.has(level)) {
      throw new Error(`Unsupported MySQL isolation level "${isolation}".`);
    }
    try {
      await this.query(
        `SET TRANSACTION ISOLATION LEVEL ${level}`,
        [],
        false,
        false,
      );
      await this.query("START TRANSACTION", [], false, false);
    } catch (error) {
      throw new BatchWriteError(errorText(error), false);
    }
    const results: WriteResult[] = [];
    try {
      for (const operation of operations) {
        const [result] = await this.query(
          operation.sql,
          mysqlParams(parseJson<SqlParameters>(
            operation.parameters,
            `operation "${operation.id}" parameters`,
            isSqlParameters,
          )),
          true,
          true,
        );
        results.push({ affectedRows: mysqlAffectedRows(result) });
      }
    } catch (error) {
      if (await this.rollbackQuietly()) {
        throw new BatchWriteError(errorText(error), false);
      }
      throw new BatchWriteError(errorText(error), true);
    }
    try {
      await this.query("COMMIT", [], true, false);
      return results;
    } catch (error) {
      await this.rollbackQuietly();
      throw new BatchWriteError(errorText(error), true);
    }
  }

  async signature(): Promise<string> {
    throwIfStopped(this.context, false);
    return "ttl";
  }

  async inspect(kind: string, table?: string): Promise<unknown> {
    await this.query("START TRANSACTION READ ONLY", [], false, false);
    try {
      const result = await this.inspectTransaction(kind, table);
      await this.query("COMMIT", [], false, false);
      return result;
    } catch (error) {
      await this.rollbackQuietly();
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    if (!this.client) return;
    await this.client.end().catch(() => undefined);
  }

  private async inspectTransaction(kind: string, table?: string): Promise<unknown> {
    if (kind === "schema") {
      const [result] = await this.query(
        `SELECT TABLE_SCHEMA AS schema, TABLE_NAME AS name, TABLE_TYPE AS type
         FROM information_schema.tables
         WHERE TABLE_SCHEMA = DATABASE()
         ORDER BY TABLE_NAME`,
        [],
        false,
        false,
      );
      return { tables: mysqlRows(result) };
    }
    if (!table) throw new Error(`Table is required for inspect ${kind}.`);
    const separator = table.indexOf(".");
    const schema = separator === -1
      ? await this.databaseName()
      : table.slice(0, separator);
    const name = separator === -1 ? table : table.slice(separator + 1);
    if (!schema || !name) throw new Error(`Invalid table name "${table}".`);
    const [columnResult] = await this.query(
      `SELECT COLUMN_NAME AS name, DATA_TYPE AS type,
              IS_NULLABLE = 'YES' AS nullable
       FROM information_schema.columns
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY ORDINAL_POSITION`,
      [schema, name],
      false,
      true,
    );
    const columns = mysqlRows(columnResult).map((column) => ({
      ...column,
      nullable: Boolean(column.nullable),
    }));
    if (columns.length === 0) {
      throw new Error(`Table "${table}" was not found.`);
    }
    if (kind === "columns") return { table: name, schema, columns };

    const [indexResult] = await this.query(
      `SELECT INDEX_NAME AS name, NON_UNIQUE AS non_unique,
              INDEX_TYPE AS index_type,
              GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') AS columns
       FROM information_schema.statistics
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
       ORDER BY INDEX_NAME`,
      [schema, name],
      false,
      true,
    );
    const indexes = mysqlRows(indexResult).map((index) => ({
      name: String(index.name),
      definition: `${Number(index.non_unique) === 0 ? "UNIQUE " : ""}${String(index.index_type)} (${String(index.columns)})`,
    }));
    const [constraintResult] = await this.query(
      `SELECT CONSTRAINT_NAME AS name, CONSTRAINT_TYPE AS type
       FROM information_schema.table_constraints
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY CONSTRAINT_NAME`,
      [schema, name],
      false,
      true,
    );
    const constraints = mysqlRows(constraintResult);
    if (kind === "indexes") return { table: name, schema, indexes };
    if (kind === "constraints") {
      return { table: name, schema, constraints };
    }
    if (kind !== "table") throw new Error(`Unknown inspection kind "${kind}".`);
    return {
      table: name,
      schema,
      columns,
      indexes: indexes.length,
      constraints: constraints.length,
    };
  }

  private async databaseName(): Promise<string> {
    const [result] = await this.query(
      "SELECT DATABASE() AS database_name",
      [],
      false,
      false,
    );
    const row = mysqlRows(result)[0];
    if (!row?.database_name) throw new Error("MySQL connection has no database selected.");
    return String(row.database_name);
  }

  private async connect(): Promise<void> {
    if (this.connected) return;
    throwIfStopped(this.context, false);
    if (this.closed) throw new Error("MySQL adapter is closed.");
    const rawClient = createMySqlConnection({
      uri: this.source,
      connectTimeout: remainingMilliseconds(this.context),
      multipleStatements: false,
      namedPlaceholders: false,
      supportBigNumbers: true,
      bigNumberStrings: true,
      dateStrings: true,
    });
    this.rawClient = rawClient;
    this.client = rawClient.promise();
    try {
      await withContext(
        this.client.connect(),
        this.context,
        () => this.stop(),
        false,
      );
      this.connected = true;
      if (this.readOnly) {
        await this.runQuery(
          "SET SESSION TRANSACTION READ ONLY",
          [],
          false,
          false,
        );
      }
    } catch (error) {
      this.stop();
      throw error;
    }
  }

  private async query(
    sql: string,
    params: ExecuteValues[],
    outcomeUnknown: boolean,
    prepared: boolean,
  ): Promise<[MySqlQueryResult, MySqlFieldPacket[]]> {
    await this.connect();
    return this.runQuery(sql, params, outcomeUnknown, prepared);
  }

  private async runQuery(
    sql: string,
    params: ExecuteValues[],
    outcomeUnknown: boolean,
    prepared: boolean,
  ): Promise<[MySqlQueryResult, MySqlFieldPacket[]]> {
    throwIfStopped(this.context, outcomeUnknown);
    if (!this.client || this.closed) throw new Error("MySQL adapter is closed.");
    const result = prepared
      ? this.client.execute<MySqlQueryResult>(sql, params)
      : this.client.query<MySqlQueryResult>(sql, params);
    return withContext(
      result,
      this.context,
      () => this.stop(),
      outcomeUnknown,
    );
  }

  private async rollbackQuietly(): Promise<boolean> {
    if (this.closed || !this.connected) return false;
    try {
      await this.query("ROLLBACK", [], false, false);
      return true;
    } catch {
      return false;
    }
  }

  private stop(): void {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    this.rawClient?.destroy();
  }
}

const MYSQL_ISOLATION_LEVELS = new Set([
  "SERIALIZABLE",
  "REPEATABLE READ",
  "READ COMMITTED",
  "READ UNCOMMITTED",
]);

const MYSQL_TRANSACTIONAL_STATEMENTS = new Set([
  "delete",
  "insert",
  "replace",
  "update",
]);

function mysqlParams(params: SqlParameters): ExecuteValues[] {
  if (!Array.isArray(params)) {
    throw new Error("MySQL parameters must be a JSON array.");
  }
  return params.map((value) => {
    if (
      value === undefined ||
      Array.isArray(value) ||
      (typeof value === "object" &&
        value !== null &&
        !(value instanceof Date) &&
        !Buffer.isBuffer(value) &&
        !(value instanceof Uint8Array))
    ) {
      throw new Error("MySQL parameter values must be scalar.");
    }
    return value as ExecuteValues;
  });
}

function mysqlRows(result: MySqlQueryResult): Row[] {
  if (!Array.isArray(result)) {
    throw new Error("MySQL statement did not return rows.");
  }
  return result as MySqlRowDataPacket[] as Row[];
}

function mysqlAffectedRows(result: MySqlQueryResult): number {
  if (Array.isArray(result) || !("affectedRows" in result)) {
    throw new Error("MySQL statement did not return a write result.");
  }
  return Number((result as MySqlResultSetHeader).affectedRows);
}

function mysqlFieldType(field: MySqlFieldPacket): string {
  return String(
    field.typeName ?? field.columnType ?? field.type ?? "unknown",
  ).toLowerCase();
}

const POSTGRES_ISOLATION_LEVELS = new Set([
  "SERIALIZABLE",
  "REPEATABLE READ",
  "READ COMMITTED",
  "READ UNCOMMITTED",
]);

function postgresParams(params: SqlParameters): unknown[] {
  if (Array.isArray(params)) return params;
  throw new Error("PostgreSQL parameters must be a JSON array.");
}

function remainingMilliseconds(context: AdapterContext): number {
  return Math.max(1, Math.ceil(context.deadline - Date.now()));
}

function throwIfStopped(
  context: AdapterContext,
  outcomeUnknown: boolean,
): void {
  if (context.signal?.aborted || context.deadline <= Date.now()) {
    throw stoppedError(context, outcomeUnknown);
  }
}

function stoppedError(
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

function withContext<T>(
  promise: Promise<T>,
  context: AdapterContext,
  onStop: () => void,
  outcomeUnknown: boolean,
): Promise<T> {
  try {
    throwIfStopped(context, outcomeUnknown);
  } catch (error) {
    try {
      onStop();
    } catch {
      // Preserve deadline/cancellation error.
    }
    return Promise.reject(error);
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
        try {
          onStop();
        } catch {
          // Preserve deadline/cancellation error.
        }
        reject(stoppedError(context, outcomeUnknown));
      });
    };
    const stopped = (): boolean =>
      Boolean(context.signal?.aborted) || context.deadline <= Date.now();
    const timer = setTimeout(stop, remainingMilliseconds(context));
    context.signal?.addEventListener("abort", stop, { once: true });
    promise.then(
      (value) => {
        if (stopped()) stop();
        else finish(() => resolve(value));
      },
      (error) => {
        if (stopped()) stop();
        else finish(() => reject(error));
      },
    );
  });
}

function postgresErrorCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
