import { existsSync, statSync } from "node:fs";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { Client, types as pgTypes } from "pg";
import type {
  Column,
  Row,
  SqlParameters,
  StateConfidence,
} from "./types.js";
import type { ConnectionRecord, OperationRecord } from "./store.js";
import { hash, parseJson, toJsonSafe } from "./util.js";

export interface ReadResult {
  rows: Row[];
  columns: Column[];
}

export interface WriteResult {
  affectedRows: number;
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

export async function createAdapter(
  connection: ConnectionRecord,
): Promise<Adapter> {
  const source = connection.secret_env
    ? process.env[connection.secret_env]
    : connection.source;
  if (!source) {
    throw new Error(
      `Environment variable ${connection.secret_env ?? "(missing)"} is not set.`,
    );
  }
  if (connection.driver === "sqlite") {
    return new SQLiteAdapter(source, Boolean(connection.read_only));
  }
  return new PostgresAdapter(source, Boolean(connection.read_only));
}

class SQLiteAdapter implements Adapter {
  readonly confidence = "database_reported" as const;
  private readonly db: DatabaseSync;

  constructor(
    private readonly source: string,
    private readonly readOnly: boolean,
  ) {
    this.db = new DatabaseSync(source, {
      readOnly,
      enableForeignKeyConstraints: true,
    });
    this.db.exec("PRAGMA busy_timeout = 5000");
  }

  async read(sql: string, params: SqlParameters): Promise<ReadResult> {
    const statement = this.db.prepare(sql);
    const rows = bindAll(statement, params) as Row[];
    return {
      rows: toJsonSafe(rows),
      columns: statement.columns().map((column) => ({
        name: column.name,
        type: column.type?.toLowerCase() ?? inferType(rows, column.name),
      })),
    };
  }

  async write(sql: string, params: SqlParameters): Promise<WriteResult> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    const result = bindRun(this.db.prepare(sql), params);
    return { affectedRows: Number(result.changes) };
  }

  async writeBatch(
    operations: OperationRecord[],
    isolation: string,
  ): Promise<WriteResult[]> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    if (isolation !== "serializable") {
      throw new Error(`SQLite does not support isolation level "${isolation}".`);
    }
    const results: WriteResult[] = [];
    try {
      this.db.exec("BEGIN");
    } catch (error) {
      throw new BatchWriteError(errorText(error), false);
    }
    try {
      for (const operation of operations) {
        results.push(
          await this.write(
            operation.sql,
            parseJson<SqlParameters>(operation.parameters, []),
          ),
        );
      }
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        throw new BatchWriteError(errorText(error), true);
      }
      throw new BatchWriteError(errorText(error), false);
    }
    try {
      this.db.exec("COMMIT");
      return results;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
        throw new BatchWriteError(errorText(error), false);
      } catch (rollbackError) {
        if (rollbackError instanceof BatchWriteError) throw rollbackError;
        throw new BatchWriteError(errorText(error), true);
      }
    }
  }

  async signature(): Promise<string> {
    if (this.source === ":memory:") return "memory";
    const stats = statSync(this.source, { bigint: true });
    const walPath = `${this.source}-wal`;
    const wal = existsSync(walPath)
      ? statSync(walPath, { bigint: true })
      : undefined;
    return hash({
      size: stats.size.toString(),
      modified: stats.mtimeNs.toString(),
      walSize: wal?.size.toString() ?? "0",
      walModified: wal?.mtimeNs.toString() ?? "0",
    });
  }

  async inspect(kind: string, table?: string): Promise<unknown> {
    if (kind === "schema") {
      const tables = this.db
        .prepare(
          `SELECT name, type
           FROM sqlite_master
           WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
           ORDER BY name`,
        )
        .all();
      return { schema: "main", tables };
    }
    if (!table) throw new Error(`Table is required for inspect ${kind}.`);
    const quoted = quoteSqliteLiteral(table);
    const columns = this.db
      .prepare(`PRAGMA table_info(${quoted})`)
      .all() as Array<Record<string, unknown>>;
    if (columns.length === 0) throw new Error(`Table "${table}" was not found.`);
    const indexes = this.db
      .prepare(`PRAGMA index_list(${quoted})`)
      .all() as Array<Record<string, unknown>>;
    const foreignKeys = this.db
      .prepare(`PRAGMA foreign_key_list(${quoted})`)
      .all() as Array<Record<string, unknown>>;

    if (kind === "columns") return { table, columns };
    if (kind === "indexes") return { table, indexes };
    if (kind === "constraints") {
      return {
        table,
        primary_key: columns
          .filter((column) => Number(column.pk) > 0)
          .map((column) => column.name),
        foreign_keys: foreignKeys,
      };
    }
    if (kind !== "table") throw new Error(`Unknown inspection kind "${kind}".`);
    return {
      table,
      schema: "main",
      columns: columns.map((column) => ({
        name: column.name,
        type: String(column.type).toLowerCase(),
        nullable: column.notnull === 0 && Number(column.pk) === 0,
        primary_key: Number(column.pk) > 0,
      })),
      indexes: indexes.length,
      foreign_keys: foreignKeys.length,
    };
  }

  async close(): Promise<void> {
    this.db.close();
  }
}

class PostgresAdapter implements Adapter {
  readonly confidence = "ttl_based" as const;
  private readonly client: Client;
  private connected = false;

  constructor(
    source: string,
    private readonly readOnly: boolean,
  ) {
    this.client = new Client({ connectionString: source });
  }

  async read(sql: string, params: SqlParameters): Promise<ReadResult> {
    await this.connect();
    await this.client.query("BEGIN READ ONLY");
    try {
      const result = await this.client.query(sql, postgresParams(params));
      await this.client.query("COMMIT");
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
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async write(sql: string, params: SqlParameters): Promise<WriteResult> {
    if (this.readOnly) throw new Error("Connection is read-only.");
    await this.connect();
    const result = await this.client.query(sql, postgresParams(params));
    return { affectedRows: result.rowCount ?? 0 };
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
      await this.client.query(`BEGIN ISOLATION LEVEL ${level}`);
    } catch (error) {
      throw new BatchWriteError(errorText(error), false);
    }
    const results: WriteResult[] = [];
    try {
      for (const operation of operations) {
        results.push(
          await this.write(
            operation.sql,
            parseJson<SqlParameters>(operation.parameters, []),
          ),
        );
      }
    } catch (error) {
      try {
        await this.client.query("ROLLBACK");
      } catch {
        throw new BatchWriteError(errorText(error), true);
      }
      throw new BatchWriteError(errorText(error), false);
    }
    try {
      await this.client.query("COMMIT");
      return results;
    } catch (error) {
      try {
        await this.client.query("ROLLBACK");
      } catch {
        // COMMIT response was lost; rollback cannot establish outcome.
      }
      throw new BatchWriteError(errorText(error), true);
    }
  }

  async signature(): Promise<string> {
    return "ttl";
  }

  async inspect(kind: string, table?: string): Promise<unknown> {
    await this.connect();
    if (kind === "schema") {
      const result = await this.client.query(
        `SELECT table_schema AS schema, table_name AS name, table_type AS type
         FROM information_schema.tables
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
         ORDER BY table_schema, table_name`,
      );
      return { tables: result.rows };
    }
    if (!table) throw new Error(`Table is required for inspect ${kind}.`);
    const [schema, name] = table.includes(".")
      ? table.split(".", 2)
      : ["public", table];
    const columns = await this.client.query(
      `SELECT column_name AS name, data_type AS type,
              is_nullable = 'YES' AS nullable
       FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [schema, name],
    );
    if (columns.rows.length === 0) {
      throw new Error(`Table "${table}" was not found.`);
    }
    if (kind === "columns") return { table: name, schema, columns: columns.rows };

    const indexes = await this.client.query(
      `SELECT indexname AS name, indexdef AS definition
       FROM pg_indexes WHERE schemaname = $1 AND tablename = $2
       ORDER BY indexname`,
      [schema, name],
    );
    const constraints = await this.client.query(
      `SELECT constraint_name AS name, constraint_type AS type
       FROM information_schema.table_constraints
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY constraint_name`,
      [schema, name],
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

  async close(): Promise<void> {
    if (this.connected) await this.client.end();
  }

  private async connect(): Promise<void> {
    if (this.connected) return;
    await this.client.connect();
    this.connected = true;
    if (this.readOnly) {
      await this.client.query(
        "SET default_transaction_read_only = on",
      );
    }
  }
}

const POSTGRES_ISOLATION_LEVELS = new Set([
  "SERIALIZABLE",
  "REPEATABLE READ",
  "READ COMMITTED",
  "READ UNCOMMITTED",
]);

function bindAll(
  statement: StatementSync,
  params: SqlParameters,
): Array<Record<string, unknown>> {
  if (Array.isArray(params)) return statement.all(...(params as never[]));
  return statement.all(params as never);
}

function bindRun(
  statement: StatementSync,
  params: SqlParameters,
): { changes: number | bigint } {
  if (Array.isArray(params)) return statement.run(...(params as never[]));
  return statement.run(params as never);
}

function postgresParams(params: SqlParameters): unknown[] {
  if (Array.isArray(params)) return params;
  throw new Error("PostgreSQL parameters must be a JSON array.");
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function inferType(rows: Row[], name: string): string {
  const value = rows.find((row) => row[name] !== null)?.[name];
  if (value === undefined) return "unknown";
  if (Buffer.isBuffer(value)) return "binary";
  return typeof value;
}

function quoteSqliteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}
