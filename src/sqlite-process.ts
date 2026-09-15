import { existsSync, statSync } from "node:fs";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { BatchWriteOperation } from "./adapters.js";
import type { CatalogObject, DescribeObjectData, ListObjectsData, ListObjectsFilter, Column, Row, SqlParameters } from "./types.js";
import { hash, isSqlParameters, parseJson, toJsonSafe } from "./util.js";

interface Request {
  id: number;
  source: string;
  readOnly: boolean;
  operation: "read" | "write" | "writeAutocommit" | "writeBatch" | "signature" | "inspect" | "listObjects" | "describeObject";
  args: unknown[];
  busyTimeoutMs: number;
}

interface Response {
  id: number;
  result?: unknown;
  error?: {
    message: string;
    outcomeUnknown?: boolean;
  };
}

let db: DatabaseSync | undefined;
let source: string | undefined;
let readOnly = true;

process.on("message", (request: Request) => {
  let response: Response;
  try {
    initialize(request);
    response = { id: request.id, result: execute(request) };
  } catch (error) {
    response = {
      id: request.id,
      error: {
        message: errorText(error),
        ...(error instanceof SQLiteBatchError
          ? { outcomeUnknown: error.outcomeUnknown }
          : {}),
      },
    };
  }
  process.send?.(response);
});

process.on("disconnect", () => {
  close();
  process.exit(0);
});
process.on("exit", close);

function initialize(request: Request): void {
  if (!db) {
    source = request.source;
    readOnly = request.readOnly;
    db = new DatabaseSync(source, {
      readOnly,
      enableForeignKeyConstraints: true,
    });
  }
  db.exec(`PRAGMA busy_timeout = ${Math.max(1, request.busyTimeoutMs)}`);
}

function execute(request: Request): unknown {
  const database = db!;
  switch (request.operation) {
    case "read": {
      const [sql, params] = request.args as [string, SqlParameters];
      const statement = database.prepare(sql);
      const rows = bindAll(statement, params) as Row[];
      return {
        rows: toJsonSafe(rows),
        columns: statement.columns().map((column) => ({
          name: column.name,
          type: column.type?.toLowerCase() ?? inferType(rows, column.name),
        })),
      };
    }
    case "write": {
      if (readOnly) throw new Error("Connection is read-only.");
      const [sql, params, expectedRows] = request.args as [string, SqlParameters, 1 | undefined];
      try {
        database.exec("BEGIN");
      } catch (error) {
        throw new SQLiteBatchError(errorText(error), false);
      }
      try {
        const result = bindRun(database.prepare(sql), params);
        if (expectedRows === 1 && Number(result.changes) !== 1) throw new Error("ROW_CONFLICT: The row changed or no longer has a unique identity.");
        database.exec("COMMIT");
        return { affectedRows: Number(result.changes) };
      } catch (error) {
        try {
          database.exec("ROLLBACK");
          throw new SQLiteBatchError(errorText(error), false);
        } catch (rollbackError) {
          if (rollbackError instanceof SQLiteBatchError) throw rollbackError;
          throw new SQLiteBatchError(errorText(error), true);
        }
      }
    }
    case "writeAutocommit": {
      if (readOnly) throw new SQLiteBatchError("Connection is read-only.", false);
      const [sql, params] = request.args as [string, SqlParameters];
      if (
        (Array.isArray(params) && params.length > 0) ||
        (!Array.isArray(params) && Object.keys(params).length > 0)
      ) {
        throw new SQLiteBatchError(
          "SQLite maintenance statements do not accept parameters.",
          false,
        );
      }
      try {
        database.exec(sql);
        return { affectedRows: 0 };
      } catch (error) {
        throw new SQLiteBatchError(errorText(error), true);
      }
    }
    case "writeBatch": {
      if (readOnly) throw new Error("Connection is read-only.");
      const [operations, isolation] = request.args as [BatchWriteOperation[], string];
      if (isolation !== "serializable") {
        throw new Error(`SQLite does not support isolation level "${isolation}".`);
      }
      const results: Array<{ affectedRows: number }> = [];
      try {
        database.exec("BEGIN");
      } catch (error) {
        throw new SQLiteBatchError(errorText(error), false);
      }
      try {
        for (const operation of operations) {
          const result = bindRun(
            database.prepare(operation.sql),
            parseJson<SqlParameters>(
              operation.parameters,
              `operation "${operation.id}" parameters`,
              isSqlParameters,
            ),
          );
          if (operation.expectedRows === 1 && Number(result.changes) !== 1) throw new Error("ROW_CONFLICT: The row changed or no longer has a unique identity.");
          results.push({ affectedRows: Number(result.changes) });
        }
      } catch (error) {
        try {
          database.exec("ROLLBACK");
        } catch {
          throw new SQLiteBatchError(errorText(error), true);
        }
        throw new SQLiteBatchError(errorText(error), false);
      }
      try {
        database.exec("COMMIT");
        return results;
      } catch (error) {
        try {
          database.exec("ROLLBACK");
          throw new SQLiteBatchError(errorText(error), false);
        } catch (rollbackError) {
          if (rollbackError instanceof SQLiteBatchError) throw rollbackError;
          throw new SQLiteBatchError(errorText(error), true);
        }
      }
    }
    case "signature": {
      if (source === ":memory:") return "memory";
      const stats = statSync(source!, { bigint: true });
      const walPath = `${source}-wal`;
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
    case "inspect": {
      const [kind, table] = request.args as [string, string | undefined];
      return inspect(database, kind, table);
    }
    case "listObjects": {
      const [filter] = request.args as [ListObjectsFilter];
      return listObjects(database, filter);
    }
    case "describeObject": {
      const [object] = request.args as [CatalogObject];
      return describeObject(database, object);
    }
  }
}

function inspect(database: DatabaseSync, kind: string, table?: string): unknown {
  if (kind === "schema") {
    const tables = database
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
  if (kind === "editable") {
    const object = database.prepare("SELECT type FROM sqlite_master WHERE name = ?").get(table) as { type: string } | undefined;
    const columns = database.prepare(`PRAGMA table_xinfo(${quoted})`).all() as Array<Record<string, unknown>>;
    return { writable: object?.type === "table", columns: columns.map(column => ({ name: String(column.name), type: String(column.type).toLowerCase(), nullable: !column.notnull && !column.pk, generated: Number(column.hidden) !== 0, key: Number(column.pk) })) };
  }
  const columns = database
    .prepare(`PRAGMA table_info(${quoted})`)
    .all() as Array<Record<string, unknown>>;
  if (columns.length === 0) throw new Error(`Table "${table}" was not found.`);
  const indexes = database
    .prepare(`PRAGMA index_list(${quoted})`)
    .all() as Array<Record<string, unknown>>;
  const foreignKeys = database
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

function listObjects(database: DatabaseSync, filter: ListObjectsFilter): ListObjectsData {
  const supported = ["table", "view", "trigger"] as const;
  if (filter.kind !== undefined && !supported.includes(filter.kind as typeof supported[number])) throw new Error(`SQLite does not support catalog kind "${filter.kind}".`);
  if (filter.schema !== undefined && filter.schema !== "main") throw new Error("SQLite only supports the main schema.");
  const offset = filter.offset ?? 0;
  const limit = filter.limit ?? 50;
  if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0 || offset > 1_000_000 || !Number.isSafeInteger(limit) || limit < 1 || limit > 200) throw new Error("Invalid SQLite catalog page bounds.");
  if (filter.search !== undefined && (!filter.search || filter.search.length > 200 || filter.search.includes("\0"))) throw new Error("Invalid SQLite catalog search.");
  const types = filter.kind ? [filter.kind] : [...supported];
  const placeholders = types.map(() => "?").join(", ");
  const rows = database.prepare(
    `SELECT type AS kind, 'main' AS schema, name, 'main.' || name AS identity
     FROM sqlite_master WHERE type IN (${placeholders}) AND name NOT LIKE 'sqlite_%'
       AND (? IS NULL OR instr(lower(name), lower(?)) > 0)
     ORDER BY kind, name LIMIT ? OFFSET ?`,
  ).all(...types, filter.search ?? null, filter.search ?? null, limit + 1, offset) as unknown as CatalogObject[];
  const more = rows.length > limit;
  return { objects: rows.slice(0, limit), next_offset: more ? offset + limit : null, supported_kinds: [...supported] };
}

function describeObject(database: DatabaseSync, object: CatalogObject): DescribeObjectData {
  if (!["table", "view", "trigger"].includes(object.kind) || (object.schema !== undefined && object.schema !== "main")) throw new Error(`SQLite does not support catalog kind "${object.kind}".`);
  const row = database.prepare("SELECT type AS kind, 'main' AS schema, name, 'main.' || name AS identity, sql AS definition FROM sqlite_master WHERE type = ? AND name = ?").get(object.kind, object.name) as Record<string, unknown> | undefined;
  if (!row) throw new Error("Catalog object was not found.");
  const definition = row.definition === null ? null : String(row.definition);
  delete row.definition;
  return { object: row as CatalogObject, definition };
}


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

function inferType(rows: Row[], name: string): string {
  const value = rows.find((row) => row[name] !== null)?.[name];
  if (value === undefined) return "unknown";
  if (Buffer.isBuffer(value)) return "binary";
  return typeof value;
}

function quoteSqliteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function close(): void {
  try {
    db?.close();
  } catch {
    // Process teardown closes the database handle.
  }
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

class SQLiteBatchError extends Error {
  constructor(
    message: string,
    readonly outcomeUnknown: boolean,
  ) {
    super(message);
  }
}
