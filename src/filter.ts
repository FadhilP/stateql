import { DatabaseSync, type StatementSync } from "node:sqlite";
import { StateQLError } from "./errors.js";
import { analyzeSql } from "./sql.js";
import type { Column, Row, SqlParameters } from "./types.js";

interface PreparedFilter {
  sql: string;
  normalized: string;
  tableName: string;
  indexColumn: string;
  columnNames: string[];
  positionalParameters: number;
  namedParameters: string[];
}

export function prepareFilterStatement(
  columns: Column[],
  predicate: string,
): PreparedFilter {
  const text = predicate.trim();
  if (!text) {
    throw new StateQLError("INVALID_SQL", "Filter predicate is required.");
  }

  const columnNames = columns.map((column) => column.name);
  if (columnNames.length === 0) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter requires at least one result column.",
    );
  }
  const names = new Set<string>();
  for (const name of columnNames) {
    const normalized = name.toLowerCase();
    if (!name || name.includes("\0") || names.has(normalized)) {
      throw new StateQLError(
        "INVALID_SQL",
        "Filter requires unique, non-empty result column names.",
      );
    }
    names.add(normalized);
  }

  const tableName = "__stateql_filter_source";
  let indexColumn = "__stateql_row_index";
  while (names.has(indexColumn.toLowerCase())) indexColumn += "_";
  const sql =
    `SELECT ${quoteIdentifier(indexColumn)} ` +
    `FROM ${quoteIdentifier(tableName)} WHERE (${text})`;
  const analysis = analyzeSql(sql, "sqlite");
  const details = analysis.ast as unknown as Record<string, unknown>;
  const from = details.from;
  const source = Array.isArray(from)
    ? (from[0] as Record<string, unknown> | undefined)
    : undefined;
  if (
    !analysis.read ||
    !Array.isArray(from) ||
    from.length !== 1 ||
    source?.table !== tableName ||
    details.with ||
    details.groupby ||
    details.having ||
    details.orderby ||
    details.limit ||
    details.for_update ||
    details._next ||
    details.set_op ||
    containsSelect(details.where)
  ) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter accepts one scalar predicate only.",
    );
  }
  validateFilterExpression(details.where, names, tableName);
  const bindings = filterBindings(details.where);
  return {
    sql,
    normalized: analysis.normalized,
    tableName,
    indexColumn,
    columnNames,
    positionalParameters: bindings.positional,
    namedParameters: [...bindings.named],
  };
}

export function filterMaterializedRows(
  rows: Row[],
  filter: PreparedFilter,
  parameters: SqlParameters,
): Row[] {
  const db = new DatabaseSync(":memory:");
  try {
    const definitions = [
      `${quoteIdentifier(filter.indexColumn)} INTEGER PRIMARY KEY`,
      ...filter.columnNames.map(quoteIdentifier),
    ];
    db.exec(
      `CREATE TABLE ${quoteIdentifier(filter.tableName)} ` +
        `(${definitions.join(", ")})`,
    );
    const placeholders = filter.columnNames.map(() => "?").join(", ");
    const insert = db.prepare(
      `INSERT INTO ${quoteIdentifier(filter.tableName)} (` +
        `${quoteIdentifier(filter.indexColumn)}, ` +
        `${filter.columnNames.map(quoteIdentifier).join(", ")}) ` +
        `VALUES (?, ${placeholders})`,
    );
    db.exec("BEGIN");
    try {
      rows.forEach((row, index) => {
        const values = filter.columnNames.map((name) =>
          sqliteFilterValue(row[name]),
        );
        insert.run(index, ...(values as never[]));
      });
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }

    const statement = db.prepare(
      `${filter.sql}\nORDER BY ${quoteIdentifier(filter.indexColumn)}`,
    );
    const selected = filterAll(statement, parameters);
    return selected.map((row) => {
      const index = Number(row[filter.indexColumn]);
      if (!Number.isInteger(index) || !rows[index]) {
        throw new StateQLError(
          "INVALID_SQL",
          "Filter produced an invalid source row index.",
        );
      }
      return rows[index];
    });
  } catch (error) {
    if (error instanceof StateQLError) throw error;
    throw new StateQLError("INVALID_SQL", errorMessage(error));
  } finally {
    db.close();
  }
}

function filterAll(
  statement: StatementSync,
  parameters: SqlParameters,
): Array<Record<string, unknown>> {
  if (Array.isArray(parameters)) {
    return statement.all(...(parameters as never[])) as Array<
      Record<string, unknown>
    >;
  }
  return statement.all(parameters as never) as Array<Record<string, unknown>>;
}

function sqliteFilterValue(
  value: unknown,
): string | number | bigint | Uint8Array | null {
  if (value === null || value === undefined) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value instanceof Uint8Array) return value;
  try {
    return JSON.stringify(value) ?? null;
  } catch {
    return String(value);
  }
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function containsSelect(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  if (record.type === "select") return true;
  return Object.values(record).some(containsSelect);
}

const FILTER_FUNCTIONS = new Set([
  "abs",
  "coalesce",
  "ifnull",
  "instr",
  "json_extract",
  "json_type",
  "json_valid",
  "length",
  "lower",
  "ltrim",
  "nullif",
  "round",
  "rtrim",
  "substr",
  "substring",
  "trim",
  "typeof",
  "upper",
]);

function validateFilterExpression(
  value: unknown,
  columns: Set<string>,
  tableName: string,
): void {
  if (!value || typeof value !== "object") return;
  const record = value as Record<string, unknown>;
  if (record.type === "column_ref") {
    const column = record.column;
    const table = record.table;
    if (
      typeof column !== "string" ||
      !columns.has(column.toLowerCase()) ||
      (table !== null && table !== undefined && table !== tableName)
    ) {
      throw new StateQLError(
        "INVALID_SQL",
        `Unknown filter column "${String(column)}".`,
      );
    }
  } else if (record.type === "double_quote_string") {
    const column = String(record.value);
    if (!columns.has(column.toLowerCase())) {
      throw new StateQLError(
        "INVALID_SQL",
        `Unknown filter column "${column}".`,
      );
    }
  } else if (record.type === "function") {
    const name = filterFunctionName(record);
    if (!name || !FILTER_FUNCTIONS.has(name)) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter function "${name ?? "unknown"}" is not allowed.`,
      );
    }
  }
  Object.values(record).forEach((item) =>
    validateFilterExpression(item, columns, tableName),
  );
}

function filterFunctionName(record: Record<string, unknown>): string | undefined {
  const name = record.name as Record<string, unknown> | undefined;
  const parts = name?.name;
  if (!Array.isArray(parts)) return undefined;
  const last = parts.at(-1) as Record<string, unknown> | undefined;
  return typeof last?.value === "string" ? last.value.toLowerCase() : undefined;
}

function filterBindings(value: unknown): {
  positional: number;
  named: Set<string>;
} {
  const named = new Set<string>();
  const prefixes = new Map<string, string>();
  let positional = 0;
  const visit = (item: unknown): void => {
    if (!item || typeof item !== "object") return;
    const record = item as Record<string, unknown>;
    if (record.type === "origin" && record.value === "?") positional += 1;
    if (record.type === "param" && typeof record.value === "string") {
      addNamed(String(record.value), `:${String(record.value)}`);
    }
    if (
      record.type === "var" &&
      typeof record.name === "string" &&
      (record.prefix === "$" || record.prefix === "@")
    ) {
      addNamed(record.name, `${String(record.prefix)}${record.name}`);
    }
    Object.values(record).forEach(visit);
  };
  const addNamed = (name: string, token: string): void => {
    const previous = prefixes.get(name);
    if (previous && previous !== token) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter parameter "${name}" uses conflicting prefixes.`,
      );
    }
    prefixes.set(name, token);
    named.add(name);
  };
  visit(value);
  if (positional && named.size) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter cannot mix positional and named parameters.",
    );
  }
  return { positional, named };
}

export function validateFilterParameters(
  filter: PreparedFilter,
  parameters: SqlParameters,
): void {
  if (filter.positionalParameters) {
    if (
      !Array.isArray(parameters) ||
      parameters.length !== filter.positionalParameters
    ) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter requires exactly ${filter.positionalParameters} positional parameters.`,
      );
    }
    return;
  }
  if (filter.namedParameters.length) {
    if (Array.isArray(parameters)) {
      throw new StateQLError(
        "INVALID_SQL",
        "Filter requires named parameters.",
      );
    }
    const supplied = Object.keys(parameters).sort();
    const expected = [...filter.namedParameters].sort();
    if (JSON.stringify(supplied) !== JSON.stringify(expected)) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter requires named parameters: ${expected.join(", ")}.`,
      );
    }
    return;
  }
  if (
    (Array.isArray(parameters) && parameters.length) ||
    (!Array.isArray(parameters) && Object.keys(parameters).length)
  ) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter predicate has no parameters.",
    );
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
