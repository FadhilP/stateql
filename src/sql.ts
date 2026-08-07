import { createRequire } from "node:module";
import type { AST } from "node-sql-parser";
import { StateQLError } from "./errors.js";
import type { Driver } from "./types.js";

const { Parser } = createRequire(import.meta.url)(
  "node-sql-parser",
) as typeof import("node-sql-parser");
const parser = new Parser();

export type StatementType =
  | "select"
  | "insert"
  | "replace"
  | "update"
  | "delete"
  | "create"
  | "alter"
  | "drop"
  | "truncate";

const SUPPORTED_STATEMENTS = new Set<StatementType>([
  "select",
  "insert",
  "replace",
  "update",
  "delete",
  "create",
  "alter",
  "drop",
  "truncate",
]);

export interface SqlAnalysis {
  ast: AST;
  normalized: string;
  statementType: StatementType;
  read: boolean;
  unboundedMutation: boolean;
  destructive: boolean;
  ordered: boolean;
}

export function analyzeSql(sql: string, driver: Driver): SqlAnalysis {
  const trimmed = sql.trim();
  if (!trimmed) throw new StateQLError("INVALID_SQL", "SQL is empty.");

  try {
    const database =
      driver === "postgres"
        ? "Postgresql"
        : driver === "mysql"
          ? "MySQL"
          : "Sqlite";
    const parserSql = driver === "postgres"
      ? postgresParserSql(trimmed)
      : trimmed;
    const parsed = parser.astify(parserSql, { database });
    if (Array.isArray(parsed)) {
      if (parsed.length !== 1) {
        throw new StateQLError(
          "INVALID_SQL",
          "Exactly one SQL statement is required.",
        );
      }
    }
    const ast = (Array.isArray(parsed) ? parsed[0] : parsed) as AST | undefined;
    if (!ast) throw new StateQLError("INVALID_SQL", "SQL is empty.");

    const rawType = String(ast.type);
    if (!SUPPORTED_STATEMENTS.has(rawType as StatementType)) {
      throw new StateQLError(
        "INVALID_SQL",
        `Unsupported SQL statement type "${rawType}".`,
      );
    }
    const statementType = rawType as StatementType;
    if (statementType === "select" && selectContainsWrite(ast)) {
      throw new StateQLError(
        "INVALID_SQL",
        "Read statements cannot contain writes or SELECT INTO.",
      );
    }
    const normalized = parserSql === trimmed
      ? parser
        .sqlify(ast, { database })
        .replace(/;\s*$/, "")
        .replace(/\s+/g, " ")
        .trim()
      // Keep the exact ordering modifiers in cache and idempotency fingerprints.
      // The parser copy is analysis-only; adapters execute the original SQL.
      : trimmed.replace(/;\s*$/, "");
    const details = ast as unknown as Record<string, unknown>;
    const read = statementType === "select";
    const mutation =
      statementType === "update" ||
      statementType === "delete" ||
      statementType === "truncate";
    const destructive =
      statementType === "drop" ||
      statementType === "alter" ||
      statementType === "delete" ||
      statementType === "replace" ||
      statementType === "truncate" ||
      (driver === "sqlite" &&
        /^(?:INSERT|UPDATE) OR REPLACE\b/i.test(normalized));

    return {
      ast,
      normalized,
      statementType,
      read,
      unboundedMutation:
        statementType === "truncate" || (mutation && !details.where),
      destructive,
      ordered: read && Boolean(details.orderby),
    };
  } catch (error) {
    if (error instanceof StateQLError) throw error;
    const message = error instanceof Error ? error.message : "Invalid SQL.";
    throw new StateQLError("INVALID_SQL", message);
  }
}

function postgresParserSql(sql: string): string {
  const output = sql.split("");
  const orderDepths = new Set<number>();
  let previousWord: string | undefined;
  let changed = false;
  let depth = 0;
  let index = 0;

  while (index < sql.length) {
    if (sql.startsWith("--", index)) {
      index = lineCommentEnd(sql, index + 2);
      continue;
    }
    if (sql.startsWith("/*", index)) {
      index = blockCommentEnd(sql, index + 2);
      continue;
    }
    const character = sql[index];
    if (character === "'" || character === '"') {
      previousWord = undefined;
      index = quotedEnd(sql, index + 1, character);
      continue;
    }
    if (character === "$") {
      const delimiter = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
      if (delimiter) {
        previousWord = undefined;
        const end = sql.indexOf(delimiter, index + delimiter.length);
        index = end < 0 ? sql.length : end + delimiter.length;
        continue;
      }
    }
    if (character === "(") {
      previousWord = undefined;
      depth += 1;
      index += 1;
      continue;
    }
    if (character === ")") {
      previousWord = undefined;
      orderDepths.delete(depth);
      depth = Math.max(0, depth - 1);
      index += 1;
      continue;
    }
    if (!identifierStart(character)) {
      if (!/\s/u.test(character!)) previousWord = undefined;
      if (character === ";") orderDepths.clear();
      index += 1;
      continue;
    }

    let end = index + 1;
    while (identifierPart(sql[end])) end += 1;
    const word = sql.slice(index, end).toUpperCase();
    if (word === "NULLS" && orderDepths.has(depth)) {
      const ordering = triviaEnd(sql, end);
      const modifier = sql.slice(ordering, ordering + 5).toUpperCase();
      const length = modifier === "FIRST" ? 5 : modifier.startsWith("LAST") ? 4 : 0;
      if (length && !identifierPart(sql[ordering + length])) {
        for (let cursor = index; cursor < end; cursor += 1) output[cursor] = " ";
        for (let cursor = ordering; cursor < ordering + length; cursor += 1) output[cursor] = " ";
        changed = true;
        previousWord = undefined;
        index = ordering + length;
        continue;
      }
    }

    if (word === "BY" && previousWord === "ORDER") orderDepths.add(depth);
    if (["LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT"].includes(word)) {
      orderDepths.delete(depth);
    }
    previousWord = word === "ORDER" ? word : undefined;
    index = end;
  }

  return changed ? output.join("") : sql;
}

function triviaEnd(sql: string, start: number): number {
  let index = start;
  while (index < sql.length) {
    if (/\s/u.test(sql[index]!)) {
      index += 1;
    } else if (sql.startsWith("--", index)) {
      index = lineCommentEnd(sql, index + 2);
    } else if (sql.startsWith("/*", index)) {
      index = blockCommentEnd(sql, index + 2);
    } else {
      break;
    }
  }
  return index;
}

function lineCommentEnd(sql: string, start: number): number {
  const end = sql.indexOf("\n", start);
  return end < 0 ? sql.length : end + 1;
}

function blockCommentEnd(sql: string, start: number): number {
  let depth = 1;
  let index = start;
  while (index < sql.length && depth > 0) {
    if (sql.startsWith("/*", index)) {
      depth += 1;
      index += 2;
    } else if (sql.startsWith("*/", index)) {
      depth -= 1;
      index += 2;
    } else {
      index += 1;
    }
  }
  return index;
}

function quotedEnd(sql: string, start: number, quote: string): number {
  let index = start;
  while (index < sql.length) {
    if (sql[index] === "\\" && quote === "'") {
      index += 2;
    } else if (sql[index] !== quote) {
      index += 1;
    } else if (sql[index + 1] === quote) {
      index += 2;
    } else {
      return index + 1;
    }
  }
  return index;
}

function identifierStart(value: string | undefined): boolean {
  return value !== undefined && /[A-Za-z_\u0080-\uFFFF]/u.test(value);
}

function identifierPart(value: string | undefined): boolean {
  return value !== undefined && /[A-Za-z0-9_$\u0080-\uFFFF]/u.test(value);
}

function selectContainsWrite(ast: AST): boolean {
  const details = ast as unknown as Record<string, unknown>;
  const into = details.into as Record<string, unknown> | undefined;
  if (into?.type === "into" || into?.expr) return true;

  const withStatements = details.with;
  if (!Array.isArray(withStatements)) return false;
  return withStatements.some((entry: unknown) => {
    if (!entry || typeof entry !== "object") return false;
    const statement = (entry as Record<string, unknown>).stmt;
    if (!statement || typeof statement !== "object") return false;
    const wrapper = statement as Record<string, unknown>;
    const child = (wrapper.ast ?? statement) as AST;
    if (child.type !== "select") return true;
    return selectContainsWrite(child);
  });
}
