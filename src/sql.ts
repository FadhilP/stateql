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
    const database = driver === "postgres" ? "Postgresql" : "Sqlite";
    const parsed = parser.astify(trimmed, { database });
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
    const normalized = parser
      .sqlify(ast, { database })
      .replace(/;\s*$/, "")
      .replace(/\s+/g, " ")
      .trim();
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
      statementType === "truncate";

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
