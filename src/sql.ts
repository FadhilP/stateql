import { createRequire } from "node:module";
import type { AST } from "node-sql-parser";
import { StateQLError } from "./errors.js";
import type { Driver, SqlDriver } from "./types.js";

const { Parser } = createRequire(import.meta.url)(
  "node-sql-parser",
) as typeof import("node-sql-parser");
const parser = new Parser();

export type StatementType =
  | "select"
  | "insert"
  | "upsert"
  | "replace"
  | "update"
  | "delete"
  | "create"
  | "alter"
  | "drop"
  | "truncate"
  | "values"
  | "show"
  | "explain"
  | "vacuum"
  | "analyze"
  | "reindex"
  | "optimize"
  | "check"
  | "cluster";

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
  wrapForLimit: boolean;
  cacheable: boolean;
  requiresAutocommit: boolean;
  /** Analysis-only SQL with trailing trivia removed when limit wrapping needs it. */
  limitSql?: string;
}

export function analyzeSql(sql: string, driver: SqlDriver): SqlAnalysis;
/** @internal Compatibility for existing connection records during Mongo rollout. */
export function analyzeSql(sql: string, driver: Driver): SqlAnalysis;
export function analyzeSql(sql: string, driver: Driver): SqlAnalysis {
  if (driver === "mongodb" || driver === "redis") {
    throw new StateQLError("INVALID_SQL", `SQL is not supported for ${driver} connections.`);
  }
  const trimmed = sql.trim();
  if (!trimmed) throw new StateQLError("INVALID_SQL", "SQL is empty.");

  try {
    if (driver === "postgres") {
      const postgresCommand = analyzePostgresCommand(trimmed);
      if (postgresCommand) return postgresCommand;
    } else if (driver === "sqlite") {
      const sqliteCommand = analyzeSqliteCommand(trimmed);
      if (sqliteCommand) return sqliteCommand;
    } else if (driver === "mysql") {
      return analyzeMySqlSql(trimmed);
    }
    return analyzeParsedSql(trimmed, driver);
  } catch (error) {
    if (error instanceof StateQLError) throw error;
    const message = error instanceof Error ? error.message : "Invalid SQL.";
    throw new StateQLError("INVALID_SQL", message);
  }
}

interface ParsedSql {
  ast: AST;
  normalized: string;
}

function parseSqlStatement(
  sql: string,
  driver: Exclude<Driver, "mongodb" | "redis">,
): ParsedSql {
  const database =
    driver === "postgres"
      ? "Postgresql"
      : driver === "mysql"
        ? "MySQL"
        : "Sqlite";
  const parserSql = driver === "postgres" ? postgresParserSql(sql) : sql;
  const parsed = parser.astify(parserSql, { database });
  if (Array.isArray(parsed) && parsed.length !== 1) {
    throw new StateQLError(
      "INVALID_SQL",
      "Exactly one SQL statement is required.",
    );
  }
  const ast = (Array.isArray(parsed) ? parsed[0] : parsed) as AST | undefined;
  if (!ast) throw new StateQLError("INVALID_SQL", "SQL is empty.");
  const normalized = parserSql === sql
    ? parser
      .sqlify(ast, { database })
      .replace(/;\s*$/, "")
      .replace(/\s+/g, " ")
      .trim()
    // Keep the exact ordering modifiers in cache and idempotency fingerprints.
    // The parser copy is analysis-only; adapters execute the original SQL.
    : sql.replace(/;\s*$/, "");
  return { ast, normalized };
}

function analyzeParsedSql(sql: string, driver: Exclude<Driver, "mongodb" | "redis">): SqlAnalysis {
  return analyzeParsedStatement(parseSqlStatement(sql, driver), driver);
}

function analyzeParsedStatement(
  { ast, normalized }: ParsedSql,
  driver: Exclude<Driver, "mongodb" | "redis">,
): SqlAnalysis {
  const rawType = String(ast.type);
  if (!SUPPORTED_STATEMENTS.has(rawType as StatementType)) {
    throw new StateQLError(
      "INVALID_SQL",
      `Unsupported SQL statement type "${rawType}".`,
    );
  }
  const rawStatementType = rawType as StatementType;
  const insert = rawStatementType === "insert"
    ? analyzeInsert(ast, driver)
    : undefined;
  const statementType = insert?.statementType ?? rawStatementType;
  if (statementType === "select" && containsUnexpectedWrite(ast)) {
    throw new StateQLError(
      "INVALID_SQL",
      "Read statements cannot contain writes or SELECT INTO.",
    );
  }

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
      insert?.unboundedMutation ??
      (statementType === "truncate" || (mutation && !details.where)),
    destructive,
    ordered: read && Boolean(details.orderby),
    wrapForLimit: read,
    cacheable: true,
    requiresAutocommit: false,
  };
}

interface InsertAnalysis {
  statementType: "insert" | "upsert";
  unboundedMutation: boolean;
}

function analyzeInsert(
  ast: AST,
  driver: Exclude<Driver, "mongodb" | "redis">,
): InsertAnalysis {
  const details = ast as unknown as Record<string, unknown>;
  const conflict = record(details.conflict);
  const duplicate = record(details.on_duplicate_update);
  const allowedWrites = new Set<object>([ast as object]);
  let upsert = false;
  let updatesOnConflict = false;

  if (conflict) {
    if (driver !== "postgres") invalidUpsertSyntax(driver);
    const action = record(conflict.action);
    const expression = record(action?.expr);
    if (
      conflict.type !== "conflict" ||
      conflict.keyword !== "on" ||
      action?.keyword !== "do" ||
      !expression
    ) {
      invalidUpsertSyntax(driver);
    }
    upsert = true;
    if (expression.type === "update") {
      if (!Array.isArray(expression.set) || expression.set.length === 0) {
        invalidUpsertSyntax(driver);
      }
      allowedWrites.add(expression);
      updatesOnConflict = true;
    } else if (
      expression.type !== "origin" ||
      String(expression.value).toLowerCase() !== "nothing"
    ) {
      invalidUpsertSyntax(driver);
    }
  }

  if (duplicate) {
    if (
      driver !== "mysql" ||
      duplicate.keyword !== "on duplicate key update" ||
      !Array.isArray(duplicate.set) ||
      duplicate.set.length === 0
    ) {
      invalidUpsertSyntax(driver);
    }
    upsert = true;
    updatesOnConflict = true;
  }

  if (containsUnexpectedWrite(ast, allowedWrites)) {
    throw new StateQLError(
      "INVALID_SQL",
      "INSERT statements cannot contain additional write statements or SELECT INTO.",
    );
  }

  const source = record(details.values);
  const assignmentSource = driver === "mysql" && !source &&
    Array.isArray(details.set) && details.set.length > 0;
  if (
    upsert && !assignmentSource &&
    (!source || !["select", "values"].includes(String(source.type)))
  ) {
    invalidUpsertSyntax(driver);
  }
  return {
    statementType: upsert ? "upsert" : "insert",
    unboundedMutation: updatesOnConflict && source?.type === "select",
  };
}

function invalidUpsertSyntax(driver: string): never {
  throw new StateQLError(
    "INVALID_SQL",
    `Unsupported or invalid ${driver} upsert syntax.`,
  );
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}


const EXPLAIN_INNER_STATEMENTS = new Set<StatementType>([
  "select",
  "insert",
  "update",
  "delete",
  "upsert",
]);
const EXPLAIN_BOOLEAN_OPTIONS = new Set([
  "ANALYZE",
  "VERBOSE",
  "COSTS",
  "SETTINGS",
  "GENERIC_PLAN",
  "BUFFERS",
  "WAL",
  "TIMING",
  "SUMMARY",
  "MEMORY",
]);
const EXPLAIN_FORMATS = new Set(["TEXT", "XML", "JSON", "YAML"]);
const EXPLAIN_SERIALIZE = new Set(["NONE", "TEXT", "BINARY"]);
const BOOLEAN_VALUES = new Set(["TRUE", "FALSE", "ON", "OFF", "1", "0"]);

const POSTGRES_SHOW_SETTINGS = new Set([
  "default_transaction_isolation",
  "server_version",
  "server_version_num",
  "transaction_isolation",
  "transaction_read_only",
]);

function analyzePostgresCommand(sql: string): SqlAnalysis | undefined {
  const scanner = new SqlPrefixScanner(sql, "postgres");
  const command = scanner.readWord();
  if (!command) return undefined;
  switch (command.value) {
    case "EXPLAIN":
      return analyzePostgresExplain(sql, scanner);
    case "SHOW":
      return analyzePostgresShow(sql);
    case "VALUES":
      return analyzePostgresValues(sql);
    case "VACUUM":
    case "ANALYZE":
    case "REINDEX":
    case "CLUSTER":
      return analyzePostgresMaintenance(sql, command.value);
    default:
      return undefined;
  }
}

function analyzePostgresShow(sql: string): SqlAnalysis {
  const parsed = parseSqlStatement(sql, "postgres");
  const details = parsed.ast as unknown as Record<string, unknown>;
  const variable = details.var as Record<string, unknown> | undefined;
  if (
    details.type !== "show" ||
    details.keyword !== "var" ||
    variable?.type !== "var" ||
    typeof variable.name !== "string" ||
    (Array.isArray(variable.members) && variable.members.length > 0) ||
    !POSTGRES_SHOW_SETTINGS.has(variable.name.toLowerCase())
  ) {
    invalidPostgresSyntax("SHOW");
  }
  return readDiagnostic(parsed, "show");
}

function analyzePostgresValues(sql: string): SqlAnalysis {
  const statement = postgresStatementBody(sql);
  const wrapped = analyzeParsedSql(
    `SELECT * FROM (${statement}) AS _stateql_values`,
    "postgres",
  );
  return {
    ...wrapped,
    normalized: statement,
    statementType: "values",
    ordered: false,
    cacheable: false,
    limitSql: statement,
  };
}

const MYSQL_SHOW_KEYWORDS = new Set(["columns", "index", "indexes", "tables"]);

function analyzeMySqlSql(sql: string): SqlAnalysis {
  const scanner = new MySqlPrefixScanner(sql);
  scanner.validateComments();
  const command = scanner.readWord();
  if (
    command?.value === "ANALYZE" ||
    command?.value === "OPTIMIZE" ||
    command?.value === "CHECK"
  ) {
    return analyzeMySqlMaintenance(sql, scanner, command.value);
  }
  const parsed = parseSqlStatement(sql, "mysql");
  const details = parsed.ast as unknown as Record<string, unknown>;
  if (details.type === "explain") {
    const inner = details.expr as AST | undefined;
    if (!inner || String(inner.type) !== "select" || containsUnexpectedWrite(inner)) {
      throw new StateQLError(
        "INVALID_SQL",
        "MySQL EXPLAIN accepts read-only SELECT statements only.",
      );
    }
    return readDiagnostic(parsed, "explain");
  }
  if (details.type === "show") {
    const keyword = typeof details.keyword === "string"
      ? details.keyword.toLowerCase()
      : "";
    const allowedKeys = keyword === "tables"
      ? new Set(["type", "keyword"])
      : new Set(["type", "keyword", "from"]);
    const hasUnexpectedShape = Object.entries(details).some(
      ([key, value]) => value !== undefined && value !== null && !allowedKeys.has(key),
    );
    const from = details.from;
    if (
      !MYSQL_SHOW_KEYWORDS.has(keyword) ||
      hasUnexpectedShape ||
      (keyword !== "tables" && (!Array.isArray(from) || from.length !== 1))
    ) {
      throw new StateQLError(
        "INVALID_SQL",
        `Unsupported MySQL SHOW form "${keyword || "unknown"}".`,
      );
    }
    return readDiagnostic(parsed, "show");
  }
  return analyzeParsedStatement(parsed, "mysql");
}

function analyzeMySqlMaintenance(
  sql: string,
  scanner: MySqlPrefixScanner,
  command: "ANALYZE" | "OPTIMIZE" | "CHECK",
): SqlAnalysis {
  if (scanner.readWord()?.value !== "TABLE") invalidMySqlSyntax(command);
  if (!scanner.readQualifiedIdentifier()) invalidMySqlSyntax(command);
  if (scanner.consume(";")) {
    if (scanner.triviaEnd() !== sql.length) invalidMySqlSyntax(command);
  } else if (scanner.triviaEnd() !== sql.length) {
    invalidMySqlSyntax(command);
  }
  const read = command === "CHECK";
  return {
    ast: { type: command.toLowerCase() } as unknown as AST,
    normalized: sql.replace(/;\s*$/, ""),
    statementType: command.toLowerCase() as StatementType,
    read,
    unboundedMutation: false,
    destructive: !read,
    ordered: false,
    wrapForLimit: false,
    cacheable: false,
    requiresAutocommit: true,
  };
}

function invalidMySqlSyntax(command: string): never {
  throw new StateQLError(
    "INVALID_SQL",
    `Unsupported or invalid MySQL ${command} TABLE syntax.`,
  );
}

function analyzeSqliteCommand(sql: string): SqlAnalysis | undefined {
  const scanner = new SqlPrefixScanner(sql, "sqlite");
  const command = scanner.readWord();
  switch (command?.value) {
    case "EXPLAIN":
      return analyzeSqliteExplain(sql, scanner);
    case "VACUUM":
    case "ANALYZE":
    case "REINDEX":
      return analyzeSqliteMaintenance(sql, scanner, command.value);
    default:
      return undefined;
  }
}

function analyzeSqliteExplain(
  sql: string,
  scanner: SqlPrefixScanner,
): SqlAnalysis {
  if (scanner.readWord()?.value !== "QUERY" || scanner.readWord()?.value !== "PLAN") {
    throw new StateQLError(
      "INVALID_SQL",
      "Only SQLite EXPLAIN QUERY PLAN is supported.",
    );
  }
  const innerSql = sql.slice(scanner.triviaEnd());
  if (!innerSql) {
    throw new StateQLError("INVALID_SQL", "Invalid SQLite EXPLAIN QUERY PLAN syntax.");
  }
  const inner = analyzeParsedSql(innerSql, "sqlite");
  if (inner.statementType !== "select") {
    throw new StateQLError(
      "INVALID_SQL",
      "SQLite EXPLAIN QUERY PLAN accepts read-only SELECT statements only.",
    );
  }
  return readDiagnostic(
    { ast: inner.ast, normalized: sql.replace(/;\s*$/, "") },
    "explain",
  );
}

function analyzeSqliteMaintenance(
  sql: string,
  scanner: SqlPrefixScanner,
  command: "VACUUM" | "ANALYZE" | "REINDEX",
): SqlAnalysis {
  if (command !== "VACUUM" && scanner.peek() !== ";" && scanner.peek() !== undefined) {
    if (!readSqliteIdentifier(sql, scanner)) invalidSqliteSyntax(command);
  }
  if (scanner.consume(";")) {
    if (scanner.triviaEnd() !== sql.length) invalidSqliteSyntax(command);
  } else if (scanner.triviaEnd() !== sql.length) {
    invalidSqliteSyntax(command);
  }
  return {
    ast: { type: command.toLowerCase() } as unknown as AST,
    normalized: sql.replace(/;\s*$/, ""),
    statementType: command.toLowerCase() as StatementType,
    read: false,
    unboundedMutation: false,
    destructive: true,
    ordered: false,
    wrapForLimit: false,
    cacheable: false,
    requiresAutocommit: true,
  };
}

function readSqliteIdentifier(
  sql: string,
  scanner: SqlPrefixScanner,
): boolean {
  scanner.triviaEnd();
  if (sql[scanner.position] === '"') {
    const end = postgresQuotedIdentifierEnd(sql, scanner.position);
    if (end === undefined) return false;
    scanner.position = end;
    return true;
  }
  return Boolean(scanner.readWord(false));
}

function invalidSqliteSyntax(command: string): never {
  throw new StateQLError(
    "INVALID_SQL",
    `Unsupported or invalid SQLite ${command} syntax.`,
  );
}

function readDiagnostic(
  { ast, normalized }: ParsedSql,
  statementType: "explain" | "show",
): SqlAnalysis {
  return {
    ast,
    normalized,
    statementType,
    read: true,
    unboundedMutation: false,
    destructive: false,
    ordered: false,
    wrapForLimit: false,
    cacheable: false,
    requiresAutocommit: false,
  };
}

function analyzePostgresExplain(
  sql: string,
  scanner: SqlPrefixScanner,
): SqlAnalysis {
  let analyze = false;
  const seen = new Set<string>();
  if (scanner.consume("(")) {
    while (true) {
      const option = scanner.readWord();
      if (!option || seen.has(option.value)) invalidPostgresSyntax("EXPLAIN");
      seen.add(option.value);
      const next = scanner.peek();
      let value: string | undefined;
      if (next !== "," && next !== ")") {
        value = scanner.readWord()?.value;
        if (!value) invalidPostgresSyntax("EXPLAIN");
      }
      validateExplainOption(option.value, value);
      if (option.value === "ANALYZE") {
        analyze = value === undefined || value === "TRUE" || value === "ON";
      }
      if (scanner.consume(")")) break;
      if (!scanner.consume(",")) invalidPostgresSyntax("EXPLAIN");
    }
  } else {
    while (true) {
      const option = scanner.peekWord();
      if (option !== "ANALYZE" && option !== "VERBOSE") break;
      scanner.readWord();
      if (seen.has(option)) invalidPostgresSyntax("EXPLAIN");
      seen.add(option);
      if (option === "ANALYZE") analyze = true;
    }
  }

  const innerSql = sql.slice(scanner.triviaEnd());
  if (!innerSql) invalidPostgresSyntax("EXPLAIN");
  const inner = analyzeParsedSql(innerSql, "postgres");
  if (!EXPLAIN_INNER_STATEMENTS.has(inner.statementType)) {
    throw new StateQLError(
      "INVALID_SQL",
      `EXPLAIN does not support ${inner.statementType.toUpperCase()} statements.`,
    );
  }
  if (analyze && inner.statementType !== "select") {
    throw new StateQLError(
      "INVALID_SQL",
      "EXPLAIN ANALYZE accepts read-only SELECT statements only.",
    );
  }
  return {
    ast: inner.ast,
    normalized: sql.replace(/;\s*$/, ""),
    statementType: "explain",
    read: true,
    unboundedMutation: false,
    destructive: false,
    ordered: false,
    wrapForLimit: false,
    cacheable: false,
    requiresAutocommit: false,
  };
}

function validateExplainOption(option: string, value?: string): void {
  if (EXPLAIN_BOOLEAN_OPTIONS.has(option)) {
    if (value !== undefined && !BOOLEAN_VALUES.has(value)) invalidPostgresSyntax("EXPLAIN");
    return;
  }
  if (option === "FORMAT" && value && EXPLAIN_FORMATS.has(value)) return;
  if (option === "SERIALIZE" && value && EXPLAIN_SERIALIZE.has(value)) return;
  invalidPostgresSyntax("EXPLAIN");
}

type UtilityToken = {
  kind: "word" | "identifier" | "number" | "string" | "punctuation";
  value: string;
};

type UtilityOptionKind =
  | "boolean"
  | "number"
  | "identifier"
  | "size"
  | ReadonlySet<string>;
type UtilityOptions = ReadonlyMap<string, UtilityOptionKind>;

const VACUUM_OPTIONS: UtilityOptions = new Map<string, UtilityOptionKind>([
  ["FULL", "boolean"],
  ["FREEZE", "boolean"],
  ["VERBOSE", "boolean"],
  ["ANALYZE", "boolean"],
  ["DISABLE_PAGE_SKIPPING", "boolean"],
  ["SKIP_LOCKED", "boolean"],
  ["INDEX_CLEANUP", new Set(["AUTO", "ON", "OFF"])],
  ["PROCESS_MAIN", "boolean"],
  ["PROCESS_TOAST", "boolean"],
  ["TRUNCATE", "boolean"],
  ["PARALLEL", "number"],
  ["SKIP_DATABASE_STATS", "boolean"],
  ["ONLY_DATABASE_STATS", "boolean"],
  ["BUFFER_USAGE_LIMIT", "size"],
]);
const ANALYZE_OPTIONS: UtilityOptions = new Map<string, UtilityOptionKind>([
  ["VERBOSE", "boolean"],
  ["SKIP_LOCKED", "boolean"],
  ["BUFFER_USAGE_LIMIT", "size"],
]);
const REINDEX_OPTIONS: UtilityOptions = new Map<string, UtilityOptionKind>([
  ["CONCURRENTLY", "boolean"],
  ["VERBOSE", "boolean"],
  ["TABLESPACE", "identifier"],
]);
const CLUSTER_OPTIONS: UtilityOptions = new Map<string, UtilityOptionKind>([
  ["VERBOSE", "boolean"],
]);

function analyzePostgresMaintenance(
  sql: string,
  command: "VACUUM" | "ANALYZE" | "REINDEX" | "CLUSTER",
): SqlAnalysis {
  const parser = new UtilityParser(tokenizePostgresMaintenance(sql), command);
  parser.expectWord(command);
  switch (command) {
    case "VACUUM":
      parser.options(VACUUM_OPTIONS, ["FULL", "FREEZE", "VERBOSE", "ANALYZE"]);
      parser.optionalTargets(true);
      break;
    case "ANALYZE":
      parser.options(ANALYZE_OPTIONS, ["VERBOSE"]);
      parser.optionalTargets(true);
      break;
    case "REINDEX": {
      const options = parser.options(REINDEX_OPTIONS);
      const target = parser.expectOneOf([
        "INDEX",
        "TABLE",
        "SCHEMA",
        "DATABASE",
        "SYSTEM",
      ]);
      const postTargetConcurrent = parser.consumeWord("CONCURRENTLY");
      if (postTargetConcurrent && options.has("CONCURRENTLY")) {
        invalidPostgresSyntax(command);
      }
      if (target === "INDEX" || target === "TABLE") {
        parser.qualifiedIdentifier();
      } else if (target === "SCHEMA") {
        parser.identifier();
      } else if (!parser.done()) {
        parser.identifier();
      }
      break;
    }
    case "CLUSTER":
      parser.options(CLUSTER_OPTIONS, ["VERBOSE"]);
      if (!parser.done()) {
        parser.qualifiedIdentifier();
        if (parser.consumeWord("USING")) parser.identifier();
      }
      break;
  }
  parser.expectDone();
  return {
    ast: { type: command.toLowerCase() } as unknown as AST,
    normalized: sql.replace(/;\s*$/, ""),
    statementType: command.toLowerCase() as StatementType,
    read: false,
    unboundedMutation: false,
    destructive: true,
    ordered: false,
    wrapForLimit: false,
    cacheable: false,
    requiresAutocommit: true,
  };
}

function tokenizePostgresMaintenance(sql: string): UtilityToken[] {
  const scanner = new SqlPrefixScanner(sql, "postgres");
  const tokens: UtilityToken[] = [];
  while (scanner.triviaEnd() < sql.length) {
    const character = sql[scanner.position]!;
    if (character === ";") {
      scanner.position += 1;
      if (scanner.triviaEnd() !== sql.length) {
        throw new StateQLError("INVALID_SQL", "Exactly one SQL statement is required.");
      }
      break;
    }
    const word = scanner.readWord(false);
    if (word) {
      tokens.push({ kind: "word", value: word.value });
      continue;
    }
    if (character === '"') {
      const end = postgresQuotedIdentifierEnd(sql, scanner.position);
      if (end === undefined) invalidPostgresSyntax("maintenance");
      tokens.push({ kind: "identifier", value: sql.slice(scanner.position, end) });
      scanner.position = end;
      continue;
    }
    if (character === "'") {
      const end = postgresQuotedStringScanEnd(sql, scanner.position);
      if (end <= scanner.position + 1 || sql[end - 1] !== "'") {
        invalidPostgresSyntax("maintenance");
      }
      tokens.push({ kind: "string", value: sql.slice(scanner.position, end) });
      scanner.position = end;
      continue;
    }
    if (/[0-9]/u.test(character)) {
      const start = scanner.position;
      scanner.position += 1;
      while (/[0-9]/u.test(sql[scanner.position] ?? "")) scanner.position += 1;
      tokens.push({ kind: "number", value: sql.slice(start, scanner.position) });
      continue;
    }
    if (["(", ")", ",", ".", "*"].includes(character)) {
      tokens.push({ kind: "punctuation", value: character });
      scanner.position += 1;
      continue;
    }
    invalidPostgresSyntax("maintenance");
  }
  return tokens;
}

class UtilityParser {
  private index = 0;

  constructor(
    private readonly tokens: UtilityToken[],
    private readonly command: string,
  ) {}

  done(): boolean {
    return this.index >= this.tokens.length;
  }

  expectDone(): void {
    if (!this.done()) invalidPostgresSyntax(this.command);
  }

  expectWord(word: string): void {
    if (!this.consumeWord(word)) invalidPostgresSyntax(this.command);
  }

  consumeWord(word: string): boolean {
    const token = this.tokens[this.index];
    if (token?.kind !== "word" || token.value !== word) return false;
    this.index += 1;
    return true;
  }

  expectOneOf(words: string[]): string {
    const token = this.tokens[this.index];
    if (token?.kind !== "word" || !words.includes(token.value)) {
      invalidPostgresSyntax(this.command);
    }
    this.index += 1;
    return token.value;
  }

  options(options: UtilityOptions, legacy: string[] = []): Set<string> {
    if (this.consumePunctuation("(")) {
      const seen = new Set<string>();
      while (true) {
        const option = this.tokens[this.index];
        if (option?.kind !== "word" || seen.has(option.value)) {
          invalidPostgresSyntax(this.command);
        }
        const kind = options.get(option.value);
        if (!kind) invalidPostgresSyntax(this.command);
        seen.add(option.value);
        this.index += 1;
        const next = this.tokens[this.index];
        if (next?.value !== "," && next?.value !== ")") {
          this.optionValue(kind);
        } else if (kind !== "boolean") {
          invalidPostgresSyntax(this.command);
        }
        if (this.consumePunctuation(")")) return seen;
        if (!this.consumePunctuation(",")) invalidPostgresSyntax(this.command);
      }
    }
    const seen = new Set<string>();
    while (true) {
      const option = this.tokens[this.index];
      if (option?.kind !== "word" || !legacy.includes(option.value)) return seen;
      if (seen.has(option.value)) invalidPostgresSyntax(this.command);
      seen.add(option.value);
      this.index += 1;
    }
  }

  optionalTargets(allowOnlyAndStar = false): void {
    if (this.done()) return;
    while (true) {
      if (allowOnlyAndStar) this.consumeWord("ONLY");
      this.qualifiedIdentifier();
      if (allowOnlyAndStar) this.consumePunctuation("*");
      if (this.consumePunctuation("(")) {
        this.identifier();
        while (this.consumePunctuation(",")) this.identifier();
        if (!this.consumePunctuation(")")) invalidPostgresSyntax(this.command);
      }
      if (!this.consumePunctuation(",")) return;
    }
  }

  qualifiedIdentifier(): void {
    this.identifier();
    while (this.consumePunctuation(".")) this.identifier();
  }

  identifier(): void {
    const token = this.tokens[this.index];
    if (token?.kind !== "word" && token?.kind !== "identifier") {
      invalidPostgresSyntax(this.command);
    }
    this.index += 1;
  }

  private optionValue(kind: UtilityOptionKind): void {
    const token = this.tokens[this.index];
    if (!token) invalidPostgresSyntax(this.command);
    if (kind === "boolean") {
      if (
        (token.kind !== "word" && token.kind !== "number") ||
        !BOOLEAN_VALUES.has(token.value)
      ) {
        invalidPostgresSyntax(this.command);
      }
    } else if (kind === "number") {
      if (token.kind !== "number") invalidPostgresSyntax(this.command);
    } else if (kind === "identifier") {
      if (token.kind !== "word" && token.kind !== "identifier") {
        invalidPostgresSyntax(this.command);
      }
    } else if (kind === "size") {
      if (!validMaintenanceSize(token)) invalidPostgresSyntax(this.command);
    } else if (token.kind !== "word" || !kind.has(token.value)) {
      invalidPostgresSyntax(this.command);
    }
    this.index += 1;
  }

  private consumePunctuation(value: string): boolean {
    const token = this.tokens[this.index];
    if (token?.kind !== "punctuation" || token.value !== value) return false;
    this.index += 1;
    return true;
  }
}

function validMaintenanceSize(token: UtilityToken): boolean {
  let amountText: string;
  let unit: "B" | "KB" | "MB" | "GB" | "TB";
  if (token.kind === "number") {
    amountText = token.value;
    unit = "KB";
  } else if (token.kind === "string") {
    const match = token.value.match(
      /^'([0-9]+)(?:\s*(B|KB|MB|GB|TB))?'$/iu,
    );
    if (!match) return false;
    amountText = match[1]!;
    unit = (match[2]?.toUpperCase() ?? "KB") as typeof unit;
  } else {
    return false;
  }
  const factors = {
    B: 1n,
    KB: 1_024n,
    MB: 1_024n ** 2n,
    GB: 1_024n ** 3n,
    TB: 1_024n ** 4n,
  } as const;
  const bytes = BigInt(amountText) * factors[unit];
  return bytes === 0n ||
    (bytes >= 128n * factors.KB && bytes <= 16n * factors.GB);
}

class MySqlPrefixScanner {
  position = 0;

  constructor(private readonly sql: string) {}

  validateComments(): void {
    if (!this.sql.includes("/*!")) return;
    // sql_mode is unknown: cover string escapes, ANSI_QUOTES, and
    // NO_BACKSLASH_ESCAPES. Reject mode-dependent executable comments.
    for (const [singleEscapes, doubleEscapes] of [
      [true, true], [true, false], [false, false],
    ] as const) {
      this.position = 0;
      while (this.triviaEnd() < this.sql.length) {
        const quote = this.sql[this.position]!;
        if (quote === "'" || quote === '"' || quote === "`") {
          const escaped = quote === "'" ? singleEscapes : quote === '"' && doubleEscapes;
          if (!this.readQuoted(quote, escaped)) {
            throw new StateQLError("INVALID_SQL", "Unterminated quoted SQL value.");
          }
        } else {
          this.position += 1;
        }
      }
    }
    this.position = 0;
  }

  triviaEnd(): number {
    while (this.position < this.sql.length) {
      if (/\s/u.test(this.sql[this.position]!)) {
        this.position += 1;
      } else if (this.sql[this.position] === "#") {
        this.position = lineCommentEnd(this.sql, this.position + 1);
      } else if (
        this.sql.startsWith("--", this.position) &&
        (this.sql[this.position + 2] === undefined || /[\x00-\x20]/u.test(this.sql[this.position + 2]!))
      ) {
        this.position = lineCommentEnd(this.sql, this.position + 2);
      } else if (this.sql.startsWith("/*", this.position)) {
        if (this.sql[this.position + 2] === "!") {
          throw new StateQLError(
            "INVALID_SQL",
            "MySQL executable comments are not supported.",
          );
        }
        const end = nonNestedBlockCommentEnd(this.sql, this.position + 2);
        if (end === undefined) {
          throw new StateQLError("INVALID_SQL", "Unterminated SQL comment.");
        }
        this.position = end;
      } else {
        break;
      }
    }
    return this.position;
  }

  readWord(skipTrivia = true): { value: string } | undefined {
    if (skipTrivia) this.triviaEnd();
    const start = this.position;
    if (!identifierStart(this.sql[start])) return undefined;
    this.position += 1;
    while (identifierPart(this.sql[this.position])) this.position += 1;
    return { value: this.sql.slice(start, this.position).toUpperCase() };
  }

  readQualifiedIdentifier(): boolean {
    if (!this.readIdentifier()) return false;
    if (!this.consume(".")) return true;
    return this.readIdentifier();
  }

  consume(value: string): boolean {
    this.triviaEnd();
    if (!this.sql.startsWith(value, this.position)) return false;
    this.position += value.length;
    return true;
  }

  private readIdentifier(): boolean {
    this.triviaEnd();
    if (this.sql[this.position] !== "`") return Boolean(this.readWord(false));
    return this.readQuoted("`", false);
  }

  private readQuoted(quote: string, backslashEscapes: boolean): boolean {
    let index = this.position + 1;
    while (index < this.sql.length) {
      if (backslashEscapes && this.sql[index] === "\\") {
        index += 2;
      } else if (this.sql[index] !== quote) {
        index += 1;
      } else if (this.sql[index + 1] === quote) {
        index += 2;
      } else {
        this.position = index + 1;
        return true;
      }
    }
    return false;
  }
}


class SqlPrefixScanner {
  position = 0;

  constructor(
    private readonly sql: string,
    private readonly dialect: "postgres" | "sqlite",
  ) {}

  triviaEnd(): number {
    while (this.position < this.sql.length) {
      if (/\s/u.test(this.sql[this.position]!)) {
        this.position += 1;
      } else if (this.sql.startsWith("--", this.position)) {
        this.position = lineCommentEnd(this.sql, this.position + 2);
      } else if (this.sql.startsWith("/*", this.position)) {
        const end = this.dialect === "postgres"
          ? postgresBlockCommentEnd(this.sql, this.position + 2)
          : nonNestedBlockCommentEnd(this.sql, this.position + 2);
        if (end === undefined) {
          throw new StateQLError("INVALID_SQL", "Unterminated SQL comment.");
        }
        this.position = end;
      } else {
        break;
      }
    }
    return this.position;
  }

  readWord(skipTrivia = true): { value: string; start: number; end: number } | undefined {
    if (skipTrivia) this.triviaEnd();
    const start = this.position;
    if (!identifierStart(this.sql[start])) return undefined;
    this.position += 1;
    while (identifierPart(this.sql[this.position])) this.position += 1;
    return {
      value: this.sql.slice(start, this.position).toUpperCase(),
      start,
      end: this.position,
    };
  }

  peekWord(): string | undefined {
    const position = this.position;
    const value = this.readWord()?.value;
    this.position = position;
    return value;
  }

  peek(): string | undefined {
    this.triviaEnd();
    return this.sql[this.position];
  }

  consume(value: string): boolean {
    this.triviaEnd();
    if (!this.sql.startsWith(value, this.position)) return false;
    this.position += value.length;
    return true;
  }
}

function postgresStatementBody(sql: string): string {
  let index = 0;
  let lastTokenStart = 0;
  let lastTokenEnd = 0;
  while (index < sql.length) {
    if (/\s/u.test(sql[index]!)) {
      index += 1;
      continue;
    }
    if (sql.startsWith("--", index)) {
      index = lineCommentEnd(sql, index + 2);
      continue;
    }
    if (sql.startsWith("/*", index)) {
      const end = postgresBlockCommentEnd(sql, index + 2);
      if (end === undefined) {
        throw new StateQLError("INVALID_SQL", "Unterminated SQL comment.");
      }
      index = end;
      continue;
    }
    const start = index;
    const character = sql[index];
    if (character === "'") {
      index = postgresQuotedStringScanEnd(sql, index);
    } else if (character === '"') {
      const end = postgresQuotedIdentifierEnd(sql, index);
      if (end === undefined) {
        throw new StateQLError("INVALID_SQL", "Unterminated quoted SQL identifier.");
      }
      index = end;
    } else if (character === "$") {
      const delimiter = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
      if (delimiter) {
        const end = sql.indexOf(delimiter, index + delimiter.length);
        if (end < 0) {
          throw new StateQLError("INVALID_SQL", "Unterminated dollar-quoted SQL value.");
        }
        index = end + delimiter.length;
      } else {
        index += 1;
      }
    } else {
      index += 1;
    }
    lastTokenStart = start;
    lastTokenEnd = index;
  }
  const end = sql[lastTokenStart] === ";" ? lastTokenStart : lastTokenEnd;
  return sql.slice(0, end).trim();
}

function invalidPostgresSyntax(command: string): never {
  throw new StateQLError("INVALID_SQL", `Unsupported or invalid PostgreSQL ${command} syntax.`);
}

function postgresQuotedIdentifierEnd(
  sql: string,
  start: number,
): number | undefined {
  let index = start + 1;
  while (index < sql.length) {
    if (sql[index] !== '"') {
      index += 1;
    } else if (sql[index + 1] === '"') {
      index += 2;
    } else {
      return index + 1;
    }
  }
  return undefined;
}

function postgresQuotedStringScanEnd(sql: string, start: number): number {
  let index = start + 1;
  while (index < sql.length) {
    if (sql[index] === "\\") {
      index += 2;
    } else if (sql[index] !== "'") {
      index += 1;
    } else if (sql[index + 1] === "'") {
      index += 2;
    } else {
      return index + 1;
    }
  }
  return index;
}

function nonNestedBlockCommentEnd(sql: string, start: number): number | undefined {
  const end = sql.indexOf("*/", start);
  return end < 0 ? undefined : end + 2;
}

function postgresBlockCommentEnd(
  sql: string,
  start: number,
): number | undefined {
  let depth = 1;
  let index = start;
  while (index < sql.length) {
    if (sql.startsWith("/*", index)) {
      depth += 1;
      index += 2;
    } else if (sql.startsWith("*/", index)) {
      depth -= 1;
      index += 2;
      if (depth === 0) return index;
    } else {
      index += 1;
    }
  }
  return undefined;
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
      const end = postgresBlockCommentEnd(sql, index + 2);
      if (end === undefined) {
        throw new StateQLError("INVALID_SQL", "Unterminated SQL comment.");
      }
      index = end;
      continue;
    }
    const character = sql[index];
    if (character === "'") {
      previousWord = undefined;
      index = postgresQuotedStringScanEnd(sql, index);
      continue;
    }
    if (character === '"') {
      previousWord = undefined;
      const end = postgresQuotedIdentifierEnd(sql, index);
      if (end === undefined) {
        throw new StateQLError("INVALID_SQL", "Unterminated quoted SQL identifier.");
      }
      index = end;
      continue;
    }
    if (character === "$") {
      const delimiter = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
      if (delimiter) {
        previousWord = undefined;
        const end = sql.indexOf(delimiter, index + delimiter.length);
        if (end < 0) {
          throw new StateQLError("INVALID_SQL", "Unterminated dollar-quoted SQL value.");
        }
        index = end + delimiter.length;
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


function identifierStart(value: string | undefined): boolean {
  return value !== undefined && /[A-Za-z_\u0080-\uFFFF]/u.test(value);
}

function identifierPart(value: string | undefined): boolean {
  return value !== undefined && /[A-Za-z0-9_$\u0080-\uFFFF]/u.test(value);
}

function containsUnexpectedWrite(
  ast: AST,
  allowedWrites: ReadonlySet<object> = new Set(),
): boolean {
  const visited = new Set<object>();
  const writeTypes = new Set([
    "insert",
    "replace",
    "update",
    "delete",
    "create",
    "alter",
    "drop",
    "truncate",
  ]);
  const visit = (value: unknown): boolean => {
    if (!value || typeof value !== "object") return false;
    if (visited.has(value)) return false;
    visited.add(value);
    if (Array.isArray(value)) return value.some(visit);
    const details = value as Record<string, unknown>;
    const type = typeof details.type === "string" ? details.type : undefined;
    if (type && writeTypes.has(type) && !allowedWrites.has(value)) return true;
    if (type === "select") {
      const into = details.into as Record<string, unknown> | undefined;
      if (into?.type === "into" || into?.expr) return true;
    }
    return Object.values(details).some(visit);
  };
  return visit(ast);
}
