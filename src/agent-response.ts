import type { Response } from "./types.js";
import { isColumns, isRecord } from "./util.js";

const RESULT_COMMANDS = new Set(["query", "mongo.query", "redis.query", "filter", "show"]);
const FALSE_DEFAULT_KEYS = ["cached", "duplicate", "duplicate_override"];
const SHARED_HISTORY_KEYS = ["session_id", "actor_id", "actor_name", "origin"];
// The caller already holds the handle it passed to these commands.
const HANDLE_ECHO_COMMANDS = new Set(["rows", "count", "columns", "summarize"]);
const WRITE_COMMANDS = new Set(["exec", "mongo.exec", "redis.exec", "apply"]);
const COMMAND_ALIASES: Record<string, string> = {
  "objects.list": "objects",
  "object.describe": "object",
};

/** Compacts a response for model context, as the CLI `agent` output mode does. */
export function agentResponse(
  result: Response<unknown>,
  commandName: string,
): Record<string, unknown> {
  const currentCommand = COMMAND_ALIASES[commandName] ?? commandName;
  if (!result.ok) return { ok: false, error: result.error };
  if (!isRecord(result.data)) return { ok: true, data: result.data };

  const data = { ...result.data };
  const handleKey = primaryHandleKey(currentCommand);
  let handle =
    handleKey && typeof data[handleKey] === "string" && data[handleKey]
      ? data[handleKey]
      : undefined;
  if (handle && handleKey) delete data[handleKey];
  const generatedAlias = data.display_alias;
  delete data.display_alias;
  // Generated aliases cannot be reassigned, so they are stable short result handles.
  if (RESULT_COMMANDS.has(currentCommand) && typeof generatedAlias === "string") {
    handle = generatedAlias;
    if (data.alias === generatedAlias) delete data.alias;
  }

  const isResult =
    RESULT_COMMANDS.has(currentCommand) &&
    typeof data.rows === "number" &&
    Array.isArray(data.preview);
  if (isResult) {
    data.total = data.rows;
    data.next_offset = data.truncated ? (data.preview as unknown[]).length : null;
    data.rows = data.preview;
    delete data.preview;
    delete data.preview_count;
    delete data.truncated;
    delete data.storage;
    delete data.state_version;
    delete data.duplicate_of;
  } else if (currentCommand === "rows") {
    delete data.offset;
    delete data.limit;
    delete data.returned;
    delete data.truncated;
  } else if (currentCommand === "count" && typeof data.rows === "number") {
    data.total = data.rows;
    delete data.rows;
  } else if (currentCommand === "summarize" && Array.isArray(data.columns)) {
    const table = columnarRows(data.columns, undefined);
    Object.assign(data, { columns: table.columns, rows: table.rows });
  } else if (WRITE_COMMANDS.has(currentCommand)) {
    delete data.actor_id;
    delete data.plan_id;
    delete data.state_version_before;
    if (typeof data.status === "string") delete data.committed;
  } else if (
    currentCommand === "history" &&
    Array.isArray(data.history) &&
    data.history.every(isRecord)
  ) {
    Object.assign(data, compactHistory(data.history));
  } else if (currentCommand === "inspect" && Array.isArray(data.columns)) {
    data.columns = data.columns.map(withoutFalsePrimaryKey);
  } else if (currentCommand === "capabilities" && isRecord(data.features)) {
    const features = data.features;
    data.features = Object.keys(features).filter((key) => features[key] === true);
  } else if (
    currentCommand === "objects" &&
    Array.isArray(data.objects) &&
    data.objects.every(isRecord)
  ) {
    const objects = data.objects.every(hasDerivedIdentity)
      ? data.objects.map(withoutIdentity)
      : data.objects;
    const table = columnarRows(objects, undefined);
    delete data.objects;
    Object.assign(data, { columns: table.columns, objects: table.rows });
  } else if (
    currentCommand === "object" &&
    isRecord(data.object) &&
    hasDerivedIdentity(data.object)
  ) {
    data.object = withoutIdentity(data.object);
  }

  if (isResult || (currentCommand === "rows" && Array.isArray(data.rows))) {
    const table = columnarRows(data.rows as unknown[], data.columns);
    delete data.columns;
    delete data.rows;
    Object.assign(data, table);
  }

  const hasMoreRows =
    data.next_offset !== null && data.next_offset !== undefined;
  const warnings = result.warnings.filter(
    (warning) =>
      warning.code !== "NON_DETERMINISTIC_PAGINATION" || hasMoreRows,
  );

  dropFalseDefaults(data);
  for (const key of ["ok", "error", "handle", "warnings"]) {
    if (!(key in data)) continue;
    data[`data_${key}`] = data[key];
    delete data[key];
  }

  return {
    ok: true,
    ...(handle && !HANDLE_ECHO_COMMANDS.has(currentCommand) ? { handle } : {}),
    ...withoutNulls(data),
    ...(warnings.length ? { warnings } : {}),
  };
}

// Rows with differing keys (e.g. MongoDB documents) stay objects so missing and null remain distinct.
function columnarRows(
  rows: unknown[],
  columns: unknown,
): { columns?: string[]; rows: unknown[] } {
  const names = isColumns(columns)
    ? columns.map((column) => column.name)
    : Object.keys(rows[0] ?? {});
  if (names.length === 0) return { rows };
  const nameSet = new Set(names);
  const hasColumnKeys = (row: unknown): row is Record<string, unknown> => {
    if (!isRecord(row)) return false;
    const keys = Object.keys(row);
    return keys.length === names.length && keys.every((key) => nameSet.has(key));
  };
  if (!rows.every(hasColumnKeys)) return { rows };
  return {
    columns: names,
    rows: rows.map((row) => names.map((name) => row[name])),
  };
}

function compactHistory(
  entries: Record<string, unknown>[],
): Record<string, unknown> {
  const shared: Record<string, unknown> = {};
  for (const key of SHARED_HISTORY_KEYS) {
    const value = entries[0]?.[key];
    if (value === undefined) continue;
    if (entries.every((entry) => entry[key] === value)) shared[key] = value;
  }
  const history = entries.map((entry) => {
    const compact = withoutNulls(entry);
    dropFalseDefaults(compact);
    for (const key of Object.keys(shared)) delete compact[key];
    return compact;
  });
  return { ...shared, history };
}

function hasDerivedIdentity(object: Record<string, unknown>): boolean {
  const qualifiedName =
    typeof object.schema === "string"
      ? `${object.schema}.${String(object.name)}`
      : object.name;
  return object.identity === undefined || object.identity === qualifiedName;
}

function withoutIdentity(object: Record<string, unknown>): Record<string, unknown> {
  const compact = { ...object };
  delete compact.identity;
  return compact;
}

function withoutFalsePrimaryKey(column: unknown): unknown {
  if (!isRecord(column) || column.primary_key !== false) return column;
  const compact = { ...column };
  delete compact.primary_key;
  return compact;
}

function dropFalseDefaults(record: Record<string, unknown>): void {
  for (const key of FALSE_DEFAULT_KEYS) {
    if (record[key] === false) delete record[key];
  }
}

function withoutNulls(record: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).filter(([, value]) => value !== null),
  );
}


function primaryHandleKey(currentCommand: string): string | undefined {
  const keys: Record<string, string> = {
    query: "result_id",
    "mongo.query": "result_id",
    "redis.query": "result_id",
    filter: "result_id",
    show: "result_id",
    rows: "result_id",
    count: "result_id",
    columns: "result_id",
    summarize: "result_id",
    export: "result_id",
    alias: "result_id",
    "alias.set": "result_id",
    exec: "operation_id",
    "mongo.exec": "operation_id",
    "redis.exec": "operation_id",
    receipt: "operation_id",
    apply: "operation_id",
    plan: "plan_id",
    "mongo.plan": "plan_id",
    "redis.plan": "plan_id",
    connect: "connection_id",
    transaction: "transaction_id",
    "transaction.begin": "transaction_id",
    "transaction.status": "transaction_id",
    "transaction.commit": "transaction_id",
    "transaction.rollback": "transaction_id",
    session: "session_id",
    "session.start": "session_id",
    "session.show": "session_id",
    "session.close": "session_id",
  };
  return keys[currentCommand];
}
