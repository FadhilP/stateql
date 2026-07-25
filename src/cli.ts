#!/usr/bin/env node

import { createReadStream, readFileSync } from "node:fs";
import { extname, resolve } from "node:path";
import { createInterface } from "node:readline";
import { parseArgs } from "node:util";
import { exitCodeFor } from "./errors.js";
import { StateQL } from "./stateql.js";
import type {
  BatchCommand,
  Failure,
  Response,
  SqlParameters,
} from "./types.js";

type OutputMode = "agent" | "json" | "jsonl" | "text" | "silent";

const parsed = parseArgs({
  allowPositionals: true,
  strict: true,
  options: {
    name: { type: "string" },
    profile: { type: "string" },
    env: { type: "string" },
    "read-only": { type: "boolean" },
    "read-write": { type: "boolean" },
    params: { type: "string" },
    param: { type: "string", multiple: true },
    "params-file": { type: "string" },
    cache: { type: "string" },
    replay: { type: "boolean" },
    "idempotency-key": { type: "string" },
    "allow-unbounded": { type: "boolean" },
    "allow-destructive": { type: "boolean" },
    offset: { type: "string" },
    limit: { type: "string" },
    "timeout-ms": { type: "string" },
    format: { type: "string" },
    output: { type: "string" },
    isolation: { type: "string" },
    "continue-on-error": { type: "boolean" },
    help: { type: "boolean", short: "h" },
    version: { type: "boolean", short: "v" },
  },
});

const [command, subcommand, ...rest] = parsed.positionals;
const values = parsed.values;

if (values.version) {
  console.log("0.1.0");
  process.exit(0);
}
if (values.help || !command) {
  console.log(helpText());
  process.exit(0);
}

const abortController = new AbortController();
process.once("SIGINT", () => abortController.abort());
const stateql = new StateQL({
  ...(values["timeout-ms"] === undefined
    ? {}
    : { timeoutMs: Number(values["timeout-ms"]) }),
  signal: abortController.signal,
});

try {
  if (command === "batch" || command === "pipe") {
    await runBatch();
  } else {
    await runSingle();
  }
} finally {
  stateql.close();
}

async function runSingle(): Promise<void> {
  let response: Response<unknown>;
  let mode: OutputMode = "agent";
  try {
    mode = outputMode(command ?? "", values.output);
    response = await dispatch();
  } catch (error) {
    response = cliFailure(error);
  }
  print(response, mode, command ?? "");
  if (!response.ok) process.exitCode = exitCodeFor(response.error.code);
}

async function runBatch(): Promise<void> {
  const mode = outputMode(command ?? "batch", values.output);
  const jsonResponses: Response<unknown>[] = [];
  const pendingCommands: string[] = [];
  const commands = readBatchCommands(subcommand ?? "-");
  const trackedCommands = (async function* (): AsyncGenerator<BatchCommand> {
    for await (const item of commands) {
      pendingCommands.push(item.command);
      yield item;
    }
  })();
  try {
    for await (const response of stateql.batch(trackedCommands, {
      continueOnError: values["continue-on-error"] ?? false,
    })) {
      const responseCommand = pendingCommands.shift() ?? command ?? "batch";
      if (mode === "json") jsonResponses.push(response);
      else print(response, mode, responseCommand);
      if (!response.ok && process.exitCode === undefined) {
        process.exitCode = exitCodeFor(response.error.code);
      }
    }
  } catch (error) {
    const response = cliFailure(error);
    if (mode === "json") jsonResponses.push(response);
    else print(response, mode, pendingCommands.shift() ?? command ?? "batch");
    process.exitCode = exitCodeFor(response.error.code);
  }
  if (mode === "json") console.log(JSON.stringify(jsonResponses, null, 2));
}

async function dispatch(): Promise<Response<unknown>> {
  const params = parseParameters(
    values.params,
    values.param,
    values["params-file"],
  );
  const sql = [subcommand, ...rest].filter(Boolean).join(" ");

  switch (command) {
    case "connect": {
      if (values["read-only"] && values["read-write"]) {
        throw new Error("--read-only and --read-write are mutually exclusive.");
      }
      if (values.profile && subcommand) {
        throw new Error("Use either --profile or a connection target.");
      }
      return stateql.connect(subcommand, {
        ...(values.name ? { name: values.name } : {}),
        ...(values.env ? { secretEnv: values.env } : {}),
        ...(values.profile ? { profile: values.profile } : {}),
        ...(values["read-only"]
          ? { readOnly: true }
          : values["read-write"]
            ? { readOnly: false }
            : {}),
      });
    }
    case "disconnect":
      return stateql.disconnect();
    case "status":
      return stateql.status();
    case "profile":
      return dispatchProfile(subcommand, rest);
    case "session":
      return dispatchSession(subcommand, rest);
    case "query":
      return stateql.query(sql, {
        params,
        cache: cacheMode(values.cache),
      });
    case "filter":
      return stateql.filter(
        requireValue(subcommand, "result handle"),
        requireValue(rest.join(" ").trim(), "filter predicate"),
        { params },
      );
    case "exec":
      return stateql.exec(sql, {
        params,
        replay: values.replay ?? false,
        ...(values["idempotency-key"]
          ? { idempotencyKey: values["idempotency-key"] }
          : {}),
        allowUnbounded: values["allow-unbounded"] ?? false,
        allowDestructive: values["allow-destructive"] ?? false,
      });
    case "show":
      return stateql.show(requireValue(subcommand, "result handle"));
    case "rows":
      return stateql.rows(requireValue(subcommand, "result handle"), {
        offset: numberOption(values.offset, 0),
        limit: numberOption(values.limit, 20),
      });
    case "count":
      return stateql.count(requireValue(subcommand, "result handle"));
    case "columns":
      return stateql.columns(requireValue(subcommand, "result handle"));
    case "export":
      return stateql.exportResult(
        requireValue(subcommand, "result handle"),
        requireValue(values.output, "--output"),
        exportFormat(values.format),
      );
    case "alias":
      if (subcommand !== "set") throw new Error("Expected: alias set NAME RESULT");
      return stateql.setAlias(
        requireValue(rest[0], "alias"),
        requireValue(rest[1], "result handle"),
      );
    case "inspect":
      return stateql.inspect(
        normalizeInspectKind(requireValue(subcommand, "inspection kind")),
        rest[0],
      );
    case "transaction":
      return dispatchTransaction(subcommand, rest[0]);
    case "plan":
      return stateql.plan(sql, {
        params,
        allowUnbounded: values["allow-unbounded"] ?? false,
        ...(values["allow-destructive"]
          ? { allowDestructive: true }
          : {}),
      });
    case "apply":
      return stateql.apply(requireValue(subcommand, "plan handle"));
    case "history":
      return stateql.history(numberOption(values.limit, 20));
    case "receipt":
      return stateql.receipt(requireValue(subcommand, "operation handle"));
    case "capabilities":
      return stateql.capabilities();
    default:
      throw new Error(`Unknown command "${command}".`);
  }
}

async function dispatchSession(
  action: string | undefined,
  args: string[],
): Promise<Response<unknown>> {
  switch (action) {
    case "start":
      return stateql.startSession(
        requireValue(values.name ?? args[0], "session name"),
      );
    case "list":
      return stateql.listSessions();
    case "show":
      return stateql.showSession(args[0]);
    case "summary":
      return stateql.sessionSummary();
    case "close":
      return stateql.closeSession();
    default:
      throw new Error(`Unknown session command "${action ?? ""}".`);
  }
}

async function dispatchProfile(
  action: string | undefined,
  args: string[],
): Promise<Response<unknown>> {
  switch (action) {
    case "add":
      if (values["read-only"] && values["read-write"]) {
        throw new Error("--read-only and --read-write are mutually exclusive.");
      }
      return stateql.addProfile(
        requireValue(args[0], "profile name"),
        args[1],
        {
          ...(values.env ? { secretEnv: values.env } : {}),
          readOnly: !values["read-write"],
        },
      );
    case "list":
      return stateql.listProfiles();
    case "show":
      return stateql.showProfile(requireValue(args[0], "profile name"));
    case "remove":
      return stateql.removeProfile(requireValue(args[0], "profile name"));
    default:
      throw new Error(`Unknown profile command "${action ?? ""}".`);
  }
}

async function dispatchTransaction(
  action: string | undefined,
  id: string | undefined,
): Promise<Response<unknown>> {
  switch (action) {
    case "begin":
      return stateql.beginTransaction(values.isolation);
    case "status":
      return stateql.transactionStatus(id);
    case "commit":
      return stateql.commitTransaction(id);
    case "rollback":
      return stateql.rollbackTransaction(id);
    default:
      throw new Error(`Unknown transaction command "${action ?? ""}".`);
  }
}

function parseParameters(
  value: string | undefined,
  values: string[] | undefined,
  file: string | undefined,
): SqlParameters {
  const modes = [Boolean(value), Boolean(values?.length), Boolean(file)].filter(
    Boolean,
  ).length;
  if (modes > 1) {
    throw new Error(
      "Use only one of --params, repeated --param, or --params-file.",
    );
  }
  if (values?.length) return values.map(parseParameter);
  if (!value && !file) return [];

  const json = file
    ? readFileSync(file === "-" ? 0 : resolve(file), "utf8")
    : value!;
  let parsedValue: unknown;
  try {
    parsedValue = JSON.parse(json) as unknown;
  } catch {
    throw new Error(
      `Invalid JSON from ${file ? "--params-file" : "--params"}. ` +
        "In PowerShell, use repeated --param values.",
    );
  }
  if (
    !Array.isArray(parsedValue) &&
    (!parsedValue || typeof parsedValue !== "object")
  ) {
    throw new Error("--params must be a JSON array or object.");
  }
  return parsedValue as SqlParameters;
}

function parseParameter(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

async function* readBatchCommands(
  source: string,
): AsyncGenerator<BatchCommand> {
  if (source !== "-" && extname(source).toLowerCase() === ".json") {
    const parsedInput = parseBatchJson(
      readFileSync(resolve(source), "utf8"),
      source,
    );
    if (!Array.isArray(parsedInput)) {
      throw new Error("Batch JSON file must contain an array.");
    }
    for (let index = 0; index < parsedInput.length; index += 1) {
      yield parseBatchCommand(parsedInput[index], `item ${index + 1}`);
    }
    return;
  }

  const input = source === "-" ? process.stdin : createReadStream(resolve(source));
  const lines = createInterface({ input, crlfDelay: Infinity });
  let lineNumber = 0;
  for await (const line of lines) {
    lineNumber += 1;
    if (!line.trim()) continue;
    yield parseBatchCommand(
      parseBatchJson(line, `line ${lineNumber}`),
      `line ${lineNumber}`,
    );
  }
}

function parseBatchCommand(value: unknown, location: string): BatchCommand {
  if (
    !value ||
    typeof value !== "object" ||
    typeof (value as Record<string, unknown>).command !== "string"
  ) {
    throw new Error(`Invalid batch command at ${location}.`);
  }
  return value as BatchCommand;
}

function parseBatchJson(value: string, location: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new Error(`Invalid JSON at ${location}.`);
  }
}

function numberOption(value: string | undefined, fallback: number): number {
  return value === undefined ? fallback : Number(value);
}

function cacheMode(
  value: string | undefined,
): "auto" | "bypass" | "require" {
  if (!value || value === "auto") return "auto";
  if (value === "bypass" || value === "require") return value;
  throw new Error("--cache must be auto, bypass, or require.");
}

function exportFormat(
  value: string | undefined,
): "json" | "jsonl" | "csv" {
  if (!value || value === "csv") return "csv";
  if (value === "json" || value === "jsonl") return value;
  throw new Error("--format must be json, jsonl, or csv.");
}

function normalizeInspectKind(value: string): string {
  const aliases: Record<string, string> = {
    index: "indexes",
    constraint: "constraints",
  };
  return aliases[value] ?? value;
}

function requireValue(
  value: string | undefined,
  description: string,
): string {
  if (value) return value;
  throw new Error(`Missing ${description}.`);
}

function outputMode(
  currentCommand: string,
  value: string | undefined,
): OutputMode {
  const mode =
    currentCommand === "export"
      ? process.env.STQL_OUTPUT ?? "agent"
      : value ?? process.env.STQL_OUTPUT ?? "agent";
  if (
    mode === "agent" ||
    mode === "json" ||
    mode === "jsonl" ||
    mode === "text" ||
    mode === "silent"
  ) {
    return mode;
  }
  throw new Error(
    "--output must be agent, json, jsonl, text, or silent.",
  );
}

function print(
  result: Response<unknown>,
  mode: OutputMode,
  currentCommand: string,
): void {
  if (mode === "silent") {
    if (result.ok) {
      const handle = extractHandle(result.data);
      if (handle) console.log(handle);
    }
    return;
  }
  if (mode === "text") {
    if (!result.ok) {
      console.log(`${result.error.code}: ${result.error.message}`);
      return;
    }
    const handle = extractHandle(result.data);
    console.log(handle ? `ok ${handle}` : "ok");
    return;
  }
  if (mode === "agent") {
    console.log(JSON.stringify(toAgentResponse(result, currentCommand)));
    return;
  }
  console.log(
    mode === "json" ? JSON.stringify(result, null, 2) : JSON.stringify(result),
  );
}

function toAgentResponse(
  result: Response<unknown>,
  currentCommand: string,
): Record<string, unknown> {
  if (!result.ok) return { ok: false, error: result.error };
  if (
    !result.data ||
    typeof result.data !== "object" ||
    Array.isArray(result.data)
  ) {
    return { ok: true, data: result.data };
  }

  const data = { ...(result.data as Record<string, unknown>) };
  const handleKey = primaryHandleKey(currentCommand);
  const handle =
    handleKey && typeof data[handleKey] === "string" && data[handleKey]
      ? data[handleKey]
      : undefined;
  if (handle && handleKey) delete data[handleKey];

  if (
    (currentCommand === "query" ||
      currentCommand === "filter" ||
      currentCommand === "show") &&
    typeof data.rows === "number" &&
    Array.isArray(data.preview)
  ) {
    const total = data.rows;
    const rows = data.preview;
    const truncated = Boolean(data.truncated);
    data.rows = rows;
    data.total = total;
    data.next_offset = truncated ? rows.length : null;
    delete data.preview;
    delete data.preview_count;
    delete data.columns;
    delete data.storage;
    delete data.state_version;
    delete data.duplicate_of;
  } else if (currentCommand === "rows") {
    delete data.offset;
    delete data.limit;
    delete data.returned;
  } else if (currentCommand === "count" && typeof data.rows === "number") {
    data.total = data.rows;
    delete data.rows;
  }

  for (const key of ["ok", "error", "handle", "warnings"]) {
    if (!(key in data)) continue;
    data[`data_${key}`] = data[key];
    delete data[key];
  }

  return {
    ok: true,
    ...(handle ? { handle } : {}),
    ...data,
    ...(result.warnings.length ? { warnings: result.warnings } : {}),
  };
}

function primaryHandleKey(currentCommand: string): string | undefined {
  const keys: Record<string, string> = {
    query: "result_id",
    filter: "result_id",
    show: "result_id",
    rows: "result_id",
    count: "result_id",
    columns: "result_id",
    export: "result_id",
    alias: "result_id",
    "alias.set": "result_id",
    exec: "operation_id",
    receipt: "operation_id",
    apply: "operation_id",
    plan: "plan_id",
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

function cliFailure(error: unknown): Failure {
  return {
    ok: false,
    command_id: "cmd_0",
    session_id: process.env.STQL_SESSION ?? "default",
    error: {
      code: "INVALID_COMMAND",
      message: error instanceof Error ? error.message : String(error),
      retryable: false,
      executed: false,
    },
    meta: { duration_ms: 0 },
  };
}

function extractHandle(data: unknown): string | undefined {
  if (!data || typeof data !== "object") return undefined;
  const record = data as Record<string, unknown>;
  for (const key of [
    "result_id",
    "operation_id",
    "plan_id",
    "transaction_id",
    "connection_id",
    "session_id",
  ]) {
    if (typeof record[key] === "string") return record[key];
  }
  return undefined;
}

function helpText(): string {
  return `StateQL 0.1.0

Usage: stql <command> [arguments] [options]

Commands:
  connect, disconnect, status
  profile add|list|show|remove
  session start|list|show|summary|close
  query, filter, exec, show, rows, count, columns, export
  alias set
  inspect schema|table|columns|indexes|constraints
  transaction begin|status|commit|rollback
  plan, apply, history, receipt, capabilities
  batch [file.json|file.jsonl|-]
  pipe

SQL parameters: --params JSON, repeated --param VALUE, or --params-file FILE.
Deadline: --timeout-ms N (default: 30000). Ctrl+C cancels database work.
Output: --output agent|json|jsonl|text|silent (default: agent).
Batch/pipe accept JSON array files or JSONL streams. Stop on first error.`;
}
