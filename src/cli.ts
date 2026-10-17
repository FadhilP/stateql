#!/usr/bin/env node

import { createReadStream, readFileSync } from "node:fs";
import { extname, resolve } from "node:path";
import { createInterface } from "node:readline";
import { parseArgs } from "node:util";
import { BSON } from "mongodb";
import { exitCodeFor } from "./errors.js";
import { StateQL } from "./stateql.js";
import { agentResponse } from "./agent-response.js";
import { askPassword, cliCredentialResolver, deleteOsPassword, saveOsPassword } from "./os-credentials.js";
import type {
  BatchCommand,
  CatalogObject,
  CatalogObjectKind,
  Failure,
  MongoReadCommand,
  MongoWriteCommand,
  Response,
  SqlParameters,
  RedisCommand,
} from "./types.js";

type OutputMode = "agent" | "json" | "jsonl" | "text" | "silent";

const USAGE = `stql connect <target> [--name NAME] [--read-only|--read-write]
stql connect --env ENV | --credential-ref REF | --profile NAME [--name NAME] [--read-only|--read-write]
stql disconnect|status
stql actor show | set-name <display-name>
stql profile add|update NAME [TARGET [--ask-password] | --env ENV | --credential-ref REF]
                           [--read-only|--read-write]
stql profile list|show|remove NAME
stql session start|list|show|summary|close
stql query <sql> | --file FILE [--params JSON | --param VALUE... | --params-file FILE]
                 [--cache auto|bypass|require] [--preview-rows 0..200]
stql filter <result-handle> <predicate> [--params JSON | --param VALUE...]
stql exec <sql> | --file FILE [--params JSON | --param VALUE...] [--idempotency-key KEY]
                [--replay] [--allow-unbounded] [--allow-destructive]
stql plan <sql> | --file FILE [--params JSON | --param VALUE...] [--allow-unbounded]
                [--allow-destructive]
stql apply <plan-handle>
stql mongo query|exec|plan '<EJSON command>' | --file FILE [--cache MODE] [--idempotency-key KEY]
                                  [--replay] [--allow-unbounded] [--allow-destructive]
stql redis query|exec|plan '<JSON command>' | --file FILE [--cache MODE] [--idempotency-key KEY]
                                  [--replay]
stql show|count|columns|summarize <result-handle>
stql rows <result-handle> [--offset N] [--limit N] [--columns a,b]
stql alias set <name> <result-handle>
stql export <result-handle> --output FILE [--format json|jsonl|csv] [--columns a,b]
stql inspect schema|table|collection|collections|columns|indexes|constraints [name]
stql objects [KIND] [--schema NAME] [--search TEXT] [--offset N|--cursor CURSOR] [--limit N]
stql object KIND NAME [IDENTITY] [--schema NAME]
stql transaction begin|status|commit|rollback [--isolation LEVEL]
stql history [--limit N] [--offset N] [--category statement|introspection|management]
             [--internal|--external]
stql receipt <operation-handle>
stql doctor|capabilities
stql purge [expired|results|history|all]
stql batch [commands.json|commands.jsonl|-] [--continue-on-error]
stql pipe [--continue-on-error]`;

const parsed = parseArgs({
  allowPositionals: true,
  strict: true,
  options: {
    name: { type: "string" },
    profile: { type: "string" },
    env: { type: "string" },
    "credential-ref": { type: "string" },
    "ask-password": { type: "boolean" },
    "read-only": { type: "boolean" },
    "read-write": { type: "boolean" },
    params: { type: "string" },
    param: { type: "string", multiple: true },
    "params-file": { type: "string" },
    file: { type: "string" },
    columns: { type: "string" },
    cache: { type: "string" },
    "preview-rows": { type: "string" },
    "max-cell-characters": { type: "string" },
    replay: { type: "boolean" },
    "idempotency-key": { type: "string" },
    "allow-unbounded": { type: "boolean" },
    "allow-destructive": { type: "boolean" },
    offset: { type: "string" },
    limit: { type: "string" },
    cursor: { type: "string" },
    schema: { type: "string" },
    search: { type: "string" },
    category: { type: "string" },
    internal: { type: "boolean" },
    external: { type: "boolean" },
    "timeout-ms": { type: "string" },
    "max-state-bytes": { type: "string" },
    "cache-ttl-seconds": { type: "string" },
    "result-ttl-seconds": { type: "string" },
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
  console.log(packageVersion());
  process.exit(0);
}
if (values.help || !command || command === "help") {
  const topic = command === "help" ? subcommand : command;
  const usage = topic ? commandUsage(topic) : helpText();
  if (usage === undefined) {
    console.error(`Unknown command "${topic}". Run "stql help" to list commands.`);
    process.exit(exitCodeFor("INVALID_COMMAND"));
  }
  console.log(usage);
  process.exit(0);
}

const abortController = new AbortController();
process.once("SIGINT", () => abortController.abort());
const stateql = new StateQL({
  ...(values["timeout-ms"] === undefined
    ? {}
    : { timeoutMs: Number(values["timeout-ms"]) }),
  ...(process.env.STQL_ACTOR ? { actor: process.env.STQL_ACTOR } : {}),
  ...(values["max-state-bytes"] === undefined
    ? {}
    : { maxStateBytes: Number(values["max-state-bytes"]) }),
  ...(values["cache-ttl-seconds"] === undefined
    ? {}
    : { cacheTtlSeconds: Number(values["cache-ttl-seconds"]) }),
  ...(values["result-ttl-seconds"] === undefined
    ? {}
    : { resultTtlSeconds: Number(values["result-ttl-seconds"]) }),
  ...(values["max-cell-characters"] === undefined
    ? {}
    : { maxCellCharacters: Number(values["max-cell-characters"]) }),
  credentialResolver: cliCredentialResolver,
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
  const responseCommand = (command === "mongo" || command === "redis") && subcommand
    ? `${command}.${subcommand}`
    : command ?? "";
  print(response, mode, responseCommand);
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
  const sql = (): string => statementText([subcommand, ...rest]);

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
        ...(values["credential-ref"] ? { credentialRef: values["credential-ref"] } : {}),
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
    case "actor":
      return dispatchActor(subcommand, rest);
    case "profile":
      return dispatchProfile(subcommand, rest);
    case "session":
      return dispatchSession(subcommand, rest);
    case "mongo":
      return dispatchMongo(subcommand, rest);
    case "redis":
      return dispatchRedis(subcommand, rest);
    case "query":
      return stateql.query(sql(), {
        params,
        cache: cacheMode(values.cache),
        ...(values["preview-rows"] === undefined
          ? {}
          : { previewRows: Number(values["preview-rows"]) }),
      });
    case "filter":
      return stateql.filter(
        requireValue(subcommand, "result handle"),
        requireValue(rest.join(" ").trim(), "filter predicate"),
        { params },
      );
    case "exec":
      return stateql.exec(sql(), {
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
        ...(values.columns === undefined ? {} : { columns: columnList(values.columns) }),
      });
    case "count":
      return stateql.count(requireValue(subcommand, "result handle"));
    case "columns":
      return stateql.columns(requireValue(subcommand, "result handle"));
    case "summarize":
      return stateql.summarize(requireValue(subcommand, "result handle"));
    case "export":
      return stateql.exportResult(
        requireValue(subcommand, "result handle"),
        requireValue(values.output, "--output"),
        exportFormat(values.format),
        values.columns === undefined ? undefined : columnList(values.columns),
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
    case "objects":
      return stateql.listObjects({
        ...(subcommand ? { kind: subcommand as CatalogObjectKind } : {}),
        ...(values.schema ? { schema: values.schema } : {}),
        ...(values.search ? { search: values.search } : {}),
        offset: values.cursor ?? numberOption(values.offset, 0),
        limit: numberOption(values.limit, 50),
      });
    case "object": {
      const object: CatalogObject = {
        kind: requireValue(subcommand, "object kind") as CatalogObjectKind,
        name: requireValue(rest[0], "object name"),
        ...(values.schema ? { schema: values.schema } : {}),
        ...(rest[1] ? { identity: rest[1] } : {}),
      };
      return stateql.describeObject(object);
    }
    case "transaction":
      return dispatchTransaction(subcommand, rest[0]);
    case "plan":
      return stateql.plan(sql(), {
        params,
        allowUnbounded: values["allow-unbounded"] ?? false,
        ...(values["allow-destructive"]
          ? { allowDestructive: true }
          : {}),
      });
    case "apply":
      return stateql.apply(requireValue(subcommand, "plan handle"));
    case "history":
      if (values.internal && values.external) throw new Error("--internal and --external are mutually exclusive.");
      return stateql.history(numberOption(values.limit, 20), {
        ...(values.category ? { category: values.category as "statement" | "introspection" | "management" } : {}),
        ...(values.internal ? { internal: true } : values.external ? { internal: false } : {}),
        offset: numberOption(values.offset, 0),
      });
    case "receipt":
      return stateql.receipt(requireValue(subcommand, "operation handle"));
    case "doctor":
      return stateql.doctor();
    case "purge":
      return stateql.purge(purgeScope(subcommand));
    case "capabilities":
      return stateql.capabilities();
    default:
      throw new Error(`Unknown command "${command}".`);
  }
}

async function dispatchMongo(
  action: string | undefined,
  args: string[],
): Promise<Response<unknown>> {
  const payload = requireValue(statementText(args), "MongoDB EJSON command");
  switch (action) {
    case "query":
      return stateql.mongoQuery(parseMongoCommand(payload) as MongoReadCommand, {
        cache: cacheMode(values.cache),
      });
    case "exec":
      return stateql.mongoExec(parseMongoCommand(payload) as MongoWriteCommand, {
        replay: values.replay ?? false,
        ...(values["idempotency-key"]
          ? { idempotencyKey: values["idempotency-key"] }
          : {}),
        allowUnbounded: values["allow-unbounded"] ?? false,
        allowDestructive: values["allow-destructive"] ?? false,
      });
    case "plan":
      return stateql.mongoPlan(parseMongoCommand(payload) as MongoWriteCommand, {
        allowUnbounded: values["allow-unbounded"] ?? false,
        ...(values["allow-destructive"]
          ? { allowDestructive: true }
          : {}),
      });
    default:
      throw new Error(`Unknown MongoDB command "${action ?? ""}".`);
  }
}

async function dispatchRedis(
  action: string | undefined,
  args: string[],
): Promise<Response<unknown>> {
  const payload = parseRedisCommand(requireValue(statementText(args), "Redis JSON command"));
  switch (action) {
    case "query": return stateql.redisQuery(payload, { cache: cacheMode(values.cache) });
    case "exec": return stateql.redisExec(payload, { replay: values.replay ?? false, ...(values["idempotency-key"] ? { idempotencyKey: values["idempotency-key"] } : {}) });
    case "plan": return stateql.redisPlan(payload);
    default: throw new Error(`Unknown Redis command "${action ?? ""}".`);
  }
}

function parseRedisCommand(value: string): RedisCommand {
  try {
    const parsed = JSON.parse(value) as RedisCommand;
    if (!parsed || typeof parsed !== "object") throw new Error();
    return parsed;
  } catch { throw new Error("Invalid Redis JSON command."); }
}


function parseMongoCommand(value: string): unknown {
  try {
    return BSON.EJSON.parse(value, { relaxed: false }) as unknown;
  } catch {
    throw new Error("Invalid MongoDB EJSON command.");
  }
}
async function dispatchActor(
  action: string | undefined,
  args: string[],
): Promise<Response<unknown>> {
  switch (action) {
    case "show":
      return stateql.showActor();
    case "set-name":
      return stateql.setActorName(
        requireValue(args.join(" ").trim(), "actor name"),
      );
    default:
      throw new Error(`Unknown actor command "${action ?? ""}".`);
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
    case "add": {
      if (values["read-only"] && values["read-write"]) {
        throw new Error("--read-only and --read-write are mutually exclusive.");
      }
      const name = requireValue(args[0], "profile name");
      const passwordRef = values["ask-password"]
        ? await saveOsPassword(await askPassword(name, requireValue(args[1], "TARGET for --ask-password")))
        : undefined;
      const added = await stateql.addProfile(name, args[1], {
        ...(values.env ? { secretEnv: values.env } : {}),
        ...(values["credential-ref"] ? { credentialRef: values["credential-ref"] } : {}),
        ...(passwordRef ? { passwordRef } : {}),
        readOnly: !values["read-write"],
      });
      if (!added.ok) await deleteOsPassword(passwordRef);
      return added;
    }
    case "update": {
      if (values["read-only"] && values["read-write"]) throw new Error("--read-only and --read-write are mutually exclusive.");
      const name = requireValue(args[0], "profile name");
      const before = await stateql.showProfile(name);
      if (!before.ok) return before;
      const target = args[1] ?? before.data.target;
      const passwordRef = values["ask-password"]
        ? await saveOsPassword(await askPassword(name, requireValue(target ?? undefined, "TARGET for --ask-password")))
        : undefined;
      const updated = await stateql.updateProfile(name, {
        ...(args[1] !== undefined ? { target: args[1] } : {}),
        ...(values.env ? { secretEnv: values.env } : {}),
        ...(values["credential-ref"] ? { credentialRef: values["credential-ref"] } : {}),
        ...(passwordRef ? { passwordRef } : {}),
        ...(values["read-only"] ? { readOnly: true } : values["read-write"] ? { readOnly: false } : {}),
      });
      if (!updated.ok) {
        await deleteOsPassword(passwordRef);
        return updated;
      }
      if (updated.data.password_ref !== before.data.password_ref) await deleteOsPassword(before.data.password_ref);
      return updated;
    }
    case "list":
      return stateql.listProfiles();
    case "show":
      return stateql.showProfile(requireValue(args[0], "profile name"));
    case "remove": {
      const name = requireValue(args[0], "profile name");
      const before = await stateql.showProfile(name);
      const removed = await stateql.removeProfile(name);
      if (removed.ok && before.ok) await deleteOsPassword(before.data.password_ref);
      return removed;
    }
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
    parsedValue = JSON.parse(withoutBom(json)) as unknown;
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

function statementText(args: Array<string | undefined>): string {
  const inline = args.filter(Boolean).join(" ").trim();
  const file = values.file;
  if (file === undefined) return inline;
  if (inline) throw new Error("Use either inline text or --file, not both.");
  if (file === "-" && values["params-file"] === "-") {
    throw new Error("--file and --params-file cannot both read standard input.");
  }
  return withoutBom(readFileSync(file === "-" ? 0 : resolve(file), "utf8")).trim();
}

function columnList(value: string): string[] {
  return value.split(",").map((name) => name.trim()).filter(Boolean);
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

// PowerShell pipes and Out-File/Notepad files can start with a UTF-8 BOM, which JSON.parse rejects.
function withoutBom(value: string): string {
  return value.startsWith("﻿") ? value.slice(1) : value;
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
    return JSON.parse(withoutBom(value)) as unknown;
  } catch {
    throw new Error(`Invalid JSON at ${location}.`);
  }
}

function numberOption(value: string | undefined, fallback: number): number {
  return value === undefined ? fallback : Number(value);
}

function purgeScope(
  value: string | undefined,
): "expired" | "results" | "history" | "all" {
  if (!value || value === "expired") return "expired";
  if (value === "results" || value === "history" || value === "all") return value;
  throw new Error("purge scope must be expired, results, history, or all.");
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
    console.log(JSON.stringify(agentResponse(result, currentCommand)));
    return;
  }
  console.log(
    mode === "json" ? JSON.stringify(result, null, 2) : JSON.stringify(result),
  );
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

function commandUsage(name: string): string | undefined {
  const entries = USAGE.split(/\n(?=stql )/).filter((entry) =>
    entry.slice("stql ".length).split(" ")[0]!.split("|").includes(name),
  );
  return entries.length > 0 ? entries.join("\n") : undefined;
}

function helpText(): string {
  return `StateQL ${packageVersion()}

Usage: stql <command> [arguments] [options]

Commands (stql help COMMAND shows one):
${USAGE}

SQL parameters: --params JSON, repeated --param VALUE, or --params-file FILE.
Long SQL or native commands: --file FILE (- reads standard input) instead of inline text.
Query preview: --preview-rows N (default: 5, maximum: 200).
Long cells: --max-cell-characters N (default: 200); use export for full values.
Deadline: --timeout-ms N (default: 30000). Ctrl+C cancels database work.
Saved passwords: --ask-password opens a dialog and keeps the password in the OS credential store.
Other credential refs require a trusted host CredentialResolver; the standalone CLI cannot resolve them.
State: --max-state-bytes N, --cache-ttl-seconds N, --result-ttl-seconds N.
Output: --output agent|json|jsonl|text|silent (default: agent).
Agent rows: arrays in "columns" order (objects when shapes differ). next_offset only when more rows exist.
Agent output omits null fields and false cached/duplicate flags. Agent skill: skills/stateql/SKILL.md.
Batch/pipe accept JSON array files or JSONL streams. Stop on first error.`;
}

function packageVersion(): string {
  const packageJson = JSON.parse(
    readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
  ) as { version: string };
  return packageJson.version;
}
