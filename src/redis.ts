import { createClient, RESP_TYPES, WatchError } from "@redis/client";
import { AdapterExecutionError, AdapterWriteError, type AdapterContext } from "./adapters.js";
import type {
  CatalogObject,
  DescribeObjectData,
  ListObjectsData,
  ListObjectsFilter,
  RedisCommand,
  RedisWriteOutcome,
  Row,
  Column,
} from "./types.js";
import { hash, toJsonSafe } from "./util.js";

const MAX_ARGUMENTS = 100;
const MAX_ARGUMENT_BYTES = 256 * 1024;
const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_COLLECTION_VALUES = 200;
const READ_COMMANDS = new Set([
  "GET", "MGET", "TYPE", "EXISTS", "TTL", "PTTL", "HGET", "HMGET",
  "LRANGE", "SCAN", "HSCAN", "SSCAN", "ZSCAN",
]);
const WRITE_COMMANDS = new Set([
  "SET", "DEL", "HSET", "HDEL", "LPUSH", "RPUSH", "SADD", "SREM", "ZADD", "ZREM",
]);

export interface RedisReadResult {
  rows: Row[];
  columns: Column[];
  nextCursor?: string | null;
}

export interface RedisPrecondition {
  key: string;
  fingerprint: string;
  /** Absolute expiry in epoch milliseconds, or Redis -1/-2 sentinel. */
  expiresAt: number;
}

export interface RedisWriteResult {
  affectedRows: number;
  outcome: RedisWriteOutcome;
}

export function validateRedisReadCommand(value: unknown): RedisCommand {
  const command = validateCommandShape(value);
  if (!READ_COMMANDS.has(command.command)) {
    throw new Error(`Unsupported Redis read command "${command.command}".`);
  }
  const args = command.args ?? [];
  switch (command.command) {
    case "GET": case "TYPE": case "TTL": case "PTTL": requireCount(args, 1); break;
    case "MGET": case "EXISTS": requireRange(args, 1, MAX_ARGUMENTS); break;
    case "HGET": requireCount(args, 2); break;
    case "HMGET": requireRange(args, 2, MAX_ARGUMENTS + 1); break;
    case "LRANGE":
      requireCount(args, 3);
      boundedRange(args[1]!, args[2]!);
      break;
    case "SCAN": validateScanArgs(args, false); break;
    case "HSCAN": case "SSCAN": case "ZSCAN": validateScanArgs(args, true); break;
  }
  return command;
}

export function validateRedisWriteCommand(value: unknown): RedisCommand {
  const command = validateCommandShape(value);
  if (!WRITE_COMMANDS.has(command.command)) {
    throw new Error(`Unsupported Redis write command "${command.command}".`);
  }
  const args = command.args ?? [];
  switch (command.command) {
    case "SET": requireCount(args, 2); break;
    case "DEL": requireCount(args, 1); break;
    case "HSET": requireCount(args, 3); break;
    case "HDEL": requireCount(args, 2); break;
    case "LPUSH": case "RPUSH": case "SADD": case "SREM": case "ZREM": requireCount(args, 2); break;
    case "ZADD":
      requireCount(args, 3);
      if (!Number.isFinite(Number(args[1]))) throw new Error("Redis ZADD score must be finite.");
      break;
  }
  return command;
}

export function serializeRedisCommand(command: RedisCommand): string {
  return JSON.stringify({ command: command.command.toUpperCase(), args: command.args ?? [] });
}

export function deserializeRedisCommand(value: string): RedisCommand {
  try { return validateRedisWriteCommand(JSON.parse(value)); }
  catch { throw new Error("Stored Redis command is invalid."); }
}

export class RedisAdapter {
  readonly confidence = "ttl_based" as const;
  private readonly client;
  private connected = false;
  private closed = false;

  constructor(source: string, private readonly readOnly: boolean, private readonly context: AdapterContext) {
    this.client = createClient({
      url: source,
      socket: {
        connectTimeout: remainingMilliseconds(context),
        reconnectStrategy: false,
      },
    });
    this.client.on("error", () => undefined);
  }

  async ping(): Promise<void> {
    await this.connect();
    await this.execute(["PING"], false);
  }

  async signature(): Promise<string> {
    throwIfStopped(this.context, false);
    return "redis:watched-key";
  }

  async read(input: RedisCommand): Promise<RedisReadResult> {
    const command = validateRedisReadCommand(input);
    await this.connect();
    const raw = await this.execute([command.command, ...(command.args ?? [])], false);
    const bytes = Buffer.byteLength(JSON.stringify(toJsonSafe(raw)), "utf8");
    if (bytes > MAX_RESPONSE_BYTES) throw new Error("Redis response exceeds the 1 MiB materialization limit.");
    return redisRows(command, raw);
  }

  async precondition(input: RedisCommand): Promise<RedisPrecondition> {
    const command = validateRedisWriteCommand(input);
    await this.connect();
    const key = command.args![0]!;
    const [fingerprint, expiresAt] = await Promise.all([this.keyFingerprint(key), this.expirationIdentity(key)]);
    return { key, fingerprint, expiresAt };
  }

  async write(input: RedisCommand, precondition?: RedisPrecondition): Promise<RedisWriteResult> {
    const command = validateRedisWriteCommand(input);
    if (this.readOnly) throw new AdapterWriteError("Connection is read-only.", false);
    await this.connect();
    const key = command.args![0]!;
    if (precondition && precondition.key !== key) {
      throw new AdapterWriteError("Redis plan key does not match its command.", false);
    }
    let dispatched = false;
    try {
      let raw: unknown;
      if (precondition) {
        await this.execute(["WATCH", key], false);
        const [fingerprint, expiresAt] = await Promise.all([this.keyFingerprint(key), this.expirationIdentity(key)]);
        const expiryChanged = precondition.expiresAt < 0
          ? expiresAt !== precondition.expiresAt
          : expiresAt < 0 || Math.abs(expiresAt - precondition.expiresAt) > 2_000;
        if (fingerprint !== precondition.fingerprint || expiryChanged) {
          await this.execute(["UNWATCH"], false).catch(() => undefined);
          throw new AdapterWriteError("ROW_CONFLICT: The Redis key changed. Reload before editing.", false);
        }
        const transaction = this.client.multi();
        transaction.addCommand([command.command, ...(command.args ?? [])]);
        dispatched = true;
        const result = await withDeadline(transaction.exec(), this.context, true, () => this.stop());
        if (result === null) throw new AdapterWriteError("ROW_CONFLICT: The Redis key changed. Reload before editing.", false);
        raw = result[0];
      } else {
        dispatched = true;
        raw = await this.execute([command.command, ...(command.args ?? [])], true);
      }
      if (raw instanceof Error) throw new AdapterWriteError(raw.message, false);
      const normalized = normalizeScalar(raw);
      return {
        affectedRows: redisAffectedRows(command.command, normalized),
        outcome: { acknowledged: true, result: normalized },
      };
    } catch (error) {
      if (error instanceof AdapterWriteError) throw error;
      if (error instanceof WatchError) throw new AdapterWriteError("ROW_CONFLICT: The Redis key changed. Reload before editing.", false);
      if (error instanceof AdapterExecutionError) throw error;
      throw new AdapterWriteError(errorText(error), dispatched);
    }
  }

  async listObjects(filter: ListObjectsFilter): Promise<ListObjectsData> {
    if (filter.kind !== undefined && filter.kind !== "key") throw new Error(`Redis does not support catalog kind "${filter.kind}".`);
    if (filter.schema !== undefined) throw new Error("Redis keys do not have schemas.");
    const cursor = filter.offset === undefined ? "0" : String(filter.offset);
    if (!/^\d+$/.test(cursor)) throw new Error("Redis offset must be an opaque numeric SCAN cursor.");
    const limit = boundedLimit(filter.limit);
    const args = [cursor];
    if (filter.search) args.push("MATCH", `*${escapeGlob(filter.search)}*`);
    args.push("COUNT", String(limit));
    const result = await this.read({ command: "SCAN", args });
    if (result.rows.length > MAX_COLLECTION_VALUES) throw new Error("Redis SCAN page exceeded the hard response bound; retry with a smaller COUNT.");
    return {
      objects: result.rows.map((row) => ({ kind: "key", name: String(row.key), identity: String(row.key) })),
      next_offset: result.nextCursor ?? null,
      supported_kinds: ["key"],
    };
  }

  async describeObject(object: CatalogObject): Promise<DescribeObjectData> {
    if (object.kind !== "key" || !object.name || object.schema !== undefined) throw new Error("Redis describeObject requires a key identity.");
    await this.connect();
    const key = object.name;
    const type = String(await this.execute(["TYPE", key], false));
    const ttl = Number(await this.execute(["PTTL", key], false));
    let definition: Record<string, unknown> = { type, ttl_ms: ttl };
    switch (type) {
      case "none": break;
      case "string": {
        const length = Number(await this.execute(["STRLEN", key], false));
        if (length > MAX_RESPONSE_BYTES) definition = { ...definition, length, value_omitted: true };
        else definition = { ...definition, length, value: await this.execute(["GET", key], false) };
        break;
      }
      case "hash": definition = { ...definition, ...(await this.scanValue("HSCAN", key)) }; break;
      case "list": {
        const length = Number(await this.execute(["LLEN", key], false));
        definition = { ...definition, length, values: await this.execute(["LRANGE", key, "0", "99"], false), truncated: length > 100 };
        break;
      }
      case "set": definition = { ...definition, ...(await this.scanValue("SSCAN", key)) }; break;
      case "zset": definition = { ...definition, ...(await this.scanValue("ZSCAN", key)) }; break;
      default: definition = { ...definition, value_omitted: true, reason: "Unsupported Redis value type." };
    }
    if (Buffer.byteLength(JSON.stringify(toJsonSafe(definition)), "utf8") > MAX_RESPONSE_BYTES) throw new Error("Redis value page exceeds 1 MiB.");
    return { object: { kind: "key", name: key, identity: key }, definition: toJsonSafe(definition) as Record<string, unknown> };
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.client.isOpen) this.client.destroy();
    this.connected = false;
  }

  private async scanValue(command: "HSCAN" | "SSCAN" | "ZSCAN", key: string): Promise<Record<string, unknown>> {
    const raw = await this.execute([command, key, "0", "COUNT", "100"], false) as unknown[];
    const cursor = String(raw[0]);
    return { values: toJsonSafe(raw[1]), next_cursor: cursor === "0" ? null : cursor, truncated: cursor !== "0" };
  }

  private async expirationIdentity(key: string): Promise<number> {
    const ttl = Number(await this.execute(["PTTL", key], false));
    return ttl >= 0 ? Date.now() + ttl : ttl;
  }


  private async keyFingerprint(key: string): Promise<string> {
    const usage = await this.execute(["MEMORY", "USAGE", key], false);
    if (typeof usage === "number" && usage > MAX_RESPONSE_BYTES) {
      throw new AdapterWriteError("Redis key is too large for guarded writes.", false);
    }
    const [type, dump] = await Promise.all([
      this.execute(["TYPE", key], false),
      withDeadline(
        this.client.sendCommand<Buffer | null>(["DUMP", key], { typeMapping: { [RESP_TYPES.BLOB_STRING]: Buffer } }),
        this.context,
        false,
        () => this.stop(),
      ),
    ]);
    const encoded = Buffer.isBuffer(dump) ? dump.toString("base64") : dump;
    if (Buffer.byteLength(JSON.stringify(encoded), "utf8") > MAX_RESPONSE_BYTES * 2) {
      throw new AdapterWriteError("Redis key is too large for guarded writes.", false);
    }
    return hash({ type, dump: encoded });
  }

  private async connect(): Promise<void> {
    throwIfStopped(this.context, false);
    if (this.closed) throw new Error("Redis adapter is closed.");
    if (this.connected) return;
    await withDeadline(this.client.connect(), this.context, false, () => this.stop());
    this.connected = true;
  }

  private async execute(command: string[], outcomeUnknown: boolean): Promise<unknown> {
    throwIfStopped(this.context, outcomeUnknown);
    return withDeadline(this.client.sendCommand(command), this.context, outcomeUnknown, () => this.stop());
  }

  private stop(): void { if (this.client.isOpen) this.client.destroy(); this.closed = true; }
}

function validateCommandShape(value: unknown): RedisCommand {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Redis command must be an object.");
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some((key) => key !== "command" && key !== "args")) throw new Error("Redis command contains unknown fields.");
  if (typeof record.command !== "string" || !/^[A-Za-z]+$/.test(record.command)) throw new Error("Redis command name is invalid.");
  const args = record.args ?? [];
  if (!Array.isArray(args) || !args.every((arg) => typeof arg === "string") || args.some((arg) => arg.includes("\0"))) throw new Error("Redis command arguments must be strings without NUL bytes.");
  if (args.length > MAX_ARGUMENTS + 4 || Buffer.byteLength(JSON.stringify(args), "utf8") > MAX_ARGUMENT_BYTES) throw new Error("Redis command arguments exceed safety bounds.");
  return { command: record.command.toUpperCase(), args: args as string[] };
}

function validateScanArgs(args: string[], keyed: boolean): void {
  const base = keyed ? 2 : 1;
  requireRange(args, base, base + 4);
  if (!/^\d+$/.test(args[keyed ? 1 : 0]!)) throw new Error("Redis SCAN cursor must be numeric.");
  for (let index = base; index < args.length; index += 2) {
    const option = args[index]?.toUpperCase();
    const value = args[index + 1];
    if (!value || (option !== "MATCH" && option !== "COUNT")) throw new Error("Redis SCAN only supports MATCH and COUNT.");
    if (option === "COUNT" && (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > MAX_COLLECTION_VALUES)) throw new Error(`Redis SCAN COUNT must be 1-${MAX_COLLECTION_VALUES}.`);
  }
}

function requireCount(args: string[], count: number): void { if (args.length !== count) throw new Error(`Redis command requires ${count} argument(s).`); }
function requireRange(args: string[], minimum: number, maximum: number): void { if (args.length < minimum || args.length > maximum) throw new Error(`Redis command requires ${minimum}-${maximum} arguments.`); }
function boundedRange(startText: string, stopText: string): void {
  const start = Number(startText); const stop = Number(stopText);
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(stop) || start < 0 || stop < start || stop - start >= MAX_COLLECTION_VALUES) throw new Error(`Redis LRANGE must request at most ${MAX_COLLECTION_VALUES} non-negative elements.`);
}
function boundedLimit(value: number | undefined): number { const limit = value ?? 50; if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_COLLECTION_VALUES) throw new Error(`limit must be 1-${MAX_COLLECTION_VALUES}.`); return limit; }
function escapeGlob(value: string): string { if (!value || value.length > 200 || value.includes("\0")) throw new Error("Redis search must be 1-200 characters."); return value.replace(/[\\*?\[\]]/g, "\\$&"); }

function redisRows(command: RedisCommand, raw: unknown): RedisReadResult {
  if (["SCAN", "HSCAN", "SSCAN", "ZSCAN"].includes(command.command)) {
    if (!Array.isArray(raw) || raw.length !== 2 || !Array.isArray(raw[1])) throw new Error("Unexpected Redis SCAN response.");
    const values = raw[1] as unknown[];
    const field = command.command === "SCAN" ? "key" : command.command === "HSCAN" ? "field" : "value";
    const rows = command.command === "HSCAN" || command.command === "ZSCAN"
      ? Array.from({ length: Math.ceil(values.length / 2) }, (_, index) => ({ [field]: values[index * 2], value: values[index * 2 + 1] }))
      : values.map((value) => ({ [field]: value }));
    const cursor = String(raw[0]);
    return { rows: toJsonSafe(rows), columns: inferColumns(rows), nextCursor: cursor === "0" ? null : cursor };
  }
  if (Array.isArray(raw)) {
    const rows = raw.map((value, index) => ({ index, value }));
    return { rows: toJsonSafe(rows), columns: inferColumns(rows) };
  }
  const rows = [{ value: raw }];
  return { rows: toJsonSafe(rows), columns: inferColumns(rows) };
}
function inferColumns(rows: Row[]): Column[] { const first = rows[0] ?? {}; return Object.keys(first).map((name) => ({ name, type: typeof first[name] })); }
function normalizeScalar(value: unknown): string | number | null { if (value === null || typeof value === "string" || typeof value === "number") return value; return JSON.stringify(toJsonSafe(value)); }
function redisAffectedRows(command: string, result: string | number | null): number { if (typeof result === "number") return result; return command === "SET" && result === "OK" ? 1 : 0; }
function remainingMilliseconds(context: AdapterContext): number { const remaining = context.deadline - Date.now(); if (remaining <= 0) throw new AdapterExecutionError("Database operation timed out.", "timeout", false); return Math.min(remaining, 2_147_483_647); }
function throwIfStopped(context: AdapterContext, outcomeUnknown: boolean): void { if (context.signal?.aborted) throw new AdapterExecutionError("Database operation was cancelled.", "aborted", outcomeUnknown); if (context.deadline <= Date.now()) throw new AdapterExecutionError("Database operation timed out.", "timeout", outcomeUnknown); }
async function withDeadline<T>(promise: Promise<T>, context: AdapterContext, outcomeUnknown: boolean, stop: () => void): Promise<T> {
  throwIfStopped(context, outcomeUnknown);
  let timer: NodeJS.Timeout | undefined;
  let abort: (() => void) | undefined;
  const stopped = new Promise<never>((_, reject) => {
    timer = setTimeout(() => { stop(); reject(new AdapterExecutionError("Database operation timed out.", "timeout", outcomeUnknown)); }, remainingMilliseconds(context));
    timer.unref();
    if (context.signal) { abort = () => { stop(); reject(new AdapterExecutionError("Database operation was cancelled.", "aborted", outcomeUnknown)); }; context.signal.addEventListener("abort", abort, { once: true }); }
  });
  try { return await Promise.race([promise, stopped]); }
  finally { if (timer) clearTimeout(timer); if (abort && context.signal) context.signal.removeEventListener("abort", abort); }
}
function errorText(error: unknown): string { return error instanceof Error ? error.message : String(error); }
