import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { env, platform } from "node:process";
import { StateQLError } from "./errors.js";
import type { Column, Row, SqlParameters } from "./types.js";

export function defaultHome(): string {
  if (env.STQL_HOME) return resolve(env.STQL_HOME);
  if (platform === "win32") {
    return join(env.LOCALAPPDATA ?? join(homedir(), "AppData", "Local"), "stql");
  }
  return join(env.XDG_DATA_HOME ?? join(homedir(), ".local", "share"), "stql");
}

export function hash(value: unknown): string {
  return createHash("sha256").update(stableStringify(value)).digest("hex");
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, sortValue(item)]),
  );
}

export function parseJson<T>(
  value: string,
  context: string,
  validate?: (parsed: unknown) => parsed is T,
): T {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value) as unknown;
  } catch {
    throw corruptedState(context);
  }
  if (validate && !validate(parsed)) throw corruptedState(context);
  return parsed as T;
}

export function isSqlParameters(value: unknown): value is SqlParameters {
  return Array.isArray(value) || isRecord(value);
}

export function isRows(value: unknown): value is Row[] {
  return Array.isArray(value) && value.every(isRecord);
}

export function isColumns(value: unknown): value is Column[] {
  return Array.isArray(value) && value.every((column) =>
    isRecord(column) &&
    typeof column.name === "string" &&
    typeof column.type === "string"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function corruptedState(context: string): StateQLError {
  return new StateQLError(
    "STATE_CORRUPTED",
    `Stored ${context} is corrupted.`,
    { suggestedAction: "Run stql doctor, then purge the affected data." },
  );
}

export function redact(value: string): string {
  try {
    const url = new URL(value);
    if (url.password) url.password = "***";
    if (url.username) url.username = "***";
    for (const key of url.searchParams.keys()) {
      if (/pass|token|secret|key/i.test(key)) url.searchParams.set(key, "***");
    }
    return url.toString();
  } catch {
    return value.replace(
      /(password|token|secret|api[_-]?key)\s*=\s*([^\s;&]+)/gi,
      "$1=***",
    );
  }
}

export function compactRows(
  rows: Row[],
  maxCellCharacters: number,
): Row[] {
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        compactValue(value, maxCellCharacters),
      ]),
    ),
  );
}

function compactValue(value: unknown, max: number): unknown {
  if (typeof value === "bigint") return value.toString();
  if (Buffer.isBuffer(value)) {
    return { type: "binary", length: value.length };
  }
  if (typeof value !== "string" || value.length <= max) return value;
  return {
    type: "text",
    length: value.length,
    preview: value.slice(0, max),
  };
}

export function toJsonSafe<T>(value: T): T {
  return JSON.parse(
    JSON.stringify(value, (_key, item: unknown) =>
      typeof item === "bigint" ? item.toString() : item,
    ),
  ) as T;
}
