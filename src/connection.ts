import { resolve } from "node:path";
import { StateQLError } from "./errors.js";
import type { ConnectionRecord } from "./store.js";
import type { Driver, StateConfidence } from "./types.js";

export function databaseIdentity(connection: ConnectionRecord): unknown {
  return {
    driver: connection.driver,
    database: connection.database_name,
    source: connection.source,
    secretEnvironment: connection.secret_env,
  };
}

export function detectDriver(target: string): Driver {
  if (/^postgres(?:ql)?:\/\//i.test(target)) return "postgres";
  if (/^mysql:\/\//i.test(target)) return "mysql";
  if (/^[a-z][a-z\d+.-]*:\/\//i.test(target)) {
    throw new StateQLError(
      "UNSUPPORTED_DRIVER",
      "Only MySQL, PostgreSQL, and SQLite are supported.",
    );
  }
  return "sqlite";
}

export function normalizeSqliteSource(target: string): string {
  const source = target.startsWith("sqlite:") ? target.slice(7) : target;
  if (source === ":memory:") return source;
  return resolve(source);
}

export function databaseUrlHasSecret(target: string): boolean {
  try {
    const url = new URL(target);
    return (
      Boolean(url.password) ||
      [...url.searchParams.keys()].some((key) =>
        /pass|token|secret|private[_-]?key|api[_-]?key/i.test(key),
      )
    );
  } catch {
    throw new StateQLError("INVALID_COMMAND", "Invalid database URL.");
  }
}

export function version(connection: ConnectionRecord): string {
  return `sv_${connection.version}`;
}

export function confidence(connection: ConnectionRecord): StateConfidence {
  return connection.driver === "sqlite" ? "database_reported" : "ttl_based";
}

export function validateProfileName(name: string): void {
  if (/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(name)) return;
  throw new StateQLError(
    "INVALID_COMMAND",
    "Profile name must be 1-64 letters, numbers, dots, underscores, or hyphens.",
  );
}

export function isEnvironmentName(name: string): boolean {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}
