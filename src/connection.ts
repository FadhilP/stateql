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

export function credentialSource(
  value: string,
  expectedDriver?: Driver,
): { driver: Driver; source: string } {
  const explicitSqlite = /^sqlite:(?!\/\/)/i.test(value);
  const driver = explicitSqlite ? "sqlite" : detectDriver(value);
  if (driver === "sqlite" && (!explicitSqlite || value.length === 7)) {
    throw new StateQLError(
      "INVALID_COMMAND",
      "Secret environment variable must contain a complete PostgreSQL/MySQL URL or an explicit sqlite: source.",
      {
        suggestedAction:
          "Store the full database URL, or prefix an SQLite path with sqlite:.",
      },
    );
  }
  if (driver !== "sqlite") {
    try {
      const url = new URL(value);
      if (!url.hostname && !url.pathname.replaceAll("/", "")) throw new Error();
    } catch {
      throw new StateQLError(
        "INVALID_COMMAND",
        "Secret environment variable must contain a valid database URL.",
      );
    }
  }
  if (expectedDriver && driver !== expectedDriver) {
    throw new StateQLError(
      "INVALID_COMMAND",
      "Resolved credential driver does not match the selected database connection.",
    );
  }
  return {
    driver,
    source: driver === "sqlite" ? normalizeSqliteSource(value) : value,
  };
}

export function normalizeSqliteSource(target: string): string {
  const source = target.replace(/^sqlite:/i, "");
  if (source === ":memory:") {
    throw new StateQLError(
      "INVALID_COMMAND",
      "SQLite :memory: databases cannot persist across StateQL commands.",
    );
  }
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
