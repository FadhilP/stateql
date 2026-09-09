import { resolve } from "node:path";
import { StateQLError } from "./errors.js";
import type { ConnectionRecord } from "./store.js";
import type { CredentialSource, Driver, StateConfidence } from "./types.js";

export function databaseIdentity(connection: ConnectionRecord): unknown {
  return {
    driver: connection.driver,
    database: connection.database_name,
    source: connection.source,
    secretEnvironment: connection.secret_env,
    credentialReference: connection.credential_ref,
    passwordReference: connection.password_ref,
  };
}

export function detectDriver(target: string): Driver {
  if (/^postgres(?:ql)?:\/\//i.test(target)) return "postgres";
  if (/^mysql:\/\//i.test(target)) return "mysql";
  if (/^mongodb(?:\+srv)?:\/\//i.test(target)) return "mongodb";
  if (/^rediss?:\/\//i.test(target)) return "redis";
  if (/^[a-z][a-z\d+.-]*:\/\//i.test(target)) {
    throw new StateQLError(
      "UNSUPPORTED_DRIVER",
      "Only MongoDB, MySQL, PostgreSQL, Redis, and SQLite are supported.",
    );
  }
  return "sqlite";
}
export function mongoDatabaseName(target: string): string {
  let pathname: string;
  try {
    const url = new URL(target);
    if (!["mongodb:", "mongodb+srv:"].includes(url.protocol.toLowerCase()) || !url.hostname) throw new Error();
    pathname = url.pathname;
  } catch {
    const match = /^mongodb(?:\+srv)?:\/\/([^/?#]+)(\/[^?#]*)?(?:[?#]|$)/i.exec(target);
    if (!match) throw new StateQLError("INVALID_COMMAND", "Invalid MongoDB URL.");
    const authority = match[1]!;
    const at = authority.lastIndexOf("@");
    validateMongoHosts(authority.slice(at + 1), /^mongodb\+srv:/i.test(target));
    pathname = match[2] ?? "";
  }
  try {
    const database = decodeURIComponent(pathname.replace(/^\//, ""));
    if (database && !database.includes("/") && !database.includes("\0")) return database;
  } catch {
    // Report malformed escaping as an invalid explicit database name.
  }
  throw new StateQLError(
    "INVALID_COMMAND",
    "MongoDB URL must include an explicit database name.",
  );
}

export function redisDatabaseName(target: string): string {
  try {
    const url = new URL(target);
    if (!url.hostname || !["redis:", "rediss:"].includes(url.protocol.toLowerCase())) throw new Error();
    const path = url.pathname.replace(/^\//, "");
    if (path && !/^\d+$/.test(path)) throw new Error();
    return `db${path || "0"}`;
  } catch {
    throw new StateQLError("INVALID_COMMAND", "Invalid Redis URL or database number.");
  }
}


export function credentialSource(
  value: string,
  expectedDriver?: Driver,
  referenceSource: Exclude<CredentialSource, "password_ref"> = "secret_env",
): { driver: Driver; source: string } {
  const sourceLabel = referenceSource === "credential_ref"
    ? "Credential reference"
    : "Secret environment variable";
  const explicitSqlite = /^sqlite:(?!\/\/)/i.test(value);
  const driver = explicitSqlite ? "sqlite" : detectDriver(value);
  if (driver === "sqlite" && (!explicitSqlite || value.length === 7)) {
    throw new StateQLError(
      "INVALID_COMMAND",
      `${sourceLabel} must contain a complete PostgreSQL/MySQL/Redis URL or an explicit sqlite: source; MongoDB URLs are also supported.`,
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
        `${sourceLabel} must contain a valid database URL.`,
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
    const match = /^([a-z][a-z\d+.-]*:\/\/)([^/?#]*)([\s\S]*)$/i.exec(target);
    if (!match) throw new Error();
    const authority = match[2]!;
    const at = authority.lastIndexOf("@");
    const url = /^mongodb(?:\+srv)?:\/\//i.test(target)
      ? new URL(`http://placeholder${match[3]}`)
      : new URL(target);
    return (
      (at >= 0 && authority.slice(0, at).includes(":")) ||
      Boolean(url.password) ||
      [...url.searchParams.keys()].some((key) =>
        /pass|token|secret|private[_-]?key|api[_-]?key/i.test(key),
      )
    );
  } catch {
    throw new StateQLError("INVALID_COMMAND", "Invalid database URL.");
  }
}

const AMBIGUOUS_PASSWORD_TARGET_PARAMETERS = new Set([
  "host",
  "hostaddr",
  "hostname",
  "port",
  "socket",
  "socketpath",
  "user",
  "username",
]);

export function validatePasswordReferenceTarget(target: string): Driver {
  if (/\s/.test(target)) {
    throw new StateQLError("INVALID_COMMAND", "Password-reference target must not contain whitespace.");
  }
  const driver = detectDriver(target);
  if (driver === "sqlite") {
    throw new StateQLError("INVALID_COMMAND", "Password references require a remote PostgreSQL, MySQL, MongoDB, or Redis target.");
  }

  const authorityMatch = /^([a-z][a-z\d+.-]*:\/\/)([^/?#]*)([\s\S]*)$/i.exec(target);
  if (!authorityMatch) {
    throw new StateQLError("INVALID_COMMAND", "Password-reference target must be a valid remote database URL.");
  }
  const authority = authorityMatch[2]!;
  const at = authority.lastIndexOf("@");
  const userinfo = at >= 0 ? authority.slice(0, at) : "";
  if (userinfo.includes(":") || userinfo.includes("@")) {
    throw new StateQLError("PERMISSION_DENIED", "Password-reference target must not contain an embedded password.");
  }
  if (/%(?![0-9a-f]{2})/i.test(userinfo)) {
    throw new StateQLError("INVALID_COMMAND", "Password-reference target contains malformed username escaping.");
  }
  try { if (userinfo) decodeURIComponent(userinfo); } catch {
    throw new StateQLError("INVALID_COMMAND", "Password-reference target contains malformed username escaping.");
  }

  let url: URL;
  try {
    if (driver === "mongodb") {
      validateMongoHosts(authority.slice(at + 1), /^mongodb\+srv:/i.test(target));
      url = new URL(`http://placeholder${authorityMatch[3]}`);
    } else {
      url = new URL(target);
      if (!url.hostname || url.password) throw new Error();
    }
  } catch {
    throw new StateQLError("INVALID_COMMAND", "Password-reference target must be a valid remote database URL.");
  }
  for (const key of url.searchParams.keys()) {
    const normalized = key.toLowerCase().replaceAll("_", "").replaceAll("-", "");
    if (
      AMBIGUOUS_PASSWORD_TARGET_PARAMETERS.has(normalized) ||
      /pass|token|secret|privatekey|apikey/.test(normalized)
    ) {
      throw new StateQLError(
        "PERMISSION_DENIED",
        "Password-reference target must not contain endpoint or credential query overrides.",
      );
    }
  }
  if (driver === "mongodb" && !userinfo) {
    throw new StateQLError("INVALID_COMMAND", "MongoDB password-reference targets require a username.");
  }
  return driver;
}

function validateMongoHosts(hosts: string, srv: boolean): void {
  const entries = hosts.split(",");
  if (!hosts || (srv && entries.length !== 1) || entries.some((host) => {
    if (srv) return !/^[^:[\],%]+$/u.test(host);
    return !(/^[^:[\],%]+(?::\d+)?$/u.test(host) || /^\[[0-9a-f:.]+\](?::\d+)?$/iu.test(host));
  })) {
    throw new StateQLError("INVALID_COMMAND", "Password-reference target must be a valid remote database URL.");
  }
}

/** Injects only password userinfo while retaining every nonsecret target byte. */
export function injectPassword(target: string, password: string): { driver: Driver; source: string } {
  const driver = validatePasswordReferenceTarget(target);
  let encoded: string;
  try {
    encoded = encodeURIComponent(password);
  } catch {
    throw new StateQLError("CREDENTIAL_RESOLUTION_FAILED", "Resolved password could not be encoded.");
  }
  const match = /^([a-z][a-z\d+.-]*:\/\/)([^/?#]*)([\s\S]*)$/i.exec(target)!;
  const authority = match[2]!;
  const at = authority.lastIndexOf("@");
  const injectedAuthority = at >= 0
    ? `${authority.slice(0, at)}:${encoded}${authority.slice(at)}`
    : `:${encoded}@${authority}`;
  return { driver, source: `${match[1]}${injectedAuthority}${match[3]}` };
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

export function validateCredentialRef(reference: unknown): asserts reference is string {
  if (
    typeof reference === "string" &&
    reference.trim().length > 0 &&
    reference.length <= 1_024
  ) return;
  throw new StateQLError(
    "INVALID_COMMAND",
    "Credential reference must be a nonempty string of at most 1024 characters.",
  );
}
