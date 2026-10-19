import { basename } from "node:path";
import {
  credentialSource,
  databaseUrlHasSecret,
  detectDriver,
  injectPassword,
  isEnvironmentName,
  mongoDatabaseName,
  normalizeSqliteSource,
  redisDatabaseName,
  validateCredentialRef,
  validatePasswordReferenceTarget,
} from "./connection.js";
import {
  closeAdapterQuietly,
  driverDisplayName,
  safeCredentialErrorMessage,
  type ActionResult,
  type Core,
} from "./core.js";
import { StateQLError } from "./errors.js";
import { AdapterExecutionError, stoppedStateQLError, type AdapterContext } from "./execution.js";
import type { ConnectionRecord, ProfileRecord, SessionRecord } from "./store.js";
import type { ConnectionData, ConnectOptions, CredentialSource, Driver } from "./types.js";
import { redact } from "./util.js";

interface ResolvedTarget {
  driver: Driver;
  adapterSource: string;
}

/** Resolves the target, pings the database, and stores the connection as active. */
export async function connect(
  core: Core,
  session: SessionRecord,
  target: string | undefined,
  options: ConnectOptions,
): Promise<ActionResult<ConnectionData>> {
  if (session.active_transaction_id) {
    throw new StateQLError(
      "TRANSACTION_FAILED",
      "Commit or roll back the active transaction before connecting again.",
    );
  }
  const sourceCount = [target, options.profile, options.secretEnv, options.credentialRef]
    .filter((value) => value !== undefined).length;
  if (sourceCount !== 1) {
    throw new StateQLError(
      "INVALID_COMMAND",
      "Use exactly one connection target, profile, secret environment variable, or credential reference.",
    );
  }
  const passwordWithoutTarget = target === undefined || options.profile !== undefined ||
    options.secretEnv !== undefined || options.credentialRef !== undefined;
  if (options.passwordRef !== undefined && passwordWithoutTarget) {
    throw new StateQLError(
      "INVALID_COMMAND",
      "A password reference requires a literal remote connection target.",
    );
  }
  const profile = connectionProfile(core, target, options);

  const resolvedTarget = profile?.target ?? target;
  const secretEnv = options.secretEnv ?? profile?.secret_env ?? undefined;
  const credentialRef = options.credentialRef ?? profile?.credential_ref ?? undefined;
  const passwordRef = options.passwordRef ?? profile?.password_ref ?? undefined;
  if (secretEnv !== undefined && !isEnvironmentName(secretEnv)) {
    throw new StateQLError("INVALID_COMMAND", "Secret environment variable name is invalid.");
  }
  if (credentialRef !== undefined) validateCredentialRef(credentialRef);
  if (passwordRef !== undefined) {
    validateCredentialRef(passwordRef);
    if (!resolvedTarget) {
      throw new StateQLError("INVALID_COMMAND", "A password reference requires a literal remote connection target.");
    }
    validatePasswordReferenceTarget(resolvedTarget);
  }
  const readOnly = options.readOnly ?? (profile ? Boolean(profile.read_only) : true);
  const context = core.executionContext(options);
  const credentialDetails = {
    ...(profile ? { profile: { name: profile.name } } : {}),
    requestedReadOnly: readOnly,
  };
  const access = readOnly ? "read" : "write";
  const reference = secretEnv ?? credentialRef;
  const referenceSource: Exclude<CredentialSource, "password_ref"> | undefined = secretEnv !== undefined
    ? "secret_env"
    : credentialRef !== undefined ? "credential_ref" : undefined;

  let resolved: ResolvedTarget;
  if (passwordRef !== undefined) {
    const password = await core.resolveCredential(
      passwordRef, "password_ref", session, "connect", access, context, credentialDetails, resolvedTarget!,
    );
    const injected = injectPassword(resolvedTarget!, password);
    resolved = { driver: injected.driver, adapterSource: injected.source };
  } else {
    const secret = reference && referenceSource
      ? await core.resolveCredential(reference, referenceSource, session, "connect", access, context, credentialDetails)
      : resolvedTarget;
    resolved = resolveSecretTarget(secret, referenceSource);
  }
  const { driver, adapterSource } = resolved;

  const persistedSource = passwordRef !== undefined
    ? resolvedTarget!
    : driver !== "sqlite" && referenceSource ? redact(adapterSource) : adapterSource;
  const identitySource = passwordRef !== undefined ? resolvedTarget! : adapterSource;
  const databaseName = connectionDatabaseName(driver, adapterSource, identitySource);
  const draft: ConnectionRecord = {
    id: "pending",
    session_id: session.id,
    name: options.name ?? profile?.name ?? databaseName,
    driver,
    database_name: databaseName,
    source: persistedSource,
    secret_env: secretEnv ?? null,
    credential_ref: credentialRef ?? null,
    password_ref: passwordRef ?? null,
    read_only: readOnly ? 1 : 0,
    version: 0,
    created_at: core.now().toISOString(),
  };
  await pingConnection(core, draft, context, adapterSource);

  const connection = core.store.addConnection({
    sessionId: session.id,
    actorId: core.actorId,
    name: draft.name,
    driver,
    databaseName,
    source: persistedSource,
    ...(secretEnv ? { secretEnv } : {}),
    ...(credentialRef ? { credentialRef } : {}),
    ...(passwordRef !== undefined ? { passwordRef } : {}),
    readOnly,
  });
  if (!connection) {
    throw new StateQLError("TRANSACTION_FAILED", "A transaction became active while changing the connection.");
  }
  const stateConfidence = driver === "sqlite" ? "database_reported" : "ttl_based";
  return {
    data: {
      connection_id: connection.id,
      alias: connection.alias!,
      display_alias: connection.alias!,
      driver,
      database: databaseName,
      name: connection.name,
      profile: profile?.name ?? null,
      read_only: readOnly,
      state_version: "sv_0",
      state_confidence: stateConfidence,
    },
    handle: connection.id,
    executed: true,
    stateVersion: "sv_0",
    confidence: stateConfidence,
  };
}

function connectionProfile(
  core: Core,
  target: string | undefined,
  options: ConnectOptions,
): ProfileRecord | undefined {
  const usesReference = Boolean(options.secretEnv || options.credentialRef) || options.passwordRef !== undefined;
  const implicitProfile = !options.profile && !usesReference && target
    ? core.store.getProfile(target)
    : undefined;
  const profile = options.profile ? core.store.getProfile(options.profile) : implicitProfile;
  if (options.profile && !profile) {
    throw new StateQLError(
      "CONNECTION_NOT_FOUND",
      `Profile "${options.profile}" was not found.`,
      { suggestedAction: "Run stql profile list." },
    );
  }
  if (!profile) return undefined;
  const sources = [profile.target, profile.secret_env, profile.credential_ref].filter((value) => value !== null);
  const passwordWithoutTarget = profile.password_ref !== null && profile.target === null;
  if (sources.length !== 1 || passwordWithoutTarget) {
    throw new StateQLError("STATE_CORRUPTED", `Profile "${profile.name}" has an invalid connection source.`);
  }
  return profile;
}

function resolveSecretTarget(
  secret: string | undefined,
  referenceSource: Exclude<CredentialSource, "password_ref"> | undefined,
): ResolvedTarget {
  if (!secret) throw new StateQLError("INVALID_COMMAND", "Connection target is required.");
  if (referenceSource) {
    const resolved = credentialSource(secret, undefined, referenceSource);
    return { driver: resolved.driver, adapterSource: resolved.source };
  }
  const driver = detectDriver(secret);
  if (driver === "sqlite") return { driver, adapterSource: normalizeSqliteSource(secret) };
  if (databaseUrlHasSecret(secret)) {
    throw new StateQLError(
      "PERMISSION_DENIED",
      `Credential-bearing ${driverDisplayName(driver)} URLs must use --env or --credential-ref.`,
      { suggestedAction: "Use an environment variable or trusted host credential reference." },
    );
  }
  return { driver, adapterSource: secret };
}

function connectionDatabaseName(driver: Driver, adapterSource: string, identitySource: string): string {
  switch (driver) {
    case "sqlite": return basename(adapterSource);
    case "mongodb": return mongoDatabaseName(identitySource);
    case "redis": return redisDatabaseName(identitySource);
    default: return new URL(identitySource).pathname.replace(/^\//, "") || driver;
  }
}

async function pingConnection(
  core: Core,
  draft: ConnectionRecord,
  context: AdapterContext,
  source: string,
): Promise<void> {
  const adapter = await core.openAdapter(draft, context, source);
  try {
    await adapter.ping();
  } catch (error) {
    if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, false);
    throw new StateQLError("CONNECTION_FAILED", safeCredentialErrorMessage(error, source), { retryable: true });
  } finally {
    await closeAdapterQuietly(adapter);
  }
}

export function validatedProfileSource(input: {
  target?: string;
  secretEnv?: string;
  credentialRef?: string;
  passwordRef?: string;
}): {
  target: string | null;
  secretEnv: string | null;
  credentialRef: string | null;
  passwordRef: string | null;
} {
  const sourceCount = [input.target, input.secretEnv, input.credentialRef].filter((value) => value !== undefined).length;
  const emptySource = input.target === "" || input.secretEnv === "" || input.credentialRef === "";
  if (sourceCount !== 1 || emptySource) {
    throw new StateQLError("INVALID_COMMAND", "Profile requires exactly one target, secret environment variable, or credential reference.");
  }
  if (input.secretEnv !== undefined && !isEnvironmentName(input.secretEnv)) throw new StateQLError("INVALID_COMMAND", "Secret environment variable name is invalid.");
  if (input.credentialRef !== undefined) validateCredentialRef(input.credentialRef);
  if (input.passwordRef !== undefined) validateCredentialRef(input.passwordRef);
  let target = input.target ?? null;
  if (input.passwordRef !== undefined && target === null) {
    throw new StateQLError("INVALID_COMMAND", "A password reference requires a literal remote profile target.");
  }
  if (target) {
    const driver = input.passwordRef !== undefined
      ? validatePasswordReferenceTarget(target)
      : detectDriver(target);
    if (input.passwordRef === undefined && driver !== "sqlite" && databaseUrlHasSecret(target)) {
      throw new StateQLError("PERMISSION_DENIED", `Credential-bearing ${driverDisplayName(driver)} URLs must use --env or --credential-ref.`);
    }
    if (driver === "sqlite") target = normalizeSqliteSource(target);
  }
  return {
    target,
    secretEnv: input.secretEnv ?? null,
    credentialRef: input.credentialRef ?? null,
    passwordRef: input.passwordRef ?? null,
  };
}
