import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { compileTableUpdate, editableRow, parseTableUpdate, parseTableUpdates, type EditableTable, type TableIdentity, type TableChange, type TableUpdate } from "./table-editor.js";
import { writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { env } from "node:process";
import {
  AdapterExecutionError,
  AdapterWriteError,
  BatchWriteError,
  createAdapter,
  createAdapterContext,
  type Adapter,
  type AdapterContext,
  type BatchWriteOperation,
} from "./adapters.js";
import {
  confidence,
  credentialSource,
  databaseIdentity,
  databaseUrlHasSecret,
  detectDriver,
  isEnvironmentName,
  injectPassword,
  mongoDatabaseName,
  redisDatabaseName,
  normalizeSqliteSource,
  validateCredentialRef,
  validatePasswordReferenceTarget,
  validateProfileName,
  version,
} from "./connection.js";
import {
  asStateQLError,
  CredentialResolutionError,
  StateQLError,
} from "./errors.js";
import {
  analyzeMongoWriteSafety,
  deserializeMongoWriteCommand,
  serializeMongoCommand,
  validateMongoReadCommand,
  validateMongoWriteCommand,
  MongoAdapter,
} from "./mongodb.js";
import {
  deserializeRedisCommand,
  RedisAdapter,
  serializeRedisCommand,
  validateRedisReadCommand,
  validateRedisWriteCommand,
  type RedisPrecondition,
} from "./redis.js";
import {
  filterMaterializedRows,
  prepareFilterStatement,
  validateFilterParameters,
} from "./filter.js";
import {
  operationData,
  paginationWarnings,
  profileData,
  rowsToCsv,
  sessionData,
  transactionData,
} from "./response-data.js";
import { analyzeSql } from "./sql.js";
import {
  StateStore,
  type ConnectionRecord,
  type HistoryRecord,
  type OperationRecord,
  type ResultRecord,
  type SessionRecord,
} from "./store.js";
import type {
  ActorLinkData,
  ActorResolutionData,
  ActorsData,
  ActorUnlinkData,
  AliasData,
  ApplyData,
  BatchCommand,
  CommandExecutionContext,
  CommandOrigin,
  BatchOptions,
  CapabilitiesData,
  CatalogObject,
  DescribeObjectData,
  ListObjectsData,
  ListObjectsFilter,
  CloseSessionData,
  ColumnsData,
  CommitTransactionData,
  ConnectOptions,
  ConnectionData,
  CountData,
  CredentialAccess,
  CredentialOperation,
  CredentialSource,
  CredentialRequest,
  CredentialResolver,
  DisconnectData,
  DoctorData,
  Driver,
  ExecData,
  ExecOptions,
  ExecutionOptions,
  ExportData,
  Failure,
  FilterOptions,
  HistoryData,
  HistoryEntry,
  HistoryOptions,
  HistoryCategory,
  OperationData,
  PlanData,
  PlanOptions,
  ProfileData,
  ProfilesData,
  ProfileOptions,
  ProfileUpdateOptions,
  MongoExecOptions,
  MongoPlanOptions,
  MongoQueryOptions,
  MongoReadCommand,
  MongoWriteCommand,
  MongoWriteOutcome,
  RedisCommand,
  RedisExecOptions,
  RedisPlanOptions,
  RedisQueryOptions,
  PurgeData,
  QueryOptions,
  RemovedProfileData,
  Response,
  ResultData,
  RollbackTransactionData,
  RowsData,
  RowsOptions,
  Row,
  Column,
  SqlParameters,
  StateConfidence,
  StateQLActorOptions,
  StateQLOptions,
  StateQLWorkspaceOptions,
  StateQLSnapshot,
  StateQLSnapshotOptions,
  StatusData,
  Success,
  SessionData,
  SessionsData,
  SessionSummaryData,
  TransactionData,
  Warning,
} from "./types.js";
import {
  compactRows,
  defaultHome,
  hash,
  isSqlParameters,
  parseJson,
  redact,
} from "./util.js";

const DEFAULT_SNAPSHOT_HISTORY_LIMIT = 50;
const MAX_SNAPSHOT_HISTORY_LIMIT = 100;
const DEFAULT_CREDENTIAL_RESOLUTION_TIMEOUT_MS = 120_000;
const WORKSPACE_BOOTSTRAP = Symbol("StateQL.workspaceBootstrap");

type StateQLInternalOptions = StateQLOptions & {
  [WORKSPACE_BOOTSTRAP]?: true;
};

interface ActionResult<T> {
  data: T;
  handle?: string;
  executed?: boolean;
  cached?: boolean;
  warnings?: Warning[];
  stateVersion?: string;
  confidence?: StateConfidence;
  session?: SessionRecord;
}

export class StateQL {
  /** Runtime contract marker for passwordRef/password_ref support. */
  static readonly passwordReferenceVersion = 1 as const;

  static forActor(options: StateQLActorOptions): StateQL {
    if (!options.actor.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Actor ID is required.");
    }
    const now = options.now ?? (() => new Date());
    const store = new StateStore(options.home ?? defaultHome(), now);
    try {
      const session = store.resolveActor(options.actor);
      if (session) return new StateQL({ ...options, session: session.name });
      const { actor, ...legacyOptions } = options;
      return new StateQL({ ...legacyOptions, session: actor });
    } finally {
      store.close();
    }
  }

  /** Opens one actor in a named shared workspace for a trusted library host. */
  static forWorkspace(options: StateQLWorkspaceOptions): StateQL {
    if (!options.workspace.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Workspace name is required.");
    }
    if (!options.actor.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Actor ID is required.");
    }
    const { workspace, ...actorOptions } = options;
    return new StateQL({
      ...actorOptions,
      session: workspace,
      [WORKSPACE_BOOTSTRAP]: true,
    } as StateQLInternalOptions);
  }

  private readonly store: StateStore;
  private readonly sessionName: string;
  private readonly actorId: string;
  private readonly previewRows: number;
  private readonly cacheTtlSeconds: number;
  private readonly resultTtlSeconds: number;
  private readonly maxCellCharacters: number;
  private readonly maxResultRows: number;
  private readonly maxResultBytes: number;
  private readonly timeoutMs: number;
  private readonly credentialTimeoutMs: number;
  private readonly signal?: AbortSignal;
  private readonly commandContexts = new AsyncLocalStorage<CommandExecutionContext>();
  private readonly credentialResolver?: CredentialResolver;
  private readonly now: () => Date;
  private closed = false;
  private readonly tableResults = new Map<string, EditableTable>();
  private readonly editTokens = new Map<string, { metadata: EditableTable; original: Row; sessionId: string; connectionId: string; stateVersion: string; expires: number }>();
  // ponytail: cache one bounded immutable result; use indexed result storage if larger results are needed.
  private panelRows?: { id: string; json: string; rows: Row[] };

  constructor(options: StateQLOptions = {}) {
    this.now = options.now ?? (() => new Date());
    this.sessionName = options.session ?? env.STQL_SESSION ?? "default";
    this.actorId = options.actor ?? this.sessionName;
    if (!this.actorId.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Actor ID is required.");
    }
    this.previewRows = nonNegativeInteger(
      options.previewRows ?? 5,
      "previewRows",
    );
    this.cacheTtlSeconds = nonNegativeInteger(
      options.cacheTtlSeconds ?? 300,
      "cacheTtlSeconds",
    );
    this.resultTtlSeconds = positiveInteger(
      options.resultTtlSeconds ?? 86_400,
      "resultTtlSeconds",
    );
    this.maxCellCharacters = positiveInteger(
      options.maxCellCharacters ?? 200,
      "maxCellCharacters",
    );
    this.maxResultRows = positiveInteger(
      options.maxResultRows ?? 10_000,
      "maxResultRows",
    );
    this.maxResultBytes = positiveInteger(
      options.maxResultBytes ?? 16 * 1024 * 1024,
      "maxResultBytes",
    );
    this.timeoutMs = executionTimeout(options.timeoutMs ?? 30_000);
    this.credentialTimeoutMs = executionTimeout(
      options.credentialTimeoutMs ?? DEFAULT_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
      "credentialTimeoutMs",
    );
    const maxStateBytes = positiveInteger(
      options.maxStateBytes ?? 256 * 1024 * 1024,
      "maxStateBytes",
    );
    this.signal = options.signal;
    this.credentialResolver = options.credentialResolver;
    if (this.maxResultRows >= Number.MAX_SAFE_INTEGER) {
      throw new StateQLError(
        "INVALID_COMMAND",
        "maxResultRows is too large.",
      );
    }
    const store = new StateStore(
      options.home ?? defaultHome(),
      this.now,
      maxStateBytes,
    );
    try {
      if ((options as StateQLInternalOptions)[WORKSPACE_BOOTSTRAP]) {
        store.bootstrapWorkspace(this.sessionName, this.actorId);
      } else {
        store.bootstrapSession(
          this.sessionName,
          this.actorId,
          options.actor === undefined,
        );
      }
    } catch (error) {
      store.close();
      throw error;
    }
    this.store = store;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.panelRows = undefined;
    this.tableResults.clear();
    this.editTokens.clear();
    this.store.close();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  async connect(
    target?: string,
    options: ConnectOptions = {},
  ): Promise<Response<ConnectionData>> {
    return this.run("connect", async (session) => {
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Commit or roll back the active transaction before connecting again.",
        );
      }
      const sourceCount = [
        target,
        options.profile,
        options.secretEnv,
        options.credentialRef,
      ].filter((value) => value !== undefined).length;
      if (sourceCount !== 1) {
        throw new StateQLError(
          "INVALID_COMMAND",
          "Use exactly one connection target, profile, secret environment variable, or credential reference.",
        );
      }
      if (
        options.passwordRef !== undefined &&
        (target === undefined || options.profile !== undefined ||
          options.secretEnv !== undefined || options.credentialRef !== undefined)
      ) {
        throw new StateQLError(
          "INVALID_COMMAND",
          "A password reference requires a literal remote connection target.",
        );
      }
      const implicitProfile =
        !options.profile && !options.secretEnv && !options.credentialRef &&
          options.passwordRef === undefined && target
          ? this.store.getProfile(target)
          : undefined;
      const profile = options.profile
        ? this.store.getProfile(options.profile)
        : implicitProfile;
      if (options.profile && !profile) {
        throw new StateQLError(
          "CONNECTION_NOT_FOUND",
          `Profile "${options.profile}" was not found.`,
          { suggestedAction: "Run stql profile list." },
        );
      }
      if (
        profile &&
        ([profile.target, profile.secret_env, profile.credential_ref]
          .filter((value) => value !== null).length !== 1 ||
          (profile.password_ref !== null && profile.target === null))
      ) {
        throw new StateQLError(
          "STATE_CORRUPTED",
          `Profile "${profile.name}" has an invalid connection source.`,
        );
      }

      const resolvedTarget = profile?.target ?? target;
      const secretEnv = options.secretEnv ?? profile?.secret_env ?? undefined;
      const credentialRef = options.credentialRef ?? profile?.credential_ref ?? undefined;
      const passwordRef = options.passwordRef ?? profile?.password_ref ?? undefined;
      if (secretEnv !== undefined && !isEnvironmentName(secretEnv)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          "Secret environment variable name is invalid.",
        );
      }
      if (credentialRef !== undefined) validateCredentialRef(credentialRef);
      if (passwordRef !== undefined) {
        validateCredentialRef(passwordRef);
        if (!resolvedTarget) {
          throw new StateQLError("INVALID_COMMAND", "A password reference requires a literal remote connection target.");
        }
        validatePasswordReferenceTarget(resolvedTarget);
      }
      const credentialReference = secretEnv ?? credentialRef;
      const credentialReferenceSource: Exclude<CredentialSource, "password_ref"> | undefined = secretEnv !== undefined
        ? "secret_env"
        : credentialRef !== undefined ? "credential_ref" : undefined;
      const readOnly =
        options.readOnly ??
        (profile ? Boolean(profile.read_only) : true);
      const context = this.executionContext(options);
      let adapterSource: string;
      let driver: Driver;
      if (passwordRef !== undefined) {
        const password = await this.resolveCredential(
          passwordRef,
          "password_ref",
          session,
          "connect",
          readOnly ? "read" : "write",
          context,
          {
            ...(profile ? { profile: { name: profile.name } } : {}),
            requestedReadOnly: readOnly,
          },
          resolvedTarget!,
        );
        ({ driver, source: adapterSource } = injectPassword(resolvedTarget!, password));
      } else {
        const secret = credentialReference && credentialReferenceSource
          ? await this.resolveCredential(
              credentialReference,
              credentialReferenceSource,
              session,
              "connect",
              readOnly ? "read" : "write",
              context,
              {
                ...(profile ? { profile: { name: profile.name } } : {}),
                requestedReadOnly: readOnly,
              },
            )
          : resolvedTarget;
        if (!secret) {
          throw new StateQLError("INVALID_COMMAND", "Connection target is required.");
        }
        const resolvedSource = credentialReferenceSource
          ? credentialSource(secret, undefined, credentialReferenceSource)
          : { driver: detectDriver(secret), source: secret };
        driver = resolvedSource.driver;
        if (
          driver !== "sqlite" &&
          !credentialReference &&
          databaseUrlHasSecret(secret)
        ) {
          throw new StateQLError(
            "PERMISSION_DENIED",
            `Credential-bearing ${databaseDisplayName(driver)} URLs must use --env or --credential-ref.`,
            {
              suggestedAction:
                "Use an environment variable or trusted host credential reference.",
            },
          );
        }
        adapterSource = credentialReferenceSource
          ? resolvedSource.source
          : driver === "sqlite" ? normalizeSqliteSource(secret) : secret;
      }

      const persistedSource = passwordRef !== undefined
        ? resolvedTarget!
        : driver === "sqlite"
          ? adapterSource
          : credentialReferenceSource
            ? redact(adapterSource)
            : adapterSource;
      const identitySource = passwordRef !== undefined ? resolvedTarget! : adapterSource;
      const databaseName =
        driver === "sqlite"
          ? basename(adapterSource)
          : driver === "mongodb"
            ? mongoDatabaseName(identitySource)
            : driver === "redis"
              ? redisDatabaseName(identitySource)
              : new URL(identitySource).pathname.replace(/^\//, "") || driver;
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
        created_at: this.now().toISOString(),
      };

      const adapter = driver === "mongodb"
        ? await this.openMongoAdapter(draft, context, adapterSource)
        : driver === "redis"
          ? await this.openRedisAdapter(draft, context, adapterSource)
          : await this.openAdapter(draft, context, adapterSource);
      try {
        await adapter.ping();
      } catch (error) {
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, false);
        }
        throw new StateQLError(
          "CONNECTION_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          { retryable: true },
        );
      } finally {
        await closeAdapterQuietly(adapter);
      }

      const connection = this.store.addConnection({
        sessionId: session.id,
        actorId: this.actorId,
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
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "A transaction became active while changing the connection.",
        );
      }
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
          state_confidence: driver === "sqlite" ? "database_reported" : "ttl_based",
        },
        handle: connection.id,
        executed: true,
        stateVersion: "sv_0",
        confidence:
          driver === "sqlite" ? "database_reported" : "ttl_based",
      };
    });
  }

  async addProfile(
    name: string,
    target?: string,
    options: ProfileOptions = {},
  ): Promise<Response<ProfileData>> {
    return this.run("profile.add", async () => {
      validateProfileName(name);
      if (this.store.getProfile(name)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          `Profile "${name}" already exists.`,
        );
      }
      const source = validatedProfileSource({
        target,
        secretEnv: options.secretEnv,
        credentialRef: options.credentialRef,
        passwordRef: options.passwordRef,
      });

      const profile = this.store.addProfile({
        name,
        target: source.target ?? undefined,
        secretEnv: source.secretEnv ?? undefined,
        credentialRef: source.credentialRef ?? undefined,
        passwordRef: source.passwordRef ?? undefined,
        readOnly: options.readOnly ?? true,
      });
      return {
        data: profileData(profile),
        handle: `profile:${profile.name}`,
        executed: true,
      };
    });
  }

  async updateProfile(
    name: string,
    changes: ProfileUpdateOptions,
  ): Promise<Response<ProfileData>> {
    return this.run("profile.update", async () => {
      validateProfileName(name);
      const existing = this.store.getProfile(name);
      if (!existing) throw new StateQLError("CONNECTION_NOT_FOUND", `Profile "${name}" was not found.`);
      if (!changes || typeof changes !== "object" || Array.isArray(changes) ||
        Object.keys(changes).some((key) => !["target", "secretEnv", "credentialRef", "passwordRef", "readOnly"].includes(key))) {
        throw new StateQLError("INVALID_COMMAND", "Profile update contains unknown fields.");
      }
      if (changes.readOnly !== undefined && typeof changes.readOnly !== "boolean") throw new StateQLError("INVALID_COMMAND", "Profile readOnly must be boolean.");
      const changesSource = Object.hasOwn(changes, "target") || Object.hasOwn(changes, "secretEnv") || Object.hasOwn(changes, "credentialRef");
      const changesPassword = Object.hasOwn(changes, "passwordRef");
      if (!changesSource && !changesPassword && changes.readOnly === undefined) throw new StateQLError("INVALID_COMMAND", "Profile update has no changes.");
      const targetSource = changesSource ? changes.target ?? undefined : existing.target ?? undefined;
      const source = validatedProfileSource({
        target: targetSource,
        secretEnv: changesSource ? changes.secretEnv ?? undefined : existing.secret_env ?? undefined,
        credentialRef: changesSource ? changes.credentialRef ?? undefined : existing.credential_ref ?? undefined,
        passwordRef: changesPassword
          ? changes.passwordRef ?? undefined
          : !changesSource || targetSource === existing.target
            ? existing.password_ref ?? undefined
            : undefined,
      });
      const profile = this.store.updateProfile({
        name,
        target: source.target,
        secretEnv: source.secretEnv,
        credentialRef: source.credentialRef,
        passwordRef: source.passwordRef,
        readOnly: changes.readOnly ?? Boolean(existing.read_only),
      });
      if (!profile) throw new StateQLError("CONNECTION_NOT_FOUND", `Profile "${name}" was not found.`);
      return { data: profileData(profile), handle: `profile:${name}`, executed: true };
    });
  }


  async listProfiles(): Promise<Response<ProfilesData>> {
    return this.run("profile.list", async () => ({
      data: { profiles: this.store.listProfiles().map(profileData) },
    }));
  }

  async showProfile(name: string): Promise<Response<ProfileData>> {
    return this.run("profile.show", async () => {
      const profile = this.store.getProfile(name);
      if (!profile) {
        throw new StateQLError(
          "CONNECTION_NOT_FOUND",
          `Profile "${name}" was not found.`,
        );
      }
      return {
        data: profileData(profile),
        handle: `profile:${profile.name}`,
      };
    });
  }

  async removeProfile(name: string): Promise<Response<RemovedProfileData>> {
    return this.run("profile.remove", async () => {
      if (!this.store.removeProfile(name)) {
        throw new StateQLError(
          "CONNECTION_NOT_FOUND",
          `Profile "${name}" was not found.`,
        );
      }
      return {
        data: { profile: name, removed: true },
        handle: `profile:${name}`,
        executed: true,
      };
    });
  }

  async disconnect(): Promise<Response<DisconnectData>> {
    return this.run("disconnect", async (session) => {
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Commit or roll back the active transaction before disconnecting.",
        );
      }
      if (!this.store.disconnect(session.id, this.actorId)) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "A transaction became active while disconnecting.",
        );
      }
      return { data: { disconnected: true }, executed: true };
    });
  }

  snapshot(options: StateQLSnapshotOptions = {}): StateQLSnapshot {
    const session = this.store
      .listSessions()
      .find((candidate) => candidate.name === this.sessionName);
    if (!session) {
      throw new StateQLError("INVALID_COMMAND", "The active session was not found.");
    }
    if (!this.store.isSessionMember(session.id, this.actorId)) {
      throw new StateQLError(
        "PERMISSION_DENIED",
        `Actor "${this.actorId}" is not attached to session "${session.name}".`,
      );
    }
    const connection = this.store.activeConnection(session);
    const transaction = session.active_transaction_id
      ? this.store.getTransaction(session.active_transaction_id)
      : undefined;
    const historyLimit = positiveInteger(
      options.historyLimit ?? DEFAULT_SNAPSHOT_HISTORY_LIMIT,
      "historyLimit",
    );
    if (historyLimit > MAX_SNAPSHOT_HISTORY_LIMIT) {
      throw new StateQLError(
        "INVALID_COMMAND",
        `historyLimit cannot exceed ${MAX_SNAPSHOT_HISTORY_LIMIT}.`,
      );
    }
    if (options.historyInternal !== undefined && typeof options.historyInternal !== "boolean") {
      throw new StateQLError("INVALID_COMMAND", "Snapshot historyInternal filter must be boolean.");
    }
    const historyOptions = {
      ...(options.historyCategory === undefined ? {} : { category: parseHistoryCategory(options.historyCategory) }),
      ...(options.historyInternal === undefined ? {} : { internal: options.historyInternal }),
    };

    return {
      session: {
        session_id: session.id,
        name: session.name,
        status: session.status,
      },
      actor_id: this.actorId,
      connection: connection
        ? {
            connection_id: connection.id,
            alias: connection.alias!,
            display_alias: connection.alias!,
            name: connection.name,
            status: "connected",
            driver: connection.driver,
            database: connection.database_name,
            read_only: Boolean(connection.read_only),
          }
        : null,
      transaction: transaction
        ? {
            transaction_id: transaction.id,
            owner_actor_id: transaction.owner_actor_id,
            state: transaction.state,
          }
        : null,
      state_version: connection ? version(connection) : null,
      state_confidence: connection ? confidence(connection) : null,
      recent_results: this.store.knownResults(session.id, 10).map((result) => ({
        alias: result.alias,
        handle: result.id,
        rows: result.row_count,
      })),
      recent_operations: this.store
        .recentOperations(session.id, 10)
        .map((operation) => ({
          handle: operation.id,
          actor_id: operation.actor_id,
          type: operation.statement_type,
          affected_rows: operation.affected_rows,
          status: operation.status,
        })),
      history: this.store
        .history(session.id, historyLimit, historyOptions)
        .map(historyEntry),
    };
  }

  async status(): Promise<Response<StatusData>> {
    return this.run("status", async (session) => {
      const connection = this.store.activeConnection(session);
      const transaction = session.active_transaction_id
        ? this.store.getTransaction(session.active_transaction_id)
        : undefined;
      return {
        data: {
          session_id: session.id,
          session_name: session.name,
          actor_id: this.actorId,
          connection: connection
            ? {
                connection_id: connection.id,
                name: connection.name,
                driver: connection.driver,
                database: connection.database_name,
                read_only: Boolean(connection.read_only),
              }
            : null,
          transaction: transaction
            ? {
                transaction_id: transaction.id,
                owner_actor_id: transaction.owner_actor_id,
                state: transaction.state,
              }
            : null,
          state_version: connection ? version(connection) : null,
        },
        stateVersion: connection ? version(connection) : undefined,
        confidence: connection ? confidence(connection) : undefined,
      };
    });
  }

  async linkActor(
    session: string,
    actorId: string,
  ): Promise<Response<ActorLinkData>> {
    return this.run("actor.link", async (current) => {
      this.requireSelectedSession(current, session);
      this.validateActorId(actorId);
      const result = this.store.linkActor(current.id, this.actorId, actorId);
      if (result === "actor_conflict") {
        throw new StateQLError(
          "PERMISSION_DENIED",
          `Actor "${actorId}" is already attached to another session.`,
        );
      }
      if (result === "denied") this.throwMembershipDenied(current);
      return {
        data: {
          session_id: current.id,
          actor_id: actorId,
          linked: result === "linked",
        },
        executed: result === "linked",
      };
    });
  }

  async unlinkActor(
    session: string,
    actorId: string,
  ): Promise<Response<ActorUnlinkData>> {
    return this.run("actor.unlink", async (current) => {
      this.requireSelectedSession(current, session);
      this.validateActorId(actorId);
      const result = this.store.unlinkActor(current.id, this.actorId, actorId);
      if (result === "owns_transaction") {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          `Actor "${actorId}" owns the active transaction.`,
        );
      }
      if (result === "denied") this.throwMembershipDenied(current);
      return {
        data: {
          session_id: current.id,
          actor_id: actorId,
          unlinked: result === "unlinked",
        },
        executed: result === "unlinked",
      };
    });
  }

  async listActors(session: string): Promise<Response<ActorsData>> {
    return this.run("actor.list", async (current) => {
      this.requireSelectedSession(current, session);
      return {
        data: {
          session_id: current.id,
          actors: this.store.listActors(current.id).map((member) => ({
            actor_id: member.actor_id,
            attached_at: member.attached_at,
          })),
        },
      };
    });
  }

  async resolveActor(actorId: string): Promise<Response<ActorResolutionData>> {
    return this.run("actor.resolve", async () => {
      this.validateActorId(actorId);
      const session = this.store.resolveActor(actorId);
      return {
        data: {
          actor_id: actorId,
          session: session
            ? {
                session_id: session.id,
                name: session.name,
                status: session.status,
              }
            : null,
        },
      };
    });
  }

  async startSession(name: string): Promise<Response<SessionData>> {
    return this.run("session.start", async () => {
      if (!name.trim()) {
        throw new StateQLError("INVALID_COMMAND", "Session name is required.");
      }
      if (this.store.getSessionByName(name)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          `Active session "${name}" already exists.`,
        );
      }
      const session = this.store.bootstrapSession(name, name, true);
      return {
        data: sessionData(session),
        handle: session.id,
        executed: true,
        session,
      };
    });
  }

  async listSessions(): Promise<Response<SessionsData>> {
    return this.run("session.list", async () => ({
      data: {
        sessions: this.store.listSessions().map((session) => ({
          session_id: session.id,
          name: session.name,
          status: session.status,
          active_connection: session.active_connection_id,
          active_transaction: session.active_transaction_id,
        })),
      },
    }));
  }

  async showSession(idOrName = this.sessionName): Promise<Response<SessionData>> {
    return this.run("session.show", async () => {
      const session = this.store.getSession(idOrName);
      if (!session) {
        throw new StateQLError(
          "INVALID_COMMAND",
          `Session "${idOrName}" was not found.`,
        );
      }
      return { data: sessionData(session), session };
    });
  }

  async sessionSummary(): Promise<Response<SessionSummaryData>> {
    return this.run("session.summary", async (session) => {
      const connection = this.store.activeConnection(session);
      return {
        data: {
          session_id: session.id,
          name: session.name,
          connection: connection?.name ?? null,
          state_version: connection ? version(connection) : null,
          transaction: session.active_transaction_id,
          known_results: this.store.knownResults(session.id, 10).map((result) => ({
            alias: result.alias,
            handle: result.id,
            rows: result.row_count,
          })),
          recent_operations: this.store
            .recentOperations(session.id, 10)
            .map((operation) => ({
              handle: operation.id,
              actor_id: operation.actor_id,
              type: operation.statement_type,
              affected_rows: operation.affected_rows,
              status: operation.status,
            })),
        },
        stateVersion: connection ? version(connection) : undefined,
        confidence: connection ? confidence(connection) : undefined,
      };
    });
  }

  async closeSession(): Promise<Response<CloseSessionData>> {
    return this.run("session.close", async (session) => {
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Roll back or commit the active transaction first.",
        );
      }
      if (!this.store.closeSession(session.id, this.actorId)) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "A transaction became active while closing the session.",
        );
      }
      return {
        data: { session_id: session.id, state: "closed" },
        handle: session.id,
        executed: true,
      };
    });
  }

  async query(sql: string, options: QueryOptions = {}): Promise<Response<ResultData>> {
    return this.run("query", async (session) => {
      const previewRows = options.previewRows === undefined
        ? this.previewRows
        : queryPreviewRows(options.previewRows);
      const connection = this.requireConnection(session);
      this.rejectMongoSql(connection, "mongoQuery");
      this.rejectDuringStagedTransaction(session, "Queries");
      const analysis = analyzeSql(sql, connection.driver);
      if (!analysis.read) {
        throw new StateQLError(
          "INVALID_SQL",
          "query accepts read statements only; use exec for writes.",
        );
      }
      const cacheMode = options.cache ?? "auto";
      if (!analysis.cacheable && cacheMode === "require") {
        throw new StateQLError("CACHE_MISS", "This statement is not cacheable.", {
          retryable: true,
          suggestedAction: "Run with --cache auto or --cache bypass.",
        });
      }
      const parameters = options.params ?? [];
      if (analysis.requiresAutocommit && sqlParametersLength(parameters) > 0) {
        throw new StateQLError(
          "INVALID_SQL",
          "Autocommit diagnostic statements do not accept StateQL parameters.",
        );
      }
      const context = this.executionContext(options);
      const adapterSource = await this.resolveConnectionSource(
        connection,
        session,
        "query",
        "read",
        context,
      );
      const adapter = await this.openAdapter(
        connection,
        context,
        adapterSource,
      );
      try {
        const stateVersion = version(connection);
        const stateSignature = await adapter.signature();
        const fingerprint = hash({
          sql: analysis.normalized,
          parameters,
          driver: connection.driver,
          connection: connection.id,
          database: connection.database_name,
          transaction: session.active_transaction_id,
          stateVersion,
        });
        const cached = this.store.findResult(fingerprint);
        if (
          analysis.cacheable &&
          cacheMode !== "bypass" &&
          cached &&
          cached.row_count <= this.maxResultRows &&
          this.cacheValid(cached, stateVersion, stateSignature)
        ) {
          return {
            data: this.resultData(cached, true, previewRows),
            handle: cached.id,
            cached: true,
            warnings: paginationWarnings(analysis.ordered),
            stateVersion,
            confidence: cached.state_confidence,
          };
        }
        if (cacheMode === "require") {
          throw new StateQLError("CACHE_MISS", "No valid cached result exists.", {
            retryable: true,
            suggestedAction: "Run with --cache auto or --cache bypass.",
          });
        }

        const executionSql = analysis.wrapForLimit
          ? boundedReadSql(analysis.limitSql ?? sql, this.maxResultRows + 1)
          : sql;
        if (analysis.requiresAutocommit && !adapter.readAutocommit) {
          throw new StateQLError(
            "UNSUPPORTED_DRIVER",
            `${analysis.statementType.toUpperCase()} requires adapter autocommit reads.`,
          );
        }
        const result = analysis.requiresAutocommit
          ? await adapter.readAutocommit!(executionSql, parameters)
          : await adapter.read(executionSql, parameters);
        if (result.rows.length > this.maxResultRows) {
          throw new StateQLError(
            "OUTPUT_LIMIT_EXCEEDED",
            `Query exceeds the ${this.maxResultRows}-row materialization limit.`,
            { suggestedAction: "Add a narrower WHERE clause or LIMIT." },
          );
        }
        const resultBytes =
          Buffer.byteLength(JSON.stringify(parameters), "utf8") +
          Buffer.byteLength(JSON.stringify(result.rows), "utf8") +
          Buffer.byteLength(JSON.stringify(result.columns), "utf8");
        if (resultBytes > this.maxResultBytes) {
          throw new StateQLError(
            "OUTPUT_LIMIT_EXCEEDED",
            `Query exceeds the ${this.maxResultBytes}-byte materialization limit.`,
            { suggestedAction: "Select fewer rows or smaller columns." },
          );
        }
        const expiresAt = new Date(
          this.now().getTime() + this.resultTtlSeconds * 1000,
        ).toISOString();
        const saved = this.store.saveResult({
          sessionId: session.id,
          connectionId: connection.id,
          fingerprint,
          sql,
          parameters,
          rows: result.rows,
          columns: result.columns,
          stateVersion,
          stateSignature,
          stateConfidence: adapter.confidence,
          expiresAt,
        });
        return {
          data: this.resultData(saved, false, previewRows),
          handle: saved.id,
          executed: true,
          warnings: paginationWarnings(analysis.ordered),
          stateVersion,
          confidence: adapter.confidence,
        };
      } catch (error) {
        if (error instanceof StateQLError) throw error;
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, true);
        }
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          {
            retryable: true,
            executed: true,
          },
        );
      } finally {
        await closeAdapterQuietly(adapter);
      }
    }, sql);
  }

  async mongoQuery(
    command: MongoReadCommand,
    options: MongoQueryOptions = {},
  ): Promise<Response<ResultData>> {
    return this.run("mongo.query", async (session) => {
      const value = validatedMongoRead(command);
      const serializedCommand = serializeMongoCommand(value);
      const connection = this.requireMongoConnection(session, "mongoQuery");
      this.rejectDuringStagedTransaction(session, "MongoDB queries");
      const context = this.executionContext(options);
      const adapterSource = await this.resolveConnectionSource(
        connection,
        session,
        "query",
        "read",
        context,
      );
      const adapter = await this.openMongoAdapter(
        connection,
        context,
        adapterSource,
      );
      try {
        const stateVersion = version(connection);
        const stateSignature = await adapter.signature();
        const fingerprint = hash({
          command: serializedCommand,
          driver: connection.driver,
          connection: connection.id,
          database: connection.database_name,
          transaction: session.active_transaction_id,
          stateVersion,
        });
        const cached = this.store.findResult(fingerprint);
        const cacheMode = options.cache ?? "auto";
        const warnings = mongoPaginationWarnings(value);
        if (
          cacheMode !== "bypass" &&
          cached &&
          cached.row_count <= this.maxResultRows &&
          this.cacheValid(cached, stateVersion, stateSignature)
        ) {
          return {
            data: this.resultData(cached, true),
            handle: cached.id,
            cached: true,
            warnings,
            stateVersion,
            confidence: cached.state_confidence,
          };
        }
        if (cacheMode === "require") {
          throw new StateQLError("CACHE_MISS", "No valid cached result exists.", {
            retryable: true,
            suggestedAction: "Run with cache auto or cache bypass.",
          });
        }

        const result = await adapter.read(value, this.maxResultRows + 1);
        if (result.rows.length > this.maxResultRows) {
          throw new StateQLError(
            "OUTPUT_LIMIT_EXCEEDED",
            `Query exceeds the ${this.maxResultRows}-row materialization limit.`,
            { suggestedAction: "Use a narrower filter or limit." },
          );
        }
        const parameters: SqlParameters = [serializedCommand];
        const resultBytes =
          Buffer.byteLength(JSON.stringify(parameters), "utf8") +
          Buffer.byteLength(JSON.stringify(result.rows), "utf8") +
          Buffer.byteLength(JSON.stringify(result.columns), "utf8");
        if (resultBytes > this.maxResultBytes) {
          throw new StateQLError(
            "OUTPUT_LIMIT_EXCEEDED",
            `Query exceeds the ${this.maxResultBytes}-byte materialization limit.`,
            { suggestedAction: "Return fewer or smaller documents." },
          );
        }
        const expiresAt = new Date(
          this.now().getTime() + this.resultTtlSeconds * 1000,
        ).toISOString();
        const saved = this.store.saveResult({
          sessionId: session.id,
          connectionId: connection.id,
          fingerprint,
          sql: mongoDescriptor(value.operation),
          parameters,
          rows: result.rows,
          columns: result.columns,
          stateVersion,
          stateSignature,
          stateConfidence: adapter.confidence,
          expiresAt,
        });
        return {
          data: this.resultData(saved, false),
          handle: saved.id,
          executed: true,
          warnings,
          stateVersion,
          confidence: adapter.confidence,
        };
      } catch (error) {
        if (error instanceof StateQLError) throw error;
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, true);
        }
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          { retryable: true, executed: true },
        );
      } finally {
        await closeAdapterQuietly(adapter);
      }
    });
  }

  async redisQuery(
    command: RedisCommand,
    options: RedisQueryOptions = {},
  ): Promise<Response<ResultData>> {
    return this.run("redis.query", async (session) => {
      const value = validatedRedisRead(command);
      const serialized = serializeRedisCommand(value);
      const connection = this.requireRedisConnection(session, "redisQuery");
      this.rejectDuringStagedTransaction(session, "Redis queries");
      const context = this.executionContext(options);
      const source = await this.resolveConnectionSource(connection, session, "query", "read", context);
      const adapter = await this.openRedisAdapter(connection, context, source);
      try {
        const stateVersion = version(connection);
        const stateSignature = await adapter.signature();
        const fingerprint = hash({ command: serialized, connection: connection.id, database: connection.database_name, stateVersion });
        const cached = this.store.findResult(fingerprint);
        const cacheMode = options.cache ?? "auto";
        if (cacheMode !== "bypass" && cached && cached.row_count <= this.maxResultRows && this.cacheValid(cached, stateVersion, stateSignature)) {
          return { data: this.resultData(cached, true), handle: cached.id, cached: true, stateVersion, confidence: cached.state_confidence };
        }
        if (cacheMode === "require") throw new StateQLError("CACHE_MISS", "No valid cached Redis result exists.", { retryable: true });
        const result = await adapter.read(value);
        const parameters: SqlParameters = [serialized, result.nextCursor ?? null];
        const resultBytes = Buffer.byteLength(JSON.stringify(parameters), "utf8") + Buffer.byteLength(JSON.stringify(result.rows), "utf8") + Buffer.byteLength(JSON.stringify(result.columns), "utf8");
        if (result.rows.length > this.maxResultRows || resultBytes > this.maxResultBytes) throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "Redis response exceeds materialization limits.");
        const saved = this.store.saveResult({
          sessionId: session.id, connectionId: connection.id, fingerprint,
          sql: `Redis native ${value.command}`, parameters, rows: result.rows, columns: result.columns,
          stateVersion, stateSignature, stateConfidence: adapter.confidence,
          expiresAt: new Date(this.now().getTime() + this.resultTtlSeconds * 1000).toISOString(),
        });
        return { data: this.resultData(saved, false), handle: saved.id, executed: true, stateVersion, confidence: adapter.confidence };
      } catch (error) {
        if (error instanceof StateQLError) throw error;
        if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, true);
        throw new StateQLError("QUERY_FAILED", safeCredentialErrorMessage(error, source), { retryable: true, executed: true });
      } finally { await closeAdapterQuietly(adapter); }
    });
  }


  async show(idOrAlias: string): Promise<Response<ResultData>> {
    return this.withResult("show", idOrAlias, async (result) => ({
      data: this.resultData(result, true),
      handle: result.id,
      cached: true,
      stateVersion: result.state_version,
      confidence: result.state_confidence,
    }));
  }

  async filter(
    idOrAlias: string,
    predicate: string,
    options: FilterOptions = {},
  ): Promise<Response<ResultData>> {
    return this.withResult("filter", idOrAlias, async (source) => {
      const columns = this.store.resultColumns(source);
      const filter = prepareFilterStatement(columns, predicate);
      const parameters = options.params ?? [];
      validateFilterParameters(filter, parameters);
      const fingerprint = hash({
        command: "filter",
        source: source.id,
        predicate: filter.normalized,
        parameters,
      });
      const cached = this.store.findResult(fingerprint);
      if (
        cached &&
        cached.session_id === source.session_id &&
        cached.connection_id === source.connection_id &&
        Date.parse(cached.expires_at) > this.now().getTime()
      ) {
        return {
          data: this.resultData(cached, true),
          handle: cached.id,
          cached: true,
          stateVersion: cached.state_version,
          confidence: cached.state_confidence,
        };
      }

      const rows = filterMaterializedRows(
        this.store.resultRows(source),
        filter,
        parameters,
      );
      const saved = this.store.saveResult({
        sessionId: source.session_id,
        connectionId: source.connection_id,
        fingerprint,
        sql: `FILTER ${source.id} WHERE ${predicate.trim()}`,
        parameters,
        rows,
        columns,
        stateVersion: source.state_version,
        stateSignature: source.state_signature,
        stateConfidence: source.state_confidence,
        expiresAt: source.expires_at,
      });
      return {
        data: this.resultData(saved, false),
        handle: saved.id,
        executed: true,
        stateVersion: saved.state_version,
        confidence: saved.state_confidence,
      };
    });
  }

  async rows(
    idOrAlias: string,
    options: RowsOptions = {},
  ): Promise<Response<RowsData>> {
    return this.withResult("rows", idOrAlias, async (result) => {
      const offset = nonNegativeInteger(options.offset ?? 0, "offset");
      const limit = positiveInteger(options.limit ?? 20, "limit");
      if (limit > 1_000) {
        throw new StateQLError(
          "OUTPUT_LIMIT_EXCEEDED",
          "limit cannot exceed 1000 rows.",
          { suggestedAction: "Fetch another page or use export." },
        );
      }
      const allRows = this.store.resultRows(result);
      const rows = allRows.slice(offset, offset + limit);
      return {
        data: {
          result_id: result.id,
          offset,
          limit,
          rows: compactRows(rows, this.maxCellCharacters),
          returned: rows.length,
          total: result.row_count,
          truncated: offset + rows.length < result.row_count,
          next_offset:
            offset + rows.length < result.row_count ? offset + rows.length : null,
        },
        handle: result.id,
        cached: true,
        stateVersion: result.state_version,
        confidence: result.state_confidence,
      };
    });
  }

  async count(idOrAlias: string): Promise<Response<CountData>> {
    return this.withResult("count", idOrAlias, async (result) => ({
      data: { result_id: result.id, rows: result.row_count },
      handle: result.id,
      cached: true,
      stateVersion: result.state_version,
      confidence: result.state_confidence,
    }));
  }

  async columns(idOrAlias: string): Promise<Response<ColumnsData>> {
    return this.withResult("columns", idOrAlias, async (result) => ({
      data: {
        result_id: result.id,
        columns: this.store.resultColumns(result),
      },
      handle: result.id,
      cached: true,
      stateVersion: result.state_version,
      confidence: result.state_confidence,
    }));
  }

  async setAlias(name: string, id: string): Promise<Response<AliasData>> {
    return this.run("alias.set", async (session) => {
      const result = this.requireResult(id, session);
      this.store.setAlias(session.id, name, result.id);
      return {
        data: { alias: name, result_id: result.id },
        handle: result.id,
        executed: true,
      };
    });
  }

  /** Owned, full-value pages for host UIs. Reading a page does not add a command to history. */
  readMaterialized(id: string, options: RowsOptions & { signal?: AbortSignal } = {}): RowsData & { columns: Column[]; row_tokens: Array<string | null>; writable_columns: string[]; editing_reason?: string } {
    options.signal?.throwIfAborted();
    const result = this.panelResult(id);
    const offset = nonNegativeInteger(options.offset ?? 0, "offset");
    const limit = positiveInteger(options.limit ?? 100, "limit");
    if (limit > 100 || offset > 10_000) throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "Page bounds exceeded.");
    const all = this.fullResultRows(result);
    const rows: Row[] = [];
    let bytes = 0;
    for (const row of all.slice(offset, offset + limit)) {
      const size = Buffer.byteLength(JSON.stringify(row), "utf8");
      if (bytes + size > 200 * 1024) {
        if (!rows.length) throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "A row exceeds the browser page limit. Export this result instead.");
        break;
      }
      rows.push(row);
      bytes += size;
    }
    const next = offset + rows.length;
    const metadata = this.tableResults.get(id);
    const connection = this.store.activeConnection(this.store.ensureSession(this.sessionName));
    const eligible = metadata && connection?.id === result.connection_id && !connection.read_only && version(connection) === result.state_version;
    const rowTokens = rows.map(row => {
      if (!eligible || !editableRow(metadata, row)) return null;
      const token = randomUUID();
      for (const [key, entry] of this.editTokens) if (entry.expires <= this.now().getTime()) this.editTokens.delete(key);
      if (this.editTokens.size >= 1000) this.editTokens.delete(this.editTokens.keys().next().value!);
      this.editTokens.set(token, { metadata, original: structuredClone(row), sessionId: result.session_id, connectionId: result.connection_id, stateVersion: result.state_version, expires: this.now().getTime() + 10 * 60_000 });
      return token;
    });
    const writable = metadata?.driver === "mongodb" ? [...new Set(rows.flatMap(row => Object.keys(row)))].filter(name => name !== "_id")
      : metadata?.columns.filter(column => !column.key && !column.generated).map(column => column.name) ?? [];
    return { row_tokens: rowTokens, writable_columns: writable, ...(!metadata?.writable ? { editing_reason: metadata?.reason ?? "Query results are read-only." } : {}),
      result_id: result.id, offset, limit, rows: structuredClone(rows), columns: this.store.resultColumns(result),
      returned: rows.length, total: all.length, truncated: next < all.length, next_offset: next < all.length ? next : null };
  }

  /** No caller-selected filesystem paths. The host delivers these bounded attachment bytes. */
  async serializeResult(id: string, format: "json" | "jsonl" | "csv", signal?: AbortSignal, origin: CommandOrigin = "api"): Promise<Response<{ content: string; format: string; rows: number }>> {
    return this.commandContexts.run(mergeCommandExecutionContext(this.commandContexts.getStore(), { signal, origin }), () => this.run("export", async () => {
      if (!["json", "jsonl", "csv"].includes(format)) throw new StateQLError("INVALID_COMMAND", "Unsupported export format.");
      const result = this.panelResult(id);
      const rows = this.fullResultRows(result);
      const columns = this.store.resultColumns(result).map(column => column.name);
      const chunks: string[] = [];
      let bytes = 0;
      const append = (chunk: string) => {
        bytes += Buffer.byteLength(chunk, "utf8");
        if (bytes > 32 * 1024 * 1024) throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "Export exceeds 32 MiB.");
        chunks.push(chunk);
      };
      const csvCell = (value: unknown) => {
        let valueText = value === null || value === undefined ? "" : typeof value === "object" ? JSON.stringify(value) : String(value);
        if (typeof value !== "number" && /^[\s\u0000-\u001f]*[=+@-]|^[\t\r\n]/u.test(valueText)) valueText = "'" + valueText;
        return /[",\r\n]/u.test(valueText) ? '"' + valueText.replaceAll('"', '""') + '"' : valueText;
      };
      if (format === "json") append("[");
      if (format === "csv") append(columns.map(csvCell).join(",") + "\n");
      const deadline = Date.now() + 30_000;
      for (let index = 0; index < rows.length; index++) {
        if (index % 100 === 0) {
          await new Promise<void>(resolve => setImmediate(resolve));
          signal?.throwIfAborted();
          this.signal?.throwIfAborted();
          if (Date.now() > deadline) throw new StateQLError("DEADLINE_EXCEEDED", "Export preparation timed out.");
        }
        const row = rows[index]!;
        append(format === "csv" ? columns.map(column => csvCell(row[column])).join(",") + "\n"
          : (format === "json" && index ? "," : "") + JSON.stringify(row) + (format === "jsonl" ? "\n" : ""));
      }
      signal?.throwIfAborted();
      if (format === "json") append("]\n");
      this.panelResult(id);
      return { data: { content: chunks.join(""), format, rows: rows.length }, handle: id };
    }));
  }

  async readTable(table: { schema?: string; name: string }, limit = 1000, options: QueryOptions & { origin?: CommandOrigin } = {}): Promise<Response<ResultData & { table: { schema?: string; name: string }; sample_limit: number; query: string }>> {
    return this.commandContexts.run(mergeCommandExecutionContext(this.commandContexts.getStore(), { signal: options.signal, origin: options.origin ?? "api", internal: true }), async () => {
    if (!table || typeof table.name !== "string" || !table.name || table.name.length > 500 || table.name.includes("\0") ||
      (table.schema !== undefined && (typeof table.schema !== "string" || !table.schema || table.schema.length > 500 || table.schema.includes("\0"))) ||
      !Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) throw new StateQLError("INVALID_COMMAND", "Invalid table or sample limit.");
    const snapshot = this.snapshot({ historyLimit: 1 });
    const driver = snapshot.connection?.driver;
    if (!driver) throw new StateQLError("CONNECTION_NOT_FOUND", "Connect to a database first.");
    if (driver === "sqlite" && table.schema && table.schema !== "main") throw new StateQLError("INVALID_COMMAND", "Only the main SQLite schema is supported.");
    if (driver === "mongodb" && table.schema) throw new StateQLError("INVALID_COMMAND", "MongoDB collections do not accept a schema.");
    if (driver === "redis") throw new StateQLError("INVALID_COMMAND", "Redis keys are not SQL tables; use redisQuery and describeObject.");
    const quote = (name: string) => driver === "mysql" ? "\`" + name.replaceAll("\`", "\`\`") + "\`" : '"' + name.replaceAll('"', '""') + '"';
    const qualified = [table.schema, table.name].filter((part): part is string => Boolean(part)).map(quote).join(".");
    const query = driver === "mongodb" ? JSON.stringify({ operation: "find", collection: table.name, options: { limit } }, null, 2)
      : "SELECT * FROM " + qualified + " LIMIT " + limit;
    const metadata = await this.editableMetadata(table, options);
    const response = driver === "mongodb"
      ? await this.mongoQuery({ operation: "find", collection: table.name, options: { limit } }, options)
      : await this.query(query, options);
    if (!response.ok) return response;
    if (this.tableResults.size >= 20) this.tableResults.delete(this.tableResults.keys().next().value!);
    this.tableResults.set(response.data.result_id, metadata);
    return { ...response, data: { ...response.data, table, sample_limit: limit, query } };
    });
  }

  async planTableUpdate(token: string, changes: TableChange, options: ExecutionOptions & { origin?: CommandOrigin } = {}): Promise<Response<PlanData>> {
    return this.commandContexts.run(mergeCommandExecutionContext(this.commandContexts.getStore(), { signal: options.signal, origin: options.origin ?? "api" }), () => this.run("table.plan", async session => {
      this.rejectDuringStagedTransaction(session, "Table edits");
      const connection = this.requireConnection(session);
      const original = this.editTokens.get(token);
      if (!original || original.sessionId !== session.id || original.connectionId !== connection.id ||
        original.stateVersion !== version(connection) || original.expires <= this.now().getTime())
        throw new StateQLError("STALE_PLAN", "Row identity expired or the connection changed. Reload the row.");
      if (connection.read_only) throw new StateQLError("READ_ONLY_CONNECTION", "This connection is read-only.");
      const metadata = await this.editableMetadata(original.metadata.table, options);
      if (JSON.stringify(metadata) !== JSON.stringify(original.metadata)) throw new StateQLError("STALE_PLAN", "Table metadata changed. Reload the table.");
      const update: TableUpdate = { metadata, original: original.original, changes };
      const compiled = compileTableUpdate(update);
      const context = this.executionContext(options);
      const source = await this.resolveConnectionSource(connection, session, "plan", "read", context);
      const adapter = connection.driver === "mongodb" ? await this.openMongoAdapter(connection, context, source) : await this.openAdapter(connection, context, source);
      try {
        const plan = this.store.savePlan({ sessionId: session.id, ownerActorId: this.actorId, connectionId: connection.id,
          sql: compiled.sql, parameters: [JSON.stringify(update)], statementType: "table.update", stateVersion: version(connection),
          stateSignature: await adapter.signature(), destructive: false, allowUnbounded: false, allowDestructive: false,
          expiresAt: new Date(original.expires).toISOString() });
        return { data: { plan_id: plan.id, statement_type: plan.statement_type, destructive: false, requires_confirmation: true, required_overrides: [],
          state_version: plan.state_version, owner_actor_id: this.actorId, expires_at: plan.expires_at }, handle: plan.id };
      } finally { await closeAdapterQuietly(adapter); }
    }));
  }

  async planTableUpdates(
    changes: Array<{ row_token: string; changes: TableChange }>,
    options: ExecutionOptions & { origin?: CommandOrigin } = {},
  ): Promise<Response<PlanData>> {
    return this.commandContexts.run(
      mergeCommandExecutionContext(this.commandContexts.getStore(), { signal: options.signal, origin: options.origin ?? "api" }),
      () => this.run("table.plan", async (session) => {
        this.rejectDuringStagedTransaction(session, "Table edits");
        if (!Array.isArray(changes) || changes.length < 1 || changes.length > 100 || Buffer.byteLength(JSON.stringify(changes), "utf8") > 256 * 1024) {
          throw new StateQLError("INVALID_COMMAND", "Table edit batch must contain 1-100 bounded rows.");
        }
        const connection = this.requireConnection(session);
        if (connection.read_only) throw new StateQLError("READ_ONLY_CONNECTION", "This connection is read-only.");
        if (connection.driver === "redis") throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support table edits.");
        const now = this.now().getTime();
        const tokens = new Set<string>();
        const identities = new Set<string>();
        const updates: TableUpdate[] = [];
        let expires = Number.MAX_SAFE_INTEGER;
        const metadataByTable = new Map<string, EditableTable>();
        for (const item of changes) {
          if (!item || typeof item.row_token !== "string" || tokens.has(item.row_token)) throw new StateQLError("INVALID_COMMAND", "Table edit row tokens must be unique.");
          tokens.add(item.row_token);
          const original = this.editTokens.get(item.row_token);
          if (!original || original.sessionId !== session.id || original.connectionId !== connection.id || original.stateVersion !== version(connection) || original.expires <= now) {
            throw new StateQLError("STALE_PLAN", "A row identity expired or the connection changed. Reload the rows.");
          }
          const tableKey = JSON.stringify(original.metadata.table);
          let metadata = metadataByTable.get(tableKey);
          if (!metadata) {
            metadata = await this.editableMetadata(original.metadata.table, options);
            metadataByTable.set(tableKey, metadata);
          }
          if (JSON.stringify(metadata) !== JSON.stringify(original.metadata)) throw new StateQLError("STALE_PLAN", "Table metadata changed. Reload the table.");
          const identity = tableUpdateIdentity(metadata, original.original);
          if (identities.has(identity)) throw new StateQLError("INVALID_COMMAND", "The same row cannot appear twice in one edit batch.");
          identities.add(identity);
          const update = { metadata, original: original.original, changes: item.changes };
          compileTableUpdate(update);
          updates.push(update);
          expires = Math.min(expires, original.expires);
        }
        const payload = JSON.stringify({ version: 1, updates });
        const context = this.executionContext(options);
        const source = await this.resolveConnectionSource(connection, session, "plan", "read", context);
        const adapter = connection.driver === "mongodb" ? await this.openMongoAdapter(connection, context, source) : await this.openAdapter(connection, context, source);
        try {
          const plan = this.store.savePlan({
            sessionId: session.id, ownerActorId: this.actorId, connectionId: connection.id,
            sql: `Conditional table update batch (${updates.length} rows)`, parameters: [payload], statementType: "table.updates",
            stateVersion: version(connection), stateSignature: await adapter.signature(), destructive: false,
            allowUnbounded: false, allowDestructive: false, expiresAt: new Date(expires).toISOString(),
          });
          return { data: { plan_id: plan.id, statement_type: plan.statement_type, destructive: false, requires_confirmation: true,
            required_overrides: [], state_version: plan.state_version, owner_actor_id: plan.owner_actor_id, expires_at: plan.expires_at },
            handle: plan.id, executed: true, stateVersion: plan.state_version, confidence: adapter.confidence };
        } finally { await closeAdapterQuietly(adapter); }
      }),
    );
  }


  private async editableMetadata(table: TableIdentity, options: ExecutionOptions): Promise<EditableTable> {
    const snapshot = this.snapshot({ historyLimit: 1 });
    const driver = snapshot.connection!.driver;
    const unavailable: EditableTable = { table, driver, columns: [], writable: false, reason: "This table or its values cannot be edited safely." };
    // The legacy inspection API splits qualified names. Fail closed for ambiguous names.
    if (table.name.includes(".") || table.schema?.includes(".")) return { ...unavailable, reason: "Editing identifiers containing dots is not supported." };
    const name = driver === "sqlite" || driver === "mongodb" ? table.name : [table.schema, table.name].filter(Boolean).join(".");
    const response = await this.inspect("editable", name, options);
    if (!response.ok) return { ...unavailable, reason: response.error.code };
    const value = response.data as { writable?: boolean; columns?: EditableTable["columns"] };
    if (!value || !Array.isArray(value.columns) || value.columns.length > 100) return unavailable;
    return { table, driver, columns: value.columns, writable: value.writable === true && !snapshot.connection!.read_only,
      ...(value.writable ? {} : { reason: "Only ordinary tables with transactional writes and a primary key can be edited." }) };
  }

  private panelResult(id: string): ResultRecord {
    if (typeof id !== "string" || !id || id.length > 200) throw new StateQLError("INVALID_COMMAND", "A result ID is required.");
    const session = this.store.ensureSession(this.sessionName);
    if (!this.store.isSessionMember(session.id, this.actorId)) this.throwMembershipDenied(session);
    const result = this.requireResult(id, session);
    if (result.id !== id) throw new StateQLError("INVALID_COMMAND", "Use an immutable result ID, not an alias.");
    if (Date.parse(result.expires_at) <= this.now().getTime()) throw new StateQLError("RESULT_EXPIRED", "Result expired. Run the query again.");
    if (result.row_count > 10_000 || Buffer.byteLength(result.rows_json, "utf8") > 16 * 1024 * 1024)
      throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "Stored result exceeds browser limits.");
    return result;
  }

  private fullResultRows(result: ResultRecord): Row[] {
    if (this.panelRows?.id !== result.id || this.panelRows.json !== result.rows_json)
      this.panelRows = { id: result.id, json: result.rows_json, rows: this.store.resultRows(result) };
    return this.panelRows.rows;
  }

  async exportResult(
    idOrAlias: string,
    output: string,
    format: "json" | "jsonl" | "csv" = "csv",
  ): Promise<Response<ExportData>> {
    return this.withResult("export", idOrAlias, async (result) => {
      const rows = this.store.resultRows(result);
      const content =
        format === "json"
          ? `${JSON.stringify(rows, null, 2)}\n`
          : format === "jsonl"
            ? `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`
            : rowsToCsv(rows, this.store.resultColumns(result).map((column) => column.name));
      writeFileSync(resolve(output), content, "utf8");
      return {
        data: {
          result_id: result.id,
          output: resolve(output),
          format,
          rows: rows.length,
        },
        handle: result.id,
        executed: true,
      };
    });
  }

  async exec(sql: string, options: ExecOptions = {}): Promise<Response<ExecData>> {
    return this.run("exec", async (session) => {
      const connection = this.requireConnection(session);
      this.rejectMongoSql(connection, "mongoExec");
      return this.performExec(
        session,
        connection,
        sql,
        options,
        this.executionContext(options),
      );
    }, sql);
  }

  async mongoExec(
    command: MongoWriteCommand,
    options: MongoExecOptions = {},
  ): Promise<Response<ExecData>> {
    return this.run("mongo.exec", async (session) => {
      const value = validatedMongoWrite(command);
      const connection = this.requireMongoConnection(session, "mongoExec");
      return this.performMongoExec(
        session,
        connection,
        value,
        options,
        this.executionContext(options),
      );
    });
  }

  async redisExec(
    command: RedisCommand,
    options: RedisExecOptions = {},
  ): Promise<Response<ExecData>> {
    return this.run("redis.exec", async (session) => {
      const value = validatedRedisWrite(command);
      const connection = this.requireRedisConnection(session, "redisExec");
      this.rejectDuringStagedTransaction(session, "Redis writes");
      return this.performRedisExec(session, connection, value, options, this.executionContext(options));
    });
  }


  async receipt(id: string): Promise<Response<OperationData>> {
    return this.run("receipt", async (session) => {
      const operation = this.store.getOperation(id);
      if (!operation || operation.session_id !== session.id) {
        throw new StateQLError(
          "RESULT_NOT_FOUND",
          `Operation "${id}" was not found.`,
        );
      }
      return {
        data: operationData(operation),
        handle: operation.id,
        stateVersion:
          operation.state_version_after ?? operation.state_version_before,
      };
    });
  }

  async beginTransaction(
    isolation?: string,
  ): Promise<Response<TransactionData>> {
    return this.run("transaction.begin", async (session) => {
      const connection = this.requireConnection(session);
      if (connection.driver === "redis") throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support staged StateQL transactions; use redisPlan/apply for one guarded mutation.");
      if (connection.read_only) {
        throw new StateQLError(
          "READ_ONLY_CONNECTION",
          "Cannot begin a write transaction on a read-only connection.",
        );
      }
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          `Transaction "${session.active_transaction_id}" is already active.`,
        );
      }
      const normalizedIsolation = normalizeIsolation(
        isolation ?? (connection.driver === "mongodb" ? "snapshot" : "serializable"),
        connection.driver,
      );
      const transaction = this.store.createTransaction({
        sessionId: session.id,
        actorId: this.actorId,
        connectionId: connection.id,
        isolation: normalizedIsolation,
      });
      if (!transaction) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Another actor acquired the active transaction.",
        );
      }
      return {
        data: transactionData(transaction, 0),
        handle: transaction.id,
        executed: true,
        stateVersion: transaction.start_version,
      };
    });
  }

  async transactionStatus(id?: string): Promise<Response<TransactionData>> {
    return this.run("transaction.status", async (session) => {
      const transactionId = id ?? session.active_transaction_id;
      if (!transactionId) {
        throw new StateQLError(
          "TRANSACTION_NOT_FOUND",
          "No active transaction.",
        );
      }
      const transaction = this.store.getTransaction(transactionId);
      if (!transaction || transaction.session_id !== session.id) {
        throw new StateQLError(
          "TRANSACTION_NOT_FOUND",
          `Transaction "${transactionId}" was not found.`,
        );
      }
      const operations = this.store.transactionOperations(transaction.id);
      return {
        data: transactionData(transaction, operations.length),
        handle: transaction.id,
        stateVersion: transaction.start_version,
      };
    });
  }

  async commitTransaction(
    id?: string,
    options: ExecutionOptions = {},
  ): Promise<Response<CommitTransactionData>> {
    return this.run("transaction.commit", async (session) => {
      const transaction = this.requireActiveTransaction(session, id);
      const connection = this.store.getConnection(transaction.connection_id);
      if (!connection) {
        throw new StateQLError(
          "CONNECTION_NOT_FOUND",
          "Transaction connection was not found.",
        );
      }
      if (version(connection) !== transaction.start_version) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Connection state changed after the transaction began.",
          { suggestedAction: "Roll back and begin a new transaction." },
        );
      }
      // Validate durable payloads before opening a database adapter or changing
      // the transaction state.
      const operations = this.store.validatedTransactionOperations(transaction.id);
      if (operations.some((operation) => operation.connection_id !== connection.id)) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Transaction contains writes staged for another connection.",
        );
      }
      const mongoCommands = connection.driver === "mongodb"
        ? operations.map(storedMongoOperation)
        : undefined;
      if (
        connection.driver !== "mongodb" &&
        operations.some((operation) => operation.statement_type.startsWith("mongo."))
      ) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Transaction contains native MongoDB writes for a SQL connection.",
        );
      }
      const context = this.executionContext(options);
      const adapterSource = await this.resolveConnectionSource(
        connection,
        session,
        "transaction.commit",
        "write",
        context,
      );
      const adapter = connection.driver === "mongodb"
        ? await this.openMongoAdapter(connection, context, adapterSource)
        : await this.openAdapter(connection, context, adapterSource);
      try {
        if (
          !this.store.claimTransactionForCommit(
            transaction.id,
            session.id,
            this.actorId,
            operations,
          )
        ) {
          throw new StateQLError(
            "TRANSACTION_FAILED",
            "Transaction is no longer active.",
          );
        }

        let results: Array<{
          affectedRows: number;
          outcome?: MongoWriteOutcome;
        }>;
        try {
          results = mongoCommands
            ? await (adapter as MongoAdapter).writeBatch(
                mongoCommands,
                transaction.isolation_level,
              )
            : await (adapter as Adapter).writeBatch(
                operations,
                transaction.isolation_level,
              );
        } catch (error) {
          if (
            (error instanceof BatchWriteError && !error.outcomeUnknown) ||
            (error instanceof AdapterExecutionError && !error.outcomeUnknown)
          ) {
            this.store.finishTransaction(
              transaction.id,
              session.id,
              this.actorId,
              "failed",
            );
            if (error instanceof AdapterExecutionError) {
              throw stoppedStateQLError(error, false);
            }
            throw new StateQLError(
              "TRANSACTION_FAILED",
              safeCredentialErrorMessage(error, adapterSource),
              { retryable: true },
            );
          }
          markTransactionOutcomeUnknown(
            this.store,
            transaction.id,
            session.id,
            this.actorId,
          );
          throw new StateQLError(
            "OUTCOME_UNKNOWN",
            safeCredentialErrorMessage(error, adapterSource),
            {
              executed: true,
              suggestedAction:
                "Inspect database state before issuing any replacement write.",
            },
          );
        }

        if (results.length !== operations.length) {
          markTransactionOutcomeUnknown(
            this.store,
            transaction.id,
            session.id,
            this.actorId,
          );
          throw new StateQLError(
            "OUTCOME_UNKNOWN",
            "Database returned an incomplete transaction result.",
            {
              executed: true,
              suggestedAction:
                "Inspect database state before issuing any replacement write.",
            },
          );
        }

        let stateVersion: string;
        try {
          stateVersion = this.store.commitTransactionMetadata({
            transactionId: transaction.id,
            sessionId: session.id,
            actorId: this.actorId,
            connectionId: connection.id,
            operations: operations.map((operation, index) => ({
              id: operation.id,
              affectedRows: results[index]!.affectedRows,
              ...(results[index]!.outcome
                ? { outcome: results[index]!.outcome }
                : {}),
            })),
          });
        } catch (error) {
          markTransactionOutcomeUnknown(
            this.store,
            transaction.id,
            session.id,
            this.actorId,
          );
          throw new StateQLError(
            "OUTCOME_UNKNOWN",
            safeCredentialErrorMessage(error, adapterSource),
            {
              executed: true,
              suggestedAction:
                "Inspect database state before issuing any replacement write.",
            },
          );
        }

        return {
          data: {
            transaction_id: transaction.id,
            state: "committed",
            statements_executed: operations.length,
            affected_rows: results.reduce(
              (total, result) => total + result.affectedRows,
              0,
            ),
            state_version: stateVersion,
          },
          handle: transaction.id,
          executed: true,
          stateVersion,
          confidence: adapter.confidence,
        };
      } finally {
        try {
          await adapter.close();
        } catch {
          // Transaction outcome and metadata are already recorded.
        }
      }
    });
  }

  async rollbackTransaction(id?: string): Promise<Response<RollbackTransactionData>> {
    return this.run("transaction.rollback", async (session) => {
      const transaction = this.requireActiveTransaction(session, id);
      const count = this.store.transactionOperations(transaction.id).length;
      if (
        !this.store.finishTransaction(
          transaction.id,
          session.id,
          this.actorId,
          "rolled_back",
        )
      ) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Transaction is no longer active.",
        );
      }
      return {
        data: {
          transaction_id: transaction.id,
          state: "rolled_back",
          discarded_statements: count,
        },
        handle: transaction.id,
        executed: true,
        stateVersion: transaction.start_version,
      };
    });
  }

  async inspect(
    kind: string,
    table?: string,
    options: ExecutionOptions = {},
  ): Promise<Response<unknown>> {
    return this.run(`inspect.${kind}`, async (session) => {
      const connection = this.requireConnection(session);
      this.rejectDuringStagedTransaction(session, "Schema inspection");
      const context = this.executionContext(options);
      const adapterSource = await this.resolveConnectionSource(
        connection,
        session,
        "inspect",
        "read",
        context,
      );
      if (connection.driver === "redis") throw new StateQLError("INVALID_COMMAND", "Legacy inspect is not supported for Redis; use listObjects or describeObject.");
      const adapter = connection.driver === "mongodb"
        ? await this.openMongoAdapter(connection, context, adapterSource)
        : await this.openAdapter(connection, context, adapterSource);
      try {
        const data = await adapter.inspect(kind, table);
        return {
          data,
          executed: true,
          stateVersion: version(connection),
          confidence: adapter.confidence,
        };
      } catch (error) {
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, true);
        }
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          {
            retryable: false,
            executed: true,
          },
        );
      } finally {
        await closeAdapterQuietly(adapter);
      }
    }, undefined, table);
  }

  async listObjects(
    filter: ListObjectsFilter = {},
    options: ExecutionOptions = {},
  ): Promise<Response<ListObjectsData>> {
    return this.run("objects.list", async (session) => {
      validateCatalogFilter(filter);
      const connection = this.requireConnection(session);
      this.rejectDuringStagedTransaction(session, "Catalog discovery");
      const context = this.executionContext(options);
      const source = await this.resolveConnectionSource(connection, session, "inspect", "read", context);
      const adapter = connection.driver === "mongodb"
        ? await this.openMongoAdapter(connection, context, source)
        : connection.driver === "redis"
          ? await this.openRedisAdapter(connection, context, source)
          : await this.openAdapter(connection, context, source);
      try {
        return { data: await adapter.listObjects(filter), executed: true, stateVersion: version(connection), confidence: adapter.confidence };
      } catch (error) {
        if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, true);
        throw new StateQLError("QUERY_FAILED", safeCredentialErrorMessage(error, source), { executed: true });
      } finally { await closeAdapterQuietly(adapter); }
    });
  }

  async describeObject(
    object: CatalogObject,
    options: ExecutionOptions = {},
  ): Promise<Response<DescribeObjectData>> {
    return this.run("object.describe", async (session) => {
      validateCatalogObject(object);
      const connection = this.requireConnection(session);
      this.rejectDuringStagedTransaction(session, "Catalog description");
      const context = this.executionContext(options);
      const source = await this.resolveConnectionSource(connection, session, "inspect", "read", context);
      const adapter = connection.driver === "mongodb"
        ? await this.openMongoAdapter(connection, context, source)
        : connection.driver === "redis"
          ? await this.openRedisAdapter(connection, context, source)
          : await this.openAdapter(connection, context, source);
      try {
        return { data: await adapter.describeObject(object), executed: true, stateVersion: version(connection), confidence: adapter.confidence };
      } catch (error) {
        if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, true);
        throw new StateQLError("QUERY_FAILED", safeCredentialErrorMessage(error, source), { executed: true });
      } finally { await closeAdapterQuietly(adapter); }
    });
  }


  async plan(sql: string, options: PlanOptions = {}): Promise<Response<PlanData>> {
    return this.run("plan", async (session) => {
      const connection = this.requireConnection(session);
      this.rejectMongoSql(connection, "mongoPlan");
      this.rejectDuringStagedTransaction(session, "Plans");
      const analysis = analyzeSql(sql, connection.driver);
      if (analysis.read) {
        throw new StateQLError(
          "INVALID_SQL",
          "plan accepts write statements only.",
        );
      }
      const context = this.executionContext(options);
      const adapterSource = await this.resolveConnectionSource(
        connection,
        session,
        "plan",
        "read",
        context,
      );
      const adapter = await this.openAdapter(
        connection,
        context,
        adapterSource,
      );
      try {
        const stateSignature = await adapter.signature();
        const expiresAt = new Date(this.now().getTime() + 10 * 60_000).toISOString();
        const plan = this.store.savePlan({
          sessionId: session.id,
          ownerActorId: this.actorId,
          connectionId: connection.id,
          sql,
          parameters: options.params ?? [],
          statementType: analysis.statementType,
          stateVersion: version(connection),
          stateSignature,
          destructive: analysis.destructive || analysis.unboundedMutation,
          allowUnbounded: options.allowUnbounded ?? false,
          allowDestructive: options.allowDestructive ?? false,
          expiresAt,
        });
        return {
          data: {
            plan_id: plan.id,
            statement_type: plan.statement_type,
            destructive: Boolean(plan.destructive),
            requires_confirmation:
              (analysis.unboundedMutation && !Boolean(plan.allow_unbounded)) ||
              (analysis.destructive && !Boolean(plan.allow_destructive)),
            required_overrides: [
              ...(analysis.unboundedMutation && !Boolean(plan.allow_unbounded)
                ? ["--allow-unbounded"]
                : []),
              ...(analysis.destructive && !Boolean(plan.allow_destructive)
                ? ["--allow-destructive"]
                : []),
            ],
            state_version: plan.state_version,
            owner_actor_id: plan.owner_actor_id,
            expires_at: plan.expires_at,
          },
          handle: plan.id,
          executed: true,
          stateVersion: plan.state_version,
          confidence: adapter.confidence,
        };
      } catch (error) {
        if (error instanceof StateQLError) throw error;
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, true);
        }
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          { retryable: true, executed: true },
        );
      } finally {
        await closeAdapterQuietly(adapter);
      }
    }, sql);
  }

  async mongoPlan(
    command: MongoWriteCommand,
    options: MongoPlanOptions = {},
  ): Promise<Response<PlanData>> {
    return this.run("mongo.plan", async (session) => {
      const value = validatedMongoWrite(command);
      const serializedCommand = serializeMongoCommand(value);
      const connection = this.requireMongoConnection(session, "mongoPlan");
      this.rejectDuringStagedTransaction(session, "Plans");
      const safety = analyzeMongoWriteSafety(value);
      const context = this.executionContext(options);
      const adapterSource = await this.resolveConnectionSource(
        connection,
        session,
        "plan",
        "read",
        context,
      );
      const adapter = await this.openMongoAdapter(
        connection,
        context,
        adapterSource,
      );
      try {
        const stateSignature = await adapter.signature();
        const expiresAt = new Date(this.now().getTime() + 10 * 60_000).toISOString();
        const plan = this.store.savePlan({
          sessionId: session.id,
          ownerActorId: this.actorId,
          connectionId: connection.id,
          sql: mongoDescriptor(value.operation),
          parameters: [serializedCommand],
          statementType: `mongo.${value.operation}`,
          stateVersion: version(connection),
          stateSignature,
          destructive: safety.destructive || safety.unbounded,
          allowUnbounded: options.allowUnbounded ?? false,
          allowDestructive: options.allowDestructive ?? false,
          expiresAt,
        });
        return {
          data: {
            plan_id: plan.id,
            statement_type: plan.statement_type,
            destructive: Boolean(plan.destructive),
            requires_confirmation:
              (safety.unbounded && !Boolean(plan.allow_unbounded)) ||
              (safety.destructive && !Boolean(plan.allow_destructive)),
            required_overrides: [
              ...(safety.unbounded && !Boolean(plan.allow_unbounded)
                ? ["--allow-unbounded"]
                : []),
              ...(safety.destructive && !Boolean(plan.allow_destructive)
                ? ["--allow-destructive"]
                : []),
            ],
            state_version: plan.state_version,
            owner_actor_id: plan.owner_actor_id,
            expires_at: plan.expires_at,
          },
          handle: plan.id,
          executed: true,
          stateVersion: plan.state_version,
          confidence: adapter.confidence,
        };
      } catch (error) {
        if (error instanceof StateQLError) throw error;
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, true);
        }
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          { retryable: true, executed: true },
        );
      } finally {
        await closeAdapterQuietly(adapter);
      }
    });
  }

  async redisPlan(
    command: RedisCommand,
    options: RedisPlanOptions = {},
  ): Promise<Response<PlanData>> {
    return this.run("redis.plan", async (session) => {
      const value = validatedRedisWrite(command);
      const connection = this.requireRedisConnection(session, "redisPlan");
      this.rejectDuringStagedTransaction(session, "Plans");
      if (connection.read_only) throw new StateQLError("READ_ONLY_CONNECTION", "Connection is read-only.");
      const context = this.executionContext(options);
      const source = await this.resolveConnectionSource(connection, session, "plan", "read", context);
      const adapter = await this.openRedisAdapter(connection, context, source);
      try {
        const precondition = await adapter.precondition(value);
        const payload = JSON.stringify({ command: serializeRedisCommand(value), precondition });
        const plan = this.store.savePlan({
          sessionId: session.id, ownerActorId: this.actorId, connectionId: connection.id,
          sql: `Redis native ${value.command}`, parameters: [payload], statementType: `redis.${value.command.toLowerCase()}`,
          stateVersion: version(connection), stateSignature: await adapter.signature(), destructive: value.command === "DEL",
          allowUnbounded: false, allowDestructive: true,
          expiresAt: new Date(this.now().getTime() + 10 * 60_000).toISOString(),
        });
        return { data: { plan_id: plan.id, statement_type: plan.statement_type, destructive: Boolean(plan.destructive),
          requires_confirmation: true, required_overrides: [], state_version: plan.state_version,
          owner_actor_id: plan.owner_actor_id, expires_at: plan.expires_at }, handle: plan.id, executed: true,
          stateVersion: plan.state_version, confidence: adapter.confidence };
      } catch (error) {
        if (error instanceof StateQLError) throw error;
        if (error instanceof AdapterExecutionError) throw stoppedStateQLError(error, true);
        throw new StateQLError("QUERY_FAILED", safeCredentialErrorMessage(error, source), { executed: true });
      } finally { await closeAdapterQuietly(adapter); }
    });
  }


  async apply(
    planId: string,
    options: ExecutionOptions = {},
  ): Promise<Response<ApplyData>> {
    let historySql: string | undefined;
    return this.run("apply", async (session) => {
      this.rejectDuringStagedTransaction(session, "Plans");
      const plan = this.store.getPlan(planId);
      if (!plan || plan.session_id !== session.id) {
        throw new StateQLError("STALE_PLAN", `Plan "${planId}" was not found.`);
      }
      if (plan.owner_actor_id !== this.actorId) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          "Only the actor that created this plan may apply it.",
        );
      }
      if (plan.applied_operation_id) {
        throw new StateQLError("STALE_PLAN", "Plan was already applied.", {
          extra: { previous_operation_id: plan.applied_operation_id },
        });
      }
      if (Date.parse(plan.expires_at) <= this.now().getTime()) {
        throw new StateQLError("STALE_PLAN", "Plan has expired.");
      }
      const tableUpdate = plan.statement_type === "table.update" ? parseTableUpdate(plan.parameters) : undefined;
      const tableUpdates = plan.statement_type === "table.updates" ? parseTableUpdates(plan.parameters) : undefined;
      const compiled = tableUpdate ? compileTableUpdate(tableUpdate) : undefined;
      const compiledUpdates = tableUpdates?.map(compileTableUpdate);
      if (compiled && compiled.sql !== plan.sql) throw new StateQLError("STALE_PLAN", "Stored update does not match its plan.");
      const mongoPlan = tableUpdate?.metadata.driver === "mongodb" || tableUpdates?.[0]?.metadata.driver === "mongodb" || plan.statement_type.startsWith("mongo.");
      const redisPlan = plan.statement_type.startsWith("redis.");
      const nativePlan = Boolean(mongoPlan || redisPlan);
      historySql = nativePlan || tableUpdates ? undefined : plan.sql;
      const mongoCommand = compiled?.mongo ?? (plan.statement_type.startsWith("mongo.")
        ? storedMongoPlan(plan.parameters, plan.statement_type, plan.id)
        : undefined);
      const redisStored = redisPlan ? storedRedisPlan(plan.parameters, plan.statement_type, plan.id) : undefined;
      const planParameters = compiled?.params ?? (nativePlan || tableUpdates
        ? undefined
        : parseJson<SqlParameters>(
            plan.parameters,
            `plan "${plan.id}" parameters`,
            isSqlParameters,
          ));
      const claimToken = this.store.randomId("claim");
      const claimed = this.store.claimPlan(
        plan.id,
        session.id,
        this.actorId,
        claimToken,
      );
      if (!claimed) {
        throw new StateQLError("STALE_PLAN", "Plan is already being applied.");
      }

      let retainClaim = false;
      try {
        const connection = this.requireConnection(session);
        if (
          connection.id !== claimed.connection_id ||
          version(connection) !== claimed.state_version
        ) {
          throw new StateQLError(
            "STALE_PLAN",
            "Database state changed after this plan was created.",
          );
        }
        if (mongoPlan && connection.driver !== "mongodb") {
          throw new StateQLError("STALE_PLAN", "MongoDB plan is not attached to a MongoDB connection.");
        }
        if (redisPlan && connection.driver !== "redis") {
          throw new StateQLError("STALE_PLAN", "Redis plan is not attached to a Redis connection.");
        }
        if (!nativePlan && (connection.driver === "mongodb" || connection.driver === "redis")) {
          this.rejectMongoSql(connection, "mongoPlan");
        }
        if (tableUpdate && JSON.stringify(await this.editableMetadata(tableUpdate.metadata.table, options)) !== JSON.stringify(tableUpdate.metadata))
          throw new StateQLError("STALE_PLAN", "Table metadata changed. Reload and plan again.");
        if (tableUpdates) {
          for (const update of tableUpdates) {
            if (JSON.stringify(await this.editableMetadata(update.metadata.table, options)) !== JSON.stringify(update.metadata))
              throw new StateQLError("STALE_PLAN", "Table metadata changed. Reload and plan again.");
          }
        }
        const context = this.executionContext(options);
        const adapterSource = await this.resolveConnectionSource(
          connection,
          session,
          "apply",
          "write",
          context,
        );
        const adapter = mongoPlan
          ? await this.openMongoAdapter(connection, context, adapterSource)
          : redisPlan
            ? await this.openRedisAdapter(connection, context, adapterSource)
            : await this.openAdapter(connection, context, adapterSource);
        try {
          if ((await adapter.signature()) !== claimed.state_signature) {
            throw new StateQLError(
              "STALE_PLAN",
              "Database state changed after this plan was created.",
            );
          }
        } catch (error) {
          if (error instanceof StateQLError) throw error;
          if (error instanceof AdapterExecutionError) {
            throw stoppedStateQLError(error, true);
          }
          throw new StateQLError(
            "QUERY_FAILED",
            safeCredentialErrorMessage(error, adapterSource),
            { retryable: true, executed: true },
          );
        } finally {
          await closeAdapterQuietly(adapter);
        }
        const result = tableUpdates && compiledUpdates
          ? await this.performTableBatch(session, connection, tableUpdates, compiledUpdates, context, { planId: claimed.id, claimToken }, adapterSource)
          : redisStored
            ? await this.performRedisExec(session, connection, redisStored.command, {}, context, { planId: claimed.id, claimToken }, adapterSource, redisStored.precondition)
            : mongoCommand
              ? await this.performMongoExec(
                  session,
                  connection,
                  mongoCommand,
                  {
                    allowUnbounded: Boolean(claimed.allow_unbounded),
                    allowDestructive: Boolean(claimed.allow_destructive),
                    ...(tableUpdate ? { expectedRows: 1 as const } : {}),
                  },
                  context,
                  { planId: claimed.id, claimToken },
                  adapterSource,
                )
              : await this.performExec(
                  session,
                  connection,
                  claimed.sql,
                  {
                    params: planParameters,
                    ...(tableUpdate ? { expectedRows: 1 as const } : {}),
                    allowUnbounded: Boolean(claimed.allow_unbounded),
                    allowDestructive: Boolean(claimed.allow_destructive),
                  },
                  context,
                  { planId: claimed.id, claimToken },
                  adapterSource,
                );
        return {
          ...result,
          data: { plan_id: claimed.id, ...result.data },
        };
      } catch (error) {
        if (
          error instanceof StateQLError &&
          error.details.code === "OUTCOME_UNKNOWN"
        ) {
          retainClaim = true;
        }
        throw error;
      } finally {
        if (!retainClaim) this.store.releasePlanClaim(plan.id, claimToken);
      }
    }, () => historySql);
  }

  async history(
    limit = 20,
    options: HistoryOptions = {},
  ): Promise<Response<HistoryData>> {
    return this.run("history", async (session) => {
      if (options.internal !== undefined && typeof options.internal !== "boolean") throw new StateQLError("INVALID_COMMAND", "History internal filter must be boolean.");
      return {
        data: {
          history: this.store
            .history(
              session.id,
              positiveInteger(limit, "limit"),
              {
                ...(options.origin === undefined ? {} : { origin: parseCommandOrigin(options.origin) }),
                ...(options.category === undefined ? {} : { category: parseHistoryCategory(options.category) }),
                ...(options.internal === undefined ? {} : { internal: options.internal }),
                offset: nonNegativeInteger(options.offset ?? 0, "offset"),
              },
            )
            .map(historyEntry),
        },
      };
    });
  }

  async doctor(): Promise<Response<DoctorData>> {
    return this.run("doctor", async (session) => ({
      data: this.store.diagnostics(session.id),
    }));
  }

  async purge(
    scope: "expired" | "results" | "history" | "all" = "expired",
  ): Promise<Response<PurgeData>> {
    return this.run("purge", async (session) => {
      if (!["expired", "results", "history", "all"].includes(scope)) {
        throw new StateQLError("INVALID_COMMAND", `Unknown purge scope "${scope}".`);
      }
      if (scope === "all" && session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Commit or roll back the active transaction before purging all data.",
        );
      }
      return {
        data: { scope, deleted: this.store.purge(session.id, scope) },
        executed: true,
      };
    });
  }

  async capabilities(): Promise<Response<CapabilitiesData>> {
    return this.run("capabilities", async () => ({
      data: {
        drivers: ["mongodb", "mysql", "postgres", "redis", "sqlite"],
        features: {
          result_handles: true,
          write_deduplication: true,
          transactions: true,
          query_plans: true,
          persistent_sessions: true,
          result_filtering: true,
          schema_inspection: true,
          credential_resolver: true,
          deadlines: true,
          cancellation: true,
          state_diagnostics: true,
          state_purge: true,
          state_quota: true,
          bounded_catalog: true,
          generated_aliases: true,
          multi_row_table_plans: true,
          history_classification: true,
        },
        driver_features: {
          mongodb: {
            sql: false,
            native_read: true,
            native_write: true,
            plans: true,
            transactions: true,
            transactions_require_replica_set: true,
            inspection: true,
          },
          redis: {
            sql: false,
            native_read: true,
            native_write: true,
            plans: true,
            transactions: false,
            guarded_single_key_writes: true,
            inspection: true,
          },
        },
      },
    }));
  }

  async executeCommand(
    command: BatchCommand,
    context: CommandExecutionContext = {},
  ): Promise<Response<unknown>> {
    let activeContext: CommandExecutionContext;
    try {
      activeContext = mergeCommandExecutionContext(
        this.commandContexts.getStore(),
        context,
      );
    } catch (error) {
      return this.batchFailure(errorMessage(error));
    }

    return this.commandContexts.run(activeContext, async () => {
    if (!command || typeof command !== "object") {
      return this.batchFailure("Batch command must be an object.");
    }

    try {
      switch (command.command) {
        case "connect":
          return this.connect(command.target, {
            name: command.name,
            readOnly: command.read_only,
            secretEnv: command.secret_env,
            credentialRef: command.credential_ref,
            passwordRef: command.password_ref ?? undefined,
            profile: command.profile,
            timeoutMs: command.timeout_ms,
          });
        case "disconnect":
          return this.disconnect();
        case "status":
          return this.status();
        case "profile.add":
          return this.addProfile(
            batchString(command.name, "name"),
            command.target,
            {
              readOnly: command.read_only ?? true,
              secretEnv: command.secret_env,
              credentialRef: command.credential_ref,
              passwordRef: command.password_ref ?? undefined,
            },
          );
        case "profile.update":
          return this.updateProfile(batchString(command.name, "name"), {
            ...(command.target !== undefined ? { target: command.target } : {}),
            ...(command.secret_env !== undefined ? { secretEnv: command.secret_env } : {}),
            ...(command.credential_ref !== undefined ? { credentialRef: command.credential_ref } : {}),
            ...(command.password_ref !== undefined ? { passwordRef: command.password_ref } : {}),
            ...(command.read_only !== undefined ? { readOnly: command.read_only } : {}),
          });
        case "profile.list":
          return this.listProfiles();
        case "profile.show":
          return this.showProfile(batchString(command.name, "name"));
        case "profile.remove":
          return this.removeProfile(batchString(command.name, "name"));
        case "session.start":
          return this.startSession(batchString(command.name, "name"));
        case "session.list":
          return this.listSessions();
        case "session.show":
          return this.showSession(command.name);
        case "session.summary":
          return this.sessionSummary();
        case "session.close":
          return this.closeSession();
        case "query": {
          const response = await this.query(batchString(command.sql, "sql"), {
            params: command.params ?? [],
            cache: command.cache ?? "auto",
            previewRows: command.preview_rows,
            timeoutMs: command.timeout_ms,
          });
          if (!response.ok || !command.as) return response;
          const resultId = response.data.result_id;
          if (typeof resultId !== "string") return response;
          this.store.setAlias(response.session_id, command.as, resultId);
          return {
            ...response,
            data: { ...response.data, alias: command.as },
          };
        }
        case "mongo.query": {
          const response = await this.mongoQuery(
            command.mongo as MongoReadCommand,
            {
              cache: command.cache ?? "auto",
              timeoutMs: command.timeout_ms,
            },
          );
          if (!response.ok || !command.as) return response;
          const resultId = response.data.result_id;
          if (typeof resultId !== "string") return response;
          this.store.setAlias(response.session_id, command.as, resultId);
          return {
            ...response,
            data: { ...response.data, alias: command.as },
          };
        }
        case "redis.query": {
          const response = await this.redisQuery(command.redis as RedisCommand, { cache: command.cache ?? "auto", timeoutMs: command.timeout_ms });
          if (!response.ok || !command.as) return response;
          this.store.setAlias(response.session_id, command.as, response.data.result_id);
          return { ...response, data: { ...response.data, alias: command.as } };
        }
        case "filter": {
          const response = await this.filter(
            batchString(command.handle, "handle"),
            batchString(command.where, "where"),
            { params: command.params ?? [] },
          );
          if (!response.ok || !command.as) return response;
          const resultId = response.data.result_id;
          if (typeof resultId !== "string") return response;
          this.store.setAlias(response.session_id, command.as, resultId);
          return {
            ...response,
            data: { ...response.data, alias: command.as },
          };
        }
        case "exec":
          return this.exec(batchString(command.sql, "sql"), {
            params: command.params ?? [],
            replay: command.replay ?? false,
            idempotencyKey: command.idempotency_key,
            allowUnbounded: command.allow_unbounded ?? false,
            allowDestructive: command.allow_destructive ?? false,
            timeoutMs: command.timeout_ms,
          });
        case "mongo.exec":
          return this.mongoExec(command.mongo as MongoWriteCommand, {
            replay: command.replay ?? false,
            idempotencyKey: command.idempotency_key,
            allowUnbounded: command.allow_unbounded ?? false,
            allowDestructive: command.allow_destructive ?? false,
            timeoutMs: command.timeout_ms,
          });
        case "redis.exec":
          return this.redisExec(command.redis as RedisCommand, {
            replay: command.replay ?? false,
            idempotencyKey: command.idempotency_key,
            timeoutMs: command.timeout_ms,
          });
        case "show":
          return this.show(batchString(command.handle, "handle"));
        case "rows":
          return this.rows(batchString(command.handle, "handle"), {
            offset: command.offset ?? 0,
            limit: command.limit ?? 20,
          });
        case "count":
          return this.count(batchString(command.handle, "handle"));
        case "columns":
          return this.columns(batchString(command.handle, "handle"));
        case "alias.set":
          return this.setAlias(
            batchString(command.name, "name"),
            batchString(command.handle, "handle"),
          );
        case "inspect":
          return this.inspect(batchString(command.kind, "kind"), command.table, {
            timeoutMs: command.timeout_ms,
          });
        case "objects.list":
          return this.listObjects({
            ...(command.kind ? { kind: command.kind as ListObjectsFilter["kind"] } : {}),
            ...(command.table ? { schema: command.table } : {}),
            ...(command.where ? { search: command.where } : {}),
            offset: command.cursor ?? command.offset ?? 0,
            limit: command.limit ?? 50,
          }, { timeoutMs: command.timeout_ms });
        case "object.describe":
          return this.describeObject(command.object as CatalogObject, { timeoutMs: command.timeout_ms });
        case "transaction.begin":
          return this.beginTransaction(command.isolation);
        case "transaction.status":
          return this.transactionStatus(command.handle);
        case "transaction.commit":
          return this.commitTransaction(command.handle, {
            timeoutMs: command.timeout_ms,
          });
        case "transaction.rollback":
          return this.rollbackTransaction(command.handle);
        case "plan":
          return this.plan(batchString(command.sql, "sql"), {
            params: command.params ?? [],
            allowUnbounded: command.allow_unbounded ?? false,
            allowDestructive: command.allow_destructive,
            timeoutMs: command.timeout_ms,
          });
        case "mongo.plan":
          return this.mongoPlan(command.mongo as MongoWriteCommand, {
            allowUnbounded: command.allow_unbounded ?? false,
            allowDestructive: command.allow_destructive,
            timeoutMs: command.timeout_ms,
          });
        case "redis.plan":
          return this.redisPlan(command.redis as RedisCommand, { timeoutMs: command.timeout_ms });
        case "apply":
          return this.apply(batchString(command.handle, "handle"), {
            timeoutMs: command.timeout_ms,
          });
        case "history":
          return this.history(command.limit ?? 20, {
            origin: command.history_origin,
            category: command.history_category,
            internal: command.history_internal,
            offset: command.offset,
          });
        case "receipt":
          return this.receipt(batchString(command.handle, "handle"));
        case "doctor":
          return this.doctor();
        case "purge":
          return this.purge(command.scope ?? "expired");
        case "capabilities":
          return this.capabilities();
        default:
          return this.batchFailure(
            `Unknown batch command "${String(command.command)}".`,
          );
      }
    } catch (error) {
      return this.batchFailure(errorMessage(error));
    }
    });
  }

  async *batch(
    commands: Iterable<BatchCommand> | AsyncIterable<BatchCommand>,
    options: BatchOptions = {},
  ): AsyncGenerator<Response<unknown>> {
    const maxCommands = options.maxCommands ?? 1_000;
    if (!Number.isInteger(maxCommands) || maxCommands < 1) {
      yield await this.batchFailure("maxCommands must be a positive integer.");
      return;
    }

    let count = 0;
    for await (const command of commands) {
      count += 1;
      if (count > maxCommands) {
        yield await this.batchFailure(
          `Batch cannot exceed ${maxCommands} commands.`,
        );
        return;
      }
      const response = await this.executeCommand(
        command,
        options.executionContext,
      );
      yield response;
      if (!response.ok && !options.continueOnError) return;
    }
  }

  private async performExec(
    session: SessionRecord,
    connection: ConnectionRecord,
    sql: string,
    options: ExecOptions & { expectedRows?: 1 },
    context: AdapterContext,
    planClaim?: { planId: string; claimToken: string },
    resolvedSource?: string,
  ): Promise<ActionResult<ExecData>> {
    this.rejectMongoSql(connection, "mongoExec");
    if (connection.read_only) {
      throw new StateQLError(
        "READ_ONLY_CONNECTION",
        "Connection is read-only.",
        { suggestedAction: "Reconnect with --read-write." },
      );
    }
    const analysis = analyzeSql(sql, connection.driver);
    if (analysis.read) {
      throw new StateQLError(
        "INVALID_SQL",
        "exec accepts write statements only; use query for reads.",
      );
    }
    if (analysis.unboundedMutation && !options.allowUnbounded) {
      throw new StateQLError(
        "UNBOUNDED_MUTATION",
        "Mutation has no WHERE clause.",
        { extra: { override_flag: "--allow-unbounded" } },
      );
    }
    if (analysis.destructive && !options.allowDestructive) {
      throw new StateQLError(
        "DESTRUCTIVE_OPERATION_BLOCKED",
        "Destructive operation requires an explicit override.",
        { extra: { override_flag: "--allow-destructive" } },
      );
    }

    if (
      options.idempotencyKey !== undefined &&
      !options.idempotencyKey.trim()
    ) {
      throw new StateQLError(
        "INVALID_COMMAND",
        "Idempotency key cannot be empty.",
      );
    }
    const parameters = options.params ?? [];
    if (
      analysis.requiresAutocommit &&
      (options.expectedRows !== undefined || sqlParametersLength(parameters) > 0)
    ) {
      throw new StateQLError(
        "INVALID_SQL",
        "Autocommit maintenance statements do not accept StateQL parameters or row-count preconditions.",
      );
    }
    const transactionId = session.active_transaction_id ?? undefined;
    if (analysis.requiresAutocommit && transactionId) {
      throw new StateQLError(
        "TRANSACTION_FAILED",
        `${analysis.statementType.toUpperCase()} cannot be staged in a transaction.`,
        { suggestedAction: "Rollback or commit the staged transaction, then run the maintenance statement separately." },
      );
    }
    const fingerprint = hash({
      sql: analysis.normalized,
      parameters,
      database: databaseIdentity(connection),
    });
    if (transactionId) {
      const transaction = this.store.getTransaction(transactionId);
      if (
        !transaction ||
        transaction.session_id !== session.id ||
        transaction.state !== "active" ||
        transaction.connection_id !== connection.id
      ) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Active transaction does not match the active connection.",
        );
      }
      if (transaction.owner_actor_id !== this.actorId) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          "Only the transaction owner may stage writes.",
        );
      }
    }
    const reservation = this.store.reserveOperation({
      sessionId: session.id,
      actorId: this.actorId,
      connectionId: connection.id,
      fingerprint,
      sql,
      parameters,
      statementType: analysis.statementType,
      status: transactionId ? "pending" : "executing",
      transactionId,
      replay: options.replay ?? false,
      idempotencyKey: options.idempotencyKey,
      stateVersionBefore: version(connection),
    });
    if (reservation.denied === "membership") {
      throw new StateQLError(
        "PERMISSION_DENIED",
        "Actor membership changed before the write was reserved.",
      );
    }
    if (reservation.denied === "transaction") {
      const active = this.store.getSession(session.id)?.active_transaction_id;
      const transaction = active ? this.store.getTransaction(active) : undefined;
      if (transaction && transaction.owner_actor_id !== this.actorId) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          "Only the transaction owner may stage writes.",
        );
      }
      throw new StateQLError(
        "TRANSACTION_FAILED",
        "The active transaction changed before the write was reserved.",
      );
    }
    const previous = reservation.previous;
    if (
      previous &&
      options.idempotencyKey &&
      !options.replay &&
      previous.fingerprint !== fingerprint
    ) {
      throw new StateQLError(
        "IDEMPOTENCY_CONFLICT",
        "Idempotency key was already used for a different write.",
        { extra: { previous_operation_id: previous.id } },
      );
    }
    if (previous && !reservation.operation) {
      if (
        previous.status === "executing" ||
        previous.status === "outcome_unknown"
      ) {
        throw new StateQLError(
          "OUTCOME_UNKNOWN",
          "A matching write has an unknown outcome.",
          {
            executed: true,
            suggestedAction:
              "Inspect database state, then use --replay only if another execution is safe.",
            extra: { previous_operation_id: previous.id },
          },
        );
      }
      if (options.idempotencyKey) {
        return {
          data: {
            ...operationData(previous),
            duplicate: true,
            duplicate_of: previous.id,
            idempotency_key: options.idempotencyKey,
          },
          handle: previous.id,
          cached: true,
          stateVersion:
            previous.state_version_after ?? previous.state_version_before,
        };
      }
      throw new StateQLError(
        "POTENTIAL_DUPLICATE_WRITE",
        "An equivalent operation was previously applied.",
        {
          extra: {
            previous_operation_id: previous.id,
            replay_required: true,
          },
        },
      );
    }

    const operation = reservation.operation!;
    if (transactionId) {
      return {
        data: operationData(operation),
        handle: operation.id,
        executed: false,
        stateVersion: version(connection),
      };
    }

    let adapter: Adapter;
    let adapterSource: string;
    try {
      adapterSource =
        resolvedSource ??
        (await this.resolveConnectionSource(
          connection,
          session,
          "exec",
          "write",
          context,
        ));
      adapter = await this.openAdapter(connection, context, adapterSource);
    } catch (error) {
      this.store.failOperation(operation.id);
      if (error instanceof StateQLError) throw error;
      throw new StateQLError("CONNECTION_FAILED", "Database connection failed.", {
        retryable: true,
      });
    }

    try {
      if (analysis.requiresAutocommit && !adapter.writeAutocommit) {
        throw new AdapterWriteError(
          `${analysis.statementType.toUpperCase()} requires adapter autocommit execution.`,
          false,
        );
      }
      const write = analysis.requiresAutocommit
        ? await adapter.writeAutocommit!(sql, parameters)
        : await adapter.write(sql, parameters, options.expectedRows);
      try {
        const finalized = planClaim
          ? this.store.finishPlannedOperation({
              planId: planClaim.planId,
              claimToken: planClaim.claimToken,
              operationId: operation.id,
              connectionId: connection.id,
              affectedRows: write.affectedRows,
            })
          : (() => {
              const stateVersion = this.store.bumpVersion(connection.id);
              return {
                operation: this.store.finishOperation(
                  operation.id,
                  write.affectedRows,
                  stateVersion,
                ),
                stateVersion,
              };
            })();
        const committed = finalized.operation;
        const after = finalized.stateVersion;
        return {
          data: {
            ...operationData(committed),
            duplicate: Boolean(previous),
            duplicate_override: Boolean(previous),
          },
          handle: committed.id,
          executed: true,
          stateVersion: after,
          confidence: adapter.confidence,
        };
      } catch (error) {
        this.store.markOperationOutcomeUnknown(operation.id);
        throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), {
          executed: true,
          suggestedAction:
            "Inspect database state before issuing any replacement write.",
        });
      }
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      if (error instanceof AdapterExecutionError && !error.outcomeUnknown) {
        this.store.failOperation(operation.id);
        throw stoppedStateQLError(error, false);
      }
      if (error instanceof AdapterWriteError && !error.outcomeUnknown) {
        this.store.failOperation(operation.id);
        if (error.message.startsWith("ROW_CONFLICT:")) throw new StateQLError("ROW_CONFLICT", "The row changed or no longer matches. Reload before editing.");
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          { executed: true },
        );
      }
      this.store.markOperationOutcomeUnknown(operation.id);
      throw new StateQLError(
        "OUTCOME_UNKNOWN",
        safeCredentialErrorMessage(error, adapterSource),
        {
          executed: true,
          suggestedAction:
            "Inspect database state, then use --replay only if another execution is safe.",
        },
      );
    } finally {
      try {
        await adapter.close();
      } catch {
        // Write outcome and metadata are already recorded.
      }
    }
  }

  private async performMongoExec(
    session: SessionRecord,
    connection: ConnectionRecord,
    command: MongoWriteCommand,
    options: MongoExecOptions & { expectedRows?: 1 },
    context: AdapterContext,
    planClaim?: { planId: string; claimToken: string },
    resolvedSource?: string,
  ): Promise<ActionResult<ExecData>> {
    const value = validatedMongoWrite(command);
    if (connection.driver !== "mongodb") {
      throw new StateQLError(
        "INVALID_COMMAND",
        "mongoExec requires an active MongoDB connection.",
      );
    }
    if (connection.read_only) {
      throw new StateQLError(
        "READ_ONLY_CONNECTION",
        "Connection is read-only.",
        { suggestedAction: "Reconnect with --read-write." },
      );
    }
    const safety = analyzeMongoWriteSafety(value);
    if (safety.unbounded && !options.allowUnbounded) {
      throw new StateQLError(
        "UNBOUNDED_MUTATION",
        "MongoDB mutation has an empty filter.",
        { extra: { override_flag: "--allow-unbounded" } },
      );
    }
    if (safety.destructive && !options.allowDestructive) {
      throw new StateQLError(
        "DESTRUCTIVE_OPERATION_BLOCKED",
        "Destructive MongoDB operation requires an explicit override.",
        { extra: { override_flag: "--allow-destructive" } },
      );
    }
    if (
      options.idempotencyKey !== undefined &&
      !options.idempotencyKey.trim()
    ) {
      throw new StateQLError(
        "INVALID_COMMAND",
        "Idempotency key cannot be empty.",
      );
    }

    const serializedCommand = serializeMongoCommand(value);
    const parameters: SqlParameters = [serializedCommand];
    const fingerprint = hash({
      command: serializedCommand,
      database: databaseIdentity(connection),
    });
    const transactionId = session.active_transaction_id ?? undefined;
    if (transactionId) {
      const transaction = this.store.getTransaction(transactionId);
      if (
        !transaction ||
        transaction.session_id !== session.id ||
        transaction.state !== "active" ||
        transaction.connection_id !== connection.id
      ) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Active transaction does not match the active connection.",
        );
      }
      if (transaction.owner_actor_id !== this.actorId) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          "Only the transaction owner may stage writes.",
        );
      }
    }
    const reservation = this.store.reserveOperation({
      sessionId: session.id,
      actorId: this.actorId,
      connectionId: connection.id,
      fingerprint,
      sql: mongoDescriptor(value.operation),
      parameters,
      statementType: `mongo.${value.operation}`,
      status: transactionId ? "pending" : "executing",
      transactionId,
      replay: options.replay ?? false,
      idempotencyKey: options.idempotencyKey,
      stateVersionBefore: version(connection),
    });
    if (reservation.denied === "membership") {
      throw new StateQLError(
        "PERMISSION_DENIED",
        "Actor membership changed before the write was reserved.",
      );
    }
    if (reservation.denied === "transaction") {
      const active = this.store.getSession(session.id)?.active_transaction_id;
      const transaction = active ? this.store.getTransaction(active) : undefined;
      if (transaction && transaction.owner_actor_id !== this.actorId) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          "Only the transaction owner may stage writes.",
        );
      }
      throw new StateQLError(
        "TRANSACTION_FAILED",
        "The active transaction changed before the write was reserved.",
      );
    }
    const previous = reservation.previous;
    if (
      previous &&
      options.idempotencyKey &&
      !options.replay &&
      previous.fingerprint !== fingerprint
    ) {
      throw new StateQLError(
        "IDEMPOTENCY_CONFLICT",
        "Idempotency key was already used for a different write.",
        { extra: { previous_operation_id: previous.id } },
      );
    }
    if (previous && !reservation.operation) {
      if (
        previous.status === "executing" ||
        previous.status === "outcome_unknown"
      ) {
        throw new StateQLError(
          "OUTCOME_UNKNOWN",
          "A matching write has an unknown outcome.",
          {
            executed: true,
            suggestedAction:
              "Inspect database state, then use --replay only if another execution is safe.",
            extra: { previous_operation_id: previous.id },
          },
        );
      }
      if (options.idempotencyKey) {
        return {
          data: {
            ...operationData(previous),
            duplicate: true,
            duplicate_of: previous.id,
            idempotency_key: options.idempotencyKey,
          },
          handle: previous.id,
          cached: true,
          stateVersion:
            previous.state_version_after ?? previous.state_version_before,
        };
      }
      throw new StateQLError(
        "POTENTIAL_DUPLICATE_WRITE",
        "An equivalent operation was previously applied.",
        {
          extra: {
            previous_operation_id: previous.id,
            replay_required: true,
          },
        },
      );
    }

    const operation = reservation.operation!;
    if (transactionId) {
      return {
        data: operationData(operation),
        handle: operation.id,
        executed: false,
        stateVersion: version(connection),
      };
    }

    let adapter: MongoAdapter;
    let adapterSource: string;
    try {
      adapterSource =
        resolvedSource ??
        (await this.resolveConnectionSource(
          connection,
          session,
          "exec",
          "write",
          context,
        ));
      adapter = await this.openMongoAdapter(connection, context, adapterSource);
    } catch (error) {
      this.store.failOperation(operation.id);
      if (error instanceof StateQLError) throw error;
      throw new StateQLError("CONNECTION_FAILED", "Database connection failed.", {
        retryable: true,
      });
    }

    try {
      const write = await adapter.write(value, options.expectedRows);
      try {
        const finalized = planClaim
          ? this.store.finishPlannedOperation({
              planId: planClaim.planId,
              claimToken: planClaim.claimToken,
              operationId: operation.id,
              connectionId: connection.id,
              affectedRows: write.affectedRows,
              outcome: write.outcome,
            })
          : (() => {
              const stateVersion = this.store.bumpVersion(connection.id);
              return {
                operation: this.store.finishOperation(
                  operation.id,
                  write.affectedRows,
                  stateVersion,
                  write.outcome,
                ),
                stateVersion,
              };
            })();
        const committed = finalized.operation;
        const after = finalized.stateVersion;
        return {
          data: {
            ...operationData(committed),
            duplicate: Boolean(previous),
            duplicate_override: Boolean(previous),
          },
          handle: committed.id,
          executed: true,
          stateVersion: after,
          confidence: adapter.confidence,
        };
      } catch (error) {
        this.store.markOperationOutcomeUnknown(operation.id);
        throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), {
          executed: true,
          suggestedAction:
            "Inspect database state before issuing any replacement write.",
        });
      }
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      if (error instanceof AdapterExecutionError && !error.outcomeUnknown) {
        this.store.failOperation(operation.id);
        throw stoppedStateQLError(error, false);
      }
      if (error instanceof AdapterWriteError && !error.outcomeUnknown) {
        this.store.failOperation(operation.id);
        if (error.message.startsWith("ROW_CONFLICT:")) throw new StateQLError("ROW_CONFLICT", "The document changed or was removed. Reload before editing.");
        throw new StateQLError(
          "QUERY_FAILED",
          safeCredentialErrorMessage(error, adapterSource),
          { executed: true },
        );
      }
      this.store.markOperationOutcomeUnknown(operation.id);
      throw new StateQLError(
        "OUTCOME_UNKNOWN",
        safeCredentialErrorMessage(error, adapterSource),
        {
          executed: true,
          suggestedAction:
            "Inspect database state, then use --replay only if another execution is safe.",
        },
      );
    } finally {
      try {
        await adapter.close();
      } catch {
        // Write outcome and metadata are already recorded.
      }
    }
  }

  private async performRedisExec(
    session: SessionRecord,
    connection: ConnectionRecord,
    command: RedisCommand,
    options: RedisExecOptions,
    context: AdapterContext,
    planClaim?: { planId: string; claimToken: string },
    resolvedSource?: string,
    precondition?: RedisPrecondition,
  ): Promise<ActionResult<ExecData>> {
    const value = validatedRedisWrite(command);
    if (connection.driver !== "redis") throw new StateQLError("INVALID_COMMAND", "redisExec requires an active Redis connection.");
    if (connection.read_only) throw new StateQLError("READ_ONLY_CONNECTION", "Connection is read-only.");
    if (options.idempotencyKey !== undefined && !options.idempotencyKey.trim()) throw new StateQLError("INVALID_COMMAND", "Idempotency key cannot be empty.");
    const serialized = serializeRedisCommand(value);
    const fingerprint = hash({ command: serialized, database: databaseIdentity(connection) });
    const reservation = this.store.reserveOperation({
      sessionId: session.id, actorId: this.actorId, connectionId: connection.id, fingerprint,
      sql: `Redis native ${value.command}`, parameters: [serialized], statementType: `redis.${value.command.toLowerCase()}`,
      status: "executing", replay: options.replay ?? false, idempotencyKey: options.idempotencyKey,
      stateVersionBefore: version(connection),
    });
    if (reservation.denied) throw new StateQLError(reservation.denied === "membership" ? "PERMISSION_DENIED" : "TRANSACTION_FAILED", "Redis write reservation was denied.");
    const previous = reservation.previous;
    if (previous && options.idempotencyKey && !options.replay && previous.fingerprint !== fingerprint) {
      throw new StateQLError("IDEMPOTENCY_CONFLICT", "Idempotency key was already used for a different write.");
    }
    if (previous && !reservation.operation) {
      if (previous.status === "executing" || previous.status === "outcome_unknown") throw new StateQLError("OUTCOME_UNKNOWN", "A matching Redis write has an unknown outcome.", { executed: true });
      if (options.idempotencyKey) return { data: { ...operationData(previous), duplicate: true, duplicate_of: previous.id, idempotency_key: options.idempotencyKey }, handle: previous.id, cached: true, stateVersion: previous.state_version_after ?? previous.state_version_before };
      throw new StateQLError("POTENTIAL_DUPLICATE_WRITE", "An equivalent Redis operation was previously applied.", { extra: { previous_operation_id: previous.id, replay_required: true } });
    }
    const operation = reservation.operation!;
    let adapter: RedisAdapter;
    let source: string;
    try {
      source = resolvedSource ?? await this.resolveConnectionSource(connection, session, "exec", "write", context);
      adapter = await this.openRedisAdapter(connection, context, source);
    } catch (error) {
      this.store.failOperation(operation.id);
      if (error instanceof StateQLError) throw error;
      throw new StateQLError("CONNECTION_FAILED", "Redis connection failed.", { retryable: true });
    }
    try {
      const write = await adapter.write(value, precondition);
      try {
        const finalized = planClaim
          ? this.store.finishPlannedOperation({ planId: planClaim.planId, claimToken: planClaim.claimToken, operationId: operation.id,
              connectionId: connection.id, affectedRows: write.affectedRows, outcome: write.outcome })
          : (() => { const stateVersion = this.store.bumpVersion(connection.id); return { operation: this.store.finishOperation(operation.id, write.affectedRows, stateVersion, write.outcome), stateVersion }; })();
        return { data: { ...operationData(finalized.operation), duplicate: Boolean(previous), duplicate_override: Boolean(previous) },
          handle: finalized.operation.id, executed: true, stateVersion: finalized.stateVersion, confidence: adapter.confidence };
      } catch (error) {
        this.store.markOperationOutcomeUnknown(operation.id);
        throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), { executed: true, suggestedAction: "Inspect Redis state before issuing a replacement write." });
      }
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      if ((error instanceof AdapterExecutionError || error instanceof AdapterWriteError) && !error.outcomeUnknown) {
        this.store.failOperation(operation.id);
        if (error.message.startsWith("ROW_CONFLICT:")) throw new StateQLError("ROW_CONFLICT", "The Redis key changed. Reload and plan again.");
        throw error instanceof AdapterExecutionError ? stoppedStateQLError(error, false) : new StateQLError("QUERY_FAILED", safeCredentialErrorMessage(error, source), { executed: true });
      }
      this.store.markOperationOutcomeUnknown(operation.id);
      throw new StateQLError("OUTCOME_UNKNOWN", safeCredentialErrorMessage(error, source), { executed: true, suggestedAction: "Inspect Redis state before issuing a replacement write." });
    } finally { await closeAdapterQuietly(adapter); }
  }

  private async performTableBatch(
    session: SessionRecord,
    connection: ConnectionRecord,
    updates: TableUpdate[],
    compiled: Array<ReturnType<typeof compileTableUpdate>>,
    context: AdapterContext,
    planClaim: { planId: string; claimToken: string },
    source: string,
  ): Promise<ActionResult<ExecData>> {
    if (connection.driver === "redis") throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support table edit batches.");
    const reservation = this.store.reserveOperation({
      sessionId: session.id, actorId: this.actorId, connectionId: connection.id,
      fingerprint: hash({ plan: planClaim.planId, updates }), sql: `Conditional table update batch (${updates.length} rows)`,
      parameters: [JSON.stringify({ version: 1, updates })], statementType: "table.updates", status: "executing",
      replay: true, stateVersionBefore: version(connection),
    });
    if (!reservation.operation) throw new StateQLError("STALE_PLAN", "Table edit batch could not be reserved.");
    const operation = reservation.operation;
    const adapter = connection.driver === "mongodb"
      ? await this.openMongoAdapter(connection, context, source)
      : await this.openAdapter(connection, context, source);
    try {
      let affectedRows: number;
      if (connection.driver === "mongodb") {
        const commands = compiled.map((item) => {
          if (!item.mongo) throw new StateQLError("STALE_PLAN", "Table edit batch contains mixed drivers.");
          return item.mongo;
        });
        const results = await (adapter as MongoAdapter).writeBatch(commands, "snapshot", true);
        affectedRows = results.reduce((sum, item) => sum + item.affectedRows, 0);
      } else {
        const operations: BatchWriteOperation[] = compiled.map((item, index) => ({
          ...operation, id: `${operation.id}:${index}`, sql: item.sql, parameters: JSON.stringify(item.params), statement_type: "update", expectedRows: 1,
        }));
        const results = await (adapter as Adapter).writeBatch(operations, "serializable");
        affectedRows = results.reduce((sum, item) => sum + item.affectedRows, 0);
      }
      try {
        const finalized = this.store.finishPlannedOperation({ planId: planClaim.planId, claimToken: planClaim.claimToken,
          operationId: operation.id, connectionId: connection.id, affectedRows });
        return { data: operationData(finalized.operation), handle: finalized.operation.id, executed: true,
          stateVersion: finalized.stateVersion, confidence: adapter.confidence };
      } catch (error) {
        this.store.markOperationOutcomeUnknown(operation.id);
        throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), { executed: true, suggestedAction: "Inspect every edited row before retrying." });
      }
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      if ((error instanceof BatchWriteError || error instanceof AdapterExecutionError) && !error.outcomeUnknown) {
        this.store.failOperation(operation.id);
        if (error.message.startsWith("ROW_CONFLICT:")) throw new StateQLError("ROW_CONFLICT", "At least one row changed; the entire edit batch was rolled back.");
        throw error instanceof AdapterExecutionError ? stoppedStateQLError(error, false) : new StateQLError("QUERY_FAILED", safeCredentialErrorMessage(error, source), { executed: true });
      }
      this.store.markOperationOutcomeUnknown(operation.id);
      throw new StateQLError("OUTCOME_UNKNOWN", safeCredentialErrorMessage(error, source), { executed: true, suggestedAction: "Inspect every edited row before retrying." });
    } finally { await closeAdapterQuietly(adapter); }
  }


  private batchFailure(message: string): Promise<Response<unknown>> {
    return this.run("batch", async () => {
      throw new StateQLError("INVALID_COMMAND", message);
    });
  }

  private async withResult<T>(
    command: string,
    idOrAlias: string,
    action: (result: ResultRecord, session: SessionRecord) => Promise<ActionResult<T>>,
  ): Promise<Response<T>> {
    return this.run(command, async (session) => {
      const result = this.requireResult(idOrAlias, session);
      if (Date.parse(result.expires_at) <= this.now().getTime()) {
        throw new StateQLError(
          "RESULT_EXPIRED",
          `Result "${result.id}" has expired.`,
          {
            retryable: true,
            suggestedAction: "Run the original query again.",
          },
        );
      }
      return action(result, session);
    });
  }

  private rejectDuringStagedTransaction(
    session: SessionRecord,
    operation: string,
  ): void {
    if (!session.active_transaction_id) return;
    throw new StateQLError(
      "TRANSACTION_FAILED",
      `${operation} cannot run while a staged transaction is active.`,
      { suggestedAction: "Commit or roll back the transaction first." },
    );
  }

  private requireResult(
    idOrAlias: string,
    session: SessionRecord,
  ): ResultRecord {
    const result = this.store.getResult(idOrAlias, session.id);
    if (!result) {
      throw new StateQLError(
        "RESULT_NOT_FOUND",
        `Result "${idOrAlias}" was not found.`,
      );
    }
    return result;
  }

  private requireConnection(session: SessionRecord): ConnectionRecord {
    const connection = this.store.activeConnection(session);
    if (!connection) {
      throw new StateQLError(
        "CONNECTION_NOT_FOUND",
        "No active connection.",
        { suggestedAction: "Run stql connect first." },
      );
    }
    return connection;
  }

  private requireMongoConnection(
    session: SessionRecord,
    method: "mongoQuery" | "mongoExec" | "mongoPlan",
  ): ConnectionRecord {
    const connection = this.requireConnection(session);
    if (connection.driver !== "mongodb") {
      throw new StateQLError(
        "INVALID_COMMAND",
        `${method} requires an active MongoDB connection.`,
      );
    }
    return connection;
  }

  private requireRedisConnection(
    session: SessionRecord,
    method: "redisQuery" | "redisExec" | "redisPlan",
  ): ConnectionRecord {
    const connection = this.requireConnection(session);
    if (connection.driver !== "redis") throw new StateQLError("INVALID_COMMAND", `${method} requires an active Redis connection.`);
    return connection;
  }


  private rejectMongoSql(
    connection: ConnectionRecord,
    nativeMethod: "mongoQuery" | "mongoExec" | "mongoPlan",
  ): void {
    if (connection.driver !== "mongodb" && connection.driver !== "redis") return;
    const method = connection.driver === "redis"
      ? nativeMethod === "mongoQuery" ? "redisQuery" : nativeMethod === "mongoExec" ? "redisExec" : "redisPlan"
      : nativeMethod;
    const name = connection.driver === "redis" ? "Redis" : "MongoDB";
    throw new StateQLError(
      "INVALID_COMMAND",
      `SQL is not supported for ${name} connections; use ${method} instead.`,
      { suggestedAction: `Use ${method} with a native ${name} command.` },
    );
  }

  private requireActiveTransaction(
    session: SessionRecord,
    id?: string,
  ): NonNullable<ReturnType<StateStore["getTransaction"]>> {
    const transactionId = id ?? session.active_transaction_id;
    if (!transactionId) {
      throw new StateQLError("TRANSACTION_NOT_FOUND", "No active transaction.");
    }
    const transaction = this.store.getTransaction(transactionId);
    if (
      !transaction ||
      transaction.session_id !== session.id ||
      transaction.state !== "active" ||
      session.active_transaction_id !== transaction.id
    ) {
      throw new StateQLError(
        "TRANSACTION_NOT_FOUND",
        `Active transaction "${transactionId}" was not found.`,
      );
    }
    if (transaction.owner_actor_id !== this.actorId) {
      throw new StateQLError(
        "PERMISSION_DENIED",
        "Only the transaction owner may control it.",
      );
    }
    return transaction;
  }

  private requireSelectedSession(current: SessionRecord, selected: string): void {
    if (selected !== current.id && selected !== current.name) {
      throw new StateQLError(
        "PERMISSION_DENIED",
        "Membership can only be managed for the selected session.",
      );
    }
  }

  private validateActorId(actorId: string): void {
    if (!actorId.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Actor ID is required.");
    }
  }

  private throwMembershipDenied(session: SessionRecord): never {
    throw new StateQLError(
      "PERMISSION_DENIED",
      `Actor "${this.actorId}" is not attached to session "${session.name}".`,
    );
  }

  private async resolveConnectionSource(
    connection: ConnectionRecord,
    session: SessionRecord,
    operation: CredentialOperation,
    access: CredentialAccess,
    context: AdapterContext,
  ): Promise<string> {
    const references = [connection.secret_env, connection.credential_ref, connection.password_ref]
      .filter((value) => value !== null);
    if (references.length > 1) {
      throw new StateQLError("STATE_CORRUPTED", "Connection has ambiguous credential references.");
    }
    if (connection.password_ref !== null) {
      validateCredentialRef(connection.password_ref);
      const driver = validatePasswordReferenceTarget(connection.source);
      if (driver !== connection.driver) {
        throw new StateQLError("STATE_CORRUPTED", "Password-reference target driver does not match the stored connection.");
      }
      const password = await this.resolveCredential(
        connection.password_ref,
        "password_ref",
        session,
        operation,
        access,
        context,
        {
          connection: {
            id: connection.id,
            name: connection.name,
            driver: connection.driver,
            database: connection.database_name,
            readOnly: Boolean(connection.read_only),
          },
        },
        connection.source,
      );
      return injectPassword(connection.source, password).source;
    }
    const reference = connection.secret_env ?? connection.credential_ref;
    if (!reference) return connection.source;
    const source: Exclude<CredentialSource, "password_ref"> = connection.secret_env
      ? "secret_env"
      : "credential_ref";
    const value = await this.resolveCredential(
      reference,
      source,
      session,
      operation,
      access,
      context,
      {
        connection: {
          id: connection.id,
          name: connection.name,
          driver: connection.driver,
          database: connection.database_name,
          readOnly: Boolean(connection.read_only),
        },
      },
    );
    return credentialSource(value, connection.driver, source).source;
  }

  private async resolveCredential(
    reference: string,
    source: CredentialSource,
    session: SessionRecord,
    operation: CredentialOperation,
    access: CredentialAccess,
    context: AdapterContext,
    details: Pick<
      CredentialRequest,
      "profile" | "requestedReadOnly" | "connection"
    > = {},
    passwordTarget?: string,
  ): Promise<string> {
    const resolver = this.credentialResolver;
    const credentialContext = createAdapterContext(
      this.credentialTimeoutMs,
      context.signal,
    );
    let value: string | undefined;
    if (!resolver) {
      if (credentialContext.signal?.aborted) {
        throw credentialStateQLError(
          reference,
          new CredentialResolutionError("cancelled"),
        );
      }
      if (credentialContext.deadline <= Date.now()) {
        throw credentialStateQLError(
          reference,
          new CredentialResolutionError("timeout"),
        );
      }
      if (source === "secret_env") value = env[reference];
    } else {
      const baseRequest = {
        reference,
        actorId: this.actorId,
        session: { id: session.id, name: session.name },
        operation,
        access,
        ...(context.signal ? { signal: context.signal } : {}),
        ...details,
      };
      const request: CredentialRequest = source === "password_ref"
        ? { ...baseRequest, source, target: passwordTarget! }
        : { ...baseRequest, source };
      try {
        value = await resolveCredentialBeforeDeadline(
          resolver,
          request,
          credentialContext,
        );
      } catch (error) {
        throw credentialStateQLError(reference, error);
      }
    }
    if (value === undefined || (source !== "password_ref" && value === "")) {
      throw credentialStateQLError(
        reference,
        new CredentialResolutionError("unavailable"),
      );
    }
    context.deadline = Date.now() + (context.timeoutMs ?? this.timeoutMs);
    return value;
  }

  private async openAdapter(
    connection: ConnectionRecord,
    context: AdapterContext,
    source: string,
  ): Promise<Adapter> {
    try {
      return await createAdapter(connection, context, { source });
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      throw new StateQLError(
        "CONNECTION_FAILED",
        safeCredentialErrorMessage(error, source),
        { retryable: true },
      );
    }
  }

  private async openMongoAdapter(
    connection: ConnectionRecord,
    context: AdapterContext,
    source: string,
  ): Promise<MongoAdapter> {
    try {
      return new MongoAdapter(connection, context, { source });
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      throw new StateQLError(
        "CONNECTION_FAILED",
        safeCredentialErrorMessage(error, source),
        { retryable: true },
      );
    }
  }

  private async openRedisAdapter(
    connection: ConnectionRecord,
    context: AdapterContext,
    source: string,
  ): Promise<RedisAdapter> {
    try {
      if (connection.driver !== "redis") throw new Error("Redis adapter requires a Redis connection.");
      return new RedisAdapter(source, Boolean(connection.read_only), context);
    } catch (error) {
      if (error instanceof StateQLError) throw error;
      throw new StateQLError("CONNECTION_FAILED", safeCredentialErrorMessage(error, source), { retryable: true });
    }
  }


  private executionContext(options: ExecutionOptions): AdapterContext {
    return createAdapterContext(
      executionTimeout(options.timeoutMs ?? this.timeoutMs),
      combineAbortSignals(
        options.signal,
        this.commandContexts.getStore()?.signal,
        this.signal,
      ),
    );
  }

  private resultData(
    result: ResultRecord,
    cached: boolean,
    previewRows = this.previewRows,
  ): ResultData {
    const rows = this.store.resultRows(result);
    const preview = compactRows(
      rows.slice(0, previewRows),
      this.maxCellCharacters,
    );
    return {
      result_id: result.id,
      alias: result.alias ?? this.store.generatedAlias(result.id),
      display_alias: result.alias ?? this.store.generatedAlias(result.id),
      rows: result.row_count,
      columns: this.store.resultColumns(result),
      preview,
      preview_count: preview.length,
      truncated: preview.length < result.row_count,
      cached,
      ...(cached ? { duplicate_of: result.id } : {}),
      ...(result.sql.startsWith("Redis native ") ? { next_cursor: redisResultCursor(result.parameters) } : {}),
      state_version: result.state_version,
      storage: {
        mode: "materialized",
        expires_at: result.expires_at,
      },
    };
  }

  private cacheValid(
    result: ResultRecord,
    stateVersion: string,
    stateSignature: string,
  ): boolean {
    return (
      Date.parse(result.expires_at) > this.now().getTime() &&
      Date.parse(result.created_at) + this.cacheTtlSeconds * 1000 >
        this.now().getTime() &&
      result.state_version === stateVersion &&
      result.state_signature === stateSignature
    );
  }

  private async run<T>(
    command: string,
    action: (session: SessionRecord) => Promise<ActionResult<T>>,
    historySql?: string | (() => string | undefined),
    historyTarget?: string,
  ): Promise<Response<T>> {
    const started = performance.now();
    const commandContext = this.commandContexts.getStore();
    const origin = commandContext?.origin ?? "legacy";
    const category = historyCategory(command);
    const internal = commandContext?.internal ?? false;
    let session = this.store.ensureSession(this.sessionName);
    if (!this.store.isSessionMember(session.id, this.actorId)) {
      const commandId = this.store.randomId("cmd");
      const error = new StateQLError(
        "PERMISSION_DENIED",
        `Actor "${this.actorId}" is not attached to session "${session.name}".`,
      );
      return {
        ok: false,
        command_id: commandId,
        session_id: session.id,
        error: error.details,
        meta: {
          duration_ms: Math.round((performance.now() - started) * 1000) / 1000,
        },
      } satisfies Failure;
    }
    try {
      const result = await action(session);
      const responseSession = result.session ?? session;
      const sqlText = resolveHistorySql(historySql);
      const history = this.store.addHistory({
        sessionId: session.id,
        actorId: this.actorId,
        origin,
        category,
        internal,
        command,
        target: historyTarget,
        ...(result.handle ? { handle: result.handle } : {}),
        ...(sqlText !== undefined ? { sql: sqlText } : {}),
        executed: result.executed ?? false,
        cached: result.cached ?? false,
        success: true,
      });
      return {
        ok: true,
        command_id: history.id,
        session_id: responseSession.id,
        data: result.data,
        warnings: result.warnings ?? [],
        meta: {
          duration_ms: Math.round((performance.now() - started) * 1000) / 1000,
          ...(result.stateVersion
            ? { state_version: result.stateVersion }
            : {}),
          ...(result.confidence
            ? { state_confidence: result.confidence }
            : {}),
        },
      } satisfies Success<T>;
    } catch (error) {
      const stateqlError = asStateQLError(error);
      const sqlText = resolveHistorySql(historySql);
      const history = this.store.addHistory({
        sessionId: session.id,
        actorId: this.actorId,
        origin,
        category,
        internal,
        command,
        target: historyTarget,
        ...(sqlText !== undefined ? { sql: sqlText } : {}),
        executed: stateqlError.details.executed,
        cached: false,
        success: false,
        errorCode: stateqlError.details.code,
      });
      return {
        ok: false,
        command_id: history.id,
        session_id: session.id,
        error: stateqlError.details,
        meta: {
          duration_ms: Math.round((performance.now() - started) * 1000) / 1000,
        },
      } satisfies Failure;
    }
  }
}

function resolveHistorySql(
  sql?: string | (() => string | undefined),
): string | undefined {
  return typeof sql === "function" ? sql() : sql;
}

function historyEntry(item: HistoryRecord): HistoryEntry {
  return {
    command_id: item.id,
    timestamp: item.timestamp,
    session_id: item.session_id,
    actor_id: item.actor_id,
    origin: item.origin,
    category: item.category,
    internal: Boolean(item.internal),
    command: item.command,
    sql: item.sql,
    ...(item.target ? { target: item.target } : {}),
    handle: item.handle,
    executed: Boolean(item.executed),
    cached: Boolean(item.cached),
    success: Boolean(item.success),
    error_code: item.error_code,
  };
}

const COMMAND_ORIGINS = new Set<CommandOrigin>([
  "legacy",
  "user",
  "model",
  "system",
  "api",
]);

function parseCommandOrigin(value: unknown): CommandOrigin {
  if (typeof value === "string" && COMMAND_ORIGINS.has(value as CommandOrigin)) {
    return value as CommandOrigin;
  }
  throw new StateQLError(
    "INVALID_COMMAND",
    `Unknown command origin "${String(value)}".`,
  );
}

function mergeCommandExecutionContext(
  inherited: CommandExecutionContext | undefined,
  supplied: CommandExecutionContext,
): CommandExecutionContext {
  if (!supplied || typeof supplied !== "object") {
    throw new StateQLError(
      "INVALID_COMMAND",
      "Command execution context must be an object.",
    );
  }
  if (supplied.signal !== undefined && !(supplied.signal instanceof AbortSignal)) {
    throw new StateQLError(
      "INVALID_COMMAND",
      "Command execution context signal must be an AbortSignal.",
    );
  }
  if (supplied.internal !== undefined && typeof supplied.internal !== "boolean") {
    throw new StateQLError("INVALID_COMMAND", "Command execution context internal must be boolean.");
  }
  return {
    signal: combineAbortSignals(inherited?.signal, supplied.signal),
    origin:
      supplied.origin === undefined
        ? inherited?.origin
        : parseCommandOrigin(supplied.origin),
    internal: supplied.internal ?? inherited?.internal,
  };
}

function combineAbortSignals(
  ...signals: Array<AbortSignal | undefined>
): AbortSignal | undefined {
  const present = signals.filter(
    (signal): signal is AbortSignal => signal !== undefined,
  );
  if (present.length === 0) return undefined;
  if (present.length === 1) return present[0];
  return AbortSignal.any(present);
}

function markTransactionOutcomeUnknown(
  store: StateStore,
  transactionId: string,
  sessionId: string,
  actorId: string,
): void {
  try {
    store.markTransactionOutcomeUnknown(transactionId, sessionId, actorId);
  } catch {
    // A stale committing transaction is recovered as unknown after five minutes.
  }
}

function sqlParametersLength(parameters: SqlParameters): number {
  return Array.isArray(parameters)
    ? parameters.length
    : Object.keys(parameters).length;
}

function boundedReadSql(sql: string, limit: number): string {
  const statement = sql.trim().replace(/;\s*$/, "");
  return `SELECT * FROM (${statement}) AS _stateql_bounded LIMIT ${limit}`;
}

function validatedMongoRead(command: unknown): MongoReadCommand {
  try {
    return validateMongoReadCommand(command);
  } catch (error) {
    throw new StateQLError("INVALID_COMMAND", errorMessage(error));
  }
}

function validatedMongoWrite(command: unknown): MongoWriteCommand {
  try {
    return validateMongoWriteCommand(command);
  } catch (error) {
    throw new StateQLError("INVALID_COMMAND", errorMessage(error));
  }
}

function validatedRedisRead(command: unknown): RedisCommand {
  try { return validateRedisReadCommand(command); }
  catch (error) { throw new StateQLError("INVALID_COMMAND", errorMessage(error)); }
}

function validatedRedisWrite(command: unknown): RedisCommand {
  try { return validateRedisWriteCommand(command); }
  catch (error) { throw new StateQLError("INVALID_COMMAND", errorMessage(error)); }
}



function mongoDescriptor(operation: string): string {
  return `MongoDB native ${operation}`;
}

function mongoPaginationWarnings(command: MongoReadCommand): Warning[] {
  const ordered = command.operation === "find"
    ? command.options?.sort !== undefined
    : command.pipeline.some((stage) =>
        Object.prototype.hasOwnProperty.call(stage, "$sort")
      );
  return ordered
    ? []
    : [{
        code: "NON_DETERMINISTIC_PAGINATION",
        message: "MongoDB result has no explicit sort.",
      }];
}

function storedMongoOperation(operation: OperationRecord): MongoWriteCommand {
  if (!operation.statement_type.startsWith("mongo.")) {
    throw new StateQLError(
      "TRANSACTION_FAILED",
      "MongoDB transaction contains a mixed or corrupt native payload.",
    );
  }
  return storedMongoWrite(
    operation.parameters,
    operation.statement_type,
    `operation "${operation.id}"`,
    "TRANSACTION_FAILED",
  );
}

function storedMongoPlan(
  parameters: string,
  statementType: string,
  planId: string,
): MongoWriteCommand {
  return storedMongoWrite(
    parameters,
    statementType,
    `plan "${planId}"`,
    "STALE_PLAN",
  );
}

function storedMongoWrite(
  parameters: string,
  statementType: string,
  label: string,
  errorCode: string,
): MongoWriteCommand {
  try {
    const payload = JSON.parse(parameters) as unknown;
    if (
      !Array.isArray(payload) ||
      payload.length !== 1 ||
      typeof payload[0] !== "string"
    ) {
      throw new Error("payload must contain one EJSON command string");
    }
    const command = deserializeMongoWriteCommand(payload[0]);
    if (statementType !== `mongo.${command.operation}`) {
      throw new Error("operation does not match its statement type");
    }
    return command;
  } catch {
    throw new StateQLError(
      errorCode,
      `Stored MongoDB ${label} payload is invalid.`,
    );
  }
}

function storedRedisPlan(parameters: string, statementType: string, planId: string): { command: RedisCommand; precondition: RedisPrecondition } {
  try {
    const outer = JSON.parse(parameters) as unknown;
    if (!Array.isArray(outer) || outer.length !== 1 || typeof outer[0] !== "string") throw new Error();
    const payload = JSON.parse(outer[0]) as { command?: unknown; precondition?: unknown };
    if (typeof payload.command !== "string" || !payload.precondition || typeof payload.precondition !== "object") throw new Error();
    const command = deserializeRedisCommand(payload.command);
    const precondition = payload.precondition as RedisPrecondition;
    if (typeof precondition.key !== "string" || typeof precondition.fingerprint !== "string" || typeof precondition.expiresAt !== "number" || !Number.isFinite(precondition.expiresAt) || statementType !== `redis.${command.command.toLowerCase()}`) throw new Error();
    return { command, precondition };
  } catch {
    throw new StateQLError("STALE_PLAN", `Stored Redis plan "${planId}" payload is invalid.`);
  }
}

function redisResultCursor(parameters: string): string | null {
  try {
    const value = JSON.parse(parameters) as unknown;
    return Array.isArray(value) && (typeof value[1] === "string" || value[1] === null) ? value[1] : null;
  } catch { return null; }
}

function tableUpdateIdentity(metadata: EditableTable, row: Row): string {
  const identity = metadata.driver === "mongodb"
    ? { table: metadata.table, id: row._id }
    : { table: metadata.table, keys: metadata.columns.filter((column) => column.key > 0).sort((a, b) => a.key - b.key).map((column) => [column.name, row[column.name]]) };
  return hash(identity);
}

function validateCatalogFilter(filter: ListObjectsFilter): void {
  if (!filter || typeof filter !== "object" || Array.isArray(filter) || Object.keys(filter).some((key) => !["kind", "schema", "search", "offset", "limit"].includes(key))) throw new StateQLError("INVALID_COMMAND", "Catalog filter contains unknown fields.");
  if (filter.kind !== undefined && !["table", "view", "collection", "function", "trigger", "enum", "key"].includes(filter.kind)) throw new StateQLError("INVALID_COMMAND", "Unknown catalog object kind.");
  for (const [name, value] of [["schema", filter.schema], ["search", filter.search]] as const) if (value !== undefined && (typeof value !== "string" || !value || value.length > 200 || value.includes("\0"))) throw new StateQLError("INVALID_COMMAND", `Catalog ${name} is invalid.`);
  if (filter.offset !== undefined && !((typeof filter.offset === "number" && Number.isSafeInteger(filter.offset) && filter.offset >= 0) || (typeof filter.offset === "string" && /^\d+$/.test(filter.offset)))) throw new StateQLError("INVALID_COMMAND", "Catalog offset is invalid.");
  if (filter.limit !== undefined && (!Number.isSafeInteger(filter.limit) || filter.limit < 1 || filter.limit > 200)) throw new StateQLError("INVALID_COMMAND", "Catalog limit must be 1-200.");
}

function validateCatalogObject(object: CatalogObject): void {
  if (!object || typeof object !== "object" || Array.isArray(object) || !["table", "view", "collection", "function", "trigger", "enum", "key"].includes(object.kind) || typeof object.name !== "string" || !object.name || object.name.length > 500 || object.name.includes("\0") || (object.schema !== undefined && (typeof object.schema !== "string" || !object.schema || object.schema.length > 500 || object.schema.includes("\0"))) || (object.identity !== undefined && (typeof object.identity !== "string" || !object.identity || object.identity.length > 1000 || object.identity.includes("\0")))) throw new StateQLError("INVALID_COMMAND", "Catalog object identity is invalid.");
}


function databaseDisplayName(
  driver: Exclude<ConnectionRecord["driver"], "sqlite">,
): string {
  if (driver === "mongodb") return "MongoDB";
  if (driver === "redis") return "Redis";
  return driver === "postgres" ? "PostgreSQL" : "MySQL";
}

function normalizeIsolation(
  isolation: string,
  driver: ConnectionRecord["driver"],
): string {
  const normalized = isolation.trim().toLowerCase().replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
  if (driver === "redis") throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support staged SQL-style transactions.");
  if (driver === "mongodb") {
    if (normalized === "snapshot") return normalized;
    throw new StateQLError(
      "INVALID_COMMAND",
      `MongoDB does not support isolation level "${normalized}".`,
    );
  }
  const supported = new Set([
    "serializable",
    "repeatable read",
    "read committed",
    "read uncommitted",
  ]);
  if (!supported.has(normalized)) {
    throw new StateQLError(
      "INVALID_COMMAND",
      `Unsupported isolation level "${isolation}".`,
    );
  }
  if (driver === "sqlite" && normalized !== "serializable") {
    throw new StateQLError(
      "INVALID_COMMAND",
      `SQLite does not support isolation level "${normalized}".`,
    );
  }
  return normalized;
}

function batchString(value: string | undefined, name: string): string {
  if (value?.trim()) return value;
  throw new StateQLError(
    "INVALID_COMMAND",
    `Batch command requires "${name}".`,
  );
}

function nonNegativeInteger(value: number, name: string): number {
  if (Number.isInteger(value) && value >= 0) return value;
  throw new StateQLError(
    "INVALID_COMMAND",
    `${name} must be a non-negative integer.`,
  );
}

function queryPreviewRows(value: number): number {
  const previewRows = nonNegativeInteger(value, "previewRows");
  if (previewRows <= 200) return previewRows;
  throw new StateQLError(
    "OUTPUT_LIMIT_EXCEEDED",
    "previewRows cannot exceed 200 rows.",
  );
}

function positiveInteger(value: number, name: string): number {
  if (Number.isInteger(value) && value > 0) return value;
  throw new StateQLError("INVALID_COMMAND", `${name} must be a positive integer.`);
}

async function closeAdapterQuietly(adapter: Adapter | MongoAdapter | RedisAdapter): Promise<void> {
  try {
    await adapter.close();
  } catch {
    // Preserve the operation result or primary sanitized error.
  }
}

async function resolveCredentialBeforeDeadline(
  resolver: CredentialResolver,
  request: CredentialRequest,
  context: AdapterContext,
): Promise<string | undefined> {
  if (context.signal?.aborted) {
    throw new CredentialResolutionError("cancelled");
  }
  const remaining = context.deadline - Date.now();
  if (remaining <= 0) throw new CredentialResolutionError("timeout");

  return new Promise<string | undefined>((resolve, reject) => {
    let settled = false;
    const finish = (
      action: (value?: string) => void,
      value?: string,
    ): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      context.signal?.removeEventListener("abort", abort);
      action(value);
    };
    const abort = () =>
      finish(() => reject(new CredentialResolutionError("cancelled")));
    const timer = setTimeout(
      () => finish(() => reject(new CredentialResolutionError("timeout"))),
      remaining,
    );
    context.signal?.addEventListener("abort", abort, { once: true });

    Promise.resolve()
      .then(() => resolver(request))
      .then(
        (value) => finish(resolve, value),
        (error: unknown) => finish(() => reject(error)),
      );
  });
}

function credentialStateQLError(
  reference: string,
  error: unknown,
): StateQLError {
  if (error instanceof CredentialResolutionError) {
    switch (error.reason) {
      case "unavailable":
        return new StateQLError(
          "CREDENTIAL_UNAVAILABLE",
          `Credential reference "${reference}" is unavailable.`,
          { retryable: true },
        );
      case "denied":
        return new StateQLError(
          "PERMISSION_DENIED",
          `Credential access for "${reference}" was denied.`,
        );
      case "cancelled":
        return new StateQLError(
          "OPERATION_CANCELLED",
          "Credential resolution was cancelled.",
          { retryable: true },
        );
      case "timeout":
        return new StateQLError(
          "DEADLINE_EXCEEDED",
          "Credential resolution exceeded the credential deadline.",
          { retryable: true },
        );
    }
  }
  return new StateQLError(
    "CREDENTIAL_RESOLUTION_FAILED",
    `Credential reference "${reference}" could not be resolved.`,
    { retryable: true },
  );
}

function safeCredentialErrorMessage(error: unknown, source: string): string {
  let message = errorMessage(error).split(source).join("[credential redacted]");
  const authority = /^[a-z][a-z\d+.-]*:\/\/([^/?#]*)/i.exec(source)?.[1];
  if (authority) {
    const at = authority.lastIndexOf("@");
    const colon = at < 0 ? -1 : authority.slice(0, at).indexOf(":");
    const password = colon < 0 ? "" : authority.slice(colon + 1, at);
    const secrets = new Set([password]);
    try { secrets.add(decodeURIComponent(password)); } catch { /* malformed values stay encoded */ }
    for (const secret of secrets) {
      if (secret) message = message.split(secret).join("[credential redacted]");
    }
  }
  return redact(message)
    .replace(
      /\b([a-z][a-z0-9+.-]*:\/\/)[^\s\/@]+(?::[^\s\/@]*)?@/giu,
      "$1***@",
    );
}

function executionTimeout(value: number, name = "timeoutMs"): number {
  const timeout = positiveInteger(value, name);
  if (timeout > 2_147_483_647) {
    throw new StateQLError(
      "INVALID_COMMAND",
      `${name} cannot exceed 2147483647 milliseconds.`,
    );
  }
  return timeout;
}

function stoppedStateQLError(
  error: AdapterExecutionError,
  executed: boolean,
): StateQLError {
  return new StateQLError(
    error.reason === "timeout" ? "DEADLINE_EXCEEDED" : "OPERATION_CANCELLED",
    error.message,
    { retryable: true, executed },
  );
}

function validatedProfileSource(input: {
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
  if (sourceCount !== 1 || input.target === "" || input.secretEnv === "" || input.credentialRef === "") {
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
    if (input.passwordRef === undefined && driver !== "sqlite" && databaseUrlHasSecret(target)) throw new StateQLError("PERMISSION_DENIED", `Credential-bearing ${databaseDisplayName(driver)} URLs must use --env or --credential-ref.`);
    if (driver === "sqlite") target = normalizeSqliteSource(target);
  }
  return {
    target,
    secretEnv: input.secretEnv ?? null,
    credentialRef: input.credentialRef ?? null,
    passwordRef: input.passwordRef ?? null,
  };
}

function historyCategory(command: string): HistoryCategory {
  if (["query", "exec", "plan", "apply", "mongo.query", "mongo.exec", "mongo.plan", "redis.query", "redis.exec", "redis.plan", "filter"].includes(command)) return "statement";
  if (command.startsWith("inspect.") || ["objects.list", "object.describe", "table.read"].includes(command)) return "introspection";
  return "management";
}

function parseHistoryCategory(value: unknown): HistoryCategory {
  if (value === "statement" || value === "introspection" || value === "management") return value;
  throw new StateQLError("INVALID_COMMAND", `Unknown history category "${String(value)}".`);
}


function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
