import { env } from "node:process";
import { describeObject, inspectSchema, listObjects } from "./catalog.js";
import { connect, validatedProfileSource } from "./connect.js";
import {
  confidence,
  validateProfileName,
  version,
} from "./connection.js";
import {
  Core,
  errorMessage,
  executionTimeout,
  mergeCommandExecutionContext,
  nonNegativeInteger,
  parseCommandOrigin,
  parseHistoryCategory,
  positiveInteger,
} from "./core.js";
import { StateQLError } from "./errors.js";
import { Panel, type MaterializedPage, type TableRead } from "./panel.js";
import { applyPlan, prepareMongoPlan, prepareRedisPlan, prepareSqlPlan, runPlan } from "./plans.js";
import {
  prepareMongoRead,
  prepareRedisRead,
  prepareSqlRead,
  queryPreviewRows,
  runRead,
} from "./reads.js";
import { operationData, profileData, sessionData } from "./response-data.js";
import {
  countResult,
  exportResult,
  filterResult,
  resultColumns,
  resultRows,
  showResult,
  summarizeResult,
} from "./results.js";
import { StateStore, type HistoryRecord, type SessionRecord } from "./store.js";
import type { TableChange } from "./table-editor.js";
import {
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  transactionStatus,
} from "./transactions.js";
import type {
  ActorData,
  ActorLinkData,
  ActorResolutionData,
  ActorsData,
  ActorUnlinkData,
  AliasData,
  ApplyData,
  BatchCommand,
  BatchOptions,
  CapabilitiesData,
  CatalogObject,
  CloseSessionData,
  ColumnsData,
  CommandExecutionContext,
  CommandOrigin,
  CommitTransactionData,
  ConnectionData,
  ConnectOptions,
  CountData,
  DescribeObjectData,
  DisconnectData,
  DoctorData,
  ExecData,
  ExecOptions,
  ExecutionOptions,
  ExportData,
  FilterOptions,
  HistoryData,
  HistoryEntry,
  HistoryOptions,
  ListObjectsData,
  ListObjectsFilter,
  MongoExecOptions,
  MongoPlanOptions,
  MongoQueryOptions,
  MongoReadCommand,
  MongoWriteCommand,
  OperationData,
  PlanData,
  PlanOptions,
  ProfileData,
  ProfileOptions,
  ProfilesData,
  ProfileUpdateOptions,
  PurgeData,
  QueryOptions,
  RedisCommand,
  RedisExecOptions,
  RedisPlanOptions,
  RedisQueryOptions,
  RemovedProfileData,
  Response,
  ResultData,
  RollbackTransactionData,
  RowsData,
  RowsOptions,
  SessionData,
  SessionsData,
  SessionSummaryData,
  StateQLActorOptions,
  StateQLOptions,
  StateQLSnapshot,
  StateQLSnapshotOptions,
  StateQLWorkspaceOptions,
  StatusData,
  SummaryData,
  TransactionData,
} from "./types.js";
import { defaultHome } from "./util.js";
import { prepareMongoWrite, prepareRedisWrite, prepareSqlWrite, runWrite } from "./writes.js";

const DEFAULT_SNAPSHOT_HISTORY_LIMIT = 50;
const MAX_SNAPSHOT_HISTORY_LIMIT = 100;
const DEFAULT_CREDENTIAL_RESOLUTION_TIMEOUT_MS = 120_000;
const WORKSPACE_BOOTSTRAP = Symbol("StateQL.workspaceBootstrap");

type StateQLInternalOptions = StateQLOptions & {
  [WORKSPACE_BOOTSTRAP]?: true;
};
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

  private readonly core: Core;
  private readonly store: StateStore;
  private readonly panel: Panel;
  private closed = false;

  constructor(options: StateQLOptions = {}) {
    const now = options.now ?? (() => new Date());
    const sessionName = options.session ?? env.STQL_SESSION ?? "default";
    const actorId = options.actor ?? sessionName;
    if (!actorId.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Actor ID is required.");
    }
    const settings = {
      previewRows: nonNegativeInteger(options.previewRows ?? 5, "previewRows"),
      cacheTtlSeconds: nonNegativeInteger(options.cacheTtlSeconds ?? 300, "cacheTtlSeconds"),
      resultTtlSeconds: positiveInteger(options.resultTtlSeconds ?? 86_400, "resultTtlSeconds"),
      maxCellCharacters: positiveInteger(options.maxCellCharacters ?? 200, "maxCellCharacters"),
      maxResultRows: positiveInteger(options.maxResultRows ?? 10_000, "maxResultRows"),
      maxResultBytes: positiveInteger(options.maxResultBytes ?? 16 * 1024 * 1024, "maxResultBytes"),
      timeoutMs: executionTimeout(options.timeoutMs ?? 30_000),
      credentialTimeoutMs: executionTimeout(
        options.credentialTimeoutMs ?? DEFAULT_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
        "credentialTimeoutMs",
      ),
    };
    const maxStateBytes = positiveInteger(options.maxStateBytes ?? 256 * 1024 * 1024, "maxStateBytes");
    if (settings.maxResultRows >= Number.MAX_SAFE_INTEGER) {
      throw new StateQLError("INVALID_COMMAND", "maxResultRows is too large.");
    }
    const store = new StateStore(options.home ?? defaultHome(), now, maxStateBytes);
    try {
      if ((options as StateQLInternalOptions)[WORKSPACE_BOOTSTRAP]) {
        store.bootstrapWorkspace(sessionName, actorId);
      } else {
        store.bootstrapSession(sessionName, actorId, options.actor === undefined);
      }
    } catch (error) {
      store.close();
      throw error;
    }
    this.store = store;
    this.core = new Core(store, settings, sessionName, actorId, now, options.signal, options.credentialResolver);
    this.panel = new Panel(this.core, this);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.panel.clear();
    this.store.close();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  async connect(
    target?: string,
    options: ConnectOptions = {},
  ): Promise<Response<ConnectionData>> {
    return this.core.run("connect", (session) => connect(this.core, session, target, options));
  }
  async addProfile(
    name: string,
    target?: string,
    options: ProfileOptions = {},
  ): Promise<Response<ProfileData>> {
    return this.core.run("profile.add", async () => {
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
    return this.core.run("profile.update", async () => {
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
    return this.core.run("profile.list", async () => ({
      data: { profiles: this.store.listProfiles().map(profileData) },
    }));
  }

  async showProfile(name: string): Promise<Response<ProfileData>> {
    return this.core.run("profile.show", async () => {
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
    return this.core.run("profile.remove", async () => {
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
    return this.core.run("disconnect", async (session) => {
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Commit or roll back the active transaction before disconnecting.",
        );
      }
      if (!this.store.disconnect(session.id, this.core.actorId)) {
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
      .find((candidate) => candidate.name === this.core.sessionName);
    if (!session) {
      throw new StateQLError("INVALID_COMMAND", "The active session was not found.");
    }
    if (!this.store.isSessionMember(session.id, this.core.actorId)) {
      throw new StateQLError(
        "PERMISSION_DENIED",
        `Actor "${this.core.actorId}" is not attached to session "${session.name}".`,
      );
    }
    const actor = this.store.getActor(session.id, this.core.actorId)!;
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
      actor_id: this.core.actorId,
      actor_name: actor.actor_name,
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
    return this.core.run("status", async (session) => {
      const connection = this.store.activeConnection(session);
      const transaction = session.active_transaction_id
        ? this.store.getTransaction(session.active_transaction_id)
        : undefined;
      const actor = this.store.getActor(session.id, this.core.actorId)!;
      return {
        data: {
          session_id: session.id,
          session_name: session.name,
          actor_id: this.core.actorId,
          actor_name: actor.actor_name,
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
    return this.core.run("actor.link", async (current) => {
      this.requireSelectedSession(current, session);
      this.validateActorId(actorId);
      const result = this.store.linkActor(current.id, this.core.actorId, actorId);
      if (result === "actor_conflict") {
        throw new StateQLError(
          "PERMISSION_DENIED",
          `Actor "${actorId}" is already attached to another session.`,
        );
      }
      if (result === "denied") this.core.throwMembershipDenied(current);
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
    return this.core.run("actor.unlink", async (current) => {
      this.requireSelectedSession(current, session);
      this.validateActorId(actorId);
      const result = this.store.unlinkActor(current.id, this.core.actorId, actorId);
      if (result === "owns_transaction") {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          `Actor "${actorId}" owns the active transaction.`,
        );
      }
      if (result === "denied") this.core.throwMembershipDenied(current);
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

  async showActor(): Promise<Response<ActorData>> {
    return this.core.run("actor.show", async (current) => {
      const actor = this.store.getActor(current.id, this.core.actorId)!;
      return {
        data: {
          actor_id: actor.actor_id,
          actor_name: actor.actor_name,
          attached_at: actor.attached_at,
        },
      };
    });
  }

  async setActorName(name: string): Promise<Response<ActorData>> {
    return this.core.run("actor.set-name", async (current) => {
      const actorName = this.validateActorName(name);
      const existing = this.store.getActor(current.id, this.core.actorId)!;
      if (existing.actor_name === actorName) {
        return {
          data: {
            actor_id: existing.actor_id,
            actor_name: existing.actor_name,
            attached_at: existing.attached_at,
          },
        };
      }
      const actor = this.store.setActorName(current.id, this.core.actorId, actorName)!;
      return {
        data: {
          actor_id: actor.actor_id,
          actor_name: actor.actor_name,
          attached_at: actor.attached_at,
        },
        executed: true,
      };
    }, undefined, actorNameHistoryTarget(name));
  }

  async listActors(session: string): Promise<Response<ActorsData>> {
    return this.core.run("actor.list", async (current) => {
      this.requireSelectedSession(current, session);
      return {
        data: {
          session_id: current.id,
          actors: this.store.listActors(current.id).map((member) => ({
            actor_id: member.actor_id,
            actor_name: member.actor_name,
            attached_at: member.attached_at,
          })),
        },
      };
    });
  }

  async resolveActor(actorId: string): Promise<Response<ActorResolutionData>> {
    return this.core.run("actor.resolve", async () => {
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
    return this.core.run("session.start", async () => {
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
    return this.core.run("session.list", async () => ({
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

  async showSession(idOrName = this.core.sessionName): Promise<Response<SessionData>> {
    return this.core.run("session.show", async () => {
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
    return this.core.run("session.summary", async (session) => {
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
    return this.core.run("session.close", async (session) => {
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Roll back or commit the active transaction first.",
        );
      }
      if (!this.store.closeSession(session.id, this.core.actorId)) {
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
    return this.core.run("query", async (session) => {
      const previewRows = options.previewRows === undefined ? undefined : queryPreviewRows(options.previewRows);
      const connection = this.core.requireSqlConnection(session, "query");
      this.core.rejectDuringStagedTransaction(session, "Queries");
      const statement = prepareSqlRead(connection, sql, options.params ?? []);
      return runRead(this.core, session, connection, statement, { ...options, previewRows });
    }, sql);
  }

  async mongoQuery(
    command: MongoReadCommand,
    options: MongoQueryOptions = {},
  ): Promise<Response<ResultData>> {
    return this.core.run("mongo.query", async (session) => {
      const statement = prepareMongoRead(command);
      const connection = this.core.requireNativeConnection(session, "mongodb", "query");
      this.core.rejectDuringStagedTransaction(session, "MongoDB queries");
      return runRead(this.core, session, connection, statement, options);
    });
  }

  async redisQuery(
    command: RedisCommand,
    options: RedisQueryOptions = {},
  ): Promise<Response<ResultData>> {
    return this.core.run("redis.query", async (session) => {
      const statement = prepareRedisRead(command);
      const connection = this.core.requireNativeConnection(session, "redis", "query");
      this.core.rejectDuringStagedTransaction(session, "Redis queries");
      return runRead(this.core, session, connection, statement, options);
    });
  }

  async show(idOrAlias: string): Promise<Response<ResultData>> {
    return this.core.withResult("show", idOrAlias, async (result) => showResult(this.core, result));
  }

  async filter(
    idOrAlias: string,
    predicate: string,
    options: FilterOptions = {},
  ): Promise<Response<ResultData>> {
    return this.core.withResult("filter", idOrAlias, async (source) =>
      filterResult(this.core, source, predicate, options));
  }

  async rows(
    idOrAlias: string,
    options: RowsOptions = {},
  ): Promise<Response<RowsData>> {
    return this.core.withResult("rows", idOrAlias, async (result) => resultRows(this.core, result, options));
  }

  async count(idOrAlias: string): Promise<Response<CountData>> {
    return this.core.withResult("count", idOrAlias, async (result) => countResult(result));
  }

  async columns(idOrAlias: string): Promise<Response<ColumnsData>> {
    return this.core.withResult("columns", idOrAlias, async (result) => resultColumns(this.core, result));
  }

  async summarize(idOrAlias: string): Promise<Response<SummaryData>> {
    return this.core.withResult("summarize", idOrAlias, async (result) => summarizeResult(this.core, result));
  }

  async setAlias(name: string, id: string): Promise<Response<AliasData>> {
    return this.core.run("alias.set", async (session) => {
      const result = this.core.requireResult(id, session);
      this.store.setAlias(session.id, name, result.id);
      return {
        data: { alias: name, result_id: result.id },
        handle: result.id,
        executed: true,
      };
    });
  }

  /** Owned, full-value pages for host UIs. Reading a page does not add a command to history. */
  readMaterialized(id: string, options: RowsOptions & { signal?: AbortSignal } = {}): MaterializedPage {
    return this.panel.readMaterialized(id, options);
  }

  /** No caller-selected filesystem paths. The host delivers these bounded attachment bytes. */
  async serializeResult(id: string, format: "json" | "jsonl" | "csv", signal?: AbortSignal, origin: CommandOrigin = "api"): Promise<Response<{ content: string; format: string; rows: number }>> {
    return this.panel.serializeResult(id, format, signal, origin);
  }

  async readTable(table: { schema?: string; name: string }, limit = 1000, options: QueryOptions & { origin?: CommandOrigin } = {}): Promise<Response<TableRead>> {
    return this.panel.readTable(table, limit, options);
  }

  async planTableUpdate(token: string, changes: TableChange, options: ExecutionOptions & { origin?: CommandOrigin } = {}): Promise<Response<PlanData>> {
    return this.panel.planTableUpdate(token, changes, options);
  }

  async planTableUpdates(
    changes: Array<{ row_token: string; changes: TableChange }>,
    options: ExecutionOptions & { origin?: CommandOrigin } = {},
  ): Promise<Response<PlanData>> {
    return this.panel.planTableUpdates(changes, options);
  }

  async exportResult(
    idOrAlias: string,
    output: string,
    format: "json" | "jsonl" | "csv" = "csv",
    columns?: string[],
  ): Promise<Response<ExportData>> {
    return this.core.withResult("export", idOrAlias, async (result) =>
      exportResult(this.core, result, output, format, columns));
  }

  async exec(sql: string, options: ExecOptions = {}): Promise<Response<ExecData>> {
    return this.core.run("exec", async (session) => {
      const connection = this.core.requireSqlConnection(session, "exec");
      this.core.requireWritable(connection);
      const statement = prepareSqlWrite(connection, sql, options.params ?? [], options);
      return runWrite(this.core, session, connection, statement, options, this.core.executionContext(options));
    }, sql);
  }

  async mongoExec(
    command: MongoWriteCommand,
    options: MongoExecOptions = {},
  ): Promise<Response<ExecData>> {
    return this.core.run("mongo.exec", async (session) => {
      const connection = this.core.requireNativeConnection(session, "mongodb", "exec");
      this.core.requireWritable(connection);
      const statement = prepareMongoWrite(connection, command, options);
      return runWrite(this.core, session, connection, statement, options, this.core.executionContext(options));
    });
  }

  async redisExec(
    command: RedisCommand,
    options: RedisExecOptions = {},
  ): Promise<Response<ExecData>> {
    return this.core.run("redis.exec", async (session) => {
      const connection = this.core.requireNativeConnection(session, "redis", "exec");
      this.core.rejectDuringStagedTransaction(session, "Redis writes");
      this.core.requireWritable(connection);
      const statement = prepareRedisWrite(connection, command);
      return runWrite(this.core, session, connection, statement, options, this.core.executionContext(options));
    });
  }

  async receipt(id: string): Promise<Response<OperationData>> {
    return this.core.run("receipt", async (session) => {
      const operation = this.store.getOperation(id);
      if (!operation || operation.session_id !== session.id) {
        throw new StateQLError("RESULT_NOT_FOUND", `Operation "${id}" was not found.`);
      }
      return {
        data: operationData(operation),
        handle: operation.id,
        stateVersion: operation.state_version_after ?? operation.state_version_before,
      };
    });
  }

  async beginTransaction(isolation?: string): Promise<Response<TransactionData>> {
    return this.core.run("transaction.begin", async (session) => beginTransaction(this.core, session, isolation));
  }

  async transactionStatus(id?: string): Promise<Response<TransactionData>> {
    return this.core.run("transaction.status", async (session) => transactionStatus(this.core, session, id));
  }

  async commitTransaction(
    id?: string,
    options: ExecutionOptions = {},
  ): Promise<Response<CommitTransactionData>> {
    return this.core.run("transaction.commit", (session) => commitTransaction(this.core, session, id, options));
  }

  async rollbackTransaction(id?: string): Promise<Response<RollbackTransactionData>> {
    return this.core.run("transaction.rollback", async (session) => rollbackTransaction(this.core, session, id));
  }

  async inspect(
    kind: string,
    table?: string,
    options: ExecutionOptions = {},
  ): Promise<Response<unknown>> {
    return this.core.run(`inspect.${kind}`, (session) =>
      inspectSchema(this.core, session, kind, table, options), undefined, table);
  }

  async listObjects(
    filter: ListObjectsFilter = {},
    options: ExecutionOptions = {},
  ): Promise<Response<ListObjectsData>> {
    return this.core.run("objects.list", (session) => listObjects(this.core, session, filter, options));
  }

  async describeObject(
    object: CatalogObject,
    options: ExecutionOptions = {},
  ): Promise<Response<DescribeObjectData>> {
    return this.core.run("object.describe", (session) => describeObject(this.core, session, object, options));
  }

  async plan(sql: string, options: PlanOptions = {}): Promise<Response<PlanData>> {
    return this.core.run("plan", async (session) => {
      const connection = this.core.requireSqlConnection(session, "plan");
      this.core.rejectDuringStagedTransaction(session, "Plans");
      return runPlan(this.core, session, connection, prepareSqlPlan(connection, sql, options), options);
    }, sql);
  }

  async mongoPlan(
    command: MongoWriteCommand,
    options: MongoPlanOptions = {},
  ): Promise<Response<PlanData>> {
    return this.core.run("mongo.plan", async (session) => {
      const statement = prepareMongoPlan(command, options);
      const connection = this.core.requireNativeConnection(session, "mongodb", "plan");
      this.core.rejectDuringStagedTransaction(session, "Plans");
      return runPlan(this.core, session, connection, statement, options);
    });
  }

  async redisPlan(
    command: RedisCommand,
    options: RedisPlanOptions = {},
  ): Promise<Response<PlanData>> {
    return this.core.run("redis.plan", async (session) => {
      const statement = prepareRedisPlan(command);
      const connection = this.core.requireNativeConnection(session, "redis", "plan");
      this.core.rejectDuringStagedTransaction(session, "Plans");
      this.core.requireWritable(connection);
      return runPlan(this.core, session, connection, statement, options);
    });
  }

  async apply(
    planId: string,
    options: ExecutionOptions = {},
  ): Promise<Response<ApplyData>> {
    let historySql: string | undefined;
    return this.core.run("apply", (session) => applyPlan(
      this.core,
      session,
      planId,
      options,
      (table, metadataOptions) => this.panel.editableMetadata(table, metadataOptions),
      (sql) => { historySql = sql; },
    ), () => historySql);
  }
  async history(
    limit = 20,
    options: HistoryOptions = {},
  ): Promise<Response<HistoryData>> {
    return this.core.run("history", async (session) => {
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
    return this.core.run("doctor", async (session) => ({
      data: this.store.diagnostics(session.id),
    }));
  }

  async purge(
    scope: "expired" | "results" | "history" | "all" = "expired",
  ): Promise<Response<PurgeData>> {
    return this.core.run("purge", async (session) => {
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
    return this.core.run("capabilities", async () => ({
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
          actor_display_names: true,
          result_projection: true,
          result_summary: true,
          plan_row_estimates: true,
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
        this.core.commandContexts.getStore(),
        context,
      );
    } catch (error) {
      return this.batchFailure(errorMessage(error));
    }

    return this.core.commandContexts.run(activeContext, async () => {
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
        case "actor.show":
          return this.showActor();
        case "actor.set-name":
          return this.setActorName(batchString(command.name, "name"));
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
            ...(command.columns ? { columns: command.columns } : {}),
          });
        case "count":
          return this.count(batchString(command.handle, "handle"));
        case "columns":
          return this.columns(batchString(command.handle, "handle"));
        case "summarize":
          return this.summarize(batchString(command.handle, "handle"));
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


  private batchFailure(message: string): Promise<Response<unknown>> {
    return this.core.run("batch", async () => {
      throw new StateQLError("INVALID_COMMAND", message);
    });
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

  private validateActorName(value: unknown): string {
    if (typeof value !== "string") {
      throw new StateQLError("INVALID_COMMAND", "Actor name must be a string.");
    }
    const name = value.trim();
    if (!name) throw new StateQLError("INVALID_COMMAND", "Actor name is required.");
    if ([...name].length > 80) {
      throw new StateQLError("INVALID_COMMAND", "Actor name cannot exceed 80 characters.");
    }
    if (/[\u0000-\u001f\u007f]/u.test(name)) {
      throw new StateQLError("INVALID_COMMAND", "Actor name cannot contain control characters.");
    }
    return name;
  }

}

function historyEntry(item: HistoryRecord): HistoryEntry {
  return {
    command_id: item.id,
    timestamp: item.timestamp,
    session_id: item.session_id,
    actor_id: item.actor_id,
    actor_name: item.actor_name,
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

function actorNameHistoryTarget(value: unknown): string | undefined {
  if (typeof value !== "string" || /[\u0000-\u001f\u007f]/u.test(value)) return undefined;
  const name = value.trim();
  return name ? [...name].slice(0, 80).join("") : undefined;
}

function batchString(value: string | undefined, name: string): string {
  if (value?.trim()) return value;
  throw new StateQLError(
    "INVALID_COMMAND",
    `Batch command requires "${name}".`,
  );
}
