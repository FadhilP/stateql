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
} from "./adapters.js";
import {
  confidence,
  databaseIdentity,
  databaseUrlHasSecret,
  detectDriver,
  isEnvironmentName,
  normalizeSqliteSource,
  validateProfileName,
  version,
} from "./connection.js";
import { asStateQLError, StateQLError } from "./errors.js";
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
  type ResultRecord,
  type SessionRecord,
} from "./store.js";
import type {
  BatchCommand,
  BatchOptions,
  ConnectOptions,
  ExecOptions,
  ExecutionOptions,
  Failure,
  FilterOptions,
  HistoryEntry,
  PlanOptions,
  ProfileOptions,
  QueryOptions,
  Response,
  RowsOptions,
  SqlParameters,
  StateConfidence,
  StateQLActorOptions,
  StateQLOptions,
  StateQLSnapshot,
  Success,
  Warning,
} from "./types.js";
import {
  compactRows,
  defaultHome,
  hash,
  parseJson,
  redact,
} from "./util.js";

const DEFAULT_SNAPSHOT_HISTORY_LIMIT = 50;
const MAX_SNAPSHOT_HISTORY_LIMIT = 100;

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
  private readonly signal?: AbortSignal;
  private readonly now: () => Date;

  constructor(options: StateQLOptions = {}) {
    this.now = options.now ?? (() => new Date());
    this.sessionName = options.session ?? env.STQL_SESSION ?? "default";
    this.actorId = options.actor ?? this.sessionName;
    if (!this.actorId.trim()) {
      throw new StateQLError("INVALID_COMMAND", "Actor ID is required.");
    }
    this.previewRows = options.previewRows ?? 5;
    this.cacheTtlSeconds = options.cacheTtlSeconds ?? 300;
    this.resultTtlSeconds = options.resultTtlSeconds ?? 86_400;
    this.maxCellCharacters = options.maxCellCharacters ?? 200;
    this.maxResultRows = positiveInteger(
      options.maxResultRows ?? 10_000,
      "maxResultRows",
    );
    this.maxResultBytes = positiveInteger(
      options.maxResultBytes ?? 16 * 1024 * 1024,
      "maxResultBytes",
    );
    this.timeoutMs = executionTimeout(options.timeoutMs ?? 30_000);
    this.signal = options.signal;
    if (this.maxResultRows >= Number.MAX_SAFE_INTEGER) {
      throw new StateQLError(
        "INVALID_COMMAND",
        "maxResultRows is too large.",
      );
    }
    this.store = new StateStore(options.home ?? defaultHome(), this.now);
    this.store.bootstrapSession(
      this.sessionName,
      this.actorId,
      options.actor === undefined,
    );
  }

  close(): void {
    this.store.close();
  }

  async connect(
    target?: string,
    options: ConnectOptions = {},
  ): Promise<Response<unknown>> {
    return this.run("connect", async (session) => {
      if (session.active_transaction_id) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Commit or roll back the active transaction before connecting again.",
        );
      }
      if (options.profile && target) {
        throw new StateQLError(
          "INVALID_COMMAND",
          "Use either a profile or a connection target, not both.",
        );
      }
      const implicitProfile =
        !options.profile && !options.secretEnv && target
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

      const resolvedTarget = profile?.target ?? target;
      const secretEnv = options.secretEnv ?? profile?.secret_env ?? undefined;
      const secret = secretEnv ? env[secretEnv] : resolvedTarget;
      if (!secret) {
        throw new StateQLError(
          "INVALID_COMMAND",
          secretEnv
            ? `Environment variable ${secretEnv} is not set.`
            : "Connection target is required.",
        );
      }
      const driver = detectDriver(secret);
      if (
        driver !== "sqlite" &&
        !secretEnv &&
        databaseUrlHasSecret(secret)
      ) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          `Credential-bearing ${databaseDisplayName(driver)} URLs must use --env.`,
          {
            suggestedAction:
              "Set the URL in an environment variable and reconnect with --env NAME.",
          },
        );
      }

      const source =
        driver === "sqlite"
          ? normalizeSqliteSource(secret)
          : secretEnv
            ? redact(secret)
            : secret;
      const databaseName =
        driver === "sqlite"
          ? basename(source)
          : new URL(secret).pathname.replace(/^\//, "") || driver;
      const readOnly =
        options.readOnly ??
        (profile ? Boolean(profile.read_only) : true);
      const draft: ConnectionRecord = {
        id: "pending",
        session_id: session.id,
        name: options.name ?? profile?.name ?? databaseName,
        driver,
        database_name: databaseName,
        source,
        secret_env: secretEnv ?? null,
        read_only: readOnly ? 1 : 0,
        version: 0,
        created_at: this.now().toISOString(),
      };

      const adapter = await createAdapter(
        draft,
        this.executionContext(options),
      );
      try {
        await adapter.read("SELECT 1", []);
      } catch (error) {
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, false);
        }
        throw new StateQLError(
          "CONNECTION_FAILED",
          errorMessage(error),
          { retryable: true },
        );
      } finally {
        await adapter.close();
      }

      const connection = this.store.addConnection({
        sessionId: session.id,
        actorId: this.actorId,
        name: draft.name,
        driver,
        databaseName,
        source,
        ...(secretEnv ? { secretEnv } : {}),
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
  ): Promise<Response<unknown>> {
    return this.run("profile.add", async () => {
      validateProfileName(name);
      if (Boolean(target) === Boolean(options.secretEnv)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          "Profile requires exactly one target or secret environment variable.",
        );
      }
      if (this.store.getProfile(name)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          `Profile "${name}" already exists.`,
        );
      }
      if (options.secretEnv && !isEnvironmentName(options.secretEnv)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          "Secret environment variable name is invalid.",
        );
      }

      let storedTarget = target;
      if (target) {
        const driver = detectDriver(target);
        if (driver !== "sqlite" && databaseUrlHasSecret(target)) {
          throw new StateQLError(
            "PERMISSION_DENIED",
            `Credential-bearing ${databaseDisplayName(driver)} URLs must use --env.`,
          );
        }
        if (driver === "sqlite") storedTarget = normalizeSqliteSource(target);
      }

      const profile = this.store.addProfile({
        name,
        target: storedTarget,
        secretEnv: options.secretEnv,
        readOnly: options.readOnly ?? true,
      });
      return {
        data: profileData(profile),
        handle: `profile:${profile.name}`,
        executed: true,
      };
    });
  }

  async listProfiles(): Promise<Response<unknown>> {
    return this.run("profile.list", async () => ({
      data: { profiles: this.store.listProfiles().map(profileData) },
    }));
  }

  async showProfile(name: string): Promise<Response<unknown>> {
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

  async removeProfile(name: string): Promise<Response<unknown>> {
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

  async disconnect(): Promise<Response<unknown>> {
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

  snapshot(options: { historyLimit?: number } = {}): StateQLSnapshot {
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
      history: this.store.history(session.id, historyLimit).map(historyEntry),
    };
  }

  async status(): Promise<Response<unknown>> {
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
  ): Promise<Response<unknown>> {
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
  ): Promise<Response<unknown>> {
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

  async listActors(session: string): Promise<Response<unknown>> {
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

  async resolveActor(actorId: string): Promise<Response<unknown>> {
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

  async startSession(name: string): Promise<Response<unknown>> {
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

  async listSessions(): Promise<Response<unknown>> {
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

  async showSession(idOrName = this.sessionName): Promise<Response<unknown>> {
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

  async sessionSummary(): Promise<Response<unknown>> {
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

  async closeSession(): Promise<Response<unknown>> {
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

  async query(sql: string, options: QueryOptions = {}): Promise<Response<unknown>> {
    return this.run("query", async (session) => {
      const connection = this.requireConnection(session);
      this.rejectDuringStagedTransaction(session, "Queries");
      const analysis = analyzeSql(sql, connection.driver);
      if (!analysis.read) {
        throw new StateQLError(
          "INVALID_SQL",
          "query accepts read statements only; use exec for writes.",
        );
      }
      const parameters = options.params ?? [];
      const adapter = await createAdapter(
        connection,
        this.executionContext(options),
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
        const cacheMode = options.cache ?? "auto";
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

        const result = await adapter.read(
          boundedReadSql(sql, this.maxResultRows + 1),
          parameters,
        );
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
          data: this.resultData(saved, false),
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
        throw new StateQLError("QUERY_FAILED", errorMessage(error), {
          retryable: true,
          executed: true,
        });
      } finally {
        await adapter.close();
      }
    });
  }

  async show(idOrAlias: string): Promise<Response<unknown>> {
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
  ): Promise<Response<unknown>> {
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
  ): Promise<Response<unknown>> {
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

  async count(idOrAlias: string): Promise<Response<unknown>> {
    return this.withResult("count", idOrAlias, async (result) => ({
      data: { result_id: result.id, rows: result.row_count },
      handle: result.id,
      cached: true,
      stateVersion: result.state_version,
      confidence: result.state_confidence,
    }));
  }

  async columns(idOrAlias: string): Promise<Response<unknown>> {
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

  async setAlias(name: string, id: string): Promise<Response<unknown>> {
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

  async exportResult(
    idOrAlias: string,
    output: string,
    format: "json" | "jsonl" | "csv" = "csv",
  ): Promise<Response<unknown>> {
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

  async exec(sql: string, options: ExecOptions = {}): Promise<Response<unknown>> {
    return this.run("exec", async (session) => {
      const connection = this.requireConnection(session);
      return this.performExec(
        session,
        connection,
        sql,
        options,
        this.executionContext(options),
      );
    });
  }

  async receipt(id: string): Promise<Response<unknown>> {
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
    isolation = "serializable",
  ): Promise<Response<unknown>> {
    return this.run("transaction.begin", async (session) => {
      const connection = this.requireConnection(session);
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
        isolation,
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

  async transactionStatus(id?: string): Promise<Response<unknown>> {
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
  ): Promise<Response<unknown>> {
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
      const adapter = await createAdapter(
        connection,
        this.executionContext(options),
      );
      try {
        if (
          !this.store.markTransactionCommitting(
            transaction.id,
            session.id,
            this.actorId,
          )
        ) {
          throw new StateQLError(
            "TRANSACTION_FAILED",
            "Transaction is no longer active.",
          );
        }
        const operations = this.store.transactionOperations(transaction.id);
        if (
          operations.some(
            (operation) => operation.connection_id !== connection.id,
          )
        ) {
          this.store.finishTransaction(
            transaction.id,
            session.id,
            this.actorId,
            "failed",
          );
          throw new StateQLError(
            "TRANSACTION_FAILED",
            "Transaction contains writes staged for another connection.",
          );
        }

        let results;
        try {
          results = await adapter.writeBatch(
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
            throw new StateQLError("TRANSACTION_FAILED", error.message, {
              retryable: true,
            });
          }
          markTransactionOutcomeUnknown(
            this.store,
            transaction.id,
            session.id,
            this.actorId,
          );
          throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), {
            executed: true,
            suggestedAction:
              "Inspect database state before issuing any replacement write.",
          });
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
            })),
          });
        } catch (error) {
          markTransactionOutcomeUnknown(
            this.store,
            transaction.id,
            session.id,
            this.actorId,
          );
          throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), {
            executed: true,
            suggestedAction:
              "Inspect database state before issuing any replacement write.",
          });
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

  async rollbackTransaction(id?: string): Promise<Response<unknown>> {
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
      const adapter = await createAdapter(
        connection,
        this.executionContext(options),
      );
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
        throw new StateQLError("QUERY_FAILED", errorMessage(error), {
          retryable: false,
          executed: true,
        });
      } finally {
        await adapter.close();
      }
    });
  }

  async plan(sql: string, options: PlanOptions = {}): Promise<Response<unknown>> {
    return this.run("plan", async (session) => {
      const connection = this.requireConnection(session);
      this.rejectDuringStagedTransaction(session, "Plans");
      const analysis = analyzeSql(sql, connection.driver);
      if (analysis.read) {
        throw new StateQLError(
          "INVALID_SQL",
          "plan accepts write statements only.",
        );
      }
      const adapter = await createAdapter(
        connection,
        this.executionContext(options),
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
        if (error instanceof AdapterExecutionError) {
          throw stoppedStateQLError(error, true);
        }
        throw error;
      } finally {
        await adapter.close();
      }
    });
  }

  async apply(
    planId: string,
    options: ExecutionOptions = {},
  ): Promise<Response<unknown>> {
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
      const claimToken = this.store.nextId("claim");
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
        const context = this.executionContext(options);
        const adapter = await createAdapter(connection, context);
        try {
          if ((await adapter.signature()) !== claimed.state_signature) {
            throw new StateQLError(
              "STALE_PLAN",
              "Database state changed after this plan was created.",
            );
          }
        } catch (error) {
          if (error instanceof AdapterExecutionError) {
            throw stoppedStateQLError(error, true);
          }
          throw error;
        } finally {
          await adapter.close();
        }
        const result = await this.performExec(
          session,
          connection,
          claimed.sql,
          {
            params: parseJson<SqlParameters>(claimed.parameters, []),
            allowUnbounded: Boolean(claimed.allow_unbounded),
            allowDestructive: Boolean(claimed.allow_destructive),
          },
          context,
          { planId: claimed.id, claimToken },
        );
        return {
          ...result,
          data: { plan_id: claimed.id, ...(result.data as object) },
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
    });
  }

  async history(limit = 20): Promise<Response<{ history: HistoryEntry[] }>> {
    return this.run("history", async (session) => ({
      data: {
        history: this.store
          .history(session.id, positiveInteger(limit, "limit"))
          .map(historyEntry),
      },
    }));
  }

  async capabilities(): Promise<Response<unknown>> {
    return this.run("capabilities", async () => ({
      data: {
        drivers: ["mysql", "postgres", "sqlite"],
        features: {
          result_handles: true,
          write_deduplication: true,
          transactions: true,
          query_plans: true,
          persistent_sessions: true,
          result_filtering: true,
          schema_inspection: true,
          deadlines: true,
          cancellation: true,
        },
      },
    }));
  }

  async executeCommand(command: BatchCommand): Promise<Response<unknown>> {
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
            },
          );
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
            timeoutMs: command.timeout_ms,
          });
          if (!response.ok || !command.as) return response;
          const resultId = (response.data as Record<string, unknown>).result_id;
          if (typeof resultId !== "string") return response;
          this.store.setAlias(response.session_id, command.as, resultId);
          return {
            ...response,
            data: { ...(response.data as object), alias: command.as },
          };
        }
        case "filter": {
          const response = await this.filter(
            batchString(command.handle, "handle"),
            batchString(command.where, "where"),
            { params: command.params ?? [] },
          );
          if (!response.ok || !command.as) return response;
          const resultId = (response.data as Record<string, unknown>).result_id;
          if (typeof resultId !== "string") return response;
          this.store.setAlias(response.session_id, command.as, resultId);
          return {
            ...response,
            data: { ...(response.data as object), alias: command.as },
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
        case "apply":
          return this.apply(batchString(command.handle, "handle"), {
            timeoutMs: command.timeout_ms,
          });
        case "history":
          return this.history(command.limit ?? 20);
        case "receipt":
          return this.receipt(batchString(command.handle, "handle"));
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
      const response = await this.executeCommand(command);
      yield response;
      if (!response.ok && !options.continueOnError) return;
    }
  }

  private async performExec(
    session: SessionRecord,
    connection: ConnectionRecord,
    sql: string,
    options: ExecOptions,
    context: AdapterContext,
    planClaim?: { planId: string; claimToken: string },
  ): Promise<ActionResult<unknown>> {
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
    const fingerprint = hash({
      sql: analysis.normalized,
      parameters,
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
    try {
      adapter = await createAdapter(connection, context);
    } catch (error) {
      this.store.failOperation(operation.id);
      throw new StateQLError("QUERY_FAILED", errorMessage(error), {
        retryable: true,
      });
    }

    try {
      const write = await adapter.write(sql, parameters);
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
        throw new StateQLError("QUERY_FAILED", error.message, {
          executed: true,
        });
      }
      this.store.markOperationOutcomeUnknown(operation.id);
      throw new StateQLError("OUTCOME_UNKNOWN", errorMessage(error), {
        executed: true,
        suggestedAction:
          "Inspect database state, then use --replay only if another execution is safe.",
      });
    } finally {
      try {
        await adapter.close();
      } catch {
        // Write outcome and metadata are already recorded.
      }
    }
  }

  private batchFailure(message: string): Promise<Response<unknown>> {
    return this.run("batch", async () => {
      throw new StateQLError("INVALID_COMMAND", message);
    });
  }

  private async withResult(
    command: string,
    idOrAlias: string,
    action: (result: ResultRecord, session: SessionRecord) => Promise<ActionResult<unknown>>,
  ): Promise<Response<unknown>> {
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

  private executionContext(options: ExecutionOptions): AdapterContext {
    return createAdapterContext(
      executionTimeout(options.timeoutMs ?? this.timeoutMs),
      options.signal ?? this.signal,
    );
  }

  private resultData(result: ResultRecord, cached: boolean): unknown {
    const rows = this.store.resultRows(result);
    const preview = compactRows(
      rows.slice(0, this.previewRows),
      this.maxCellCharacters,
    );
    return {
      result_id: result.id,
      rows: result.row_count,
      columns: this.store.resultColumns(result),
      preview,
      preview_count: preview.length,
      truncated: preview.length < result.row_count,
      cached,
      ...(cached ? { duplicate_of: result.id } : {}),
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
  ): Promise<Response<T>> {
    const started = performance.now();
    let session = this.store.ensureSession(this.sessionName);
    const commandId = this.store.nextId("cmd");
    if (!this.store.isSessionMember(session.id, this.actorId)) {
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
      this.store.addHistory({
        id: commandId,
        sessionId: session.id,
        actorId: this.actorId,
        command,
        ...(result.handle ? { handle: result.handle } : {}),
        executed: result.executed ?? false,
        cached: result.cached ?? false,
        success: true,
      });
      return {
        ok: true,
        command_id: commandId,
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
      this.store.addHistory({
        id: commandId,
        sessionId: session.id,
        actorId: this.actorId,
        command,
        executed: stateqlError.details.executed,
        cached: false,
        success: false,
        errorCode: stateqlError.details.code,
      });
      return {
        ok: false,
        command_id: commandId,
        session_id: session.id,
        error: stateqlError.details,
        meta: {
          duration_ms: Math.round((performance.now() - started) * 1000) / 1000,
        },
      } satisfies Failure;
    }
  }
}

function historyEntry(item: HistoryRecord): HistoryEntry {
  return {
    command_id: item.id,
    timestamp: item.timestamp,
    session_id: item.session_id,
    actor_id: item.actor_id,
    command: item.command,
    handle: item.handle,
    executed: Boolean(item.executed),
    cached: Boolean(item.cached),
    success: Boolean(item.success),
    error_code: item.error_code,
  };
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

function boundedReadSql(sql: string, limit: number): string {
  const statement = sql.trim().replace(/;\s*$/, "");
  return `SELECT * FROM (${statement}) AS _stateql_bounded LIMIT ${limit}`;
}

function databaseDisplayName(
  driver: Exclude<ConnectionRecord["driver"], "sqlite">,
): string {
  return driver === "postgres" ? "PostgreSQL" : "MySQL";
}

function normalizeIsolation(
  isolation: string,
  driver: ConnectionRecord["driver"],
): string {
  const normalized = isolation.trim().toLowerCase().replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
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

function positiveInteger(value: number, name: string): number {
  if (Number.isInteger(value) && value > 0) return value;
  throw new StateQLError("INVALID_COMMAND", `${name} must be a positive integer.`);
}

function executionTimeout(value: number): number {
  const timeout = positiveInteger(value, "timeoutMs");
  if (timeout > 2_147_483_647) {
    throw new StateQLError(
      "INVALID_COMMAND",
      "timeoutMs cannot exceed 2147483647 milliseconds.",
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
