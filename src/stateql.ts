import { writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { env } from "node:process";
import {
  BatchWriteError,
  createAdapter,
  type Adapter,
} from "./adapters.js";
import { asStateQLError, StateQLError } from "./errors.js";
import { analyzeSql } from "./sql.js";
import {
  StateStore,
  type ConnectionRecord,
  type OperationRecord,
  type ProfileRecord,
  type ResultRecord,
  type SessionRecord,
} from "./store.js";
import type {
  BatchCommand,
  BatchOptions,
  Column,
  ConnectOptions,
  ExecOptions,
  Failure,
  FilterOptions,
  PlanOptions,
  ProfileOptions,
  QueryOptions,
  Response,
  Row,
  RowsOptions,
  SqlParameters,
  StateConfidence,
  StateQLOptions,
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
  private readonly store: StateStore;
  private readonly sessionName: string;
  private readonly previewRows: number;
  private readonly cacheTtlSeconds: number;
  private readonly resultTtlSeconds: number;
  private readonly maxCellCharacters: number;
  private readonly maxResultRows: number;
  private readonly now: () => Date;

  constructor(options: StateQLOptions = {}) {
    this.now = options.now ?? (() => new Date());
    this.sessionName = options.session ?? env.STQL_SESSION ?? "default";
    this.previewRows = options.previewRows ?? 5;
    this.cacheTtlSeconds = options.cacheTtlSeconds ?? 300;
    this.resultTtlSeconds = options.resultTtlSeconds ?? 86_400;
    this.maxCellCharacters = options.maxCellCharacters ?? 200;
    this.maxResultRows = positiveInteger(
      options.maxResultRows ?? 10_000,
      "maxResultRows",
    );
    if (this.maxResultRows >= Number.MAX_SAFE_INTEGER) {
      throw new StateQLError(
        "INVALID_COMMAND",
        "maxResultRows is too large.",
      );
    }
    this.store = new StateStore(options.home ?? defaultHome(), this.now);
    this.store.ensureSession(this.sessionName);
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
        driver === "postgres" &&
        !secretEnv &&
        postgresUrlHasSecret(secret)
      ) {
        throw new StateQLError(
          "PERMISSION_DENIED",
          "Credential-bearing PostgreSQL URLs must use --env.",
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
          : new URL(secret).pathname.replace(/^\//, "") || "postgres";
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

      const adapter = await createAdapter(draft);
      try {
        await adapter.read("SELECT 1", []);
      } catch (error) {
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
        name: draft.name,
        driver,
        databaseName,
        source,
        ...(secretEnv ? { secretEnv } : {}),
        readOnly,
      });
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
        if (driver === "postgres" && postgresUrlHasSecret(target)) {
          throw new StateQLError(
            "PERMISSION_DENIED",
            "Credential-bearing PostgreSQL URLs must use --env.",
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
      this.store.disconnect(session.id);
      return { data: { disconnected: true }, executed: true };
    });
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
            ? { transaction_id: transaction.id, state: transaction.state }
            : null,
          state_version: connection ? version(connection) : null,
        },
        stateVersion: connection ? version(connection) : undefined,
        confidence: connection ? confidence(connection) : undefined,
      };
    });
  }

  async startSession(name: string): Promise<Response<unknown>> {
    return this.run("session.start", async () => {
      if (!name.trim()) {
        throw new StateQLError("INVALID_COMMAND", "Session name is required.");
      }
      if (this.store.getSession(name)) {
        throw new StateQLError(
          "INVALID_COMMAND",
          `Active session "${name}" already exists.`,
        );
      }
      const session = this.store.createSession(name);
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
      this.store.closeSession(session.id);
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
      const analysis = analyzeSql(sql, connection.driver);
      if (!analysis.read) {
        throw new StateQLError(
          "INVALID_SQL",
          "query accepts read statements only; use exec for writes.",
        );
      }
      const parameters = options.params ?? [];
      const adapter = await createAdapter(connection);
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
      return this.performExec(session, connection, sql, options);
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
        connectionId: connection.id,
        isolation: normalizedIsolation,
        startVersion: version(connection),
      });
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

  async commitTransaction(id?: string): Promise<Response<unknown>> {
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
      const operations = this.store.transactionOperations(transaction.id);
      if (operations.some((operation) => operation.connection_id !== connection.id)) {
        throw new StateQLError(
          "TRANSACTION_FAILED",
          "Transaction contains writes staged for another connection.",
          { suggestedAction: "Roll back the transaction." },
        );
      }
      const adapter = await createAdapter(connection);
      try {
        if (!this.store.markTransactionCommitting(transaction.id)) {
          throw new StateQLError(
            "TRANSACTION_FAILED",
            "Transaction is no longer active.",
          );
        }

        let results;
        try {
          results = await adapter.writeBatch(
            operations,
            transaction.isolation_level,
          );
        } catch (error) {
          if (error instanceof BatchWriteError && !error.outcomeUnknown) {
            this.store.finishTransaction(transaction.id, session.id, "failed");
            throw new StateQLError("TRANSACTION_FAILED", error.message, {
              retryable: true,
            });
          }
          markTransactionOutcomeUnknown(
            this.store,
            transaction.id,
            session.id,
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
      this.store.finishTransaction(transaction.id, session.id, "rolled_back");
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

  async inspect(kind: string, table?: string): Promise<Response<unknown>> {
    return this.run(`inspect.${kind}`, async (session) => {
      const connection = this.requireConnection(session);
      const adapter = await createAdapter(connection);
      try {
        const data = await adapter.inspect(kind, table);
        return {
          data,
          executed: true,
          stateVersion: version(connection),
          confidence: adapter.confidence,
        };
      } catch (error) {
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
      const analysis = analyzeSql(sql, connection.driver);
      if (analysis.read) {
        throw new StateQLError(
          "INVALID_SQL",
          "plan accepts write statements only.",
        );
      }
      const adapter = await createAdapter(connection);
      try {
        const stateSignature = await adapter.signature();
        const expiresAt = new Date(this.now().getTime() + 10 * 60_000).toISOString();
        const plan = this.store.savePlan({
          sessionId: session.id,
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
            expires_at: plan.expires_at,
          },
          handle: plan.id,
          executed: true,
          stateVersion: plan.state_version,
          confidence: adapter.confidence,
        };
      } finally {
        await adapter.close();
      }
    });
  }

  async apply(planId: string): Promise<Response<unknown>> {
    return this.run("apply", async (session) => {
      const plan = this.store.getPlan(planId);
      if (!plan || plan.session_id !== session.id) {
        throw new StateQLError("STALE_PLAN", `Plan "${planId}" was not found.`);
      }
      if (plan.applied_operation_id) {
        throw new StateQLError("STALE_PLAN", "Plan was already applied.", {
          extra: { previous_operation_id: plan.applied_operation_id },
        });
      }
      if (Date.parse(plan.expires_at) <= this.now().getTime()) {
        throw new StateQLError("STALE_PLAN", "Plan has expired.");
      }
      const connection = this.requireConnection(session);
      if (
        connection.id !== plan.connection_id ||
        version(connection) !== plan.state_version
      ) {
        throw new StateQLError(
          "STALE_PLAN",
          "Database state changed after this plan was created.",
        );
      }
      const adapter = await createAdapter(connection);
      try {
        if ((await adapter.signature()) !== plan.state_signature) {
          throw new StateQLError(
            "STALE_PLAN",
            "Database state changed after this plan was created.",
          );
        }
      } finally {
        await adapter.close();
      }
      const result = await this.performExec(
        session,
        connection,
        plan.sql,
        {
          params: parseJson<SqlParameters>(plan.parameters, []),
          allowUnbounded: Boolean(plan.allow_unbounded),
          allowDestructive: Boolean(plan.allow_destructive),
        },
      );
      const operationId = String(
        (result.data as Record<string, unknown>).operation_id,
      );
      this.store.markPlanApplied(plan.id, operationId);
      return {
        ...result,
        data: { plan_id: plan.id, ...(result.data as object) },
      };
    });
  }

  async history(limit = 20): Promise<Response<unknown>> {
    return this.run("history", async (session) => ({
      data: {
        history: this.store
          .history(session.id, positiveInteger(limit, "limit"))
          .map((item) => ({
            command_id: item.id,
            timestamp: item.timestamp,
            session_id: item.session_id,
            command: item.command,
            handle: item.handle,
            executed: Boolean(item.executed),
            cached: Boolean(item.cached),
            success: Boolean(item.success),
            error_code: item.error_code,
          })),
      },
    }));
  }

  async capabilities(): Promise<Response<unknown>> {
    return this.run("capabilities", async () => ({
      data: {
        drivers: ["postgres", "sqlite"],
        features: {
          result_handles: true,
          write_deduplication: true,
          transactions: true,
          query_plans: true,
          persistent_sessions: true,
          result_filtering: true,
          schema_inspection: true,
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
          return this.inspect(batchString(command.kind, "kind"), command.table);
        case "transaction.begin":
          return this.beginTransaction(command.isolation);
        case "transaction.status":
          return this.transactionStatus(command.handle);
        case "transaction.commit":
          return this.commitTransaction(command.handle);
        case "transaction.rollback":
          return this.rollbackTransaction(command.handle);
        case "plan":
          return this.plan(batchString(command.sql, "sql"), {
            params: command.params ?? [],
            allowUnbounded: command.allow_unbounded ?? false,
            allowDestructive: command.allow_destructive,
          });
        case "apply":
          return this.apply(batchString(command.handle, "handle"));
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
    }
    const reservation = this.store.reserveOperation({
      sessionId: session.id,
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
    const previous = reservation.previous;
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
      adapter = await createAdapter(connection);
    } catch (error) {
      this.store.failOperation(operation.id);
      throw new StateQLError("QUERY_FAILED", errorMessage(error), {
        retryable: true,
      });
    }

    try {
      const write = await adapter.write(sql, parameters);
      try {
        const after = this.store.bumpVersion(connection.id);
        const committed = this.store.finishOperation(
          operation.id,
          write.affectedRows,
          after,
        );
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
    return transaction;
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
    try {
      const result = await action(session);
      session = result.session ?? session;
      this.store.addHistory({
        id: commandId,
        sessionId: session.id,
        command,
        ...(result.handle ? { handle: result.handle } : {}),
        executed: result.executed ?? false,
        cached: result.cached ?? false,
        success: true,
      });
      return {
        ok: true,
        command_id: commandId,
        session_id: session.id,
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

interface PreparedFilter {
  sql: string;
  normalized: string;
  tableName: string;
  indexColumn: string;
  columnNames: string[];
  positionalParameters: number;
  namedParameters: string[];
}

function prepareFilterStatement(
  columns: Column[],
  predicate: string,
): PreparedFilter {
  const text = predicate.trim();
  if (!text) {
    throw new StateQLError("INVALID_SQL", "Filter predicate is required.");
  }

  const columnNames = columns.map((column) => column.name);
  if (columnNames.length === 0) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter requires at least one result column.",
    );
  }
  const names = new Set<string>();
  for (const name of columnNames) {
    const normalized = name.toLowerCase();
    if (!name || name.includes("\0") || names.has(normalized)) {
      throw new StateQLError(
        "INVALID_SQL",
        "Filter requires unique, non-empty result column names.",
      );
    }
    names.add(normalized);
  }

  const tableName = "__stateql_filter_source";
  let indexColumn = "__stateql_row_index";
  while (names.has(indexColumn.toLowerCase())) indexColumn += "_";
  const sql =
    `SELECT ${quoteIdentifier(indexColumn)} ` +
    `FROM ${quoteIdentifier(tableName)} WHERE (${text})`;
  const analysis = analyzeSql(sql, "sqlite");
  const details = analysis.ast as unknown as Record<string, unknown>;
  const from = details.from;
  const source = Array.isArray(from)
    ? (from[0] as Record<string, unknown> | undefined)
    : undefined;
  if (
    !analysis.read ||
    !Array.isArray(from) ||
    from.length !== 1 ||
    source?.table !== tableName ||
    details.with ||
    details.groupby ||
    details.having ||
    details.orderby ||
    details.limit ||
    details.for_update ||
    details._next ||
    details.set_op ||
    containsSelect(details.where)
  ) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter accepts one scalar predicate only.",
    );
  }
  validateFilterExpression(details.where, names, tableName);
  const bindings = filterBindings(details.where);
  return {
    sql,
    normalized: analysis.normalized,
    tableName,
    indexColumn,
    columnNames,
    positionalParameters: bindings.positional,
    namedParameters: [...bindings.named],
  };
}

function filterMaterializedRows(
  rows: Row[],
  filter: PreparedFilter,
  parameters: SqlParameters,
): Row[] {
  const db = new DatabaseSync(":memory:");
  try {
    const definitions = [
      `${quoteIdentifier(filter.indexColumn)} INTEGER PRIMARY KEY`,
      ...filter.columnNames.map(quoteIdentifier),
    ];
    db.exec(
      `CREATE TABLE ${quoteIdentifier(filter.tableName)} ` +
        `(${definitions.join(", ")})`,
    );
    const placeholders = filter.columnNames.map(() => "?").join(", ");
    const insert = db.prepare(
      `INSERT INTO ${quoteIdentifier(filter.tableName)} (` +
        `${quoteIdentifier(filter.indexColumn)}, ` +
        `${filter.columnNames.map(quoteIdentifier).join(", ")}) ` +
        `VALUES (?, ${placeholders})`,
    );
    db.exec("BEGIN");
    try {
      rows.forEach((row, index) => {
        const values = filter.columnNames.map((name) =>
          sqliteFilterValue(row[name]),
        );
        insert.run(index, ...(values as never[]));
      });
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }

    const statement = db.prepare(
      `${filter.sql}\nORDER BY ${quoteIdentifier(filter.indexColumn)}`,
    );
    const selected = filterAll(statement, parameters);
    return selected.map((row) => {
      const index = Number(row[filter.indexColumn]);
      if (!Number.isInteger(index) || !rows[index]) {
        throw new StateQLError(
          "INVALID_SQL",
          "Filter produced an invalid source row index.",
        );
      }
      return rows[index];
    });
  } catch (error) {
    if (error instanceof StateQLError) throw error;
    throw new StateQLError("INVALID_SQL", errorMessage(error));
  } finally {
    db.close();
  }
}

function filterAll(
  statement: StatementSync,
  parameters: SqlParameters,
): Array<Record<string, unknown>> {
  if (Array.isArray(parameters)) {
    return statement.all(...(parameters as never[])) as Array<
      Record<string, unknown>
    >;
  }
  return statement.all(parameters as never) as Array<Record<string, unknown>>;
}

function sqliteFilterValue(
  value: unknown,
): string | number | bigint | Uint8Array | null {
  if (value === null || value === undefined) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value instanceof Uint8Array) return value;
  try {
    return JSON.stringify(value) ?? null;
  } catch {
    return String(value);
  }
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function containsSelect(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  if (record.type === "select") return true;
  return Object.values(record).some(containsSelect);
}

const FILTER_FUNCTIONS = new Set([
  "abs",
  "coalesce",
  "ifnull",
  "instr",
  "json_extract",
  "json_type",
  "json_valid",
  "length",
  "lower",
  "ltrim",
  "nullif",
  "round",
  "rtrim",
  "substr",
  "substring",
  "trim",
  "typeof",
  "upper",
]);

function validateFilterExpression(
  value: unknown,
  columns: Set<string>,
  tableName: string,
): void {
  if (!value || typeof value !== "object") return;
  const record = value as Record<string, unknown>;
  if (record.type === "column_ref") {
    const column = record.column;
    const table = record.table;
    if (
      typeof column !== "string" ||
      !columns.has(column.toLowerCase()) ||
      (table !== null && table !== undefined && table !== tableName)
    ) {
      throw new StateQLError(
        "INVALID_SQL",
        `Unknown filter column "${String(column)}".`,
      );
    }
  } else if (record.type === "double_quote_string") {
    const column = String(record.value);
    if (!columns.has(column.toLowerCase())) {
      throw new StateQLError(
        "INVALID_SQL",
        `Unknown filter column "${column}".`,
      );
    }
  } else if (record.type === "function") {
    const name = filterFunctionName(record);
    if (!name || !FILTER_FUNCTIONS.has(name)) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter function "${name ?? "unknown"}" is not allowed.`,
      );
    }
  }
  Object.values(record).forEach((item) =>
    validateFilterExpression(item, columns, tableName),
  );
}

function filterFunctionName(record: Record<string, unknown>): string | undefined {
  const name = record.name as Record<string, unknown> | undefined;
  const parts = name?.name;
  if (!Array.isArray(parts)) return undefined;
  const last = parts.at(-1) as Record<string, unknown> | undefined;
  return typeof last?.value === "string" ? last.value.toLowerCase() : undefined;
}

function filterBindings(value: unknown): {
  positional: number;
  named: Set<string>;
} {
  const named = new Set<string>();
  const prefixes = new Map<string, string>();
  let positional = 0;
  const visit = (item: unknown): void => {
    if (!item || typeof item !== "object") return;
    const record = item as Record<string, unknown>;
    if (record.type === "origin" && record.value === "?") positional += 1;
    if (record.type === "param" && typeof record.value === "string") {
      addNamed(String(record.value), `:${String(record.value)}`);
    }
    if (
      record.type === "var" &&
      typeof record.name === "string" &&
      (record.prefix === "$" || record.prefix === "@")
    ) {
      addNamed(record.name, `${String(record.prefix)}${record.name}`);
    }
    Object.values(record).forEach(visit);
  };
  const addNamed = (name: string, token: string): void => {
    const previous = prefixes.get(name);
    if (previous && previous !== token) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter parameter "${name}" uses conflicting prefixes.`,
      );
    }
    prefixes.set(name, token);
    named.add(name);
  };
  visit(value);
  if (positional && named.size) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter cannot mix positional and named parameters.",
    );
  }
  return { positional, named };
}

function validateFilterParameters(
  filter: PreparedFilter,
  parameters: SqlParameters,
): void {
  if (filter.positionalParameters) {
    if (
      !Array.isArray(parameters) ||
      parameters.length !== filter.positionalParameters
    ) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter requires exactly ${filter.positionalParameters} positional parameters.`,
      );
    }
    return;
  }
  if (filter.namedParameters.length) {
    if (Array.isArray(parameters)) {
      throw new StateQLError(
        "INVALID_SQL",
        "Filter requires named parameters.",
      );
    }
    const supplied = Object.keys(parameters).sort();
    const expected = [...filter.namedParameters].sort();
    if (JSON.stringify(supplied) !== JSON.stringify(expected)) {
      throw new StateQLError(
        "INVALID_SQL",
        `Filter requires named parameters: ${expected.join(", ")}.`,
      );
    }
    return;
  }
  if (
    (Array.isArray(parameters) && parameters.length) ||
    (!Array.isArray(parameters) && Object.keys(parameters).length)
  ) {
    throw new StateQLError(
      "INVALID_SQL",
      "Filter predicate has no parameters.",
    );
  }
}

function markTransactionOutcomeUnknown(
  store: StateStore,
  transactionId: string,
  sessionId: string,
): void {
  try {
    store.markTransactionOutcomeUnknown(transactionId, sessionId);
  } catch {
    // A stale committing transaction is recovered as unknown after five minutes.
  }
}

function boundedReadSql(sql: string, limit: number): string {
  const statement = sql.trim().replace(/;\s*$/, "");
  return `SELECT * FROM (${statement}) AS _stateql_bounded LIMIT ${limit}`;
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

function databaseIdentity(connection: ConnectionRecord): unknown {
  return {
    driver: connection.driver,
    database: connection.database_name,
    source: connection.source,
    secretEnvironment: connection.secret_env,
  };
}

function detectDriver(target: string): "sqlite" | "postgres" {
  if (/^postgres(?:ql)?:\/\//i.test(target)) return "postgres";
  if (/^[a-z][a-z\d+.-]*:\/\//i.test(target)) {
    throw new StateQLError(
      "UNSUPPORTED_DRIVER",
      "Only PostgreSQL and SQLite are supported.",
    );
  }
  return "sqlite";
}

function normalizeSqliteSource(target: string): string {
  const source = target.startsWith("sqlite:") ? target.slice(7) : target;
  if (source === ":memory:") return source;
  return resolve(source);
}

function postgresUrlHasSecret(target: string): boolean {
  try {
    const url = new URL(target);
    return (
      Boolean(url.password) ||
      [...url.searchParams.keys()].some((key) =>
        /pass|token|secret|private[_-]?key|api[_-]?key/i.test(key),
      )
    );
  } catch {
    throw new StateQLError("INVALID_COMMAND", "Invalid PostgreSQL URL.");
  }
}

function version(connection: ConnectionRecord): string {
  return `sv_${connection.version}`;
}

function confidence(connection: ConnectionRecord): StateConfidence {
  return connection.driver === "sqlite" ? "database_reported" : "ttl_based";
}

function sessionData(session: SessionRecord): unknown {
  return {
    session_id: session.id,
    name: session.name,
    state: session.status,
    active_connection: session.active_connection_id,
    active_transaction: session.active_transaction_id,
  };
}

function profileData(profile: ProfileRecord): Record<string, unknown> {
  return {
    profile: profile.name,
    target: profile.target,
    secret_env: profile.secret_env,
    read_only: Boolean(profile.read_only),
  };
}

function validateProfileName(name: string): void {
  if (/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(name)) return;
  throw new StateQLError(
    "INVALID_COMMAND",
    "Profile name must be 1-64 letters, numbers, dots, underscores, or hyphens.",
  );
}

function isEnvironmentName(name: string): boolean {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

function operationData(operation: OperationRecord): Record<string, unknown> {
  return {
    operation_id: operation.id,
    statement_type: operation.statement_type,
    affected_rows: operation.affected_rows,
    status: operation.status,
    committed: operation.status === "committed",
    transaction_id: operation.transaction_id,
    state_version_before: operation.state_version_before,
    state_version_after: operation.state_version_after,
    ...(operation.replay_of ? { replay_of: operation.replay_of } : {}),
  };
}

function transactionData(
  transaction: NonNullable<ReturnType<StateStore["getTransaction"]>>,
  statements: number,
): unknown {
  return {
    transaction_id: transaction.id,
    state: transaction.state,
    connection_id: transaction.connection_id,
    statements,
    pending_writes: transaction.state === "active" ? statements : 0,
    start_state_version: transaction.start_version,
    isolation_level: transaction.isolation_level,
    age_ms: Date.now() - Date.parse(transaction.created_at),
  };
}

function paginationWarnings(ordered: boolean): Warning[] {
  if (ordered) return [];
  return [
    {
      code: "NON_DETERMINISTIC_PAGINATION",
      message: "Result has no explicit ORDER BY clause.",
    },
  ];
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

function rowsToCsv(rows: Row[], columns: string[]): string {
  const encode = (value: unknown): string => {
    const text =
      value === null || value === undefined
        ? ""
        : typeof value === "object"
          ? JSON.stringify(value)
          : String(value);
    return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
  };
  return [
    columns.map(encode).join(","),
    ...rows.map((row) => columns.map((column) => encode(row[column])).join(",")),
  ].join("\n") + "\n";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
