import { mkdirSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { join } from "node:path";
import type {
  Column,
  Driver,
  Row,
  SqlParameters,
  StateConfidence,
} from "./types.js";
import { parseJson, toJsonSafe } from "./util.js";

const HISTORY_LIMIT_PER_SESSION = 10_000;

export interface SessionRecord {
  id: string;
  name: string;
  status: string;
  active_connection_id: string | null;
  active_transaction_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface ConnectionRecord {
  id: string;
  session_id: string;
  name: string;
  driver: Driver;
  database_name: string;
  source: string;
  secret_env: string | null;
  read_only: number;
  version: number;
  created_at: string;
}

export interface ProfileRecord {
  name: string;
  target: string | null;
  secret_env: string | null;
  read_only: number;
  created_at: string;
  updated_at: string;
}

export interface ResultRecord {
  id: string;
  session_id: string;
  connection_id: string;
  fingerprint: string;
  sql: string;
  parameters: string;
  rows_json: string;
  columns_json: string;
  row_count: number;
  state_version: string;
  state_signature: string;
  state_confidence: StateConfidence;
  expires_at: string;
  created_at: string;
}

export interface OperationRecord {
  id: string;
  session_id: string;
  connection_id: string;
  fingerprint: string;
  sql: string;
  parameters: string;
  statement_type: string;
  affected_rows: number | null;
  status: string;
  transaction_id: string | null;
  replay_of: string | null;
  idempotency_key: string | null;
  state_version_before: string;
  state_version_after: string | null;
  created_at: string;
}

export interface TransactionRecord {
  id: string;
  session_id: string;
  connection_id: string;
  state: string;
  isolation_level: string;
  start_version: string;
  created_at: string;
  ended_at: string | null;
}

export interface PlanRecord {
  id: string;
  session_id: string;
  connection_id: string;
  sql: string;
  parameters: string;
  statement_type: string;
  state_version: string;
  state_signature: string;
  destructive: number;
  allow_unbounded: number;
  allow_destructive: number;
  expires_at: string;
  applied_operation_id: string | null;
  created_at: string;
}

export interface HistoryRecord {
  id: string;
  timestamp: string;
  session_id: string;
  command: string;
  handle: string | null;
  executed: number;
  cached: number;
  success: number;
  error_code: string | null;
}

export class StateStore {
  readonly db: DatabaseSync;

  constructor(
    home: string,
    private readonly now: () => Date,
  ) {
    const path = join(home, "state.sqlite");
    mkdirSync(home, { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.migrate();
    this.recoverStaleCommittingTransactions();
    this.deleteExpiredData();
  }

  close(): void {
    this.db.close();
  }

  nextId(prefix: string): string {
    this.db
      .prepare("INSERT OR IGNORE INTO counters(prefix, value) VALUES (?, 0)")
      .run(prefix);
    const row = this.db
      .prepare("UPDATE counters SET value = value + 1 WHERE prefix = ? RETURNING value")
      .get(prefix) as { value: number };
    return `${prefix}_${row.value}`;
  }

  ensureSession(name = "default"): SessionRecord {
    const existing = this.getSessionByName(name);
    if (existing) return existing;

    const closed = this.db
      .prepare("SELECT id FROM sessions WHERE name = ? LIMIT 1")
      .get(name) as { id: string } | undefined;
    if (!closed) return this.createSession(name);

    this.db
      .prepare(
        `UPDATE sessions
         SET status = 'active', active_transaction_id = NULL, updated_at = ?
         WHERE id = ?`,
      )
      .run(this.now().toISOString(), closed.id);
    return this.getSessionByName(name)!;
  }

  createSession(name: string): SessionRecord {
    const timestamp = this.now().toISOString();
    const id = this.nextId("s");
    this.db
      .prepare(
        `INSERT INTO sessions
          (id, name, status, created_at, updated_at)
         VALUES (?, ?, 'active', ?, ?)`,
      )
      .run(id, name, timestamp, timestamp);
    return this.getSession(id)!;
  }

  getSession(idOrName: string): SessionRecord | undefined {
    return this.db
      .prepare(
        `SELECT * FROM sessions
         WHERE (id = ? OR name = ?) AND status = 'active'
         LIMIT 1`,
      )
      .get(idOrName, idOrName) as SessionRecord | undefined;
  }

  getSessionByName(name: string): SessionRecord | undefined {
    return this.db
      .prepare(
        "SELECT * FROM sessions WHERE name = ? AND status = 'active' LIMIT 1",
      )
      .get(name) as SessionRecord | undefined;
  }

  listSessions(): SessionRecord[] {
    return this.db
      .prepare("SELECT * FROM sessions ORDER BY created_at")
      .all() as unknown as SessionRecord[];
  }

  closeSession(sessionId: string): void {
    const timestamp = this.now().toISOString();
    this.db
      .prepare(
        `UPDATE sessions
         SET status = 'closed', active_transaction_id = NULL, updated_at = ?
         WHERE id = ?`,
      )
      .run(timestamp, sessionId);
  }

  addProfile(input: {
    name: string;
    target?: string;
    secretEnv?: string;
    readOnly: boolean;
  }): ProfileRecord {
    const timestamp = this.now().toISOString();
    this.db
      .prepare(
        `INSERT INTO profiles
          (name, target, secret_env, read_only, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        input.name,
        input.target ?? null,
        input.secretEnv ?? null,
        input.readOnly ? 1 : 0,
        timestamp,
        timestamp,
      );
    return this.getProfile(input.name)!;
  }

  getProfile(name: string): ProfileRecord | undefined {
    return this.db
      .prepare("SELECT * FROM profiles WHERE name = ?")
      .get(name) as ProfileRecord | undefined;
  }

  listProfiles(): ProfileRecord[] {
    return this.db
      .prepare("SELECT * FROM profiles ORDER BY name")
      .all() as unknown as ProfileRecord[];
  }

  removeProfile(name: string): boolean {
    const result = this.db
      .prepare("DELETE FROM profiles WHERE name = ?")
      .run(name);
    return Number(result.changes) > 0;
  }

  addConnection(input: {
    sessionId: string;
    name: string;
    driver: Driver;
    databaseName: string;
    source: string;
    secretEnv?: string;
    readOnly: boolean;
  }): ConnectionRecord {
    const id = this.nextId("conn");
    this.db
      .prepare(
        `INSERT INTO connections
          (id, session_id, name, driver, database_name, source, secret_env,
           read_only, version, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
      )
      .run(
        id,
        input.sessionId,
        input.name,
        input.driver,
        input.databaseName,
        input.source,
        input.secretEnv ?? null,
        input.readOnly ? 1 : 0,
        this.now().toISOString(),
      );
    this.db
      .prepare(
        `UPDATE sessions
         SET active_connection_id = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(id, this.now().toISOString(), input.sessionId);
    return this.getConnection(id)!;
  }

  getConnection(id: string): ConnectionRecord | undefined {
    return this.db
      .prepare("SELECT * FROM connections WHERE id = ?")
      .get(id) as ConnectionRecord | undefined;
  }

  activeConnection(session: SessionRecord): ConnectionRecord | undefined {
    if (!session.active_connection_id) return undefined;
    return this.getConnection(session.active_connection_id);
  }

  disconnect(sessionId: string): void {
    this.db
      .prepare(
        `UPDATE sessions
         SET active_connection_id = NULL, active_transaction_id = NULL,
             updated_at = ?
         WHERE id = ?`,
      )
      .run(this.now().toISOString(), sessionId);
  }

  bumpVersion(connectionId: string): string {
    const row = this.db
      .prepare(
        `UPDATE connections
         SET version = version + 1
         WHERE id = ?
         RETURNING version`,
      )
      .get(connectionId) as { version: number };
    return `sv_${row.version}`;
  }

  saveResult(input: {
    sessionId: string;
    connectionId: string;
    fingerprint: string;
    sql: string;
    parameters: SqlParameters;
    rows: Row[];
    columns: Column[];
    stateVersion: string;
    stateSignature: string;
    stateConfidence: StateConfidence;
    expiresAt: string;
  }): ResultRecord {
    const id = this.nextId("q");
    this.db
      .prepare(
        `INSERT INTO results
          (id, session_id, connection_id, fingerprint, sql, parameters,
           rows_json, columns_json, row_count, state_version, state_signature,
           state_confidence, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.sessionId,
        input.connectionId,
        input.fingerprint,
        input.sql,
        JSON.stringify(toJsonSafe(input.parameters)),
        JSON.stringify(toJsonSafe(input.rows)),
        JSON.stringify(input.columns),
        input.rows.length,
        input.stateVersion,
        input.stateSignature,
        input.stateConfidence,
        input.expiresAt,
        this.now().toISOString(),
      );
    return this.getResult(id)!;
  }

  findResult(fingerprint: string): ResultRecord | undefined {
    return this.db
      .prepare(
        `SELECT * FROM results
         WHERE fingerprint = ?
         ORDER BY created_at DESC
         LIMIT 1`,
      )
      .get(fingerprint) as ResultRecord | undefined;
  }

  getResult(idOrAlias: string, sessionId?: string): ResultRecord | undefined {
    return this.db
      .prepare(
        `SELECT results.*
         FROM results
         LEFT JOIN aliases
           ON aliases.result_id = results.id
          AND aliases.session_id = results.session_id
         WHERE (results.id = ? OR aliases.name = ?)
           AND (? IS NULL OR results.session_id = ?)
         LIMIT 1`,
      )
      .get(idOrAlias, idOrAlias, sessionId ?? null, sessionId ?? null) as
      | ResultRecord
      | undefined;
  }

  resultRows(result: ResultRecord): Row[] {
    return parseJson<Row[]>(result.rows_json, []);
  }

  resultColumns(result: ResultRecord): Column[] {
    return parseJson<Column[]>(result.columns_json, []);
  }

  setAlias(sessionId: string, name: string, resultId: string): void {
    this.db
      .prepare(
        `INSERT INTO aliases(session_id, name, result_id)
         VALUES (?, ?, ?)
         ON CONFLICT(session_id, name) DO UPDATE SET result_id = excluded.result_id`,
      )
      .run(sessionId, name, resultId);
  }

  saveOperation(input: {
    sessionId: string;
    connectionId: string;
    fingerprint: string;
    sql: string;
    parameters: SqlParameters;
    statementType: string;
    affectedRows?: number;
    status: string;
    transactionId?: string;
    replayOf?: string;
    idempotencyKey?: string;
    stateVersionBefore: string;
    stateVersionAfter?: string;
  }): OperationRecord {
    const id = this.nextId("op");
    this.db
      .prepare(
        `INSERT INTO operations
          (id, session_id, connection_id, fingerprint, sql, parameters,
           statement_type, affected_rows, status, transaction_id, replay_of,
           idempotency_key, state_version_before, state_version_after, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.sessionId,
        input.connectionId,
        input.fingerprint,
        input.sql,
        JSON.stringify(toJsonSafe(input.parameters)),
        input.statementType,
        input.affectedRows ?? null,
        input.status,
        input.transactionId ?? null,
        input.replayOf ?? null,
        input.idempotencyKey ?? null,
        input.stateVersionBefore,
        input.stateVersionAfter ?? null,
        this.now().toISOString(),
      );
    return this.getOperation(id)!;
  }

  getOperation(id: string): OperationRecord | undefined {
    return this.db
      .prepare("SELECT * FROM operations WHERE id = ?")
      .get(id) as OperationRecord | undefined;
  }

  findDuplicateOperation(input: {
    connectionId: string;
    fingerprint: string;
    idempotencyKey?: string;
  }): OperationRecord | undefined {
    if (input.idempotencyKey) {
      return this.db
        .prepare(
          `SELECT operations.*
           FROM operations
           JOIN connections previous_connection
             ON previous_connection.id = operations.connection_id
           JOIN connections current_connection
             ON current_connection.id = ?
           WHERE operations.idempotency_key = ?
             AND operations.status IN
               ('committed', 'pending', 'executing', 'outcome_unknown')
             AND previous_connection.driver = current_connection.driver
             AND previous_connection.source = current_connection.source
             AND COALESCE(previous_connection.secret_env, '') =
                 COALESCE(current_connection.secret_env, '')
             AND previous_connection.database_name = current_connection.database_name
           ORDER BY operations.created_at DESC LIMIT 1`,
        )
        .get(input.connectionId, input.idempotencyKey) as
        | OperationRecord
        | undefined;
    }
    return this.db
      .prepare(
        `SELECT * FROM operations
         WHERE fingerprint = ?
           AND status IN
             ('committed', 'pending', 'executing', 'outcome_unknown')
         ORDER BY created_at DESC LIMIT 1`,
      )
      .get(input.fingerprint) as OperationRecord | undefined;
  }

  reserveOperation(input: {
    sessionId: string;
    connectionId: string;
    fingerprint: string;
    sql: string;
    parameters: SqlParameters;
    statementType: string;
    status: "executing" | "pending";
    transactionId?: string;
    replay: boolean;
    idempotencyKey?: string;
    stateVersionBefore: string;
  }): { operation?: OperationRecord; previous?: OperationRecord } {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const previous = this.findDuplicateOperation({
        connectionId: input.connectionId,
        fingerprint: input.fingerprint,
        idempotencyKey: input.idempotencyKey,
      });
      if (previous && !input.replay) {
        this.db.exec("COMMIT");
        return { previous };
      }
      const operation = this.saveOperation({
        sessionId: input.sessionId,
        connectionId: input.connectionId,
        fingerprint: input.fingerprint,
        sql: input.sql,
        parameters: input.parameters,
        statementType: input.statementType,
        status: input.status,
        transactionId: input.transactionId,
        replayOf: previous?.id,
        idempotencyKey: input.replay ? undefined : input.idempotencyKey,
        stateVersionBefore: input.stateVersionBefore,
      });
      this.db.exec("COMMIT");
      return { operation, previous };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  finishOperation(
    operationId: string,
    affectedRows: number,
    stateVersion: string,
  ): OperationRecord {
    this.db
      .prepare(
        `UPDATE operations
         SET status = 'committed', affected_rows = ?, state_version_after = ?
         WHERE id = ?`,
      )
      .run(affectedRows, stateVersion, operationId);
    return this.getOperation(operationId)!;
  }

  failOperation(operationId: string): void {
    this.db
      .prepare("UPDATE operations SET status = 'failed' WHERE id = ?")
      .run(operationId);
  }

  markOperationOutcomeUnknown(operationId: string): void {
    this.db
      .prepare("UPDATE operations SET status = 'outcome_unknown' WHERE id = ?")
      .run(operationId);
  }

  createTransaction(input: {
    sessionId: string;
    connectionId: string;
    isolation: string;
    startVersion: string;
  }): TransactionRecord {
    const id = this.nextId("tx");
    const timestamp = this.now().toISOString();
    this.db
      .prepare(
        `INSERT INTO transactions
          (id, session_id, connection_id, state, isolation_level,
           start_version, created_at)
         VALUES (?, ?, ?, 'active', ?, ?, ?)`,
      )
      .run(
        id,
        input.sessionId,
        input.connectionId,
        input.isolation,
        input.startVersion,
        timestamp,
      );
    this.db
      .prepare(
        `UPDATE sessions
         SET active_transaction_id = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(id, timestamp, input.sessionId);
    return this.getTransaction(id)!;
  }

  getTransaction(id: string): TransactionRecord | undefined {
    return this.db
      .prepare("SELECT * FROM transactions WHERE id = ?")
      .get(id) as TransactionRecord | undefined;
  }

  transactionOperations(transactionId: string): OperationRecord[] {
    return this.db
      .prepare(
        "SELECT * FROM operations WHERE transaction_id = ? ORDER BY created_at",
      )
      .all(transactionId) as unknown as OperationRecord[];
  }

  markTransactionCommitting(transactionId: string): boolean {
    const result = this.db
      .prepare(
        `UPDATE transactions
         SET state = 'committing', ended_at = ?
         WHERE id = ? AND state = 'active'`,
      )
      .run(this.now().toISOString(), transactionId);
    return Number(result.changes) === 1;
  }

  markTransactionOutcomeUnknown(
    transactionId: string,
    sessionId: string,
  ): void {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db
        .prepare(
          `UPDATE transactions
           SET state = 'outcome_unknown', ended_at = ?
           WHERE id = ?`,
        )
        .run(timestamp, transactionId);
      this.db
        .prepare(
          `UPDATE operations SET status = 'outcome_unknown'
           WHERE transaction_id = ? AND status = 'pending'`,
        )
        .run(transactionId);
      this.db
        .prepare(
          `UPDATE sessions
           SET active_transaction_id = NULL, updated_at = ?
           WHERE id = ?`,
        )
        .run(timestamp, sessionId);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  finishTransaction(
    transactionId: string,
    sessionId: string,
    state: "committed" | "rolled_back" | "failed",
  ): void {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db
        .prepare(
          "UPDATE transactions SET state = ?, ended_at = ? WHERE id = ?",
        )
        .run(state, timestamp, transactionId);
      this.db
        .prepare(
          `UPDATE sessions
           SET active_transaction_id = NULL, updated_at = ?
           WHERE id = ?`,
        )
        .run(timestamp, sessionId);
      if (state === "rolled_back" || state === "failed") {
        this.db
          .prepare(
            `UPDATE operations SET status = ?
             WHERE transaction_id = ? AND status = 'pending'`,
          )
          .run(state, transactionId);
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  commitTransactionMetadata(input: {
    transactionId: string;
    sessionId: string;
    connectionId: string;
    operations: Array<{ id: string; affectedRows: number }>;
  }): string {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      let stateVersion = `sv_${(
        this.getConnection(input.connectionId)?.version ?? 0
      )}`;
      for (const operation of input.operations) {
        const row = this.db
          .prepare(
            `UPDATE connections
             SET version = version + 1
             WHERE id = ?
             RETURNING version`,
          )
          .get(input.connectionId) as { version: number };
        stateVersion = `sv_${row.version}`;
        this.db
          .prepare(
            `UPDATE operations
             SET status = 'committed', affected_rows = ?, state_version_after = ?
             WHERE id = ?`,
          )
          .run(operation.affectedRows, stateVersion, operation.id);
      }
      this.db
        .prepare(
          `UPDATE transactions
           SET state = 'committed', ended_at = ?
           WHERE id = ?`,
        )
        .run(timestamp, input.transactionId);
      this.db
        .prepare(
          `UPDATE sessions
           SET active_transaction_id = NULL, updated_at = ?
           WHERE id = ?`,
        )
        .run(timestamp, input.sessionId);
      this.db.exec("COMMIT");
      return stateVersion;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  savePlan(input: {
    sessionId: string;
    connectionId: string;
    sql: string;
    parameters: SqlParameters;
    statementType: string;
    stateVersion: string;
    stateSignature: string;
    destructive: boolean;
    allowUnbounded: boolean;
    allowDestructive: boolean;
    expiresAt: string;
  }): PlanRecord {
    const id = this.nextId("p");
    this.db
      .prepare(
        `INSERT INTO plans
          (id, session_id, connection_id, sql, parameters, statement_type,
           state_version, state_signature, destructive, allow_unbounded,
           allow_destructive, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.sessionId,
        input.connectionId,
        input.sql,
        JSON.stringify(toJsonSafe(input.parameters)),
        input.statementType,
        input.stateVersion,
        input.stateSignature,
        input.destructive ? 1 : 0,
        input.allowUnbounded ? 1 : 0,
        input.allowDestructive ? 1 : 0,
        input.expiresAt,
        this.now().toISOString(),
      );
    return this.getPlan(id)!;
  }

  getPlan(id: string): PlanRecord | undefined {
    return this.db
      .prepare("SELECT * FROM plans WHERE id = ?")
      .get(id) as PlanRecord | undefined;
  }

  markPlanApplied(planId: string, operationId: string): void {
    this.db
      .prepare("UPDATE plans SET applied_operation_id = ? WHERE id = ?")
      .run(operationId, planId);
  }

  addHistory(input: {
    sessionId: string;
    command: string;
    handle?: string;
    executed: boolean;
    cached: boolean;
    success: boolean;
    errorCode?: string;
    id?: string;
  }): HistoryRecord {
    const id = input.id ?? this.nextId("cmd");
    this.db
      .prepare(
        `INSERT INTO history
          (id, timestamp, session_id, command, handle, executed, cached,
           success, error_code)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        this.now().toISOString(),
        input.sessionId,
        input.command,
        input.handle ?? null,
        input.executed ? 1 : 0,
        input.cached ? 1 : 0,
        input.success ? 1 : 0,
        input.errorCode ?? null,
      );
    this.db
      .prepare(
        `DELETE FROM history
         WHERE rowid IN (
           SELECT rowid FROM history
           WHERE session_id = ?
           ORDER BY rowid DESC
           LIMIT -1 OFFSET ?
         )`,
      )
      .run(input.sessionId, HISTORY_LIMIT_PER_SESSION);
    return this.db
      .prepare("SELECT * FROM history WHERE id = ?")
      .get(id) as unknown as HistoryRecord;
  }

  history(sessionId: string, limit: number): HistoryRecord[] {
    return this.db
      .prepare(
        `SELECT * FROM history
         WHERE session_id = ?
         ORDER BY rowid DESC
         LIMIT ?`,
      )
      .all(sessionId, limit) as unknown as HistoryRecord[];
  }

  recentOperations(sessionId: string, limit: number): OperationRecord[] {
    return this.db
      .prepare(
        `SELECT * FROM operations
         WHERE session_id = ?
         ORDER BY created_at DESC
         LIMIT ?`,
      )
      .all(sessionId, limit) as unknown as OperationRecord[];
  }

  knownResults(sessionId: string, limit: number): Array<ResultRecord & { alias: string | null }> {
    return this.db
      .prepare(
        `SELECT results.*, aliases.name AS alias
         FROM results
         LEFT JOIN aliases
           ON aliases.result_id = results.id
          AND aliases.session_id = results.session_id
         WHERE results.session_id = ?
         ORDER BY results.created_at DESC
         LIMIT ?`,
      )
      .all(sessionId, limit) as unknown as Array<
      ResultRecord & { alias: string | null }
    >;
  }

  private deleteExpiredData(): void {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db
        .prepare(
          `DELETE FROM aliases
           WHERE result_id IN (
             SELECT id FROM results WHERE expires_at <= ?
           )`,
        )
        .run(timestamp);
      this.db.prepare("DELETE FROM results WHERE expires_at <= ?").run(timestamp);
      this.db.prepare("DELETE FROM plans WHERE expires_at <= ?").run(timestamp);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private recoverStaleCommittingTransactions(): void {
    const timestamp = this.now().toISOString();
    const cutoff = new Date(this.now().getTime() - 5 * 60_000).toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db
        .prepare(
          `UPDATE operations SET status = 'outcome_unknown'
           WHERE status = 'pending' AND transaction_id IN (
             SELECT id FROM transactions
             WHERE state = 'committing' AND ended_at <= ?
           )`,
        )
        .run(cutoff);
      this.db
        .prepare(
          `UPDATE sessions
           SET active_transaction_id = NULL, updated_at = ?
           WHERE active_transaction_id IN (
             SELECT id FROM transactions
             WHERE state = 'committing' AND ended_at <= ?
           )`,
        )
        .run(timestamp, cutoff);
      this.db
        .prepare(
          `UPDATE transactions
           SET state = 'outcome_unknown', ended_at = ?
           WHERE state = 'committing' AND ended_at <= ?`,
        )
        .run(timestamp, cutoff);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS counters (
        prefix TEXT PRIMARY KEY,
        value INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL,
        active_connection_id TEXT,
        active_transaction_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS profiles (
        name TEXT PRIMARY KEY,
        target TEXT,
        secret_env TEXT,
        read_only INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        CHECK(target IS NOT NULL OR secret_env IS NOT NULL)
      );
      CREATE TABLE IF NOT EXISTS connections (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        name TEXT NOT NULL,
        driver TEXT NOT NULL,
        database_name TEXT NOT NULL,
        source TEXT NOT NULL,
        secret_env TEXT,
        read_only INTEGER NOT NULL,
        version INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(session_id) REFERENCES sessions(id)
      );
      CREATE TABLE IF NOT EXISTS results (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        sql TEXT NOT NULL,
        parameters TEXT NOT NULL,
        rows_json TEXT NOT NULL,
        columns_json TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        state_version TEXT NOT NULL,
        state_signature TEXT NOT NULL,
        state_confidence TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS results_fingerprint
        ON results(fingerprint, created_at);
      CREATE TABLE IF NOT EXISTS aliases (
        session_id TEXT NOT NULL,
        name TEXT NOT NULL,
        result_id TEXT NOT NULL,
        PRIMARY KEY(session_id, name),
        FOREIGN KEY(result_id) REFERENCES results(id)
      );
      CREATE TABLE IF NOT EXISTS operations (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        sql TEXT NOT NULL,
        parameters TEXT NOT NULL,
        statement_type TEXT NOT NULL,
        affected_rows INTEGER,
        status TEXT NOT NULL,
        transaction_id TEXT,
        replay_of TEXT,
        idempotency_key TEXT,
        state_version_before TEXT NOT NULL,
        state_version_after TEXT,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS operations_fingerprint
        ON operations(connection_id, fingerprint, status);
      CREATE UNIQUE INDEX IF NOT EXISTS operations_idempotency
        ON operations(connection_id, idempotency_key)
        WHERE idempotency_key IS NOT NULL AND status IN ('committed', 'pending');
      CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        state TEXT NOT NULL,
        isolation_level TEXT NOT NULL,
        start_version TEXT NOT NULL,
        created_at TEXT NOT NULL,
        ended_at TEXT
      );
      CREATE TABLE IF NOT EXISTS plans (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        sql TEXT NOT NULL,
        parameters TEXT NOT NULL,
        statement_type TEXT NOT NULL,
        state_version TEXT NOT NULL,
        state_signature TEXT NOT NULL,
        destructive INTEGER NOT NULL,
        allow_unbounded INTEGER NOT NULL,
        allow_destructive INTEGER NOT NULL,
        expires_at TEXT NOT NULL,
        applied_operation_id TEXT,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS history (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,
        session_id TEXT NOT NULL,
        command TEXT NOT NULL,
        handle TEXT,
        executed INTEGER NOT NULL,
        cached INTEGER NOT NULL,
        success INTEGER NOT NULL,
        error_code TEXT
      );
      CREATE INDEX IF NOT EXISTS history_session
        ON history(session_id);
    `);
  }
}
