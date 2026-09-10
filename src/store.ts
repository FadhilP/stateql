import { randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { join } from "node:path";
import { platform } from "node:process";
import { runMigrations } from "./migrations.js";
import { StateQLError } from "./errors.js";
import type {
  Column,
  CommandOrigin,
  Driver,
  HistoryCategory,
  MongoWriteOutcome,
  Row,
  SqlParameters,
  StateConfidence,
} from "./types.js";
import {
  isColumns,
  isRows,
  isSqlParameters,
  parseJson,
  toJsonSafe,
} from "./util.js";

const HISTORY_LIMIT_PER_SESSION = 10_000;
const MAX_HISTORY_SQL_BYTES = 4_096;
const DEFAULT_MAX_STATE_BYTES = 256 * 1024 * 1024;

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
  alias?: string;
  session_id: string;
  name: string;
  driver: Driver;
  database_name: string;
  source: string;
  secret_env: string | null;
  credential_ref: string | null;
  password_ref: string | null;
  read_only: number;
  version: number;
  created_at: string;
}

export interface ProfileRecord {
  name: string;
  target: string | null;
  secret_env: string | null;
  credential_ref: string | null;
  password_ref: string | null;
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
  alias?: string;
}

export interface OperationRecord {
  id: string;
  session_id: string;
  actor_id: string;
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
  outcome_json: string | null;
  created_at: string;
}

export interface TransactionRecord {
  id: string;
  session_id: string;
  owner_actor_id: string;
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
  owner_actor_id: string;
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
  claim_token: string | null;
  created_at: string;
}

export interface HistoryRecord {
  id: string;
  timestamp: string;
  session_id: string;
  actor_id: string;
  origin: CommandOrigin;
  category: HistoryCategory;
  internal: number;
  command: string;
  sql: string | null;
  target: string | null;
  handle: string | null;
  executed: number;
  cached: number;
  success: number;
  error_code: string | null;
}

export interface SessionMemberRecord {
  session_id: string;
  actor_id: string;
  attached_at: string;
}

export class StateStore {
  readonly db: DatabaseSync;
  private closed = false;

  constructor(
    home: string,
    private readonly now: () => Date,
    private readonly maxStateBytes = DEFAULT_MAX_STATE_BYTES,
  ) {
    const path = join(home, "state.sqlite");
    mkdirSync(home, { recursive: true, mode: 0o700 });
    if (platform !== "win32") restrictMode(home, 0o700);
    this.db = new DatabaseSync(path);
    try {
      if (platform !== "win32") restrictMode(path, 0o600);
      this.db.exec("PRAGMA journal_mode = WAL");
      if (platform !== "win32") {
        for (const suffix of ["-wal", "-shm"]) {
          const sidecar = `${path}${suffix}`;
          if (existsSync(sidecar)) restrictMode(sidecar, 0o600);
        }
      }
      this.db.exec("PRAGMA busy_timeout = 5000");
      this.db.exec("PRAGMA foreign_keys = ON");
      runMigrations(this.db, this.now);
      this.backfillConnectionAliases();
      this.backfillGeneratedAliases();
      this.recoverStaleCommittingTransactions();
      this.deleteExpiredData();
    } catch (error) {
      this.db.close();
      this.closed = true;
      throw error;
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.db.close();
  }

  randomId(prefix: string): string {
    return `${prefix}_${randomBase32Id()}`;
  }

  private insertWithRandomId(
    prefix: string,
    table: string,
    insert: (id: string) => void,
  ): string {
    for (let attempt = 0; attempt < 64; attempt++) {
      const id = this.randomId(prefix);
      try {
        insert(id);
        return id;
      } catch (error) {
        if (!isIdCollision(error, table)) throw error;
      }
    }
    throw new Error(`Could not allocate a unique ${prefix} ID.`);
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
         SET status = 'active', updated_at = ?
         WHERE id = ?`,
      )
      .run(this.now().toISOString(), closed.id);
    return this.getSessionByName(name)!;
  }

  bootstrapSession(
    name: string,
    actorId: string,
    ensureLegacyMembership: boolean,
  ): SessionRecord {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      let row = this.db
        .prepare("SELECT id, status FROM sessions WHERE name = ? LIMIT 1")
        .get(name) as { id: string; status: string } | undefined;
      const created = !row;
      if (!row) {
        const id = this.insertWithRandomId("s", "sessions", (candidate) => {
          this.db
            .prepare(
              `INSERT INTO sessions
                (id, name, status, created_at, updated_at)
               VALUES (?, ?, 'active', ?, ?)`,
            )
            .run(candidate, name, timestamp, timestamp);
        });
        row = { id, status: "active" };
      } else if (row.status !== "active") {
        this.db
          .prepare(
            `UPDATE sessions
             SET status = 'active', updated_at = ?
             WHERE id = ?`,
          )
          .run(timestamp, row.id);
      }
      if (created) {
        this.db
          .prepare(
            `INSERT INTO session_members(session_id, actor_id, attached_at)
             VALUES (?, ?, ?)`,
          )
          .run(row.id, actorId, timestamp);
      } else if (ensureLegacyMembership) {
        this.db
          .prepare(
            `INSERT OR IGNORE INTO session_members
              (session_id, actor_id, attached_at)
             VALUES (?, ?, ?)`,
          )
          .run(row.id, actorId, timestamp);
      }
      this.db.exec("COMMIT");
      return this.getSession(row.id)!;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  bootstrapWorkspace(name: string, actorId: string): SessionRecord {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      let row = this.db
        .prepare("SELECT id, status FROM sessions WHERE name = ? LIMIT 1")
        .get(name) as { id: string; status: string } | undefined;
      const identities = actorId === name ? [actorId] : [name, actorId];
      for (const identity of identities) {
        const existing = this.resolveActor(identity);
        if (existing && existing.id !== row?.id) {
          throw new StateQLError(
            "PERMISSION_DENIED",
            `Actor "${identity}" is already attached to workspace "${existing.name}" and cannot be attached to workspace "${name}".`,
          );
        }
      }

      if (!row) {
        const id = this.insertWithRandomId("s", "sessions", (candidate) => {
          this.db
            .prepare(
              `INSERT INTO sessions
                (id, name, status, created_at, updated_at)
               VALUES (?, ?, 'active', ?, ?)`,
            )
            .run(candidate, name, timestamp, timestamp);
        });
        row = { id, status: "active" };
      } else if (row.status !== "active") {
        this.db
          .prepare(
            `UPDATE sessions
             SET status = 'active', updated_at = ?
             WHERE id = ?`,
          )
          .run(timestamp, row.id);
      }

      for (const identity of identities) {
        if (this.isSessionMember(row.id, identity)) continue;
        this.db
          .prepare(
            `INSERT INTO session_members(session_id, actor_id, attached_at)
             VALUES (?, ?, ?)`,
          )
          .run(row.id, identity, timestamp);
      }
      const session = this.getSessionByName(name);
      if (!session) throw new Error(`Could not open workspace "${name}".`);
      this.db.exec("COMMIT");
      return session;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  createSession(name: string): SessionRecord {
    return this.bootstrapSession(name, name, true);
  }

  isSessionMember(sessionId: string, actorId: string): boolean {
    return Boolean(
      this.db
        .prepare(
          `SELECT 1 FROM session_members
           WHERE session_id = ? AND actor_id = ?`,
        )
        .get(sessionId, actorId),
    );
  }

  linkActor(
    sessionId: string,
    requestingActorId: string,
    actorId: string,
  ): "linked" | "already_linked" | "actor_conflict" | "denied" {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      if (!this.isSessionMember(sessionId, requestingActorId)) {
        this.db.exec("COMMIT");
        return "denied";
      }
      const existing = this.resolveActor(actorId);
      if (existing) {
        this.db.exec("COMMIT");
        return existing.id === sessionId ? "already_linked" : "actor_conflict";
      }
      this.db
        .prepare(
          `INSERT INTO session_members(session_id, actor_id, attached_at)
           VALUES (?, ?, ?)`,
        )
        .run(sessionId, actorId, this.now().toISOString());
      this.db.exec("COMMIT");
      return "linked";
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  unlinkActor(
    sessionId: string,
    requestingActorId: string,
    actorId: string,
  ): "unlinked" | "not_linked" | "owns_transaction" | "denied" {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      if (!this.isSessionMember(sessionId, requestingActorId)) {
        this.db.exec("COMMIT");
        return "denied";
      }
      const ownsTransaction = this.db
        .prepare(
          `SELECT 1 FROM transactions
           WHERE session_id = ? AND owner_actor_id = ?
             AND state IN ('active', 'committing')
           LIMIT 1`,
        )
        .get(sessionId, actorId);
      if (ownsTransaction) {
        this.db.exec("COMMIT");
        return "owns_transaction";
      }
      const result = this.db
        .prepare(
          "DELETE FROM session_members WHERE session_id = ? AND actor_id = ?",
        )
        .run(sessionId, actorId);
      this.db.exec("COMMIT");
      return Number(result.changes) ? "unlinked" : "not_linked";
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  listActors(sessionId: string): SessionMemberRecord[] {
    return this.db
      .prepare(
        `SELECT * FROM session_members
         WHERE session_id = ? ORDER BY attached_at, actor_id`,
      )
      .all(sessionId) as unknown as SessionMemberRecord[];
  }

  resolveActor(actorId: string): SessionRecord | undefined {
    return this.db
      .prepare(
        `SELECT sessions.* FROM sessions
         JOIN session_members ON session_members.session_id = sessions.id
         WHERE session_members.actor_id = ? LIMIT 1`,
      )
      .get(actorId) as SessionRecord | undefined;
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

  closeSession(sessionId: string, actorId: string): boolean {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = this.db
        .prepare(
          `UPDATE sessions
           SET status = 'closed', updated_at = ?
           WHERE id = ? AND active_transaction_id IS NULL
             AND EXISTS (
               SELECT 1 FROM session_members
               WHERE session_id = sessions.id AND actor_id = ?
             )`,
        )
        .run(this.now().toISOString(), sessionId, actorId);
      this.db.exec("COMMIT");
      return Number(result.changes) === 1;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  addProfile(input: {
    name: string;
    target?: string;
    secretEnv?: string;
    credentialRef?: string;
    passwordRef?: string;
    readOnly: boolean;
  }): ProfileRecord {
    const timestamp = this.now().toISOString();
    this.db
      .prepare(
        `INSERT INTO profiles
          (name, target, secret_env, credential_ref, password_ref, read_only, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        input.name,
        input.target ?? null,
        input.secretEnv ?? null,
        input.credentialRef ?? null,
        input.passwordRef ?? null,
        input.readOnly ? 1 : 0,
        timestamp,
        timestamp,
      );
    return this.getProfile(input.name)!;
  }

  updateProfile(input: {
    name: string;
    target: string | null;
    secretEnv: string | null;
    credentialRef: string | null;
    passwordRef: string | null;
    readOnly: boolean;
  }): ProfileRecord | undefined {
    const result = this.db.prepare(
      `UPDATE profiles
       SET target = ?, secret_env = ?, credential_ref = ?, password_ref = ?, read_only = ?, updated_at = ?
       WHERE name = ?`,
    ).run(
      input.target,
      input.secretEnv,
      input.credentialRef,
      input.passwordRef,
      input.readOnly ? 1 : 0,
      this.now().toISOString(),
      input.name,
    );
    return Number(result.changes) === 1 ? this.getProfile(input.name) : undefined;
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
    actorId: string;
    name: string;
    driver: Driver;
    databaseName: string;
    source: string;
    secretEnv?: string;
    credentialRef?: string;
    passwordRef?: string;
    readOnly: boolean;
  }): ConnectionRecord | undefined {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const allowed = this.db
        .prepare(
          `SELECT 1 FROM sessions
           JOIN session_members ON session_members.session_id = sessions.id
           WHERE sessions.id = ? AND session_members.actor_id = ?
             AND sessions.active_transaction_id IS NULL`,
        )
        .get(input.sessionId, input.actorId);
      if (!allowed) {
        this.db.exec("COMMIT");
        return undefined;
      }
      const timestamp = this.now().toISOString();
      const id = this.insertWithRandomId("conn", "connections", (candidate) => {
        this.db
          .prepare(
            `INSERT INTO connections
              (id, session_id, name, driver, database_name, source, secret_env,
               credential_ref, password_ref, read_only, version, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
          )
          .run(
            candidate,
            input.sessionId,
            input.name,
            input.driver,
            input.databaseName,
            input.source,
            input.secretEnv ?? null,
            input.credentialRef ?? null,
            input.passwordRef ?? null,
            input.readOnly ? 1 : 0,
            timestamp,
          );
      });
      this.allocateConnectionAlias(id);
      this.db
        .prepare(
          `UPDATE sessions
           SET active_connection_id = ?, updated_at = ?
           WHERE id = ?`,
        )
        .run(id, timestamp, input.sessionId);
      this.db.exec("COMMIT");
      return this.getConnection(id)!;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private allocateConnectionAlias(connectionId: string): string {
    for (let attempt = 0; attempt < 64; attempt++) {
      this.db.prepare(
        "UPDATE OR IGNORE connections SET alias = ? WHERE id = ? AND alias IS NULL",
      ).run(randomBase32Alias(), connectionId);
      const connection = this.getConnection(connectionId);
      if (connection?.alias) return connection.alias;
    }
    throw new Error("Could not allocate a unique connection alias.");
  }

  private backfillConnectionAliases(): void {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const connections = this.db.prepare(
        "SELECT id FROM connections WHERE alias IS NULL",
      ).all() as Array<{ id: string }>;
      for (const connection of connections) this.allocateConnectionAlias(connection.id);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
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

  disconnect(sessionId: string, actorId: string): boolean {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = this.db
        .prepare(
          `UPDATE sessions
           SET active_connection_id = NULL, updated_at = ?
           WHERE id = ? AND active_transaction_id IS NULL
             AND EXISTS (
               SELECT 1 FROM session_members
               WHERE session_id = sessions.id AND actor_id = ?
             )`,
        )
        .run(this.now().toISOString(), sessionId, actorId);
      this.db.exec("COMMIT");
      return Number(result.changes) === 1;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
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
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const id = this.insertWithRandomId("q", "results", (candidate) => {
        this.db
          .prepare(
            `INSERT INTO results
              (id, session_id, connection_id, fingerprint, sql, parameters,
               rows_json, columns_json, row_count, state_version, state_signature,
               state_confidence, expires_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          )
          .run(
            candidate,
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
      });
      this.enforceResultQuota(id);
      this.allocateGeneratedAlias(input.sessionId, id);
      const result = this.getResult(id)!;
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  findResult(fingerprint: string): ResultRecord | undefined {
    const result = this.db
      .prepare(
        `SELECT * FROM results
         WHERE fingerprint = ?
         ORDER BY created_at DESC
         LIMIT 1`,
      )
      .get(fingerprint) as ResultRecord | undefined;
    if (result) result.alias = this.generatedAlias(result.id);
    return result;
  }

  getResult(idOrAlias: string, sessionId?: string): ResultRecord | undefined {
    const result = this.db
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
    if (result) result.alias = this.generatedAlias(result.id);
    return result;
  }

  resultRows(result: ResultRecord): Row[] {
    return parseJson<Row[]>(
      result.rows_json,
      `result "${result.id}" rows`,
      isRows,
    );
  }

  resultColumns(result: ResultRecord): Column[] {
    return parseJson<Column[]>(
      result.columns_json,
      `result "${result.id}" columns`,
      isColumns,
    );
  }

  setAlias(sessionId: string, name: string, resultId: string): void {
    const existing = this.db.prepare(
      "SELECT result_id, generated FROM aliases WHERE session_id = ? AND name = ?",
    ).get(sessionId, name) as { result_id: string; generated: number } | undefined;
    if (existing?.generated) {
      if (existing.result_id === resultId) return;
      throw new StateQLError("INVALID_COMMAND", "Generated result aliases cannot be reassigned.");
    }
    this.db
      .prepare(
        `INSERT INTO aliases(session_id, name, result_id, generated)
         VALUES (?, ?, ?, 0)
         ON CONFLICT(session_id, name) DO UPDATE SET result_id = excluded.result_id`,
      )
      .run(sessionId, name, resultId);
  }

  generatedAlias(resultId: string): string {
    const row = this.db.prepare(
      "SELECT name FROM aliases WHERE result_id = ? AND generated = 1",
    ).get(resultId) as { name: string } | undefined;
    if (!row) throw new Error(`Result "${resultId}" has no generated alias.`);
    return row.name;
  }

  private allocateGeneratedAlias(sessionId: string, resultId: string): string {
    const existing = this.db.prepare(
      "SELECT name FROM aliases WHERE result_id = ? AND generated = 1",
    ).get(resultId) as { name: string } | undefined;
    if (existing) return existing.name;
    for (let attempt = 0; attempt < 64; attempt++) {
      const name = randomBase32Alias();
      this.db.prepare(
        "INSERT OR IGNORE INTO aliases(session_id, name, result_id, generated) VALUES (?, ?, ?, 1)",
      ).run(sessionId, name, resultId);
      const allocated = this.db.prepare(
        "SELECT name FROM aliases WHERE result_id = ? AND generated = 1",
      ).get(resultId) as { name: string } | undefined;
      if (allocated) return allocated.name;
    }
    throw new Error("Could not allocate a unique result alias.");
  }

  private backfillGeneratedAliases(): void {
    const results = this.db.prepare(
      `SELECT results.id, results.session_id
       FROM results
       LEFT JOIN aliases ON aliases.result_id = results.id AND aliases.generated = 1
       WHERE aliases.result_id IS NULL`,
    ).all() as unknown as Array<{ id: string; session_id: string }>;
    if (!results.length) return;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const result of results) this.allocateGeneratedAlias(result.session_id, result.id);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  saveOperation(input: {
    sessionId: string;
    actorId: string;
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
    const id = this.insertWithRandomId("op", "operations", (candidate) => {
      this.db
        .prepare(
          `INSERT INTO operations
            (id, session_id, actor_id, connection_id, fingerprint, sql, parameters,
             statement_type, affected_rows, status, transaction_id, replay_of,
             idempotency_key, state_version_before, state_version_after, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          candidate,
          input.sessionId,
          input.actorId,
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
    });
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
             AND COALESCE(previous_connection.credential_ref, '') =
                 COALESCE(current_connection.credential_ref, '')
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
    actorId: string;
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
  }): {
    operation?: OperationRecord;
    previous?: OperationRecord;
    denied?: "membership" | "transaction";
  } {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      if (!this.isSessionMember(input.sessionId, input.actorId)) {
        this.db.exec("COMMIT");
        return { denied: "membership" };
      }
      const session = this.db
        .prepare(
          "SELECT active_transaction_id FROM sessions WHERE id = ? AND active_connection_id = ?",
        )
        .get(input.sessionId, input.connectionId) as
        | { active_transaction_id: string | null }
        | undefined;
      const validTransaction = input.transactionId
        ? session?.active_transaction_id === input.transactionId &&
          Boolean(
            this.db
              .prepare(
                `SELECT 1 FROM transactions
                 WHERE id = ? AND session_id = ? AND connection_id = ?
                   AND owner_actor_id = ? AND state = 'active'`,
              )
              .get(
                input.transactionId,
                input.sessionId,
                input.connectionId,
                input.actorId,
              ),
          )
        : session?.active_transaction_id === null;
      if (!validTransaction) {
        this.db.exec("COMMIT");
        return { denied: "transaction" };
      }
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
        actorId: input.actorId,
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
    outcome?: MongoWriteOutcome,
  ): OperationRecord {
    this.db
      .prepare(
        `UPDATE operations
         SET status = 'committed', affected_rows = ?, state_version_after = ?,
             outcome_json = ?
         WHERE id = ?`,
      )
      .run(affectedRows, stateVersion, outcomeJson(outcome), operationId);
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
    actorId: string;
    connectionId: string;
    isolation: string;
  }): TransactionRecord | undefined {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const eligible = this.db
        .prepare(
          `SELECT connections.version FROM sessions
           JOIN session_members ON session_members.session_id = sessions.id
           JOIN connections ON connections.id = sessions.active_connection_id
           WHERE sessions.id = ? AND session_members.actor_id = ?
             AND connections.id = ? AND sessions.active_transaction_id IS NULL
             AND NOT EXISTS (
               SELECT 1 FROM operations
               WHERE operations.session_id = sessions.id
                 AND operations.status = 'executing'
             )`,
        )
        .get(input.sessionId, input.actorId, input.connectionId) as
        | { version: number }
        | undefined;
      if (!eligible) {
        this.db.exec("COMMIT");
        return undefined;
      }
      const id = this.insertWithRandomId("tx", "transactions", (candidate) => {
        this.db
          .prepare(
            `INSERT INTO transactions
              (id, session_id, owner_actor_id, connection_id, state,
               isolation_level, start_version, created_at)
             VALUES (?, ?, ?, ?, 'active', ?, ?, ?)`,
          )
          .run(
            candidate,
            input.sessionId,
            input.actorId,
            input.connectionId,
            input.isolation,
            `sv_${eligible.version}`,
            timestamp,
          );
      });
      this.db
        .prepare(
          `UPDATE sessions
           SET active_transaction_id = ?, updated_at = ?
           WHERE id = ?`,
        )
        .run(id, timestamp, input.sessionId);
      this.db.exec("COMMIT");
      return this.getTransaction(id)!;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  getTransaction(id: string): TransactionRecord | undefined {
    return this.db
      .prepare("SELECT * FROM transactions WHERE id = ?")
      .get(id) as TransactionRecord | undefined;
  }

  transactionOperations(transactionId: string): OperationRecord[] {
    return this.db
      .prepare(
        "SELECT * FROM operations WHERE transaction_id = ? ORDER BY created_at, rowid",
      )
      .all(transactionId) as unknown as OperationRecord[];
  }

  validatedTransactionOperations(transactionId: string): OperationRecord[] {
    const operations = this.transactionOperations(transactionId);
    for (const operation of operations) {
      parseJson<SqlParameters>(
        operation.parameters,
        `operation "${operation.id}" parameters`,
        isSqlParameters,
      );
    }
    return operations;
  }

  claimTransactionForCommit(
    transactionId: string,
    sessionId: string,
    actorId: string,
    expectedOperations: OperationRecord[],
  ): boolean {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const current = this.transactionOperations(transactionId);
      const unchanged =
        current.length === expectedOperations.length &&
        current.every((operation, index) => {
          const expected = expectedOperations[index];
          return expected &&
            operation.id === expected.id &&
            operation.connection_id === expected.connection_id &&
            operation.sql === expected.sql &&
            operation.parameters === expected.parameters &&
            operation.status === expected.status;
        });
      if (!unchanged) {
        this.db.exec("COMMIT");
        return false;
      }
      const result = this.db.prepare(
        `UPDATE transactions SET state = 'committing', ended_at = ?
         WHERE id = ? AND session_id = ? AND owner_actor_id = ?
           AND state = 'active' AND EXISTS (
             SELECT 1 FROM sessions
             WHERE sessions.id = transactions.session_id
               AND sessions.active_transaction_id = transactions.id
           )`,
      ).run(this.now().toISOString(), transactionId, sessionId, actorId);
      this.db.exec("COMMIT");
      return Number(result.changes) === 1;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  markTransactionCommitting(
    transactionId: string,
    sessionId: string,
    actorId: string,
  ): boolean {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = this.db
        .prepare(
          `UPDATE transactions
           SET state = 'committing', ended_at = ?
           WHERE id = ? AND session_id = ? AND owner_actor_id = ?
             AND state = 'active'
             AND EXISTS (
               SELECT 1 FROM sessions
               WHERE sessions.id = transactions.session_id
                 AND sessions.active_transaction_id = transactions.id
             )`,
        )
        .run(this.now().toISOString(), transactionId, sessionId, actorId);
      this.db.exec("COMMIT");
      return Number(result.changes) === 1;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  markTransactionOutcomeUnknown(
    transactionId: string,
    sessionId: string,
    actorId: string,
  ): void {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = this.db
        .prepare(
          `UPDATE transactions
           SET state = 'outcome_unknown', ended_at = ?
           WHERE id = ? AND session_id = ? AND owner_actor_id = ?
             AND state = 'committing'`,
        )
        .run(timestamp, transactionId, sessionId, actorId);
      if (Number(result.changes) !== 1) {
        throw new Error("Transaction ownership changed.");
      }
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
           WHERE id = ? AND active_transaction_id = ?`,
        )
        .run(timestamp, sessionId, transactionId);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  finishTransaction(
    transactionId: string,
    sessionId: string,
    actorId: string,
    state: "rolled_back" | "failed",
  ): boolean {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const expectedState = state === "rolled_back" ? "active" : "committing";
      const result = this.db
        .prepare(
          `UPDATE transactions SET state = ?, ended_at = ?
           WHERE id = ? AND session_id = ? AND owner_actor_id = ? AND state = ?`,
        )
        .run(state, timestamp, transactionId, sessionId, actorId, expectedState);
      if (Number(result.changes) !== 1) {
        this.db.exec("COMMIT");
        return false;
      }
      this.db
        .prepare(
          `UPDATE sessions
           SET active_transaction_id = NULL, updated_at = ?
           WHERE id = ? AND active_transaction_id = ?`,
        )
        .run(timestamp, sessionId, transactionId);
      this.db
        .prepare(
          `UPDATE operations SET status = ?
           WHERE transaction_id = ? AND status = 'pending'`,
        )
        .run(state, transactionId);
      this.db.exec("COMMIT");
      return true;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  commitTransactionMetadata(input: {
    transactionId: string;
    sessionId: string;
    actorId: string;
    connectionId: string;
    operations: Array<{
      id: string;
      affectedRows: number;
      outcome?: MongoWriteOutcome;
    }>;
  }): string {
    const timestamp = this.now().toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const transaction = this.db
        .prepare(
          `SELECT 1 FROM transactions
           JOIN sessions ON sessions.id = transactions.session_id
           WHERE transactions.id = ? AND transactions.session_id = ?
             AND transactions.owner_actor_id = ?
             AND transactions.connection_id = ?
             AND transactions.state = 'committing'
             AND sessions.active_transaction_id = transactions.id`,
        )
        .get(
          input.transactionId,
          input.sessionId,
          input.actorId,
          input.connectionId,
        );
      if (!transaction) throw new Error("Transaction ownership changed.");
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
             SET status = 'committed', affected_rows = ?, state_version_after = ?,
                 outcome_json = ?
             WHERE id = ?`,
          )
          .run(
            operation.affectedRows,
            stateVersion,
            outcomeJson(operation.outcome),
            operation.id,
          );
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
    ownerActorId: string;
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
    const id = this.insertWithRandomId("p", "plans", (candidate) => {
      this.db
        .prepare(
          `INSERT INTO plans
            (id, session_id, owner_actor_id, connection_id, sql, parameters,
             statement_type, state_version, state_signature, destructive,
             allow_unbounded, allow_destructive, expires_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          candidate,
          input.sessionId,
          input.ownerActorId,
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
    });
    return this.getPlan(id)!;
  }

  getPlan(id: string): PlanRecord | undefined {
    return this.db
      .prepare("SELECT * FROM plans WHERE id = ?")
      .get(id) as PlanRecord | undefined;
  }

  claimPlan(
    planId: string,
    sessionId: string,
    actorId: string,
    claimToken: string,
  ): PlanRecord | undefined {
    return this.db
      .prepare(
        `UPDATE plans SET claim_token = ?
         WHERE id = ? AND session_id = ? AND owner_actor_id = ?
           AND applied_operation_id IS NULL AND claim_token IS NULL
           AND EXISTS (
             SELECT 1 FROM session_members
             WHERE session_id = plans.session_id AND actor_id = plans.owner_actor_id
           )
         RETURNING *`,
      )
      .get(claimToken, planId, sessionId, actorId) as PlanRecord | undefined;
  }

  releasePlanClaim(planId: string, claimToken: string): void {
    this.db
      .prepare(
        "UPDATE plans SET claim_token = NULL WHERE id = ? AND claim_token = ?",
      )
      .run(planId, claimToken);
  }

  finishPlannedOperation(input: {
    planId: string;
    claimToken: string;
    operationId: string;
    connectionId: string;
    affectedRows: number;
    outcome?: MongoWriteOutcome;
  }): { operation: OperationRecord; stateVersion: string } {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const eligible = this.db
        .prepare(
          `SELECT 1 FROM operations
           JOIN plans ON plans.id = ?
           WHERE operations.id = ? AND operations.connection_id = ?
             AND operations.status = 'executing'
             AND plans.connection_id = operations.connection_id
             AND plans.session_id = operations.session_id
             AND plans.claim_token = ?
             AND plans.applied_operation_id IS NULL`,
        )
        .get(
          input.planId,
          input.operationId,
          input.connectionId,
          input.claimToken,
        );
      if (!eligible) throw new Error("Plan claim changed before finalization.");
      const row = this.db
        .prepare(
          `UPDATE connections SET version = version + 1
           WHERE id = ? RETURNING version`,
        )
        .get(input.connectionId) as { version: number };
      const stateVersion = `sv_${row.version}`;
      this.db
        .prepare(
          `UPDATE operations
           SET status = 'committed', affected_rows = ?, state_version_after = ?,
               outcome_json = ?
           WHERE id = ?`,
        )
        .run(
          input.affectedRows,
          stateVersion,
          outcomeJson(input.outcome),
          input.operationId,
        );
      this.db
        .prepare(
          `UPDATE plans SET applied_operation_id = ?, claim_token = NULL
           WHERE id = ? AND claim_token = ?`,
        )
        .run(input.operationId, input.planId, input.claimToken);
      const operation = this.getOperation(input.operationId)!;
      this.db.exec("COMMIT");
      return { operation, stateVersion };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  addHistory(input: {
    sessionId: string;
    actorId: string;
    origin?: CommandOrigin;
    category?: HistoryCategory;
    internal?: boolean;
    command: string;
    sql?: string;
    target?: string;
    handle?: string;
    executed: boolean;
    cached: boolean;
    success: boolean;
    errorCode?: string;
    id?: string;
  }): HistoryRecord {
    const insert = (id: string): void => {
      this.db
        .prepare(
          `INSERT INTO history
            (id, timestamp, session_id, actor_id, origin, category, internal, command, sql, target, handle,
             executed, cached, success, error_code)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          id,
          this.now().toISOString(),
          input.sessionId,
          input.actorId,
          input.origin ?? "legacy",
          input.category ?? "management",
          input.internal ? 1 : 0,
          input.command,
          boundedHistorySql(input.sql),
          input.target?.slice(0, 1024) ?? null,
          input.handle ?? null,
          input.executed ? 1 : 0,
          input.cached ? 1 : 0,
          input.success ? 1 : 0,
          input.errorCode ?? null,
        );
    };
    const id = input.id ?? this.insertWithRandomId("cmd", "history", insert);
    if (input.id !== undefined) insert(id);
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

  history(
    sessionId: string,
    limit: number,
    input: CommandOrigin | { origin?: CommandOrigin; category?: HistoryCategory; internal?: boolean; offset?: number } = {},
  ): HistoryRecord[] {
    const options = typeof input === "string" ? { origin: input } : input;
    const predicates = ["session_id = ?"];
    const values: Array<string | number | null> = [sessionId];
    if (options.origin !== undefined) { predicates.push("origin = ?"); values.push(options.origin); }
    if (options.category !== undefined) { predicates.push("category = ?"); values.push(options.category); }
    if (options.internal !== undefined) { predicates.push("internal = ?"); values.push(options.internal ? 1 : 0); }
    values.push(limit, options.offset ?? 0);
    return this.db
      .prepare(
        `SELECT * FROM history
         WHERE ${predicates.join(" AND ")}
         ORDER BY rowid DESC
         LIMIT ? OFFSET ?`,
      )
      .all(...values) as unknown as HistoryRecord[];
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
          AND aliases.generated = 1
         WHERE results.session_id = ?
         ORDER BY results.created_at DESC
         LIMIT ?`,
      )
      .all(sessionId, limit) as unknown as Array<
      ResultRecord & { alias: string | null }
    >;
  }

  diagnostics(sessionId: string): {
    integrity: "ok" | "issues";
    issues: Array<{ code: string; record?: string }>;
    migrations: string[];
    storage: { result_bytes: number; results: number; history: number };
  } {
    const issues: Array<{ code: string; record?: string }> = [];
    const integrity = this.db.prepare("PRAGMA integrity_check").all() as Array<{
      integrity_check: string;
    }>;
    if (integrity.some((row) => row.integrity_check !== "ok")) {
      issues.push({ code: "SQLITE_INTEGRITY" });
    }
    if (this.db.prepare("PRAGMA foreign_key_check").all().length) {
      issues.push({ code: "FOREIGN_KEY_INTEGRITY" });
    }
    const results = this.db.prepare(
      "SELECT * FROM results WHERE session_id = ?",
    ).all(sessionId) as unknown as ResultRecord[];
    for (const result of results) {
      try {
        const rows = this.resultRows(result);
        this.resultColumns(result);
        parseJson<SqlParameters>(
          result.parameters,
          `result "${result.id}" parameters`,
          isSqlParameters,
        );
        if (rows.length !== result.row_count) throw new Error("row count");
      } catch {
        issues.push({ code: "CORRUPTED_RESULT", record: result.id });
      }
    }
    for (const table of ["operations", "plans"] as const) {
      const records = this.db.prepare(
        `SELECT id, parameters FROM ${table} WHERE session_id = ?`,
      ).all(sessionId) as Array<{ id: string; parameters: string }>;
      for (const record of records) {
        try {
          parseJson<SqlParameters>(
            record.parameters,
            `${table.slice(0, -1)} "${record.id}" parameters`,
            isSqlParameters,
          );
        } catch {
          issues.push({
            code: table === "plans" ? "CORRUPTED_PLAN" : "CORRUPTED_OPERATION",
            record: record.id,
          });
        }
      }
    }
    const storage = this.db.prepare(
      `SELECT
         COUNT(*) AS results,
         COALESCE(SUM(length(CAST(sql AS BLOB)) + length(CAST(parameters AS BLOB)) +
           length(CAST(rows_json AS BLOB)) + length(CAST(columns_json AS BLOB))), 0)
           AS result_bytes,
         (SELECT COUNT(*) FROM history WHERE session_id = ?) AS history
       FROM results WHERE session_id = ?`,
    ).get(sessionId, sessionId) as {
      result_bytes: number;
      results: number;
      history: number;
    };
    return {
      integrity: issues.length ? "issues" : "ok",
      issues,
      migrations: (this.db.prepare(
        "SELECT name FROM schema_migrations ORDER BY rowid",
      ).all() as Array<{ name: string }>).map((row) => row.name),
      storage,
    };
  }

  purge(
    sessionId: string,
    scope: "expired" | "results" | "history" | "all",
  ): number {
    const before = this.db.prepare(
      `SELECT
         (SELECT COUNT(*) FROM results WHERE session_id = ?) +
         (SELECT COUNT(*) FROM plans WHERE session_id = ?) +
         (SELECT COUNT(*) FROM operations WHERE session_id = ?) +
         (SELECT COUNT(*) FROM transactions WHERE session_id = ?) +
         (SELECT COUNT(*) FROM history WHERE session_id = ?) AS count`,
    ).get(sessionId, sessionId, sessionId, sessionId, sessionId) as { count: number };
    this.db.exec("BEGIN IMMEDIATE");
    try {
      if (scope === "expired") {
        this.db.prepare(
          `DELETE FROM aliases WHERE session_id = ? AND result_id IN (
             SELECT id FROM results WHERE session_id = ? AND expires_at <= ?
           )`,
        ).run(sessionId, sessionId, this.now().toISOString());
        this.db.prepare(
          "DELETE FROM results WHERE session_id = ? AND expires_at <= ?",
        ).run(sessionId, this.now().toISOString());
        this.db.prepare(
          `DELETE FROM plans WHERE session_id = ? AND expires_at <= ?
             AND claim_token IS NULL`,
        ).run(sessionId, this.now().toISOString());
      } else {
        if (scope === "results" || scope === "all") {
          this.db.prepare("DELETE FROM aliases WHERE session_id = ?").run(sessionId);
          this.db.prepare("DELETE FROM results WHERE session_id = ?").run(sessionId);
        }
        if (scope === "history" || scope === "all") {
          this.db.prepare("DELETE FROM history WHERE session_id = ?").run(sessionId);
        }
        if (scope === "all") {
          this.db.prepare("DELETE FROM plans WHERE session_id = ?").run(sessionId);
          this.db.prepare("DELETE FROM operations WHERE session_id = ?").run(sessionId);
          this.db.prepare("DELETE FROM transactions WHERE session_id = ?").run(sessionId);
        }
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
    const after = this.db.prepare(
      `SELECT
         (SELECT COUNT(*) FROM results WHERE session_id = ?) +
         (SELECT COUNT(*) FROM plans WHERE session_id = ?) +
         (SELECT COUNT(*) FROM operations WHERE session_id = ?) +
         (SELECT COUNT(*) FROM transactions WHERE session_id = ?) +
         (SELECT COUNT(*) FROM history WHERE session_id = ?) AS count`,
    ).get(sessionId, sessionId, sessionId, sessionId, sessionId) as { count: number };
    return before.count - after.count;
  }

  private enforceResultQuota(protectedId: string): void {
    this.db.prepare(
      `DELETE FROM aliases WHERE result_id IN (
         SELECT id FROM results WHERE expires_at <= ?
       )`,
    ).run(this.now().toISOString());
    this.db.prepare("DELETE FROM results WHERE expires_at <= ?")
      .run(this.now().toISOString());
    while (this.resultBytes() > this.maxStateBytes) {
      const candidate = this.db.prepare(
        `SELECT id FROM results
         WHERE id <> ? AND NOT EXISTS (
           SELECT 1 FROM aliases WHERE aliases.result_id = results.id AND aliases.generated = 0
         )
         ORDER BY created_at, rowid LIMIT 1`,
      ).get(protectedId) as { id: string } | undefined;
      if (!candidate) {
        throw new StateQLError(
          "STATE_QUOTA_EXCEEDED",
          `Stored results exceed the ${this.maxStateBytes}-byte state quota.`,
          { suggestedAction: "Purge results or increase maxStateBytes." },
        );
      }
      this.db.prepare("DELETE FROM aliases WHERE result_id = ? AND generated = 1").run(candidate.id);
      this.db.prepare("DELETE FROM results WHERE id = ?").run(candidate.id);
    }
  }

  private resultBytes(): number {
    const row = this.db.prepare(
      `SELECT COALESCE(SUM(
         length(CAST(sql AS BLOB)) + length(CAST(parameters AS BLOB)) +
         length(CAST(rows_json AS BLOB)) + length(CAST(columns_json AS BLOB))
       ), 0) AS bytes FROM results`,
    ).get() as { bytes: number };
    return row.bytes;
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
      this.db
        .prepare("DELETE FROM plans WHERE expires_at <= ? AND claim_token IS NULL")
        .run(timestamp);
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
}

function outcomeJson(outcome: MongoWriteOutcome | undefined): string | null {
  return outcome === undefined ? null : JSON.stringify(toJsonSafe(outcome));
}

function randomBase32Alias(): string {
  return randomBase32(10);
}

function randomBase32Id(): string {
  return randomBase32(26);
}

function randomBase32(length: number): string {
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567";
  return [...randomBytes(length)].map((value) => alphabet[value & 31]).join("");
}

function isIdCollision(error: unknown, table: string): boolean {
  return error instanceof Error &&
    error.message.includes(`UNIQUE constraint failed: ${table}.id`);
}


function boundedHistorySql(sql: string | undefined): string | null {
  if (sql === undefined) return null;
  if (Buffer.byteLength(sql, "utf8") <= MAX_HISTORY_SQL_BYTES) return sql;

  const suffix = "…";
  let prefix = "";
  for (const character of sql) {
    if (
      Buffer.byteLength(`${prefix}${character}${suffix}`, "utf8") >
      MAX_HISTORY_SQL_BYTES
    ) {
      break;
    }
    prefix += character;
  }
  return `${prefix}${suffix}`;
}

function restrictMode(path: string, allowed: number): void {
  chmodSync(path, statSync(path).mode & allowed);
}
