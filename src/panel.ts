import { randomUUID } from "node:crypto";
import { version } from "./connection.js";
import {
  nonNegativeInteger,
  positiveInteger,
  type Core,
} from "./core.js";
import { StateQLError } from "./errors.js";
import { runPlan, type PlanStatement } from "./plans.js";
import { csvCell } from "./response-data.js";
import type { ConnectionRecord, ResultRecord, SessionRecord } from "./store.js";
import {
  compileTableUpdate,
  editableRow,
  type EditableTable,
  type TableChange,
  type TableIdentity,
  type TableUpdate,
} from "./table-editor.js";
import type {
  Column,
  CommandOrigin,
  ExecutionOptions,
  MongoReadCommand,
  PlanData,
  QueryOptions,
  Response,
  ResultData,
  Row,
  RowsData,
  RowsOptions,
  StateQLSnapshot,
  StateQLSnapshotOptions,
} from "./types.js";
import { hash } from "./util.js";

const EDIT_TOKEN_TTL_MS = 10 * 60_000;

/** Public StateQL commands the panel calls, so they are recorded in history as usual. */
export interface PanelHost {
  snapshot(options: StateQLSnapshotOptions): StateQLSnapshot;
  inspect(kind: string, table: string, options: ExecutionOptions): Promise<Response<unknown>>;
  query(sql: string, options: QueryOptions): Promise<Response<ResultData>>;
  mongoQuery(command: MongoReadCommand, options: QueryOptions): Promise<Response<ResultData>>;
}

interface EditToken {
  metadata: EditableTable;
  original: Row;
  sessionId: string;
  connectionId: string;
  stateVersion: string;
  expires: number;
}

export type MaterializedPage = RowsData & {
  columns: Column[];
  row_tokens: Array<string | null>;
  writable_columns: string[];
  editing_reason?: string;
};

export type TableRead = ResultData & {
  table: { schema?: string; name: string };
  sample_limit: number;
  query: string;
};

/** Bounded table browsing and row editing for host UIs, with in-memory row identities. */
export class Panel {
  private readonly tableResults = new Map<string, EditableTable>();
  private readonly editTokens = new Map<string, EditToken>();
  // ponytail: cache one bounded immutable result; use indexed result storage if larger results are needed.
  private panelRows?: { id: string; json: string; rows: Row[] };

  constructor(
    private readonly core: Core,
    private readonly host: PanelHost,
  ) {}

  clear(): void {
    this.panelRows = undefined;
    this.tableResults.clear();
    this.editTokens.clear();
  }

  /** Owned, full-value pages for host UIs. Reading a page does not add a command to history. */
  readMaterialized(id: string, options: RowsOptions & { signal?: AbortSignal } = {}): MaterializedPage {
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
    const connection = this.core.store.activeConnection(this.core.store.ensureSession(this.core.sessionName));
    const editable = metadata !== undefined &&
      connection?.id === result.connection_id &&
      !connection.read_only &&
      version(connection) === result.state_version;
    const rowTokens = rows.map((row) =>
      editable && editableRow(metadata, row) ? this.issueEditToken(metadata, row, result) : null,
    );
    const writable = metadata?.driver === "mongodb"
      ? [...new Set(rows.flatMap((row) => Object.keys(row)))].filter((name) => name !== "_id")
      : metadata?.columns.filter((column) => !column.key && !column.generated).map((column) => column.name) ?? [];
    return {
      row_tokens: rowTokens,
      writable_columns: writable,
      ...(!metadata?.writable ? { editing_reason: metadata?.reason ?? "Query results are read-only." } : {}),
      result_id: result.id,
      offset,
      limit,
      rows: structuredClone(rows),
      columns: this.core.store.resultColumns(result),
      returned: rows.length,
      total: all.length,
      truncated: next < all.length,
      next_offset: next < all.length ? next : null,
    };
  }

  /** No caller-selected filesystem paths. The host delivers these bounded attachment bytes. */
  async serializeResult(
    id: string,
    format: "json" | "jsonl" | "csv",
    signal?: AbortSignal,
    origin: CommandOrigin = "api",
  ): Promise<Response<{ content: string; format: string; rows: number }>> {
    return this.core.withCommandContext({ signal, origin }, () => this.core.run("export", async () => {
      if (!["json", "jsonl", "csv"].includes(format)) throw new StateQLError("INVALID_COMMAND", "Unsupported export format.");
      const result = this.panelResult(id);
      const rows = this.fullResultRows(result);
      const columns = this.core.store.resultColumns(result).map((column) => column.name);
      const chunks: string[] = [];
      let bytes = 0;
      const append = (chunk: string) => {
        bytes += Buffer.byteLength(chunk, "utf8");
        if (bytes > 32 * 1024 * 1024) throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "Export exceeds 32 MiB.");
        chunks.push(chunk);
      };
      if (format === "json") append("[");
      if (format === "csv") append(columns.map(csvCell).join(",") + "\n");
      const deadline = Date.now() + 30_000;
      for (let index = 0; index < rows.length; index++) {
        if (index % 100 === 0) {
          await new Promise<void>((resolve) => setImmediate(resolve));
          signal?.throwIfAborted();
          this.core.signal?.throwIfAborted();
          if (Date.now() > deadline) throw new StateQLError("DEADLINE_EXCEEDED", "Export preparation timed out.");
        }
        const row = rows[index]!;
        append(format === "csv"
          ? columns.map((column) => csvCell(row[column])).join(",") + "\n"
          : (format === "json" && index ? "," : "") + JSON.stringify(row) + (format === "jsonl" ? "\n" : ""));
      }
      signal?.throwIfAborted();
      if (format === "json") append("]\n");
      this.panelResult(id);
      return { data: { content: chunks.join(""), format, rows: rows.length }, handle: id };
    }));
  }

  async readTable(
    table: { schema?: string; name: string },
    limit = 1000,
    options: QueryOptions & { origin?: CommandOrigin } = {},
  ): Promise<Response<TableRead>> {
    const context = { signal: options.signal, origin: options.origin ?? "api", internal: true };
    return this.core.withCommandContext(context, async () => {
      if (!validTableName(table) || !Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) {
        throw new StateQLError("INVALID_COMMAND", "Invalid table or sample limit.");
      }
      const driver = this.host.snapshot({ historyLimit: 1 }).connection?.driver;
      if (!driver) throw new StateQLError("CONNECTION_NOT_FOUND", "Connect to a database first.");
      if (driver === "sqlite" && table.schema && table.schema !== "main") throw new StateQLError("INVALID_COMMAND", "Only the main SQLite schema is supported.");
      if (driver === "mongodb" && table.schema) throw new StateQLError("INVALID_COMMAND", "MongoDB collections do not accept a schema.");
      if (driver === "redis") throw new StateQLError("INVALID_COMMAND", "Redis keys are not SQL tables; use redisQuery and describeObject.");
      const quote = (name: string) => driver === "mysql"
        ? "`" + name.replaceAll("`", "``") + "`"
        : '"' + name.replaceAll('"', '""') + '"';
      const qualified = [table.schema, table.name]
        .filter((part): part is string => Boolean(part))
        .map(quote)
        .join(".");
      const find: MongoReadCommand = { operation: "find", collection: table.name, options: { limit } };
      const query = driver === "mongodb"
        ? JSON.stringify(find, null, 2)
        : "SELECT * FROM " + qualified + " LIMIT " + limit;
      const metadata = await this.editableMetadata(table, options);
      const response = driver === "mongodb"
        ? await this.host.mongoQuery(find, options)
        : await this.host.query(query, options);
      if (!response.ok) return response;
      if (this.tableResults.size >= 20) this.tableResults.delete(this.tableResults.keys().next().value!);
      this.tableResults.set(response.data.result_id, metadata);
      return { ...response, data: { ...response.data, table, sample_limit: limit, query } };
    });
  }

  async planTableUpdate(
    token: string,
    changes: TableChange,
    options: ExecutionOptions & { origin?: CommandOrigin } = {},
  ): Promise<Response<PlanData>> {
    return this.core.withCommandContext({ signal: options.signal, origin: options.origin ?? "api" }, () =>
      this.core.run("table.plan", async (session) => {
        this.core.rejectDuringStagedTransaction(session, "Table edits");
        const connection = this.core.requireConnection(session);
        const original = this.liveEditToken(token, session, connection);
        if (!original) throw new StateQLError("STALE_PLAN", "Row identity expired or the connection changed. Reload the row.");
        if (connection.read_only) throw new StateQLError("READ_ONLY_CONNECTION", "This connection is read-only.");
        const metadata = await this.editableMetadata(original.metadata.table, options);
        if (JSON.stringify(metadata) !== JSON.stringify(original.metadata)) throw new StateQLError("STALE_PLAN", "Table metadata changed. Reload the table.");
        const update: TableUpdate = { metadata, original: original.original, changes };
        const compiled = compileTableUpdate(update);
        return runPlan(this.core, session, connection, tableEditPlan(
          compiled.sql,
          JSON.stringify(update),
          "table.update",
          original.expires,
        ), options);
      }));
  }

  async planTableUpdates(
    changes: Array<{ row_token: string; changes: TableChange }>,
    options: ExecutionOptions & { origin?: CommandOrigin } = {},
  ): Promise<Response<PlanData>> {
    return this.core.withCommandContext({ signal: options.signal, origin: options.origin ?? "api" }, () =>
      this.core.run("table.plan", async (session) => {
        this.core.rejectDuringStagedTransaction(session, "Table edits");
        const bounded = Array.isArray(changes) &&
          changes.length >= 1 &&
          changes.length <= 100 &&
          Buffer.byteLength(JSON.stringify(changes), "utf8") <= 256 * 1024;
        if (!bounded) throw new StateQLError("INVALID_COMMAND", "Table edit batch must contain 1-100 bounded rows.");
        const connection = this.core.requireConnection(session);
        if (connection.read_only) throw new StateQLError("READ_ONLY_CONNECTION", "This connection is read-only.");
        if (connection.driver === "redis") throw new StateQLError("UNSUPPORTED_DRIVER", "Redis does not support table edits.");
        const tokens = new Set<string>();
        const identities = new Set<string>();
        const updates: TableUpdate[] = [];
        let expires = Number.MAX_SAFE_INTEGER;
        const metadataByTable = new Map<string, EditableTable>();
        for (const item of changes) {
          if (!item || typeof item.row_token !== "string" || tokens.has(item.row_token)) throw new StateQLError("INVALID_COMMAND", "Table edit row tokens must be unique.");
          tokens.add(item.row_token);
          const original = this.liveEditToken(item.row_token, session, connection);
          if (!original) throw new StateQLError("STALE_PLAN", "A row identity expired or the connection changed. Reload the rows.");
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
        return runPlan(this.core, session, connection, tableEditPlan(
          `Conditional table update batch (${updates.length} rows)`,
          JSON.stringify({ version: 1, updates }),
          "table.updates",
          expires,
        ), options);
      }));
  }

  async editableMetadata(table: TableIdentity, options: ExecutionOptions): Promise<EditableTable> {
    const connection = this.host.snapshot({ historyLimit: 1 }).connection!;
    const driver = connection.driver;
    const unavailable: EditableTable = { table, driver, columns: [], writable: false, reason: "This table or its values cannot be edited safely." };
    // The legacy inspection API splits qualified names. Fail closed for ambiguous names.
    if (table.name.includes(".") || table.schema?.includes(".")) return { ...unavailable, reason: "Editing identifiers containing dots is not supported." };
    const name = driver === "sqlite" || driver === "mongodb" ? table.name : [table.schema, table.name].filter(Boolean).join(".");
    const response = await this.host.inspect("editable", name, options);
    if (!response.ok) return { ...unavailable, reason: response.error.code };
    const value = response.data as { writable?: boolean; columns?: EditableTable["columns"] };
    if (!value || !Array.isArray(value.columns) || value.columns.length > 100) return unavailable;
    return {
      table,
      driver,
      columns: value.columns,
      writable: value.writable === true && !connection.read_only,
      ...(value.writable ? {} : { reason: "Only ordinary tables with transactional writes and a primary key can be edited." }),
    };
  }

  private issueEditToken(metadata: EditableTable, row: Row, result: ResultRecord): string {
    const now = this.core.now().getTime();
    for (const [key, entry] of this.editTokens) if (entry.expires <= now) this.editTokens.delete(key);
    if (this.editTokens.size >= 1000) this.editTokens.delete(this.editTokens.keys().next().value!);
    const token = randomUUID();
    this.editTokens.set(token, {
      metadata,
      original: structuredClone(row),
      sessionId: result.session_id,
      connectionId: result.connection_id,
      stateVersion: result.state_version,
      expires: now + EDIT_TOKEN_TTL_MS,
    });
    return token;
  }

  private liveEditToken(token: string, session: SessionRecord, connection: ConnectionRecord): EditToken | undefined {
    const entry = this.editTokens.get(token);
    const live = entry !== undefined &&
      entry.sessionId === session.id &&
      entry.connectionId === connection.id &&
      entry.stateVersion === version(connection) &&
      entry.expires > this.core.now().getTime();
    return live ? entry : undefined;
  }

  private panelResult(id: string): ResultRecord {
    if (typeof id !== "string" || !id || id.length > 200) throw new StateQLError("INVALID_COMMAND", "A result ID is required.");
    const session = this.core.store.ensureSession(this.core.sessionName);
    if (!this.core.store.isSessionMember(session.id, this.core.actorId)) this.core.throwMembershipDenied(session);
    const result = this.core.requireResult(id, session);
    if (result.id !== id) throw new StateQLError("INVALID_COMMAND", "Use an immutable result ID, not an alias.");
    if (Date.parse(result.expires_at) <= this.core.now().getTime()) throw new StateQLError("RESULT_EXPIRED", "Result expired. Run the query again.");
    if (result.row_count > 10_000 || Buffer.byteLength(result.rows_json, "utf8") > 16 * 1024 * 1024) {
      throw new StateQLError("OUTPUT_LIMIT_EXCEEDED", "Stored result exceeds browser limits.");
    }
    return result;
  }

  private fullResultRows(result: ResultRecord): Row[] {
    if (this.panelRows?.id !== result.id || this.panelRows.json !== result.rows_json) {
      this.panelRows = { id: result.id, json: result.rows_json, rows: this.core.store.resultRows(result) };
    }
    return this.panelRows.rows;
  }
}

function tableEditPlan(descriptor: string, payload: string, statementType: string, expires: number): PlanStatement {
  return {
    descriptor,
    parameters: [payload],
    statementType,
    destructive: false,
    allowUnbounded: false,
    allowDestructive: false,
    requiredOverrides: [],
    requiresConfirmation: true,
    expiresAt: new Date(expires).toISOString(),
  };
}

function validTableName(table: { schema?: string; name: string }): boolean {
  const validPart = (value: unknown) =>
    typeof value === "string" && value.length > 0 && value.length <= 500 && !value.includes("\0");
  return Boolean(table) && validPart(table.name) && (table.schema === undefined || validPart(table.schema));
}

function tableUpdateIdentity(metadata: EditableTable, row: Row): string {
  const identity = metadata.driver === "mongodb"
    ? { table: metadata.table, id: row._id }
    : {
        table: metadata.table,
        keys: metadata.columns
          .filter((column) => column.key > 0)
          .sort((a, b) => a.key - b.key)
          .map((column) => [column.name, row[column.name]]),
      };
  return hash(identity);
}
