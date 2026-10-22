import type { DatabaseAdapter, SqlAdapter } from "./adapters.js";
import { version } from "./connection.js";
import {
  adapterOfKind,
  rejectSqlOnNativeConnection,
  type ActionResult,
  type Core,
} from "./core.js";
import { StateQLError } from "./errors.js";
import { AdapterExecutionError } from "./execution.js";
import { analyzeMongoWriteSafety, serializeMongoCommand, validateMongoWriteCommand } from "./mongodb.js";
import { invalidCommand, mongoDescriptor, redisDescriptor } from "./reads.js";
import {
  deserializeRedisCommand,
  serializeRedisCommand,
  validateRedisWriteCommand,
  type RedisPrecondition,
} from "./redis.js";
import { analyzeSql, matchedRowsCount, type CountStatement } from "./sql.js";
import type { ConnectionRecord, PlanRecord, SessionRecord } from "./store.js";
import {
  compileTableUpdate,
  parseTableUpdate,
  parseTableUpdates,
  type EditableTable,
  type TableIdentity,
} from "./table-editor.js";
import type {
  ApplyData,
  ExecutionOptions,
  PlanData,
  PlanOptions,
  MongoPlanOptions,
  RedisCommand,
  SqlDriver,
  SqlParameters,
} from "./types.js";
import { isSqlParameters, parseJson } from "./util.js";
import {
  prepareMongoWrite,
  prepareRedisWrite,
  prepareSqlWrite,
  prepareTableBatch,
  runWrite,
  storedMongoWrite,
  type WriteStatement,
} from "./writes.js";

const PLAN_TTL_MS = 10 * 60_000;

interface PlanMeasurement {
  parameters?: SqlParameters;
  estimatedRows?: number;
}

/** A validated plan for one driver; runPlan records it against the current database state. */
export interface PlanStatement {
  descriptor: string;
  parameters: SqlParameters;
  statementType: string;
  destructive: boolean;
  allowUnbounded: boolean;
  allowDestructive: boolean;
  requiredOverrides: string[];
  requiresConfirmation: boolean;
  expiresAt?: string;
  measure?(adapter: DatabaseAdapter): Promise<PlanMeasurement>;
}

export type EditableMetadata = (table: TableIdentity, options: ExecutionOptions) => Promise<EditableTable>;

export function prepareSqlPlan(
  connection: ConnectionRecord,
  sql: string,
  options: PlanOptions,
): PlanStatement {
  const analysis = analyzeSql(sql, connection.driver);
  if (analysis.read) {
    throw new StateQLError("INVALID_SQL", "plan accepts write statements only.");
  }
  const parameters = options.params ?? [];
  return {
    ...guardedPlan(analysis.unboundedMutation, analysis.destructive, options),
    descriptor: sql,
    parameters,
    statementType: analysis.statementType,
    async measure(adapter) {
      const count = matchedRowsCount(analysis, parameters, connection.driver as SqlDriver);
      return { estimatedRows: await countMatchedRows(adapterOfKind(adapter, "sql"), count) };
    },
  };
}

export function prepareMongoPlan(command: unknown, options: MongoPlanOptions): PlanStatement {
  const value = invalidCommand(() => validateMongoWriteCommand(command));
  const unsafe = analyzeMongoWriteSafety(value);
  return {
    ...guardedPlan(unsafe.unbounded, unsafe.destructive, options),
    descriptor: mongoDescriptor(value.operation),
    parameters: [serializeMongoCommand(value)],
    statementType: `mongo.${value.operation}`,
  };
}

export function prepareRedisPlan(command: unknown): PlanStatement {
  const value = invalidCommand(() => validateRedisWriteCommand(command));
  return {
    descriptor: redisDescriptor(value.command),
    parameters: [],
    statementType: `redis.${value.command.toLowerCase()}`,
    destructive: value.command === "DEL" || value.command === "UNLINK",
    allowUnbounded: false,
    allowDestructive: true,
    requiredOverrides: [],
    requiresConfirmation: true,
    async measure(adapter) {
      const precondition = await adapterOfKind(adapter, "redis").precondition(value);
      return { parameters: [JSON.stringify({ command: serializeRedisCommand(value), precondition })] };
    },
  };
}

/** Saves a plan bound to the current state version and database signature. */
export async function runPlan(
  core: Core,
  session: SessionRecord,
  connection: ConnectionRecord,
  statement: PlanStatement,
  options: ExecutionOptions,
): Promise<ActionResult<PlanData>> {
  const context = core.executionContext(options);
  return core.withAdapter(session, connection, "plan", context, async (adapter) => {
    const stateSignature = await adapter.signature();
    const measured = await statement.measure?.(adapter) ?? {};
    const plan = core.store.savePlan({
      sessionId: session.id,
      ownerActorId: core.actorId,
      connectionId: connection.id,
      sql: statement.descriptor,
      parameters: measured.parameters ?? statement.parameters,
      statementType: statement.statementType,
      stateVersion: version(connection),
      stateSignature,
      destructive: statement.destructive,
      allowUnbounded: statement.allowUnbounded,
      allowDestructive: statement.allowDestructive,
      expiresAt: statement.expiresAt ?? core.expiresAt(PLAN_TTL_MS),
    });
    return {
      data: {
        plan_id: plan.id,
        statement_type: plan.statement_type,
        destructive: Boolean(plan.destructive),
        requires_confirmation: statement.requiresConfirmation,
        required_overrides: statement.requiredOverrides,
        state_version: plan.state_version,
        owner_actor_id: plan.owner_actor_id,
        expires_at: plan.expires_at,
        ...(measured.estimatedRows === undefined ? {} : { estimated_rows: measured.estimatedRows }),
      },
      handle: plan.id,
      executed: true,
      stateVersion: plan.state_version,
      confidence: adapter.confidence,
    };
  });
}

type StoredPlan =
  | { kind: "sql"; sql: string; parameters: SqlParameters; expectedRows?: 1 }
  | { kind: "mongo"; command: unknown; expectedRows?: 1 }
  | { kind: "redis"; command: RedisCommand; precondition: RedisPrecondition }
  | { kind: "table-batch"; updates: ReturnType<typeof parseTableUpdates>; compiled: ReturnType<typeof compileTableUpdate>[] };

/** Applies a plan once, only if the connection and database state still match it. */
export async function applyPlan(
  core: Core,
  session: SessionRecord,
  planId: string,
  options: ExecutionOptions,
  editableMetadata: EditableMetadata,
  recordSql: (sql: string | undefined) => void,
): Promise<ActionResult<ApplyData>> {
  core.rejectDuringStagedTransaction(session, "Plans");
  const plan = core.store.getPlan(planId);
  if (!plan || plan.session_id !== session.id) {
    throw new StateQLError("STALE_PLAN", `Plan "${planId}" was not found.`);
  }
  if (plan.owner_actor_id !== core.actorId) {
    throw new StateQLError("PERMISSION_DENIED", "Only the actor that created this plan may apply it.");
  }
  if (plan.applied_operation_id) {
    throw new StateQLError("STALE_PLAN", "Plan was already applied.", {
      extra: { previous_operation_id: plan.applied_operation_id },
    });
  }
  if (Date.parse(plan.expires_at) <= core.now().getTime()) {
    throw new StateQLError("STALE_PLAN", "Plan has expired.");
  }
  const tableMetadata = storedTableMetadata(plan);
  const stored = storedPlan(plan);
  const targetDriver = tableMetadata[0]?.driver === "mongodb" || stored.kind === "mongo"
    ? "mongodb"
    : stored.kind === "redis" ? "redis" : "sql";
  recordSql(stored.kind === "sql" ? plan.sql : undefined);

  const claimToken = core.store.randomId("claim");
  const claimed = core.store.claimPlan(plan.id, session.id, core.actorId, claimToken);
  if (!claimed) throw new StateQLError("STALE_PLAN", "Plan is already being applied.");

  let retainClaim = false;
  try {
    const connection = core.requireConnection(session);
    const stateChanged = connection.id !== claimed.connection_id || version(connection) !== claimed.state_version;
    if (stateChanged) {
      throw new StateQLError("STALE_PLAN", "Database state changed after this plan was created.");
    }
    if (targetDriver === "mongodb" && connection.driver !== "mongodb") {
      throw new StateQLError("STALE_PLAN", "MongoDB plan is not attached to a MongoDB connection.");
    }
    if (targetDriver === "redis" && connection.driver !== "redis") {
      throw new StateQLError("STALE_PLAN", "Redis plan is not attached to a Redis connection.");
    }
    if (targetDriver === "sql") rejectSqlOnNativeConnection(connection, "plan");
    for (const metadata of tableMetadata) {
      const current = await editableMetadata(metadata.table, options);
      if (JSON.stringify(current) !== JSON.stringify(metadata)) {
        throw new StateQLError("STALE_PLAN", "Table metadata changed. Reload and plan again.");
      }
    }

    const context = core.executionContext(options);
    const source = await core.resolveConnectionSource(connection, session, "apply", "write", context);
    await core.runOnAdapter(connection, context, source, async (adapter) => {
      if ((await adapter.signature()) !== claimed.state_signature) {
        throw new StateQLError("STALE_PLAN", "Database state changed after this plan was created.");
      }
    });
    const statement = plannedWrite(connection, claimed, stored);
    const result = await runWrite(
      core,
      session,
      connection,
      statement,
      { replay: stored.kind === "table-batch" },
      context,
      { planId: claimed.id, claimToken, source },
    );
    return { ...result, data: { plan_id: claimed.id, ...result.data } };
  } catch (error) {
    if (error instanceof StateQLError && error.details.code === "OUTCOME_UNKNOWN") retainClaim = true;
    throw error;
  } finally {
    if (!retainClaim) core.store.releasePlanClaim(plan.id, claimToken);
  }
}

function guardedPlan(
  unbounded: boolean,
  destructive: boolean,
  options: { allowUnbounded?: boolean; allowDestructive?: boolean },
): Pick<PlanStatement, "destructive" | "allowUnbounded" | "allowDestructive" | "requiredOverrides" | "requiresConfirmation"> {
  const allowUnbounded = options.allowUnbounded ?? false;
  const allowDestructive = options.allowDestructive ?? false;
  const requiredOverrides = [
    ...(unbounded && !allowUnbounded ? ["--allow-unbounded"] : []),
    ...(destructive && !allowDestructive ? ["--allow-destructive"] : []),
  ];
  return {
    destructive: destructive || unbounded,
    allowUnbounded,
    allowDestructive,
    requiredOverrides,
    requiresConfirmation: requiredOverrides.length > 0,
  };
}

function storedTableMetadata(plan: PlanRecord): EditableTable[] {
  if (plan.statement_type === "table.update") return [parseTableUpdate(plan.parameters).metadata];
  if (plan.statement_type === "table.updates") return parseTableUpdates(plan.parameters).map((update) => update.metadata);
  return [];
}

function storedPlan(plan: PlanRecord): StoredPlan {
  if (plan.statement_type === "table.updates") {
    const updates = parseTableUpdates(plan.parameters);
    return { kind: "table-batch", updates, compiled: updates.map(compileTableUpdate) };
  }
  if (plan.statement_type === "table.update") {
    const compiled = compileTableUpdate(parseTableUpdate(plan.parameters));
    if (compiled.sql !== plan.sql) throw new StateQLError("STALE_PLAN", "Stored update does not match its plan.");
    if (compiled.mongo) return { kind: "mongo", command: compiled.mongo, expectedRows: 1 };
    return { kind: "sql", sql: plan.sql, parameters: compiled.params, expectedRows: 1 };
  }
  if (plan.statement_type.startsWith("mongo.")) {
    return {
      kind: "mongo",
      command: storedMongoWrite(plan.parameters, plan.statement_type, `plan "${plan.id}"`, "STALE_PLAN"),
    };
  }
  if (plan.statement_type.startsWith("redis.")) {
    return { kind: "redis", ...storedRedisPlan(plan.parameters, plan.statement_type, plan.id) };
  }
  return {
    kind: "sql",
    sql: plan.sql,
    parameters: parseJson<SqlParameters>(plan.parameters, `plan "${plan.id}" parameters`, isSqlParameters),
  };
}

function plannedWrite(connection: ConnectionRecord, claimed: PlanRecord, stored: StoredPlan): WriteStatement {
  const safety = {
    allowUnbounded: Boolean(claimed.allow_unbounded),
    allowDestructive: Boolean(claimed.allow_destructive),
  };
  switch (stored.kind) {
    case "sql":
      return prepareSqlWrite(connection, claimed.sql, stored.parameters, {
        ...safety,
        ...(stored.expectedRows ? { expectedRows: stored.expectedRows } : {}),
      });
    case "mongo":
      return prepareMongoWrite(connection, stored.command, {
        ...safety,
        ...(stored.expectedRows ? { expectedRows: stored.expectedRows } : {}),
      });
    case "redis":
      return prepareRedisWrite(connection, stored.command, stored.precondition);
    case "table-batch":
      return prepareTableBatch(connection, claimed.id, stored.updates, stored.compiled);
  }
}

function storedRedisPlan(
  parameters: string,
  statementType: string,
  planId: string,
): { command: RedisCommand; precondition: RedisPrecondition } {
  try {
    const outer = JSON.parse(parameters) as unknown;
    if (!Array.isArray(outer) || outer.length !== 1 || typeof outer[0] !== "string") throw new Error();
    const payload = JSON.parse(outer[0]) as { command?: unknown; precondition?: unknown };
    if (typeof payload.command !== "string" || !payload.precondition || typeof payload.precondition !== "object") throw new Error();
    const command = deserializeRedisCommand(payload.command);
    const precondition = payload.precondition as RedisPrecondition;
    const valid = typeof precondition.key === "string" &&
      typeof precondition.fingerprint === "string" &&
      typeof precondition.expiresAt === "number" &&
      Number.isFinite(precondition.expiresAt) &&
      statementType === `redis.${command.command.toLowerCase()}`;
    if (!valid) throw new Error();
    return { command, precondition };
  } catch {
    throw new StateQLError("STALE_PLAN", `Stored Redis plan "${planId}" payload is invalid.`);
  }
}

async function countMatchedRows(
  adapter: SqlAdapter,
  count: CountStatement | undefined,
): Promise<number | undefined> {
  if (!count) return undefined;
  try {
    const { rows } = await adapter.read(count.sql, count.params);
    const value = Number(Object.values(rows[0] ?? {})[0]);
    return Number.isSafeInteger(value) ? value : undefined;
  } catch (error) {
    if (error instanceof AdapterExecutionError) throw error;
    // The estimate is advisory; a count the parser cannot express must not block the plan.
    return undefined;
  }
}
