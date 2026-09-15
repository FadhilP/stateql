export type SqlDriver = "sqlite" | "postgres" | "mysql";
export type Driver = SqlDriver | "mongodb" | "redis";

export type CommandOrigin = "legacy" | "user" | "model" | "system" | "api";
export type HistoryCategory = "statement" | "introspection" | "management";

/** Trusted host metadata for one executeCommand call; never part of BatchCommand input. */
export interface CommandExecutionContext {
  signal?: AbortSignal;
  origin?: CommandOrigin;
  /** Marks host-generated setup or introspection separately from user statements. */
  internal?: boolean;
}

export type CredentialAccess = "read" | "write";
export type CredentialOperation =
  | "connect"
  | "query"
  | "inspect"
  | "plan"
  | "exec"
  | "apply"
  | "transaction.commit";

export type CredentialSource = "secret_env" | "credential_ref" | "password_ref";

interface CredentialRequestBase {
  reference: string;
  actorId: string;
  session: {
    id: string;
    name: string;
  };
  operation: CredentialOperation;
  access: CredentialAccess;
  signal?: AbortSignal;
  profile?: {
    name: string;
  };
  requestedReadOnly?: boolean;
  connection?: {
    id: string;
    name: string;
    driver: Driver;
    database: string;
    readOnly: boolean;
  };
}

/** Omitted source remains backward-compatible with legacy secret_env requests. */
export type CredentialRequest = CredentialRequestBase & (
  | { source?: "secret_env" }
  | { source: "credential_ref" }
  | { source: "password_ref"; target: string }
);

export type CredentialResolver = (
  request: CredentialRequest,
) => string | undefined | Promise<string | undefined>;

export type StateConfidence =
  | "authoritative"
  | "transaction_snapshot"
  | "database_reported"
  | "local"
  | "ttl_based"
  | "unknown";

export interface Warning {
  code: string;
  message: string;
}

export interface StateQLErrorShape {
  code: string;
  message: string;
  retryable: boolean;
  executed: boolean;
  suggested_action?: string;
  [key: string]: unknown;
}

export interface ResponseMeta {
  duration_ms: number;
  state_version?: string;
  state_confidence?: StateConfidence;
}

export interface Success<T> {
  ok: true;
  command_id: string;
  session_id: string;
  data: T;
  warnings: Warning[];
  meta: ResponseMeta;
}

export interface Failure {
  ok: false;
  command_id: string;
  session_id: string;
  error: StateQLErrorShape;
  meta: ResponseMeta;
}

export type Response<T> = Success<T> | Failure;

export interface HistoryEntry {
  command_id: string;
  timestamp: string;
  session_id: string;
  actor_id: string;
  origin: CommandOrigin;
  category: HistoryCategory;
  internal: boolean;
  command: string;
  sql: string | null;
  target?: string | null;
  handle: string | null;
  executed: boolean;
  cached: boolean;
  success: boolean;
  error_code: string | null;
}

export interface StateQLSnapshotOptions {
  historyLimit?: number;
  historyCategory?: HistoryCategory;
  historyInternal?: boolean;
}


export interface StateQLSnapshot {
  session: {
    session_id: string;
    name: string;
    status: string;
  };
  actor_id: string;
  connection: {
    connection_id: string;
    /** Generated display identity; optional for older snapshot producers. */
    alias?: string;
    display_alias?: string;
    name: string;
    status: "connected";
    driver: Driver;
    database: string;
    read_only: boolean;
  } | null;
  transaction: {
    transaction_id: string;
    owner_actor_id: string;
    state: string;
  } | null;
  state_version: string | null;
  state_confidence: StateConfidence | null;
  recent_results: Array<{
    alias: string | null;
    handle: string;
    rows: number;
  }>;
  recent_operations: Array<{
    handle: string;
    actor_id: string;
    type: string;
    affected_rows: number | null;
    status: string;
  }>;
  history: HistoryEntry[];
}

export type SqlParameters = unknown[] | Record<string, unknown>;

export type MongoDocument = Record<string, unknown>;

export interface MongoFindOptions {
  projection?: MongoDocument;
  sort?: MongoDocument | Array<[string, 1 | -1]>;
  skip?: number;
  limit?: number;
  hint?: string | MongoDocument;
  collation?: MongoDocument;
}

export interface MongoAggregateOptions {
  allowDiskUse?: boolean;
  hint?: string | MongoDocument;
  collation?: MongoDocument;
}

export interface MongoMutationOptions {
  upsert?: boolean;
  collation?: MongoDocument;
  hint?: string | MongoDocument;
}


export type MongoReadCommand =
  | {
      operation: "find";
      collection: string;
      filter?: MongoDocument;
      options?: MongoFindOptions;
    }
  | {
      operation: "aggregate";
      collection: string;
      pipeline: MongoDocument[];
      options?: MongoAggregateOptions;
    };

export type MongoWriteCommand =
  | {
      operation: "insertOne";
      collection: string;
      document: MongoDocument;
      options?: Record<string, never>;
    }
  | {
      operation: "insertMany";
      collection: string;
      documents: MongoDocument[];
      options?: { ordered?: boolean };
    }
  | {
      operation: "updateOne" | "updateMany";
      collection: string;
      filter: MongoDocument;
      update: MongoDocument | MongoDocument[];
      options?: MongoMutationOptions;
    }
  | {
      operation: "replaceOne";
      collection: string;
      filter: MongoDocument;
      replacement: MongoDocument;
      options?: MongoMutationOptions;
    }
  | {
      operation: "deleteOne" | "deleteMany";
      collection: string;
      filter: MongoDocument;
      options?: Omit<MongoMutationOptions, "upsert">;
    };

export interface MongoWriteOutcome {
  acknowledged: boolean;
  inserted_id?: unknown;
  inserted_ids?: unknown[];
  inserted_count?: number;
  matched_count?: number;
  modified_count?: number;
  upserted_count?: number;
  upserted_id?: unknown;
  deleted_count?: number;
}

export interface RedisCommand {
  command: string;
  args?: string[];
}

export interface RedisWriteOutcome extends MongoWriteOutcome {
  result: string | number | null;
}

export type CatalogObjectKind =
  | "table"
  | "view"
  | "collection"
  | "function"
  | "trigger"
  | "enum"
  | "key";

export interface CatalogObject {
  kind: CatalogObjectKind;
  schema?: string;
  name: string;
  /** Stable database-native overload/object identity when name alone is ambiguous. */
  identity?: string;
  [key: string]: unknown;
}

export interface ListObjectsFilter {
  kind?: CatalogObjectKind;
  schema?: string;
  search?: string;
  /** Numeric for SQL/MongoDB; Redis uses its opaque SCAN cursor string. */
  offset?: number | string;
  limit?: number;
}

export interface ListObjectsData {
  objects: CatalogObject[];
  next_offset: number | string | null;
  supported_kinds: CatalogObjectKind[];
}

export interface DescribeObjectData {
  object: CatalogObject;
  definition?: string | Record<string, unknown> | unknown[] | null;
  [key: string]: unknown;
}


export interface ExecutionOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface HistoryOptions {
  origin?: CommandOrigin;
  category?: HistoryCategory;
  internal?: boolean;
  offset?: number;
}

export interface StateQLOptions extends ExecutionOptions {
  home?: string;
  session?: string;
  actor?: string;
  previewRows?: number;
  cacheTtlSeconds?: number;
  resultTtlSeconds?: number;
  maxCellCharacters?: number;
  maxResultRows?: number;
  maxResultBytes?: number;
  maxStateBytes?: number;
  credentialResolver?: CredentialResolver;
  /** Maximum time allowed for one credential resolution; defaults to two minutes. */
  credentialTimeoutMs?: number;
  now?: () => Date;
}

export type StateQLActorOptions = Omit<StateQLOptions, "session" | "actor"> & {
  actor: string;
};

/** Trusted-host options for opening one actor in a named shared workspace. */
export type StateQLWorkspaceOptions = StateQLActorOptions & {
  workspace: string;
};

export interface QueryOptions extends ExecutionOptions {
  params?: SqlParameters;
  cache?: "auto" | "bypass" | "require";
  previewRows?: number;
}

export interface MongoQueryOptions extends ExecutionOptions {
  cache?: "auto" | "bypass" | "require";
}

export interface FilterOptions {
  params?: SqlParameters;
}

export interface ExecOptions extends ExecutionOptions {
  params?: SqlParameters;
  replay?: boolean;
  idempotencyKey?: string;
  allowUnbounded?: boolean;
  allowDestructive?: boolean;
}

export interface MongoExecOptions extends ExecutionOptions {
  replay?: boolean;
  idempotencyKey?: string;
  allowUnbounded?: boolean;
  allowDestructive?: boolean;
}

export interface ConnectOptions extends ExecutionOptions {
  name?: string;
  readOnly?: boolean;
  secretEnv?: string;
  profile?: string;
  credentialRef?: string;
  passwordRef?: string;
}

export interface ProfileOptions {
  readOnly?: boolean;
  secretEnv?: string;
  credentialRef?: string;
  passwordRef?: string;
}

export interface ProfileUpdateOptions {
  target?: string | null;
  secretEnv?: string | null;
  credentialRef?: string | null;
  passwordRef?: string | null;
  readOnly?: boolean;
}

export interface RedisQueryOptions extends ExecutionOptions {
  cache?: "auto" | "bypass" | "require";
}

export interface RedisExecOptions extends ExecutionOptions {
  replay?: boolean;
  idempotencyKey?: string;
}

export interface RedisPlanOptions extends ExecutionOptions {}


export interface RowsOptions {
  offset?: number;
  limit?: number;
}

export interface PlanOptions extends ExecutionOptions {
  params?: SqlParameters;
  allowUnbounded?: boolean;
  allowDestructive?: boolean;
}

export interface MongoPlanOptions extends ExecutionOptions {
  allowUnbounded?: boolean;
  allowDestructive?: boolean;
}

export type BatchCommandName =
  | "connect"
  | "disconnect"
  | "status"
  | "profile.add"
  | "profile.list"
  | "profile.show"
  | "profile.remove"
  | "profile.update"
  | "session.start"
  | "session.list"
  | "session.show"
  | "session.summary"
  | "session.close"
  | "query"
  | "filter"
  | "exec"
  | "show"
  | "rows"
  | "count"
  | "columns"
  | "alias.set"
  | "inspect"
  | "transaction.begin"
  | "transaction.status"
  | "transaction.commit"
  | "transaction.rollback"
  | "plan"
  | "mongo.query"
  | "mongo.exec"
  | "mongo.plan"
  | "redis.query"
  | "redis.exec"
  | "redis.plan"
  | "objects.list"
  | "object.describe"
  | "apply"
  | "history"
  | "receipt"
  | "doctor"
  | "purge"
  | "capabilities";

export interface BatchCommand {
  command: BatchCommandName;
  target?: string;
  sql?: string;
  where?: string;
  handle?: string;
  name?: string;
  as?: string;
  kind?: string;
  table?: string;
  params?: SqlParameters;
  mongo?: MongoReadCommand | MongoWriteCommand;
  redis?: RedisCommand;
  object?: CatalogObject;
  cache?: "auto" | "bypass" | "require";
  preview_rows?: number;
  read_only?: boolean;
  secret_env?: string;
  credential_ref?: string;
  password_ref?: string | null;
  profile?: string;
  replay?: boolean;
  idempotency_key?: string;
  allow_unbounded?: boolean;
  allow_destructive?: boolean;
  offset?: number;
  cursor?: string;
  limit?: number;
  isolation?: string;
  timeout_ms?: number;
  /** Retrieval filter for the history command; does not attribute this command. */
  history_origin?: CommandOrigin;
  history_category?: HistoryCategory;
  history_internal?: boolean;
  scope?: "expired" | "results" | "history" | "all";
}

export interface BatchOptions {
  continueOnError?: boolean;
  maxCommands?: number;
  executionContext?: CommandExecutionContext;
}

export interface Column {
  name: string;
  type: string;
}

export type Row = Record<string, unknown>;

/** Data returned by the stable, non-dynamic StateQL public methods. */
export interface ConnectionData {
  connection_id: string;
  /** Persistent generated display identity; connection_id remains canonical. */
  alias: string;
  display_alias: string;
  driver: Driver;
  database: string;
  name: string;
  profile: string | null;
  read_only: boolean;
  state_version: string;
  state_confidence: StateConfidence;
}

export interface ProfileData {
  profile: string;
  target: string | null;
  secret_env: string | null;
  credential_ref: string | null;
  password_ref: string | null;
  read_only: boolean;
}

export interface ProfilesData {
  profiles: ProfileData[];
}

export interface StatusConnectionData {
  connection_id: string;
  name: string;
  driver: Driver;
  database: string;
  read_only: boolean;
}

export interface TransactionReferenceData {
  transaction_id: string;
  owner_actor_id: string;
  state: string;
}

export interface StatusData {
  session_id: string;
  session_name: string;
  actor_id: string;
  connection: StatusConnectionData | null;
  transaction: TransactionReferenceData | null;
  state_version: string | null;
}

export interface ActorLinkData {
  session_id: string;
  actor_id: string;
  linked: boolean;
}

export interface ActorUnlinkData {
  session_id: string;
  actor_id: string;
  unlinked: boolean;
}

export interface ActorData {
  actor_id: string;
  attached_at: string;
}

export interface ActorsData {
  session_id: string;
  actors: ActorData[];
}

export interface ActorResolutionData {
  actor_id: string;
  session: { session_id: string; name: string; status: string } | null;
}

export interface SessionData {
  session_id: string;
  name: string;
  state: string;
  active_connection: string | null;
  active_transaction: string | null;
}

export interface SessionListItem {
  session_id: string;
  name: string;
  status: string;
  active_connection: string | null;
  active_transaction: string | null;
}

export interface SessionsData {
  sessions: SessionListItem[];
}

export interface RecentResultData {
  alias: string | null;
  handle: string;
  rows: number;
}

export interface RecentOperationData {
  handle: string;
  actor_id: string;
  type: string;
  affected_rows: number | null;
  status: string;
}

export interface SessionSummaryData {
  session_id: string;
  name: string;
  connection: string | null;
  state_version: string | null;
  transaction: string | null;
  known_results: RecentResultData[];
  recent_operations: RecentOperationData[];
}

export interface ResultData {
  result_id: string;
  alias: string;
  /** Canonical generated alias; remains stable even when alias is an explicit caller alias. */
  display_alias: string;
  rows: number;
  columns: Column[];
  preview: Row[];
  preview_count: number;
  truncated: boolean;
  cached: boolean;
  duplicate_of?: string;
  state_version: string;
  storage: { mode: string; expires_at: string };
  next_cursor?: string | null;
}

export interface RowsData {
  result_id: string;
  offset: number;
  limit: number;
  rows: Row[];
  returned: number;
  total: number;
  truncated: boolean;
  next_offset: number | null;
}

export interface CountData {
  result_id: string;
  rows: number;
}

export interface ColumnsData {
  result_id: string;
  columns: Column[];
}

export interface AliasData {
  alias: string;
  result_id: string;
}

export interface ExportData {
  result_id: string;
  output: string;
  format: "json" | "jsonl" | "csv";
  rows: number;
}

export interface OperationData {
  operation_id: string;
  actor_id: string;
  statement_type: string;
  affected_rows: number | null;
  status: string;
  committed: boolean;
  transaction_id: string | null;
  state_version_before: string;
  state_version_after: string | null;
  replay_of?: string;
  outcome?: MongoWriteOutcome | RedisWriteOutcome;
}

export interface ExecData extends OperationData {
  duplicate?: boolean;
  duplicate_of?: string;
  duplicate_override?: boolean;
  idempotency_key?: string;
}

export interface TransactionData {
  transaction_id: string;
  state: string;
  owner_actor_id: string;
  connection_id: string;
  statements: number;
  pending_writes: number;
  start_state_version: string;
  isolation_level: string;
  age_ms: number;
}

export interface CommitTransactionData {
  transaction_id: string;
  state: string;
  statements_executed: number;
  affected_rows: number;
  state_version: string;
}

export interface RollbackTransactionData {
  transaction_id: string;
  state: string;
  discarded_statements: number;
}

export interface PlanData {
  plan_id: string;
  statement_type: string;
  destructive: boolean;
  requires_confirmation: boolean;
  required_overrides: string[];
  state_version: string;
  owner_actor_id: string;
  expires_at: string;
}

export interface ApplyData extends ExecData {
  plan_id: string;
}

export interface HistoryData {
  history: HistoryEntry[];
}

export interface DoctorData {
  integrity: "ok" | "issues";
  issues: Array<{ code: string; record?: string }>;
  migrations: string[];
  storage: { result_bytes: number; results: number; history: number };
}

export interface PurgeData {
  scope: "expired" | "results" | "history" | "all";
  deleted: number;
}

export interface CapabilitiesData {
  drivers: Driver[];
  features: Record<string, boolean>;
  driver_features?: Partial<Record<Driver, Record<string, boolean>>>;
}

export interface RemovedProfileData {
  profile: string;
  removed: boolean;
}

export interface DisconnectData {
  disconnected: boolean;
}

export interface CloseSessionData {
  session_id: string;
  state: string;
}
