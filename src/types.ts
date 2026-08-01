export type Driver = "sqlite" | "postgres" | "mysql";

export type CredentialAccess = "read" | "write";
export type CredentialOperation =
  | "connect"
  | "query"
  | "inspect"
  | "plan"
  | "exec"
  | "apply"
  | "transaction.commit";

export interface CredentialRequest {
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
  command: string;
  handle: string | null;
  executed: boolean;
  cached: boolean;
  success: boolean;
  error_code: string | null;
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

export interface ExecutionOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
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
  now?: () => Date;
}

export type StateQLActorOptions = Omit<StateQLOptions, "session" | "actor"> & {
  actor: string;
};

export interface QueryOptions extends ExecutionOptions {
  params?: SqlParameters;
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

export interface ConnectOptions extends ExecutionOptions {
  name?: string;
  readOnly?: boolean;
  secretEnv?: string;
  profile?: string;
}

export interface ProfileOptions {
  readOnly?: boolean;
  secretEnv?: string;
}

export interface RowsOptions {
  offset?: number;
  limit?: number;
}

export interface PlanOptions extends ExecutionOptions {
  params?: SqlParameters;
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
  cache?: "auto" | "bypass" | "require";
  read_only?: boolean;
  secret_env?: string;
  profile?: string;
  replay?: boolean;
  idempotency_key?: string;
  allow_unbounded?: boolean;
  allow_destructive?: boolean;
  offset?: number;
  limit?: number;
  isolation?: string;
  timeout_ms?: number;
  scope?: "expired" | "results" | "history" | "all";
}

export interface BatchOptions {
  continueOnError?: boolean;
  maxCommands?: number;
}

export interface Column {
  name: string;
  type: string;
}

export type Row = Record<string, unknown>;

/** Data returned by the stable, non-dynamic StateQL public methods. */
export interface ConnectionData {
  connection_id: string;
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
  rows: number;
  columns: Column[];
  preview: Row[];
  preview_count: number;
  truncated: boolean;
  cached: boolean;
  duplicate_of?: string;
  state_version: string;
  storage: { mode: string; expires_at: string };
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
