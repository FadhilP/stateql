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
