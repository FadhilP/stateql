export type Driver = "sqlite" | "postgres" | "mysql";
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

export type SqlParameters = unknown[] | Record<string, unknown>;

export interface ExecutionOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface StateQLOptions extends ExecutionOptions {
  home?: string;
  session?: string;
  previewRows?: number;
  cacheTtlSeconds?: number;
  resultTtlSeconds?: number;
  maxCellCharacters?: number;
  maxResultRows?: number;
  now?: () => Date;
}

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
