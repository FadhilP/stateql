import type {
  OperationRecord,
  ProfileRecord,
  SessionRecord,
  TransactionRecord,
} from "./store.js";
import type { Row, Warning } from "./types.js";

export function sessionData(session: SessionRecord): unknown {
  return {
    session_id: session.id,
    name: session.name,
    state: session.status,
    active_connection: session.active_connection_id,
    active_transaction: session.active_transaction_id,
  };
}

export function profileData(profile: ProfileRecord): Record<string, unknown> {
  return {
    profile: profile.name,
    target: profile.target,
    secret_env: profile.secret_env,
    read_only: Boolean(profile.read_only),
  };
}

export function operationData(operation: OperationRecord): Record<string, unknown> {
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

export function transactionData(
  transaction: TransactionRecord,
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

export function paginationWarnings(ordered: boolean): Warning[] {
  if (ordered) return [];
  return [
    {
      code: "NON_DETERMINISTIC_PAGINATION",
      message: "Result has no explicit ORDER BY clause.",
    },
  ];
}

export function rowsToCsv(rows: Row[], columns: string[]): string {
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
