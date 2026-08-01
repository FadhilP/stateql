import { DatabaseSync } from "node:sqlite";

interface Migration {
  name: string;
  apply(db: DatabaseSync): void;
  validate(db: DatabaseSync): void;
}

const CORE_TABLES = [
  "aliases",
  "connections",
  "counters",
  "history",
  "operations",
  "plans",
  "profiles",
  "results",
  "sessions",
  "transactions",
];

const CORE_COLUMNS: Record<string, string[]> = {
  counters: ["prefix", "value"],
  sessions: ["id", "name", "status", "active_connection_id", "active_transaction_id", "created_at", "updated_at"],
  profiles: ["name", "target", "secret_env", "read_only", "created_at", "updated_at"],
  connections: ["id", "session_id", "name", "driver", "database_name", "source", "secret_env", "read_only", "version", "created_at"],
  results: ["id", "session_id", "connection_id", "fingerprint", "sql", "parameters", "rows_json", "columns_json", "row_count", "state_version", "state_signature", "state_confidence", "expires_at", "created_at"],
  aliases: ["session_id", "name", "result_id"],
  operations: ["id", "session_id", "connection_id", "fingerprint", "sql", "parameters", "statement_type", "affected_rows", "status", "transaction_id", "replay_of", "idempotency_key", "state_version_before", "state_version_after", "created_at"],
  transactions: ["id", "session_id", "connection_id", "state", "isolation_level", "start_version", "created_at", "ended_at"],
  plans: ["id", "session_id", "connection_id", "sql", "parameters", "statement_type", "state_version", "state_signature", "destructive", "allow_unbounded", "allow_destructive", "expires_at", "applied_operation_id", "created_at"],
  history: ["id", "timestamp", "session_id", "command", "handle", "executed", "cached", "success", "error_code"],
};

const MIGRATIONS: Migration[] = [
  {
    name: "initial_schema_v1",
    apply: createInitialSchema,
    validate(db) {
      requireTables(db, CORE_TABLES);
      for (const [table, columns] of Object.entries(CORE_COLUMNS)) {
        requireColumns(db, table, columns);
      }
      requireIndexes(db, [
        "history_session",
        "operations_fingerprint",
        "operations_idempotency",
        "results_fingerprint",
      ]);
      requireForeignKey(db, "connections", "session_id", "sessions", "id");
      requireForeignKey(db, "aliases", "result_id", "results", "id");
    },
  },
  {
    name: "shared_session_actors_v1",
    apply: migrateSharedSessionActors,
    validate: validateSharedSessionActors,
  },
];

export function runMigrations(db: DatabaseSync, now: () => Date): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TEXT NOT NULL
    )
  `);

  for (const migration of MIGRATIONS) {
    db.exec("BEGIN IMMEDIATE");
    try {
      // Always reapply idempotently: a migration row is not proof that its
      // schema changes survived a manually modified or partially copied store.
      migration.apply(db);
      migration.validate(db);
      db.prepare(
        `INSERT OR IGNORE INTO schema_migrations(name, applied_at)
         VALUES (?, ?)`,
      ).run(migration.name, now().toISOString());
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }
  }

  const violations = db.prepare("PRAGMA foreign_key_check").all();
  if (violations.length) {
    throw new Error("State store failed its foreign-key integrity check.");
  }
}

function createInitialSchema(db: DatabaseSync): void {
  db.exec(`
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
    CREATE TABLE IF NOT EXISTS session_members (
      session_id TEXT NOT NULL,
      actor_id TEXT NOT NULL UNIQUE,
      attached_at TEXT NOT NULL,
      PRIMARY KEY(session_id, actor_id),
      FOREIGN KEY(session_id) REFERENCES sessions(id)
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
      actor_id TEXT NOT NULL,
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
      owner_actor_id TEXT NOT NULL,
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
      owner_actor_id TEXT NOT NULL,
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
      claim_token TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS history (
      id TEXT PRIMARY KEY,
      timestamp TEXT NOT NULL,
      session_id TEXT NOT NULL,
      actor_id TEXT NOT NULL,
      command TEXT NOT NULL,
      handle TEXT,
      executed INTEGER NOT NULL,
      cached INTEGER NOT NULL,
      success INTEGER NOT NULL,
      error_code TEXT
    );
    CREATE INDEX IF NOT EXISTS history_session ON history(session_id);
  `);
}

function migrateSharedSessionActors(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS session_members (
      session_id TEXT NOT NULL,
      actor_id TEXT NOT NULL UNIQUE,
      attached_at TEXT NOT NULL,
      PRIMARY KEY(session_id, actor_id),
      FOREIGN KEY(session_id) REFERENCES sessions(id)
    )
  `);
  addColumn(db, "operations", "actor_id", "TEXT");
  addColumn(db, "transactions", "owner_actor_id", "TEXT");
  addColumn(db, "plans", "owner_actor_id", "TEXT");
  addColumn(db, "plans", "claim_token", "TEXT");
  addColumn(db, "history", "actor_id", "TEXT");
  db.exec(`
    INSERT OR IGNORE INTO session_members(session_id, actor_id, attached_at)
    SELECT id, name, created_at FROM sessions;
    UPDATE operations SET actor_id = (
      SELECT name FROM sessions WHERE sessions.id = operations.session_id
    ) WHERE actor_id IS NULL;
    UPDATE transactions SET owner_actor_id = (
      SELECT name FROM sessions WHERE sessions.id = transactions.session_id
    ) WHERE owner_actor_id IS NULL;
    UPDATE plans SET owner_actor_id = (
      SELECT name FROM sessions WHERE sessions.id = plans.session_id
    ) WHERE owner_actor_id IS NULL;
    UPDATE history SET actor_id = (
      SELECT name FROM sessions WHERE sessions.id = history.session_id
    ) WHERE actor_id IS NULL;
  `);
}

function validateSharedSessionActors(db: DatabaseSync): void {
  requireTables(db, [...CORE_TABLES, "session_members"]);
  requireColumns(db, "operations", ["actor_id"]);
  requireColumns(db, "transactions", ["owner_actor_id"]);
  requireColumns(db, "plans", ["owner_actor_id", "claim_token"]);
  requireColumns(db, "history", ["actor_id"]);
  requireForeignKey(db, "session_members", "session_id", "sessions", "id");
  const invalidMemberships = db.prepare(
    `SELECT COUNT(*) AS count FROM sessions
     LEFT JOIN session_members
       ON session_members.actor_id = sessions.name
      AND session_members.session_id = sessions.id
     WHERE session_members.actor_id IS NULL`,
  ).get() as { count: number };
  if (invalidMemberships.count) {
    throw new Error("State migration could not establish legacy session membership.");
  }
  for (const [table, column] of [
    ["operations", "actor_id"],
    ["transactions", "owner_actor_id"],
    ["plans", "owner_actor_id"],
    ["history", "actor_id"],
  ] as const) {
    const row = db.prepare(
      `SELECT COUNT(*) AS count FROM ${table} WHERE ${column} IS NULL`,
    ).get() as { count: number };
    if (row.count) throw new Error(`State migration left ${table}.${column} empty.`);
  }
}

function addColumn(
  db: DatabaseSync,
  table: string,
  column: string,
  definition: string,
): void {
  const columns = db.prepare(`PRAGMA table_info(${table})`).all() as unknown as Array<{
    name: string;
  }>;
  if (!columns.some((candidate) => candidate.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

function requireTables(db: DatabaseSync, tables: string[]): void {
  const found = new Set(
    (db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table'").all() as Array<{
      name: string;
    }>).map((row) => row.name),
  );
  for (const table of tables) {
    if (!found.has(table)) throw new Error(`State migration did not create table "${table}".`);
  }
}

function requireIndexes(db: DatabaseSync, required: string[]): void {
  const indexes = new Set(
    (db.prepare("SELECT name FROM sqlite_schema WHERE type = 'index'").all() as Array<{
      name: string;
    }>).map((index) => index.name),
  );
  for (const index of required) {
    if (!indexes.has(index)) throw new Error(`State migration did not create index "${index}".`);
  }
}

function requireForeignKey(
  db: DatabaseSync,
  table: string,
  from: string,
  target: string,
  to: string,
): void {
  const keys = db.prepare(`PRAGMA foreign_key_list(${table})`).all() as unknown as Array<{
    table: string;
    from: string;
    to: string;
  }>;
  if (!keys.some((key) => key.table === target && key.from === from && key.to === to)) {
    throw new Error(`State migration requires ${table}.${from} to reference ${target}.${to}.`);
  }
}

function requireColumns(db: DatabaseSync, table: string, required: string[]): void {
  const columns = new Set(
    (db.prepare(`PRAGMA table_info(${table})`).all() as unknown as Array<{
      name: string;
    }>).map((column) => column.name),
  );
  for (const column of required) {
    if (!columns.has(column)) {
      throw new Error(`State migration did not create ${table}.${column}.`);
    }
  }
}
