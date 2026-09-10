import assert from "node:assert/strict";
import crypto from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { mkdirSync, statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  StateQL,
  StateQLError,
  type HistoryEntry,
  type StateQLSnapshot,
} from "../src/index.js";
import { StateStore } from "../src/store.js";
import {
  assertFailure,
  createFixture,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

test("state persists across restarts, expires, and detects external SQLite writes", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE values_table (id INTEGER PRIMARY KEY, value TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO values_table (value) VALUES ('one')"),
  );
  const initial = await succeed(
    fixture.stateql.query("SELECT * FROM values_table ORDER BY id"),
  );
  fixture.stateql.close();

  const restarted = new StateQL({
    home: fixture.home,
    now: () => clock,
    cacheTtlSeconds: 300,
  });
  assert.equal(
    (await succeed(restarted.show(String(initial.result_id)))).result_id,
    initial.result_id,
  );

  const external = new DatabaseSync(fixture.database);
  external
    .prepare("INSERT INTO values_table (value) VALUES ('external')")
    .run();
  external.close();
  const refreshed = await succeed(
    restarted.query("SELECT * FROM values_table ORDER BY id"),
  );
  assert.notEqual(refreshed.result_id, initial.result_id);
  assert.equal(refreshed.rows, 2);

  clock = new Date(clock.getTime() + 86_401_000);
  assertFailure(
    await restarted.show(String(refreshed.result_id)),
    "RESULT_EXPIRED",
  );
  assertFailure(
    await restarted.filter(String(refreshed.result_id), "value IS NOT NULL"),
    "RESULT_EXPIRED",
  );
  restarted.close();
});

test("session names do not collide with another session ID", async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  const initial = await succeed(stateql.status());
  const named = await succeed(
    stateql.startSession(String(initial.session_id)),
  );
  assert.notEqual(named.session_id, initial.session_id);
  assert.equal(named.name, initial.session_id);
  stateql.close();
});

test("closed sessions reactivate without breaking later startup", async () => {
  const root = createTemporaryDirectory();
  const stateql = new StateQL({ home: root });
  const initial = await succeed(stateql.status());
  await succeed(stateql.closeSession());
  stateql.close();

  const reopened = new StateQL({ home: root });
  assert.equal(
    (await succeed(reopened.status())).session_id,
    initial.session_id,
  );
  reopened.close();
});

test("reactivation preserves an unresolved transaction lease", async () => {
  const fixture = await createFixture();
  const transaction = await succeed(fixture.stateql.beginTransaction());
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db
    .prepare("UPDATE sessions SET status = 'closed' WHERE active_transaction_id = ?")
    .run(String(transaction.transaction_id));
  fixture.stateql.close();

  const reopened = new StateQL({ home: fixture.home });
  assert.equal(
    (await succeed(reopened.transactionStatus(String(transaction.transaction_id))))
      .state,
    "active",
  );
  await succeed(reopened.rollbackTransaction(String(transaction.transaction_id)));
  reopened.close();
});

test("startup deletes expired results, aliases, and plans", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  await succeed(fixture.stateql.exec("CREATE TABLE cleanup_rows (id INTEGER)"));
  const result = await succeed(
    fixture.stateql.query("SELECT id FROM cleanup_rows"),
  );
  await succeed(
    fixture.stateql.setAlias("cleanup", String(result.result_id)),
  );
  await succeed(
    fixture.stateql.plan("INSERT INTO cleanup_rows (id) VALUES (1)"),
  );
  fixture.stateql.close();

  clock = new Date(clock.getTime() + 86_401_000);
  const reopened = new StateQL({ home: fixture.home, now: () => clock });
  const store = (reopened as unknown as { store: StateStore }).store;
  for (const table of ["results", "aliases", "plans"]) {
    const row = store.db
      .prepare(`SELECT COUNT(*) AS count FROM ${table}`)
      .get() as { count: number };
    assert.equal(row.count, 0, table);
  }
  reopened.close();
});

test("snapshot is typed, bounded, safe, and does not record history", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE private_values (value TEXT)"),
  );
  await succeed(
    fixture.stateql.query("SELECT ? AS value", {
      params: ["snapshot-secret"],
      cache: "bypass",
    }),
  );
  await succeed(fixture.stateql.beginTransaction());

  const snapshot: StateQLSnapshot = fixture.stateql.snapshot({ historyLimit: 2 });
  const historyEntry: HistoryEntry | undefined = snapshot.history[0];

  assert.equal(snapshot.session.status, "active");
  assert.equal(snapshot.connection?.status, "connected");
  assert.equal(snapshot.transaction?.state, "active");
  assert.equal(snapshot.state_confidence, "database_reported");
  assert.ok(snapshot.state_version);
  assert.ok(snapshot.recent_results.length <= 10);
  assert.ok(snapshot.recent_operations.length <= 10);
  assert.equal(snapshot.history.length, 2);
  assert.ok(historyEntry);
  assert.deepEqual(
    Object.keys(snapshot.recent_results[0] ?? {}).sort(),
    ["alias", "handle", "rows"],
  );
  assert.deepEqual(
    Object.keys(snapshot.recent_operations[0] ?? {}).sort(),
    ["actor_id", "affected_rows", "handle", "status", "type"],
  );
  assert.equal(JSON.stringify(snapshot).includes("snapshot-secret"), false);
  assert.equal(JSON.stringify(snapshot).includes("SELECT ? AS value"), true);
  assert.equal(
    snapshot.history.find((entry) => entry.command === "query")?.sql,
    "SELECT ? AS value",
  );
  assert.equal(historyEntry.sql, null);
  assert.deepEqual(fixture.stateql.snapshot({ historyLimit: 2 }), snapshot);

  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  for (let index = 0; index < 55; index += 1) {
    store.addHistory({
      sessionId: snapshot.session.session_id,
      actorId: snapshot.actor_id,
      command: "seed",
      executed: false,
      cached: false,
      success: true,
    });
  }
  assert.equal(fixture.stateql.snapshot().history.length, 50);
  assert.throws(() => fixture.stateql.snapshot({ historyLimit: 0 }), /positive integer/);
  assert.throws(() => fixture.stateql.snapshot({ historyLimit: 1.5 }), /positive integer/);
  assert.throws(() => fixture.stateql.snapshot({ historyLimit: 101 }), /cannot exceed 100/);

  const historyBeforeSnapshot = store.history(snapshot.session.session_id, 10_000).length;
  fixture.stateql.snapshot();
  assert.equal(store.history(snapshot.session.session_id, 10_000).length, historyBeforeSnapshot);

  await succeed(fixture.stateql.rollbackTransaction());
  fixture.stateql.close();
});

test("history bounds SQL text by UTF-8 bytes", () => {
  const root = createTemporaryDirectory();
  const store = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  const session = store.ensureSession();
  const entry = store.addHistory({
    sessionId: session.id,
    actorId: "default",
    command: "query",
    sql: "😀".repeat(3_000),
    executed: false,
    cached: false,
    success: false,
  });

  assert.ok(entry.sql);
  assert.ok(Buffer.byteLength(entry.sql, "utf8") <= 4_096);
  assert.match(entry.sql, /…$/);
  store.close();
});

test("history SQL migration preserves existing rows", () => {
  const root = createTemporaryDirectory();
  const store = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  const session = store.ensureSession();
  store.addHistory({
    id: "legacy_history",
    sessionId: session.id,
    actorId: "default",
    command: "query",
    executed: false,
    cached: false,
    success: true,
  });
  store.close();

  const legacy = new DatabaseSync(join(root, "state.sqlite"));
  legacy.exec("ALTER TABLE history DROP COLUMN sql");
  legacy.close();

  const reopened = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  assert.equal(reopened.history(session.id, 1)[0]?.sql, null);
  const migration = reopened.db
    .prepare("SELECT 1 FROM schema_migrations WHERE name = 'history_sql_v1'")
    .get();
  assert.ok(migration);
  reopened.close();
});

test("history origin migration attributes legacy rows and restores filtering", () => {
  const root = createTemporaryDirectory();
  const store = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  const session = store.ensureSession();
  store.addHistory({
    id: "pre_origin_history",
    sessionId: session.id,
    actorId: "default",
    command: "query",
    executed: true,
    cached: false,
    success: true,
  });
  store.close();

  const legacy = new DatabaseSync(join(root, "state.sqlite"));
  legacy.exec("DROP INDEX history_session_origin");
  legacy.exec("DELETE FROM schema_migrations WHERE name = 'history_origin_v1'");
  legacy.exec("ALTER TABLE history DROP COLUMN origin");
  legacy.close();

  const reopened = new StateStore(root, () => new Date("2026-01-01T00:00:01Z"));
  assert.equal(reopened.history(session.id, 1)[0]?.origin, "legacy");
  assert.equal(reopened.history(session.id, 10, "user").length, 0);
  reopened.addHistory({
    sessionId: session.id,
    actorId: "default",
    origin: "user",
    command: "query",
    executed: true,
    cached: false,
    success: true,
  });
  assert.equal(reopened.history(session.id, 10, "user").length, 1);
  assert.ok(
    reopened.db
      .prepare(
        "SELECT 1 FROM schema_migrations WHERE name = 'history_origin_v1'",
      )
      .get(),
  );
  assert.ok(
    reopened.db
      .prepare(
        "SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = 'history_session_origin'",
      )
      .get(),
  );
  reopened.close();
});

test("resource IDs are random while state versions and transaction order stay semantic", async () => {
  const clock = new Date("2026-01-01T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db.prepare("INSERT INTO counters(prefix, value) VALUES ('q', 3709)").run();

  const statusResponse = await fixture.stateql.status();
  assert.equal(statusResponse.ok, true);
  assert.match(statusResponse.command_id, /^cmd_[a-z2-7]{26}$/);
  const snapshot = fixture.stateql.snapshot();
  assert.match(snapshot.session.session_id, /^s_[a-z2-7]{26}$/);
  assert.match(snapshot.connection!.connection_id, /^conn_[a-z2-7]{26}$/);

  const created = await succeed(fixture.stateql.exec("CREATE TABLE random_ids (value TEXT)"));
  assert.match(created.operation_id, /^op_[a-z2-7]{26}$/);
  const result = await succeed(fixture.stateql.query("SELECT * FROM random_ids"));
  assert.match(result.result_id, /^q_[a-z2-7]{26}$/);
  const plan = await succeed(
    fixture.stateql.plan("INSERT INTO random_ids (value) VALUES ('planned')"),
  );
  assert.match(plan.plan_id, /^p_[a-z2-7]{26}$/);

  let claimToken = "";
  const claimPlan = store.claimPlan.bind(store);
  store.claimPlan = (planId, sessionId, actorId, token) => {
    claimToken = token;
    return claimPlan(planId, sessionId, actorId, token);
  };
  try {
    await succeed(fixture.stateql.apply(plan.plan_id));
  } finally {
    store.claimPlan = claimPlan;
  }
  assert.match(claimToken, /^claim_[a-z2-7]{26}$/);

  const transaction = await succeed(fixture.stateql.beginTransaction());
  assert.match(transaction.transaction_id, /^tx_[a-z2-7]{26}$/);
  const first = await succeed(
    fixture.stateql.exec("INSERT INTO random_ids (value) VALUES ('first')"),
  );
  const second = await succeed(
    fixture.stateql.exec("INSERT INTO random_ids (value) VALUES ('second')"),
  );
  assert.match(first.operation_id, /^op_[a-z2-7]{26}$/);
  assert.match(second.operation_id, /^op_[a-z2-7]{26}$/);
  assert.deepEqual(
    store.transactionOperations(transaction.transaction_id).map((operation) => operation.id),
    [first.operation_id, second.operation_id],
  );
  const versionBeforeRollback = store.getConnection(snapshot.connection!.connection_id)!.version;
  await succeed(fixture.stateql.rollbackTransaction(transaction.transaction_id));
  assert.equal(
    store.getConnection(snapshot.connection!.connection_id)!.version,
    versionBeforeRollback,
  );
  assert.match(transaction.start_state_version, /^sv_\d+$/);
  assert.equal(
    store.db.prepare("SELECT value FROM counters WHERE prefix = 'q'").get()?.value,
    3709,
  );
  fixture.stateql.close();
});

test("canonical ID collisions retry and exhaustion rolls back atomically", (t) => {
  const store = new StateStore(createTemporaryDirectory(), () => new Date());
  store.db.prepare("INSERT INTO counters(prefix, value) VALUES ('s', 121)").run();
  let calls = 0;
  let exhaust = false;
  const random = t.mock.method(crypto, "randomBytes", (size: number) => {
    const value = exhaust ? 1 : calls === 2 ? 1 : 0;
    calls += 1;
    return Buffer.alloc(size, value);
  });
  syncBuiltinESMExports();
  try {
    const first = store.createSession("first");
    const second = store.createSession("second");
    assert.equal(first.id, `s_${"a".repeat(26)}`);
    assert.equal(second.id, `s_${"b".repeat(26)}`);
    assert.equal(calls, 3); // The second allocation retries the first ID once.

    exhaust = true;
    assert.throws(
      () => store.createSession("third"),
      /Could not allocate a unique s ID/,
    );
    assert.equal(calls, 67);
    assert.equal(store.getSessionByName("third"), undefined);
    assert.equal(store.db.prepare("SELECT COUNT(*) AS count FROM sessions").get()?.count, 2);
    assert.equal(store.db.prepare("SELECT COUNT(*) AS count FROM session_members").get()?.count, 2);
    assert.equal(store.db.prepare("SELECT value FROM counters WHERE prefix = 's'").get()?.value, 121);
  } finally {
    random.mock.restore();
    syncBuiltinESMExports();
    store.close();
  }
});

test("legacy incremental IDs and their stored references remain usable", async () => {
  const fixture = await createFixture();
  const created = await succeed(fixture.stateql.exec("CREATE TABLE legacy_ids (value TEXT)"));
  const result = await succeed(fixture.stateql.query("SELECT * FROM legacy_ids"));
  await succeed(fixture.stateql.setAlias("legacy-result", result.result_id));
  const plan = await succeed(
    fixture.stateql.plan("INSERT INTO legacy_ids (value) VALUES ('planned')"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  const pending = await succeed(
    fixture.stateql.exec("INSERT INTO legacy_ids (value) VALUES ('pending')"),
  );
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  const snapshot = fixture.stateql.snapshot();
  const commandId = store.history(snapshot.session.session_id, 1)[0]!.id;

  store.db.exec("BEGIN IMMEDIATE");
  try {
    store.db.exec("PRAGMA defer_foreign_keys = ON");
    store.db.prepare("UPDATE session_members SET session_id = 's_121' WHERE session_id = ?")
      .run(snapshot.session.session_id);
    store.db.prepare("UPDATE connections SET id = 'conn_29', session_id = 's_121' WHERE id = ?")
      .run(snapshot.connection!.connection_id);
    store.db.prepare("UPDATE results SET id = 'q_121', session_id = 's_121', connection_id = 'conn_29' WHERE id = ?")
      .run(result.result_id);
    store.db.prepare("UPDATE aliases SET session_id = 's_121', result_id = 'q_121' WHERE result_id = ?")
      .run(result.result_id);
    store.db.prepare("UPDATE operations SET session_id = 's_121', connection_id = 'conn_29', transaction_id = CASE WHEN transaction_id = ? THEN 'tx_11' ELSE transaction_id END")
      .run(transaction.transaction_id);
    store.db.prepare("UPDATE operations SET id = 'op_7' WHERE id = ?").run(pending.operation_id);
    store.db.prepare("UPDATE transactions SET id = 'tx_11', session_id = 's_121', connection_id = 'conn_29' WHERE id = ?")
      .run(transaction.transaction_id);
    store.db.prepare("UPDATE plans SET id = 'p_9', session_id = 's_121', connection_id = 'conn_29' WHERE id = ?")
      .run(plan.plan_id);
    store.db.prepare(`UPDATE history SET session_id = 's_121', handle = CASE handle
      WHEN ? THEN 'q_121' WHEN ? THEN 'op_7' WHEN ? THEN 'p_9' WHEN ? THEN 'tx_11' ELSE handle END`)
      .run(result.result_id, pending.operation_id, plan.plan_id, transaction.transaction_id);
    store.db.prepare("UPDATE history SET id = 'cmd_407' WHERE id = ?").run(commandId);
    store.db.prepare(`UPDATE sessions SET id = 's_121', active_connection_id = 'conn_29',
      active_transaction_id = 'tx_11' WHERE id = ?`).run(snapshot.session.session_id);
    store.db.exec("COMMIT");
  } catch (error) {
    store.db.exec("ROLLBACK");
    throw error;
  }
  fixture.stateql.close();

  const reopened = new StateQL({ home: fixture.home });
  assert.equal(reopened.snapshot().session.session_id, "s_121");
  assert.equal(reopened.snapshot().connection?.connection_id, "conn_29");
  assert.equal((await succeed(reopened.show("q_121"))).result_id, "q_121");
  assert.equal((await succeed(reopened.show("legacy-result"))).result_id, "q_121");
  assert.equal((await succeed(reopened.receipt("op_7"))).transaction_id, "tx_11");
  assert.equal((await succeed(reopened.transactionStatus("tx_11"))).transaction_id, "tx_11");
  const reopenedStore = (reopened as unknown as { store: StateStore }).store;
  assert.equal(reopenedStore.getPlan("p_9")?.connection_id, "conn_29");
  assert.ok(reopenedStore.history("s_121", 100).some((entry) => entry.id === "cmd_407"));
  await succeed(reopened.rollbackTransaction("tx_11"));
  await succeed(reopened.apply("p_9"));
  assert.equal(reopenedStore.getOperation(created.operation_id)?.connection_id, "conn_29");
  reopened.close();
});
test("connection aliases persist across reopen, change on reconnect, and retain canonical references", async () => {
  const home = createTemporaryDirectory();
  const database = join(home, "target.sqlite");
  let stateql = new StateQL({ home });
  try {
    const first = await succeed(stateql.connect(database, { name: "friendly", readOnly: false }));
    assert.match(first.connection_id, /^conn_[a-z2-7]{26}$/);
    assert.match(first.alias, /^[a-z2-7]{10}$/);
    assert.equal(first.display_alias, first.alias);
    assert.equal(first.name, "friendly");
    assert.equal(stateql.snapshot().connection?.alias, first.alias);
    assert.equal(stateql.snapshot().connection?.display_alias, first.alias);
    const result = await succeed(stateql.query("SELECT 1 AS value"));
    // A connection display alias does not occupy the result alias namespace.
    await succeed(stateql.setAlias(first.alias, result.result_id));
    stateql.close();
    stateql = new StateQL({ home });
    assert.equal(stateql.snapshot().connection?.connection_id, first.connection_id);
    assert.equal(stateql.snapshot().connection?.alias, first.alias);
    assert.equal((await succeed(stateql.show(first.alias))).result_id, result.result_id);
    assert.equal((await succeed(stateql.query("SELECT 1 AS value"))).result_id, result.result_id);
    await succeed(stateql.disconnect());
    assert.equal(stateql.snapshot().connection, null);
    const second = await succeed(stateql.connect(database));
    assert.notEqual(second.connection_id, first.connection_id);
    assert.notEqual(second.alias, first.alias);
    assert.equal(stateql.snapshot().connection?.alias, second.alias);
    const refreshed = await succeed(stateql.query("SELECT 1 AS value"));
    assert.notEqual(refreshed.result_id, result.result_id);
    const store = (stateql as unknown as { store: StateStore }).store;
    assert.equal(store.getResult(result.result_id)?.connection_id, first.connection_id);
    assert.equal(store.getResult(refreshed.result_id)?.connection_id, second.connection_id);
    assert.equal(store.getConnection(first.connection_id)?.alias, first.alias);
    assert.equal(store.getConnection(first.alias), undefined);
  } finally { stateql.close(); }
});

test("connection alias collisions retry and allocation exhaustion rolls back the connection", (t) => {
  const store = new StateStore(createTemporaryDirectory(), () => new Date());
  const session = store.createSession("actor");
  const input = {
    sessionId: session.id, actorId: "actor", name: "test", driver: "sqlite" as const,
    databaseName: "test", source: ":memory:", readOnly: true,
  };
  let aliasCalls = 0;
  let idCalls = 0;
  const random = t.mock.method(crypto, "randomBytes", (size: number) => {
    if (size === 26) return Buffer.alloc(size, idCalls++);
    return Buffer.alloc(size, aliasCalls++ === 2 ? 1 : 0);
  });
  syncBuiltinESMExports();
  try {
    const first = store.addConnection(input)!;
    const second = store.addConnection(input)!;
    assert.equal(first.alias, "aaaaaaaaaa");
    assert.equal(second.alias, "bbbbbbbbbb");
    assert.equal(aliasCalls, 3); // One collision before the second allocation succeeds.
    assert.throws(() => store.db.prepare("UPDATE connections SET alias = ? WHERE id = ?")
      .run(first.alias!, second.id), /UNIQUE constraint failed/);
    // Uniqueness applies across sessions too.
    const other = store.createSession("other");
    assert.throws(() => store.addConnection({ ...input, sessionId: other.id, actorId: "other" }),
      /Could not allocate a unique connection alias/);
    assert.equal(aliasCalls, 67);
    assert.equal(idCalls, 4);
    assert.equal(store.getSession(other.id)?.active_connection_id, null);
    assert.equal(store.getSession(session.id)?.active_connection_id, second.id);
    assert.equal(store.db.prepare("SELECT COUNT(*) AS count FROM connections").get()?.count, 2);
    assert.equal(store.db.prepare("SELECT value FROM counters WHERE prefix = 'conn'").get(), undefined);
    assert.equal(store.addConnection({ ...input, actorId: "not-a-member" }), undefined);
    assert.equal(aliasCalls, 67);
    assert.equal(idCalls, 4);
  } finally {
    random.mock.restore();
    syncBuiltinESMExports();
    store.close();
  }
});

for (const keepRegistry of [false, true]) {
  test(`connection alias migration backfills legacy rows and reopens stably (registry retained: ${keepRegistry})`, async () => {
    const fixture = await createFixture();
    await succeed(fixture.stateql.connect(fixture.database));
    const result = await succeed(fixture.stateql.query("SELECT 1 AS value"));
    await succeed(fixture.stateql.setAlias("saved", result.result_id));
    const active = fixture.stateql.snapshot().connection!.connection_id;
    fixture.stateql.close();
    const legacy = new DatabaseSync(join(fixture.home, "state.sqlite"));
    const before = legacy.prepare("SELECT id, session_id, name, source, version FROM connections ORDER BY id").all();
    legacy.exec("DROP INDEX connections_alias; ALTER TABLE connections DROP COLUMN alias");
    if (!keepRegistry) legacy.exec("DELETE FROM schema_migrations WHERE name = 'connection_aliases_v1'");
    legacy.close();

    const migrated = new StateStore(fixture.home, () => new Date());
    const aliases = migrated.db.prepare("SELECT id, alias FROM connections ORDER BY id").all();
    assert.equal(aliases.length, 2);
    assert.equal(new Set(aliases.map((row) => row.alias)).size, 2);
    for (const row of aliases) assert.match(String(row.alias), /^[a-z2-7]{10}$/);
    assert.deepEqual(migrated.db.prepare("SELECT id, session_id, name, source, version FROM connections ORDER BY id").all(), before);
    assert.equal(migrated.getResult("saved")?.id, result.result_id);
    assert.equal(migrated.getResult(result.alias)?.alias, result.alias);
    assert.equal(migrated.getResult(result.alias)?.connection_id, active);
    assert.ok(migrated.db.prepare("SELECT 1 FROM schema_migrations WHERE name = 'connection_aliases_v1'").get());
    migrated.close();

    const reopened = new StateStore(fixture.home, () => new Date());
    try {
      assert.deepEqual(reopened.db.prepare("SELECT id, alias FROM connections ORDER BY id").all(), aliases);
      assert.deepEqual(reopened.db.prepare("PRAGMA foreign_key_check").all(), []);
    } finally { reopened.close(); }
  });
}

test("connection alias backfill rolls back all allocations on failure and can be retried", async () => {
  const fixture = await createFixture();
  const second = await succeed(fixture.stateql.connect(fixture.database));
  fixture.stateql.close();
  const database = new DatabaseSync(join(fixture.home, "state.sqlite"));
  try {
    database.exec("UPDATE connections SET alias = NULL");
    database.exec(`CREATE TRIGGER fail_connection_alias BEFORE UPDATE OF alias ON connections
      WHEN NEW.id = '${second.connection_id}' BEGIN SELECT RAISE(ABORT, 'backfill failure'); END`);
    assert.throws(() => new StateStore(fixture.home, () => new Date()), /backfill failure/);
    assert.equal(database.prepare("SELECT COUNT(*) AS count FROM connections WHERE alias IS NOT NULL").get()?.count, 0);
    database.exec("DROP TRIGGER fail_connection_alias");
  } finally { database.close(); }
  const reopened = new StateStore(fixture.home, () => new Date());
  try {
    assert.equal(reopened.db.prepare("SELECT COUNT(*) AS count FROM connections WHERE alias IS NOT NULL").get()?.count, 2);
  } finally { reopened.close(); }
});

test("results receive stable generated aliases without allowing reassignment", async () => {
  const fixture = await createFixture();
  try {
    const first = await succeed(fixture.stateql.query("SELECT 1 AS value"));
    assert.match(first.alias, /^[a-z2-7]{10}$/);
    assert.equal((await succeed(fixture.stateql.show(first.alias))).result_id, first.result_id);
    const cached = await succeed(fixture.stateql.query("SELECT 1 AS value"));
    assert.equal(cached.result_id, first.result_id);
    assert.equal(cached.alias, first.alias);
    await succeed(fixture.stateql.setAlias("caller-name", first.result_id));
    assert.equal((await succeed(fixture.stateql.show("caller-name"))).alias, first.alias);
    const second = await succeed(fixture.stateql.query("SELECT 2 AS value", { cache: "bypass" }));
    const stolen = await fixture.stateql.setAlias(first.alias, second.result_id);
    assert.equal(stolen.ok, false);
    if (!stolen.ok) assert.equal(stolen.error.code, "INVALID_COMMAND");
    const store = (fixture.stateql as unknown as { store: StateStore }).store;
    const generated = store.db.prepare("SELECT name FROM aliases WHERE generated = 1").all() as Array<{ name: string }>;
    assert.equal(new Set(generated.map((row) => row.name)).size, generated.length);
  } finally { fixture.stateql.close(); }
});

test("history and snapshot filters run before limits without recording history", async () => {
  const fixture = await createFixture();
  try {
    await succeed(fixture.stateql.query("SELECT 42 AS statement"));
    for (let index = 0; index < 105; index++) {
      const command = index % 2 === 0
        ? { command: "objects.list" as const, limit: 1 }
        : { command: "profile.list" as const };
      await succeed(fixture.stateql.executeCommand(command, { origin: "api", internal: true }));
    }
    const statements = await succeed(fixture.stateql.history(1, { category: "statement", internal: false }));
    assert.equal(statements.history.length, 1);
    assert.equal(statements.history[0].command, "query");
    const internal = await succeed(fixture.stateql.history(2, { category: "introspection", internal: true }));
    assert.equal(internal.history.length, 2);
    assert.ok(internal.history.every((entry: HistoryEntry) => entry.internal && entry.category === "introspection"));

    const initialSnapshot = fixture.stateql.snapshot();
    const store = (fixture.stateql as unknown as { store: StateStore }).store;
    const historyCount = store.history(initialSnapshot.session.session_id, 10_000).length;
    const snapshot = fixture.stateql.snapshot({
      historyLimit: 1,
      historyCategory: "statement",
      historyInternal: false,
    });
    assert.equal(snapshot.history.length, 1);
    assert.equal(snapshot.history[0]?.command, "query");
    assert.equal(store.history(snapshot.session.session_id, 10_000).length, historyCount);
  } finally { fixture.stateql.close(); }
});


test("history keeps the latest 10,000 entries per session", () => {
  const root = createTemporaryDirectory();
  const store = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  const session = store.ensureSession();

  store.db.exec(`
    WITH RECURSIVE sequence(value) AS (
      SELECT 1
      UNION ALL
      SELECT value + 1 FROM sequence WHERE value < 10001
    )
    INSERT INTO history
      (id, timestamp, session_id, actor_id, command, executed, cached, success)
    SELECT
      'seed_' || value,
      '2026-01-01T00:00:00Z',
      '${session.id}',
      'default',
      'seed',
      0,
      0,
      1
    FROM sequence
  `);

  const latest = store.addHistory({
    sessionId: session.id,
    actorId: "default",
    command: "latest",
    executed: false,
    cached: false,
    success: true,
  });

  assert.equal(store.history(session.id, 20_000).length, 10_000);
  assert.equal(store.history(session.id, 1)[0]?.id, latest.id);
  const removed = store.db
    .prepare("SELECT COUNT(*) AS count FROM history WHERE id IN (?, ?)")
    .get("seed_1", "seed_2") as { count: number };
  assert.equal(removed.count, 0);
  store.close();
});

test("actor-first opening resolves linked workspaces and bootstraps first use", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const owner = new StateQL({ home, session: "workspace" });
  await succeed(owner.linkActor("workspace", "actor-b"));

  const linked = StateQL.forActor({ home, actor: "actor-b" });
  assert.equal(linked.snapshot().session.name, "workspace");
  assert.equal(linked.snapshot().actor_id, "actor-b");
  linked.close();

  const firstUse = StateQL.forActor({ home, actor: "actor-c" });
  assert.equal(firstUse.snapshot().session.name, "actor-c");
  assert.equal(firstUse.snapshot().actor_id, "actor-c");
  const resolved = await succeed(firstUse.resolveActor("actor-c"));
  assert.equal(resolved.session.name, "actor-c");
  firstUse.close();
  owner.close();
});

test("trusted workspace opening is actor-bound, shared, and idempotent", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "global.sqlite");
  const user = StateQL.forWorkspace({
    home,
    workspace: "global",
    actor: "pylon-user",
  });
  await succeed(user.connect(database, { readOnly: false }));
  await succeed(user.exec("CREATE TABLE shared_rows (value TEXT)"));
  await succeed(user.exec("INSERT INTO shared_rows (value) VALUES ('user')"));
  const result = await succeed(user.query("SELECT * FROM shared_rows"));

  const sessionActor = StateQL.forWorkspace({
    home,
    workspace: "global",
    actor: "pi-session-1",
    maxResultRows: 1,
  });
  assert.equal(sessionActor.snapshot().session.name, "global");
  assert.equal(sessionActor.snapshot().actor_id, "pi-session-1");
  assert.equal(
    (await succeed(sessionActor.show(String(result.result_id)))).result_id,
    result.result_id,
  );
  const operation = await succeed(
    sessionActor.exec("INSERT INTO shared_rows (value) VALUES ('session')"),
  );
  assert.equal(operation.actor_id, "pi-session-1");
  assertFailure(
    await sessionActor.query("SELECT * FROM shared_rows", { cache: "bypass" }),
    "OUTPUT_LIMIT_EXCEEDED",
  );

  const plan = await succeed(
    sessionActor.plan("INSERT INTO shared_rows (value) VALUES ('planned')"),
  );
  assertFailure(await user.apply(String(plan.plan_id)), "PERMISSION_DENIED");
  const history = await succeed(user.history(50));
  assert.ok(
    history.history.some(
      (entry: HistoryEntry) =>
        entry.actor_id === "pi-session-1" && entry.command === "exec",
    ),
  );
  const actors = await succeed(user.listActors("global"));
  assert.deepEqual(
    actors.actors.map((actor: { actor_id: string }) => actor.actor_id).sort(),
    ["global", "pi-session-1", "pylon-user"],
  );

  sessionActor.close();
  const reopened = StateQL.forWorkspace({
    home,
    workspace: "global",
    actor: "pi-session-1",
  });
  assert.equal(reopened.snapshot().actor_id, "pi-session-1");
  reopened.close();

  const resolved = StateQL.forActor({ home, actor: "pi-session-1" });
  assert.equal(resolved.snapshot().session.name, "global");
  assert.equal(resolved.snapshot().actor_id, "pi-session-1");
  resolved.close();
  user.close();
});

test("trusted workspace opening rejects actor reassignment atomically", () => {
  const home = join(createTemporaryDirectory(), "state");
  const original = StateQL.forWorkspace({
    home,
    workspace: "workspace-a",
    actor: "shared-actor",
  });

  assert.throws(
    () =>
      StateQL.forWorkspace({
        home,
        workspace: "workspace-b",
        actor: "shared-actor",
      }),
    (error: unknown) => {
      assert.ok(error instanceof StateQLError);
      assert.equal(error.details.code, "PERMISSION_DENIED");
      assert.match(error.message, /already attached to workspace "workspace-a"/);
      return true;
    },
  );

  const store = new StateStore(home, () => new Date());
  assert.equal(store.getSessionByName("workspace-b"), undefined);
  assert.equal(store.resolveActor("shared-actor")?.name, "workspace-a");
  store.close();
  original.close();
});

test("actors share workspace handles, aliases, history, and restarts", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "shared.sqlite");
  const owner = new StateQL({ home, session: "workspace" });
  await succeed(owner.connect(database, { readOnly: false }));
  await succeed(owner.exec("CREATE TABLE shared_rows (value TEXT)"));
  const result = await succeed(owner.query("SELECT * FROM shared_rows"));
  await succeed(owner.setAlias("shared", String(result.result_id)));

  const stranger = new StateQL({
    home,
    session: "workspace",
    actor: "actor-b",
  });
  assertFailure(await stranger.status(), "PERMISSION_DENIED");
  await succeed(owner.linkActor("workspace", "actor-b"));

  assert.equal(
    (await succeed(stranger.show(String(result.result_id)))).result_id,
    result.result_id,
  );
  assert.equal(
    (await succeed(stranger.show("shared"))).result_id,
    result.result_id,
  );
  const operation = await succeed(
    stranger.exec("INSERT INTO shared_rows (value) VALUES ('actor-b')"),
  );
  assert.equal(operation.actor_id, "actor-b");
  assert.equal(
    (await succeed(stranger.receipt(String(operation.operation_id)))).actor_id,
    "actor-b",
  );
  assert.equal((await succeed(owner.query("SELECT * FROM shared_rows"))).rows, 1);

  const actors = await succeed(owner.listActors("workspace"));
  assert.deepEqual(
    actors.actors.map((actor: { actor_id: string }) => actor.actor_id).sort(),
    ["actor-b", "workspace"],
  );
  const resolved = await succeed(owner.resolveActor("actor-b"));
  assert.equal(resolved.session.name, "workspace");
  const history = await succeed(owner.history(50));
  assert.ok(
    history.history.some(
      (entry: HistoryEntry) =>
        entry.actor_id === "actor-b" && entry.command === "exec",
    ),
  );
  assert.equal(stranger.snapshot().actor_id, "actor-b");

  stranger.close();
  const restarted = new StateQL({
    home,
    session: "workspace",
    actor: "actor-b",
  });
  assert.equal(
    (await succeed(restarted.show("shared"))).result_id,
    result.result_id,
  );
  restarted.close();
  owner.close();
});

test("plans and transactions remain actor-owned across concurrent clients", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "owned.sqlite");
  const owner = new StateQL({ home, session: "workspace" });
  await succeed(owner.connect(database, { readOnly: false }));
  await succeed(owner.exec("CREATE TABLE owned_rows (value TEXT)"));
  await succeed(owner.linkActor("workspace", "actor-b"));
  const actor = new StateQL({ home, session: "workspace", actor: "actor-b" });

  const plan = await succeed(
    owner.plan("INSERT INTO owned_rows (value) VALUES ('planned')"),
  );
  assertFailure(await actor.apply(String(plan.plan_id)), "PERMISSION_DENIED");

  const transaction = await succeed(owner.beginTransaction());
  const inspected = await succeed(
    actor.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(inspected.owner_actor_id, "workspace");
  assertFailure(
    await actor.exec("INSERT INTO owned_rows (value) VALUES ('blocked')"),
    "PERMISSION_DENIED",
  );
  assertFailure(
    await actor.commitTransaction(String(transaction.transaction_id)),
    "PERMISSION_DENIED",
  );
  assertFailure(
    await actor.rollbackTransaction(String(transaction.transaction_id)),
    "PERMISSION_DENIED",
  );
  assertFailure(
    await actor.unlinkActor("workspace", "workspace"),
    "TRANSACTION_FAILED",
  );
  await succeed(owner.rollbackTransaction(String(transaction.transaction_id)));
  await succeed(owner.apply(String(plan.plan_id)));

  const acquisitions = await Promise.all([
    owner.beginTransaction(),
    actor.beginTransaction(),
  ]);
  assert.equal(acquisitions.filter((response) => response.ok).length, 1);
  const winner = acquisitions[0]!.ok ? owner : actor;
  await succeed(winner.rollbackTransaction());

  const concurrentPlan = await succeed(
    owner.plan("INSERT INTO owned_rows (value) VALUES ('once')"),
  );
  const ownerClone = new StateQL({
    home,
    session: "workspace",
    actor: "workspace",
  });
  const applications = await Promise.all([
    owner.apply(String(concurrentPlan.plan_id)),
    ownerClone.apply(String(concurrentPlan.plan_id)),
  ]);
  assert.equal(applications.filter((response) => response.ok).length, 1);

  const connectionRace = await Promise.all([
    actor.connect(join(root, "replacement.sqlite"), { readOnly: false }),
    owner.beginTransaction(),
  ]);
  assert.equal(connectionRace.filter((response) => response.ok).length, 1);
  if (connectionRace[1]!.ok) await succeed(owner.rollbackTransaction());

  ownerClone.close();
  actor.close();
  owner.close();
});

test("StateQL validates durable-state options and closes idempotently", () => {
  const home = createTemporaryDirectory("stateql-options-test-");
  for (const [name, value] of [
    ["previewRows", -1],
    ["previewRows", 1.5],
    ["previewRows", Number.NaN],
    ["cacheTtlSeconds", -1],
    ["cacheTtlSeconds", 1.5],
    ["cacheTtlSeconds", Number.NaN],
    ["resultTtlSeconds", -1],
    ["resultTtlSeconds", 1.5],
    ["resultTtlSeconds", Number.NaN],
    ["maxCellCharacters", -1],
    ["maxCellCharacters", 1.5],
    ["maxCellCharacters", Number.NaN],
    ["maxStateBytes", -1],
    ["maxStateBytes", 1.5],
    ["maxStateBytes", Number.NaN],
  ] as const) {
    assert.throws(
      () => new StateQL({ home, [name]: value }),
      /positive integer|non-negative integer/,
      `${name}=${value}`,
    );
  }

  const stateql = new StateQL({ home });
  stateql.close();
  stateql.close();
  stateql[Symbol.dispose]();
});

test("credential ref migration preserves legacy profiles and enforces one source", () => {
  const home = createTemporaryDirectory("stateql-credential-ref-migration-test-");
  mkdirSync(home, { recursive: true });
  const legacy = new DatabaseSync(join(home, "state.sqlite"));
  legacy.exec(`
    CREATE TABLE profiles (
      name TEXT PRIMARY KEY,
      target TEXT,
      secret_env TEXT,
      read_only INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK(target IS NOT NULL OR secret_env IS NOT NULL)
    );
    INSERT INTO profiles VALUES
      ('local', './local.sqlite', NULL, 0, '2026-01-01', '2026-01-01'),
      ('hosted', NULL, 'APP_DATABASE_URL', 1, '2026-01-01', '2026-01-01'),
      ('legacy_dual', './ignored.sqlite', 'LEGACY_DATABASE_URL', 1, '2026-01-01', '2026-01-01');
  `);
  legacy.close();

  const migrated = new StateStore(home, () => new Date("2026-01-02T00:00:00Z"));
  assert.deepEqual(
    migrated.listProfiles().map(({ name, target, secret_env, credential_ref, password_ref }) => ({
      name,
      target,
      secret_env,
      credential_ref,
      password_ref,
    })),
    [
      { name: "hosted", target: null, secret_env: "APP_DATABASE_URL", credential_ref: null, password_ref: null },
      { name: "legacy_dual", target: null, secret_env: "LEGACY_DATABASE_URL", credential_ref: null, password_ref: null },
      { name: "local", target: "./local.sqlite", secret_env: null, credential_ref: null, password_ref: null },
    ],
  );
  assert.ok(
    migrated.db.prepare(
      "SELECT 1 FROM pragma_table_info('connections') WHERE name = 'credential_ref'",
    ).get(),
  );
  assert.ok(
    migrated.db.prepare(
      "SELECT 1 FROM pragma_table_info('profiles') WHERE name = 'password_ref'",
    ).get(),
  );
  assert.ok(
    migrated.db.prepare(
      "SELECT 1 FROM pragma_table_info('connections') WHERE name = 'password_ref'",
    ).get(),
  );
  assert.ok(
    migrated.db.prepare(
      "SELECT 1 FROM schema_migrations WHERE name = 'credential_refs_v1'",
    ).get(),
  );
  assert.ok(
    migrated.db.prepare(
      "SELECT 1 FROM schema_migrations WHERE name = 'password_refs_v1'",
    ).get(),
  );
  assert.throws(() => migrated.db.prepare(
    `INSERT INTO profiles
      (name, target, secret_env, credential_ref, read_only, created_at, updated_at)
     VALUES ('ambiguous', './db.sqlite', 'DATABASE_URL', NULL, 1, '', '')`,
  ).run());
  migrated.db.prepare(
    `INSERT INTO profiles
      (name, target, secret_env, credential_ref, password_ref, read_only, created_at, updated_at)
     VALUES ('password', 'redis://cache.example/0', NULL, NULL, 'vault://password', 1, '', '')`,
  ).run();
  assert.throws(() => migrated.db.prepare(
    `INSERT INTO profiles
      (name, target, secret_env, credential_ref, password_ref, read_only, created_at, updated_at)
     VALUES ('password_env', NULL, 'DATABASE_URL', NULL, 'vault://password', 1, '', '')`,
  ).run());
  const session = migrated.createSession("migration-password");
  const connection = migrated.addConnection({
    sessionId: session.id,
    actorId: "migration-password",
    name: "redis",
    driver: "redis",
    databaseName: "db0",
    source: "redis://cache.example/0",
    passwordRef: "vault://password",
    readOnly: true,
  });
  assert.equal(connection?.password_ref, "vault://password");
  assert.throws(() => migrated.db.prepare(
    "UPDATE connections SET secret_env = 'DATABASE_URL' WHERE id = ?",
  ).run(connection!.id));
  assert.throws(() => migrated.addConnection({
    sessionId: session.id,
    actorId: "migration-password",
    name: "sqlite",
    driver: "sqlite",
    databaseName: "local.sqlite",
    source: "./local.sqlite",
    passwordRef: "vault://password",
    readOnly: true,
  }));
  migrated.close();

  const reopened = new StateStore(home, () => new Date("2026-01-03T00:00:00Z"));
  assert.equal(reopened.listProfiles().length, 4);
  reopened.close();
});


test("password reference migration refuses an incompatible profile schema without dropping references", () => {
  const home = createTemporaryDirectory("stateql-password-ref-incompatible-migration-test-");
  const initial = new StateQL({ home });
  initial.close();
  const database = new DatabaseSync(join(home, "state.sqlite"));
  database.exec(`
    ALTER TABLE profiles RENAME TO profiles_before_incompatible_test;
    CREATE TABLE profiles (
      name TEXT PRIMARY KEY,
      target TEXT,
      secret_env TEXT,
      credential_ref TEXT,
      password_ref TEXT,
      read_only INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    INSERT INTO profiles VALUES
      ('invalid', NULL, 'DATABASE_URL', NULL, 'vault://password', 1, '', '');
    DROP TABLE profiles_before_incompatible_test;
  `);
  database.close();
  assert.throws(
    () => new StateStore(home, () => new Date()),
    /cannot safely rebuild a profile schema that already contains password references/,
  );
});


test("migrations retain their registry and repair a migration/schema mismatch", () => {
  const home = createTemporaryDirectory("stateql-migration-test-");
  const initial = new StateQL({ home });
  initial.close();

  const database = new DatabaseSync(join(home, "state.sqlite"));
  assert.deepEqual(
    (database.prepare("SELECT name FROM schema_migrations ORDER BY rowid").all() as Array<{ name: string }>).map((row) => row.name),
    [
      "initial_schema_v1",
      "shared_session_actors_v1",
      "history_sql_v1",
      "operation_outcomes_v1",
      "history_origin_v1",
      "credential_refs_v1",
      "history_target_v1",
      "generated_aliases_v1",
      "history_classification_v1",
      "connection_aliases_v1",
      "password_refs_v1",
    ],
  );
  database.exec("DELETE FROM schema_migrations WHERE name = 'shared_session_actors_v1'");
  database.exec("ALTER TABLE plans DROP COLUMN claim_token");
  database.close();

  const repaired = new StateQL({ home });
  const store = (repaired as unknown as { store: StateStore }).store;
  assert.ok(
    (store.db.prepare("SELECT 1 FROM schema_migrations WHERE name = ?").get("initial_schema_v1")),
  );
  assert.ok(
    (store.db.prepare("SELECT 1 FROM schema_migrations WHERE name = ?").get("shared_session_actors_v1")),
  );
  assert.ok(
    (store.db.prepare("SELECT 1 FROM pragma_table_info('plans') WHERE name = 'claim_token'").get()),
  );
  repaired.close();
});

test("stored result JSON corruption is contained and diagnosed without payload leakage", async () => {
  const fixture = await createFixture();
  const result = await succeed(fixture.stateql.query("SELECT 1 AS value"));
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db.prepare("UPDATE results SET rows_json = ? WHERE id = ?")
    .run("not-json-secret-payload", result.result_id);
  assertFailure(await fixture.stateql.show(String(result.result_id)), "STATE_CORRUPTED");

  const doctor = await succeed(fixture.stateql.doctor());
  assert.equal(doctor.integrity, "issues");
  assert.ok(doctor.issues.some((issue: { code: string }) => issue.code === "CORRUPTED_RESULT"));
  assert.equal(JSON.stringify(doctor).includes("secret-payload"), false);

  store.db.prepare("UPDATE results SET rows_json = ?, columns_json = ? WHERE id = ?")
    .run(JSON.stringify([{ value: 1 }]), JSON.stringify([{ name: 1 }]), result.result_id);
  assertFailure(await fixture.stateql.show(String(result.result_id)), "STATE_CORRUPTED");
  fixture.stateql.close();
});

test("doctor and purge cover expired, result, history, and all scopes", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const root = createTemporaryDirectory("stateql-purge-test-");
  const stateql = new StateQL({
    home: join(root, "state"),
    now: () => clock,
    resultTtlSeconds: 1,
  });
  await succeed(stateql.connect(join(root, "target.sqlite"), { readOnly: false }));
  const expired = await succeed(stateql.query("SELECT 1 AS value"));
  clock = new Date(clock.getTime() + 2_000);
  assert.equal((await succeed(stateql.purge("expired"))).scope, "expired");
  assertFailure(await stateql.show(String(expired.result_id)), "RESULT_NOT_FOUND");

  const retained = await succeed(stateql.query("SELECT 2 AS value", { cache: "bypass" }));
  await succeed(stateql.setAlias("retained", String(retained.result_id)));
  assert.ok((await succeed(stateql.purge("results"))).deleted >= 1);
  assertFailure(await stateql.show("retained"), "RESULT_NOT_FOUND");
  assert.ok((await succeed(stateql.history(100))).history.length > 0);
  assert.ok((await succeed(stateql.purge("history"))).deleted > 0);
  assert.equal((await succeed(stateql.history(100))).history.length, 1);

  await succeed(stateql.exec("CREATE TABLE purge_rows (id INTEGER)"));
  await succeed(stateql.plan("INSERT INTO purge_rows (id) VALUES (1)"));
  assert.ok((await succeed(stateql.purge("all"))).deleted >= 2);
  const store = (stateql as unknown as { store: StateStore }).store;
  for (const table of ["results", "plans", "operations", "transactions"]) {
    assert.equal(
      (store.db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as { count: number }).count,
      0,
      table,
    );
  }
  assert.equal(
    (store.db.prepare("SELECT COUNT(*) AS count FROM history").get() as { count: number }).count,
    1,
    "the successful purge itself is retained as audit history",
  );
  stateql.close();
});

test("result state quota protects the new result, evicts old unaliased results, and preserves aliases", async () => {
  const tooSmallRoot = createTemporaryDirectory("stateql-quota-reject-test-");
  const tooSmall = new StateQL({
    home: join(tooSmallRoot, "state"),
    maxStateBytes: 100,
  });
  await succeed(tooSmall.connect(join(tooSmallRoot, "target.sqlite"), { readOnly: false }));
  assertFailure(
    await tooSmall.query(`SELECT '${"x".repeat(300)}' AS value`),
    "STATE_QUOTA_EXCEEDED",
  );
  tooSmall.close();

  const root = createTemporaryDirectory("stateql-quota-evict-test-");
  const stateql = new StateQL({ home: join(root, "state"), maxStateBytes: 1_200 });
  await succeed(stateql.connect(join(root, "target.sqlite"), { readOnly: false }));
  const first = await succeed(stateql.query(`SELECT '${"a".repeat(250)}' AS value`));
  await succeed(stateql.setAlias("first", String(first.result_id)));
  const second = await succeed(stateql.query(`SELECT '${"b".repeat(250)}' AS value`));
  const third = await succeed(stateql.query(`SELECT '${"c".repeat(250)}' AS value`));
  assert.equal((await succeed(stateql.show("first"))).result_id, first.result_id);
  assertFailure(await stateql.show(String(second.result_id)), "RESULT_NOT_FOUND");
  assert.equal((await succeed(stateql.show(String(third.result_id)))).result_id, third.result_id);
  stateql.close();
});

test("POSIX state directories and databases are private", {
  skip: process.platform === "win32" ? "Windows does not expose POSIX modes." : false,
}, () => {
  const home = createTemporaryDirectory("stateql-permissions-test-");
  const stateql = new StateQL({ home });
  stateql.close();
  assert.equal(statSync(home).mode & 0o777, 0o700);
  assert.equal(statSync(join(home, "state.sqlite")).mode & 0o777, 0o600);
});

test("legacy sessions migrate actor attribution without losing artifacts", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "legacy.sqlite");
  const legacy = new StateQL({ home });
  await succeed(legacy.connect(database, { readOnly: false }));
  const operation = await succeed(
    legacy.exec("CREATE TABLE legacy_rows (value TEXT)"),
  );
  const result = await succeed(legacy.query("SELECT * FROM legacy_rows"));
  await succeed(legacy.setAlias("legacy", String(result.result_id)));
  const plan = await succeed(
    legacy.plan("INSERT INTO legacy_rows (value) VALUES ('planned')"),
  );
  const transaction = await succeed(legacy.beginTransaction());
  await succeed(
    legacy.exec("INSERT INTO legacy_rows (value) VALUES ('staged')"),
  );
  legacy.close();

  const state = new DatabaseSync(join(home, "state.sqlite"));
  state.exec(`
    DROP TABLE session_members;
    DROP TABLE schema_migrations;
    ALTER TABLE operations DROP COLUMN actor_id;
    ALTER TABLE transactions DROP COLUMN owner_actor_id;
    ALTER TABLE plans DROP COLUMN owner_actor_id;
    ALTER TABLE plans DROP COLUMN claim_token;
    ALTER TABLE history DROP COLUMN actor_id;
  `);
  state.close();

  const migrated = new StateQL({ home });
  assert.equal(
    (await succeed(migrated.show("legacy"))).result_id,
    result.result_id,
  );
  assert.equal(
    (await succeed(migrated.receipt(String(operation.operation_id)))).actor_id,
    "default",
  );
  const transactionStatus = await succeed(
    migrated.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(transactionStatus.owner_actor_id, "default");
  await succeed(
    migrated.rollbackTransaction(String(transaction.transaction_id)),
  );
  await succeed(migrated.apply(String(plan.plan_id)));
  assert.ok(
    (await succeed(migrated.history(100))).history.every(
      (entry: HistoryEntry) => entry.actor_id === "default",
    ),
  );
  migrated.close();

  const reopened = new StateQL({ home });
  assert.equal((await succeed(reopened.query("SELECT * FROM legacy_rows"))).rows, 1);
  reopened.close();
});
