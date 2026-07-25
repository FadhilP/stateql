import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, test } from "node:test";
import { StateQLError } from "../src/errors.js";
import { analyzeSql } from "../src/sql.js";
import { StateQL } from "../src/stateql.js";
import { StateStore } from "../src/store.js";
import type { BatchCommand, Response } from "../src/types.js";

const temporaryDirectories: string[] = [];

afterEach(() => {
  while (temporaryDirectories.length) {
    rmSync(temporaryDirectories.pop()!, { recursive: true, force: true });
  }
});

test("durable handles, normalized cache reuse, parameters, and compact rows", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, note TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO users (email, note) VALUES (?, ?)",
      { params: ["a@example.com", "x".repeat(250)] },
    ),
  );

  const first = await succeed(
    fixture.stateql.query(
      "SELECT id, email, note FROM users WHERE email = ? ORDER BY id",
      { params: ["a@example.com"] },
    ),
  );
  assert.equal(first.result_id, "q_1");
  assert.equal(first.cached, false);
  assert.deepEqual(first.preview[0].note, {
    type: "text",
    length: 250,
    preview: "x".repeat(200),
  });

  const repeated = await succeed(
    fixture.stateql.query(
      " SELECT id, email, note FROM users WHERE email=? ORDER BY id; ",
      { params: ["a@example.com"] },
    ),
  );
  assert.equal(repeated.result_id, first.result_id);
  assert.equal(repeated.cached, true);

  const otherParameter = await succeed(
    fixture.stateql.query(
      "SELECT id, email, note FROM users WHERE email = ? ORDER BY id",
      { params: ["other@example.com"] },
    ),
  );
  assert.notEqual(otherParameter.result_id, first.result_id);

  const rows = await succeed(fixture.stateql.rows("q_1", { limit: 1 }));
  assert.equal(rows.returned, 1);
  assert.equal(rows.truncated, false);
  assert.equal((await succeed(fixture.stateql.count("q_1"))).rows, 1);
  assert.equal(
    (await succeed(fixture.stateql.columns("q_1"))).columns.length,
    3,
  );
  assertFailure(
    await fixture.stateql.rows("q_1", { limit: 1_001 }),
    "OUTPUT_LIMIT_EXCEEDED",
  );

  await succeed(
    fixture.stateql.connect(fixture.database, { readOnly: true }),
  );
  assertFailure(
    await fixture.stateql.query(
      "SELECT id, email, note FROM users WHERE email = ? ORDER BY id",
      { params: ["a@example.com"], cache: "require" },
    ),
    "CACHE_MISS",
  );
  const otherConnection = await succeed(
    fixture.stateql.query(
      "SELECT id, email, note FROM users WHERE email = ? ORDER BY id",
      { params: ["a@example.com"] },
    ),
  );
  assert.notEqual(otherConnection.result_id, first.result_id);
  assertFailure(
    await fixture.stateql.exec(
      "INSERT INTO users (email) VALUES ('blocked')",
    ),
    "READ_ONLY_CONNECTION",
  );
  fixture.stateql.close();
});

test("filters materialized handles locally into durable derived results", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE filter_users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      `INSERT INTO filter_users (email, active) VALUES
       ('a@example.com', 1), ('b@other.test', 0), ('c@example.com', 1)`,
    ),
  );
  const source = await succeed(
    fixture.stateql.query(
      "SELECT id, email, active FROM filter_users ORDER BY id",
    ),
  );
  await succeed(fixture.stateql.disconnect());

  const filtered = await succeed(
    fixture.stateql.filter(String(source.result_id), "email LIKE ? AND active = ?", {
      params: ["%@example.com", 1],
    }),
  );
  assert.equal(filtered.result_id, "q_2");
  assert.equal(filtered.rows, 2);
  assert.deepEqual(
    filtered.preview.map((row: Record<string, unknown>) => row.id),
    [1, 3],
  );
  assert.equal(filtered.storage.expires_at, source.storage.expires_at);
  assert.equal(
    (await succeed(fixture.stateql.count(String(source.result_id)))).rows,
    3,
  );
  assert.equal(
    (await succeed(fixture.stateql.count(String(filtered.result_id)))).rows,
    2,
  );

  const repeated = await succeed(
    fixture.stateql.filter(String(source.result_id), "email LIKE ? AND active = ?", {
      params: ["%@example.com", 1],
    }),
  );
  assert.equal(repeated.result_id, filtered.result_id);
  assert.equal(repeated.cached, true);
  const changedParameter = await succeed(
    fixture.stateql.filter(String(source.result_id), "email LIKE ?", {
      params: ["%@other.test"],
    }),
  );
  assert.notEqual(changedParameter.result_id, filtered.result_id);
  assert.equal(changedParameter.preview[0].id, 2);

  const named = await succeed(
    fixture.stateql.filter(String(source.result_id), "lower(email) LIKE :domain", {
      params: { domain: "%@example.com" },
    }),
  );
  assert.equal(named.rows, 2);

  assertFailure(
    await fixture.stateql.filter(String(source.result_id), "missing = 1"),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(String(source.result_id), "email LIKE ?"),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(String(source.result_id), "active = 1", {
      params: [1],
    }),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(
      String(source.result_id),
      "1 = 1) UNION SELECT 0 --",
    ),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(
      String(source.result_id),
      "1 = 1); DROP TABLE source; --",
    ),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(
      String(source.result_id),
      "EXISTS (SELECT 1)",
    ),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(
      String(source.result_id),
      "randomblob(100000000) IS NOT NULL",
    ),
    "INVALID_SQL",
  );
  assertFailure(
    await fixture.stateql.filter(
      String(source.result_id),
      "email = :domain OR email = $domain",
      { params: { domain: "a@example.com" } },
    ),
    "INVALID_SQL",
  );
  fixture.stateql.close();
});

test("filters reject ambiguous result columns", async () => {
  const fixture = await createFixture();
  const source = await succeed(fixture.stateql.query("SELECT 1 AS value"));
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db
    .prepare("UPDATE results SET columns_json = ? WHERE id = ?")
    .run(
      JSON.stringify([
        { name: "value", type: "number" },
        { name: "Value", type: "number" },
      ]),
      source.result_id,
    );
  assertFailure(
    await fixture.stateql.filter(String(source.result_id), "value = 1"),
    "INVALID_SQL",
  );
  fixture.stateql.close();
});

test("writes are safe, idempotent, replayable, and invalidate reads", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, status TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO users (status) VALUES (?)", {
      params: ["trial"],
    }),
  );
  const before = await succeed(
    fixture.stateql.query("SELECT * FROM users ORDER BY id"),
  );

  const update = await succeed(
    fixture.stateql.exec("UPDATE users SET status = ? WHERE id = ?", {
      params: ["active", 1],
    }),
  );
  assert.equal(update.committed, true);

  const duplicate = await fixture.stateql.exec(
    " UPDATE users SET status=? WHERE id=?; ",
    { params: ["active", 1] },
  );
  assertFailure(duplicate, "POTENTIAL_DUPLICATE_WRITE");

  const replay = await succeed(
    fixture.stateql.exec("UPDATE users SET status = ? WHERE id = ?", {
      params: ["active", 1],
      replay: true,
    }),
  );
  assert.equal(replay.replay_of, update.operation_id);

  const keyed = await succeed(
    fixture.stateql.exec("INSERT INTO users (status) VALUES (?)", {
      params: ["keyed"],
      idempotencyKey: "insert-keyed",
    }),
  );
  const keyedAgain = await succeed(
    fixture.stateql.exec("INSERT INTO users (status) VALUES (?)", {
      params: ["changed-but-same-key"],
      idempotencyKey: "insert-keyed",
    }),
  );
  assert.equal(keyedAgain.operation_id, keyed.operation_id);
  assert.equal(keyedAgain.duplicate, true);

  assertFailure(
    await fixture.stateql.exec("UPDATE users SET status = 'bad'"),
    "UNBOUNDED_MUTATION",
  );
  assertFailure(
    await fixture.stateql.exec("DROP TABLE users"),
    "DESTRUCTIVE_OPERATION_BLOCKED",
  );

  const after = await succeed(
    fixture.stateql.query("SELECT * FROM users ORDER BY id"),
  );
  assert.notEqual(after.result_id, before.result_id);
  fixture.stateql.close();
});

test("transactions survive staging, commit atomically, and rollback clears duplicates", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)",
    ),
  );

  const rolledBack = await succeed(fixture.stateql.beginTransaction());
  const pending = await succeed(
    fixture.stateql.exec("INSERT INTO events (label) VALUES (?)", {
      params: ["discard"],
    }),
  );
  assert.equal(pending.status, "pending");
  await succeed(
    fixture.stateql.rollbackTransaction(String(rolledBack.transaction_id)),
  );

  await succeed(
    fixture.stateql.exec("INSERT INTO events (label) VALUES (?)", {
      params: ["discard"],
    }),
  );

  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec("INSERT INTO events (label) VALUES (?)", {
      params: ["keep"],
    }),
  );
  const committed = await succeed(
    fixture.stateql.commitTransaction(String(transaction.transaction_id)),
  );
  assert.equal(committed.state, "committed");
  assert.equal(committed.statements_executed, 1);
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM events ORDER BY id")))
      .rows,
    2,
  );
  fixture.stateql.close();
});

test("plans reject stale state and valid plans apply once", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE jobs (id INTEGER PRIMARY KEY, done INTEGER)",
    ),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO jobs (done) VALUES (0)"),
  );

  const stale = await succeed(
    fixture.stateql.plan("DELETE FROM jobs WHERE id = 99"),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO jobs (done) VALUES (1)"),
  );
  assertFailure(
    await fixture.stateql.apply(String(stale.plan_id)),
    "STALE_PLAN",
  );

  const valid = await succeed(
    fixture.stateql.plan("DELETE FROM jobs WHERE id = 1", {
      allowDestructive: true,
    }),
  );
  const applied = await succeed(
    fixture.stateql.apply(String(valid.plan_id)),
  );
  assert.equal(applied.plan_id, valid.plan_id);
  assert.equal(applied.committed, true);
  assertFailure(
    await fixture.stateql.apply(String(valid.plan_id)),
    "STALE_PLAN",
  );
  const batchPlan = await succeed(
    fixture.stateql.executeCommand({
      command: "plan",
      sql: "DELETE FROM jobs WHERE id = 2",
      allow_destructive: true,
    }),
  );
  assert.equal(
    (
      await succeed(
        fixture.stateql.apply(String(batchPlan.plan_id)),
      )
    ).committed,
    true,
  );
  fixture.stateql.close();
});

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

test("concurrent equivalent writes execute once", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)",
    ),
  );
  const second = new StateQL({ home: fixture.home });
  const results = await Promise.all([
    fixture.stateql.exec("INSERT INTO messages (body) VALUES (?)", {
      params: ["once"],
    }),
    second.exec("INSERT INTO messages (body) VALUES (?)", {
      params: ["once"],
    }),
  ]);
  assert.equal(results.filter((result) => result.ok).length, 1);
  assert.equal(
    results.filter(
      (result) =>
        !result.ok && result.error.code === "OUTCOME_UNKNOWN",
    ).length,
    1,
  );
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM messages"))).rows,
    1,
  );
  second.close();
  fixture.stateql.close();
});

test("transactions stay bound to one connection and enforce isolation", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE items (value TEXT)"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  assertFailure(
    await fixture.stateql.connect(join(fixture.home, "other.sqlite"), {
      readOnly: false,
    }),
    "TRANSACTION_FAILED",
  );
  assertFailure(await fixture.stateql.disconnect(), "TRANSACTION_FAILED");
  await succeed(
    fixture.stateql.rollbackTransaction(String(transaction.transaction_id)),
  );
  assertFailure(
    await fixture.stateql.beginTransaction("read committed"),
    "INVALID_COMMAND",
  );
  fixture.stateql.close();
});

test("SQL safety fails closed and plans require explicit overrides", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE policy_rows (id INTEGER PRIMARY KEY)"),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO policy_rows (id) VALUES (1)"),
  );

  assertFailure(
    await fixture.stateql.exec("TRUNCATE TABLE policy_rows"),
    "UNBOUNDED_MUTATION",
  );
  assertFailure(
    await fixture.stateql.exec("TRUNCATE TABLE policy_rows", {
      allowUnbounded: true,
    }),
    "DESTRUCTIVE_OPERATION_BLOCKED",
  );
  assertFailure(
    await fixture.stateql.exec("REPLACE INTO policy_rows (id) VALUES (1)"),
    "DESTRUCTIVE_OPERATION_BLOCKED",
  );
  assertFailure(
    await fixture.stateql.exec("ATTACH DATABASE 'other.sqlite' AS other"),
    "INVALID_SQL",
  );
  for (const sql of [
    "SELECT * INTO copied_rows FROM policy_rows",
    "WITH changed AS (UPDATE policy_rows SET id = 2 RETURNING *) SELECT * FROM changed",
  ]) {
    assert.throws(
      () => analyzeSql(sql, "postgres"),
      (error: unknown) =>
        error instanceof StateQLError && error.details.code === "INVALID_SQL",
    );
  }

  const blocked = await succeed(
    fixture.stateql.plan("DELETE FROM policy_rows WHERE id = 1"),
  );
  assert.equal(blocked.requires_confirmation, true);
  assert.deepEqual(blocked.required_overrides, ["--allow-destructive"]);
  assertFailure(
    await fixture.stateql.apply(String(blocked.plan_id)),
    "DESTRUCTIVE_OPERATION_BLOCKED",
  );
  const allowed = await succeed(
    fixture.stateql.plan("DELETE FROM policy_rows WHERE id = 1", {
      allowDestructive: true,
    }),
  );
  assert.equal(allowed.requires_confirmation, false);
  await succeed(fixture.stateql.apply(String(allowed.plan_id)));
  fixture.stateql.close();
});

test("duplicate-write protection survives reconnecting to the same database", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE reconnect_rows (value TEXT)"));
  await succeed(
    fixture.stateql.exec("INSERT INTO reconnect_rows (value) VALUES ('same')"),
  );
  await succeed(
    fixture.stateql.connect(fixture.database, { readOnly: false }),
  );
  assertFailure(
    await fixture.stateql.exec(
      "INSERT INTO reconnect_rows (value) VALUES ('same')",
    ),
    "POTENTIAL_DUPLICATE_WRITE",
  );
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM reconnect_rows"))).rows,
    1,
  );
  fixture.stateql.close();
});

test("query materialization stops at the configured row bound", async () => {
  const root = mkdtempSync(join(tmpdir(), "stateql-bound-test-"));
  temporaryDirectories.push(root);
  const stateql = new StateQL({
    home: join(root, "state"),
    maxResultRows: 2,
  });
  await succeed(
    stateql.connect(join(root, "target.sqlite"), { readOnly: false }),
  );
  await succeed(stateql.exec("CREATE TABLE bounded_rows (id INTEGER)"));
  await succeed(
    stateql.exec("INSERT INTO bounded_rows (id) VALUES (1), (2), (3)"),
  );
  assertFailure(
    await stateql.query("SELECT id FROM bounded_rows ORDER BY id"),
    "OUTPUT_LIMIT_EXCEEDED",
  );
  assert.equal(
    (
      await succeed(
        stateql.query(
          "WITH first_two AS (SELECT id FROM bounded_rows WHERE id < 3) SELECT * FROM first_two ORDER BY id",
        ),
      )
    ).rows,
    2,
  );
  stateql.close();
});

test("uncertain write outcomes stay blocked until explicit replay", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE uncertain_rows (value TEXT)"));
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  const bumpVersion = store.bumpVersion.bind(store);
  let injectFailure = true;
  store.bumpVersion = (connectionId: string): string => {
    if (injectFailure) {
      injectFailure = false;
      throw new Error("injected metadata failure");
    }
    return bumpVersion(connectionId);
  };

  const first = await fixture.stateql.exec(
    "INSERT INTO uncertain_rows (value) VALUES ('same')",
  );
  assertOutcomeUnknown(first);
  store.bumpVersion = bumpVersion;
  assertOutcomeUnknown(
    await fixture.stateql.exec(
      "INSERT INTO uncertain_rows (value) VALUES ('same')",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO uncertain_rows (value) VALUES ('same')",
      { replay: true },
    ),
  );
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM uncertain_rows"))).rows,
    2,
  );
  fixture.stateql.close();
});

test("transaction statement failures are known rollbacks", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE atomic_rows (id INTEGER PRIMARY KEY)"),
  );
  await succeed(fixture.stateql.exec("INSERT INTO atomic_rows (id) VALUES (1)"));
  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(fixture.stateql.exec("INSERT INTO atomic_rows (id) VALUES (2)"));
  await succeed(
    fixture.stateql.exec("INSERT INTO atomic_rows (id) VALUES (?)", {
      params: [1],
    }),
  );
  assertFailure(
    await fixture.stateql.commitTransaction(String(transaction.transaction_id)),
    "TRANSACTION_FAILED",
  );
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM atomic_rows"))).rows,
    1,
  );
  fixture.stateql.close();
});

test("stale committing transactions recover as unknown", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  await succeed(
    fixture.stateql.exec("CREATE TABLE recovered_transaction (value TEXT)"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO recovered_transaction (value) VALUES ('pending')",
    ),
  );
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  assert.equal(
    store.markTransactionCommitting(String(transaction.transaction_id)),
    true,
  );
  fixture.stateql.close();

  clock = new Date(clock.getTime() + 5 * 60_000 + 1);
  const recovered = new StateQL({ home: fixture.home, now: () => clock });
  const status = await succeed(
    recovered.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(status.state, "outcome_unknown");
  assert.equal((await succeed(recovered.status())).transaction, null);
  recovered.close();
});

test("transaction metadata failures preserve an unknown outcome", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE uncertain_transaction (value TEXT)"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO uncertain_transaction (value) VALUES ('committed')",
    ),
  );
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  const commitMetadata = store.commitTransactionMetadata.bind(store);
  store.commitTransactionMetadata = () => {
    throw new Error("injected transaction metadata failure");
  };
  assertOutcomeUnknown(
    await fixture.stateql.commitTransaction(String(transaction.transaction_id)),
  );
  store.commitTransactionMetadata = commitMetadata;

  const status = await succeed(
    fixture.stateql.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(status.state, "outcome_unknown");
  assert.equal(
    (
      await succeed(
        fixture.stateql.query("SELECT * FROM uncertain_transaction"),
      )
    ).rows,
    1,
  );
  fixture.stateql.close();
});

test("schema inspection and secret handling stay machine-safe", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT UNIQUE)",
    ),
  );
  const inspected = await succeed(
    fixture.stateql.inspect("table", "parents"),
  );
  assert.equal(inspected.table, "parents");
  assert.equal(inspected.columns[0].nullable, false);
  assert.equal((await succeed(fixture.stateql.capabilities())).drivers.length, 2);

  const rejected = await fixture.stateql.connect(
    "postgres://user:super-secret@example.com/app",
  );
  assertFailure(rejected, "PERMISSION_DENIED");
  assert.equal(JSON.stringify(rejected).includes("super-secret"), false);
  fixture.stateql.close();
});

test("local profiles persist and connect by bare or explicit name", async () => {
  const root = mkdtempSync(join(tmpdir(), "stateql-profile-test-"));
  temporaryDirectories.push(root);
  const home = join(root, "state");
  const database = join(root, "profile.sqlite");
  const stateql = new StateQL({ home });

  const added = await succeed(
    stateql.addProfile("local", database, { readOnly: false }),
  );
  assert.equal(added.profile, "local");
  assert.equal(added.read_only, false);
  assert.equal(JSON.stringify(added).includes("password"), false);
  assertFailure(
    await stateql.addProfile(
      "unsafe",
      "postgres://user:password@example.com/app",
    ),
    "PERMISSION_DENIED",
  );

  const connected = await succeed(stateql.connect("local"));
  assert.equal(connected.profile, "local");
  assert.equal(connected.read_only, false);
  await succeed(stateql.exec("CREATE TABLE profile_test (id INTEGER)"));
  stateql.close();

  const restarted = new StateQL({ home });
  assert.equal(
    (await succeed(restarted.listProfiles())).profiles.length,
    1,
  );
  assert.equal(
    (await succeed(restarted.connect(undefined, { profile: "local" }))).profile,
    "local",
  );
  await succeed(restarted.removeProfile("local"));
  assertFailure(await restarted.showProfile("local"), "CONNECTION_NOT_FOUND");
  restarted.close();

  const add = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "profile",
      "add",
      "cli",
      database,
      "--read-write",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: home },
      encoding: "utf8",
    },
  );
  assert.equal(add.status, 0, add.stderr);
  const cliConnect = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "connect", "cli"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: home },
      encoding: "utf8",
    },
  );
  assert.equal(cliConnect.status, 0, cliConnect.stderr);
  assert.equal(
    (JSON.parse(cliConnect.stdout) as { profile: string }).profile,
    "cli",
  );
});

test("CLI accepts shell-safe parameters and applies destructive plans", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE cli_params (id INTEGER PRIMARY KEY, name TEXT)",
    ),
  );
  fixture.stateql.close();

  const insert = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "exec",
      "INSERT INTO cli_params (id, name) VALUES (?, ?)",
      "--param",
      "7",
      "--param",
      "Ada",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(insert.status, 0, insert.stdout);

  const paramsFile = join(fixture.home, "params.json");
  writeFileSync(paramsFile, '[8,"Bob"]');
  const insertFromFile = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "exec",
      "INSERT INTO cli_params (id, name) VALUES (?, ?)",
      "--params-file",
      paramsFile,
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(insertFromFile.status, 0, insertFromFile.stdout);

  const plan = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "plan",
      "DELETE FROM cli_params WHERE id = 7",
      "--allow-destructive",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(plan.status, 0, plan.stdout);
  const planId = (JSON.parse(plan.stdout) as { handle: string }).handle;
  const apply = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "apply", planId],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(apply.status, 0, apply.stdout);
  const applied = JSON.parse(apply.stdout) as Record<string, unknown>;
  assert.match(String(applied.handle), /^op_/);
  assert.equal(applied.plan_id, planId);
  assert.equal("operation_id" in applied, false);

  const restarted = new StateQL({ home: fixture.home });
  const rows = await succeed(
    restarted.query("SELECT id, name FROM cli_params ORDER BY id"),
  );
  assert.equal(rows.rows, 1);
  assert.equal(rows.preview[0].name, "Bob");
  restarted.close();
});

test("CLI defaults to compact agent output and preserves verbose JSON", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE output_rows (id INTEGER PRIMARY KEY, name TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO output_rows (name) VALUES ('Ada'), ('Grace'), ('Linus')",
    ),
  );
  fixture.stateql.close();

  const compactQuery = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "query",
      "SELECT id, name FROM output_rows ORDER BY id",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactQuery.status, 0, compactQuery.stderr);
  assert.equal(compactQuery.stdout.trim().split(/\r?\n/).length, 1);
  const compact = JSON.parse(compactQuery.stdout) as Record<string, any>;
  assert.equal(compact.ok, true);
  assert.equal(compact.handle, "q_1");
  assert.equal(compact.total, 3);
  assert.equal(compact.rows.length, 3);
  assert.equal(compact.next_offset, null);
  assert.equal("data" in compact, false);
  assert.equal("command_id" in compact, false);
  assert.equal("session_id" in compact, false);
  assert.equal("columns" in compact, false);
  assert.equal("meta" in compact, false);

  const compactRows = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "rows", "q_1", "--limit", "2"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactRows.status, 0, compactRows.stderr);
  const page = JSON.parse(compactRows.stdout) as Record<string, any>;
  assert.equal(page.handle, "q_1");
  assert.equal(page.rows.length, 2);
  assert.equal(page.total, 3);
  assert.equal(page.next_offset, 2);
  assert.equal("offset" in page, false);
  assert.equal("limit" in page, false);
  assert.equal("returned" in page, false);

  const compactCount = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "count", "q_1"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactCount.status, 0, compactCount.stderr);
  assert.deepEqual(JSON.parse(compactCount.stdout), {
    ok: true,
    handle: "q_1",
    total: 3,
  });

  const compactFilter = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "filter",
      "q_1",
      "name LIKE ?",
      "--param",
      "A%",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactFilter.status, 0, compactFilter.stderr);
  const filtered = JSON.parse(compactFilter.stdout) as Record<string, any>;
  assert.equal(filtered.handle, "q_2");
  assert.equal(filtered.total, 1);
  assert.equal(filtered.rows[0].name, "Ada");
  assert.equal(filtered.next_offset, null);

  const compactWarning = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "query", "SELECT id FROM output_rows"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactWarning.status, 0, compactWarning.stderr);
  const warning = JSON.parse(compactWarning.stdout) as Record<string, any>;
  assert.equal(warning.warnings[0].code, "NON_DETERMINISTIC_PAGINATION");

  const verboseQuery = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "query",
      "SELECT id, name FROM output_rows ORDER BY id",
      "--cache",
      "bypass",
      "--output",
      "json",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(verboseQuery.status, 0, verboseQuery.stderr);
  const verbose = JSON.parse(verboseQuery.stdout) as Record<string, any>;
  assert.equal(verbose.ok, true);
  assert.equal(typeof verbose.command_id, "string");
  assert.equal(typeof verbose.session_id, "string");
  assert.equal(verbose.data.rows, 3);
  assert.equal(verbose.data.columns.length, 2);
  assert.equal(typeof verbose.meta.duration_ms, "number");

  const compactError = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "exec", "UPDATE output_rows SET name = 'unsafe'"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.notEqual(compactError.status, 0);
  const failure = JSON.parse(compactError.stdout) as Record<string, any>;
  assert.deepEqual(Object.keys(failure), ["ok", "error"]);
  assert.equal(failure.error.code, "UNBOUNDED_MUTATION");
  assert.equal(failure.error.executed, false);
  assert.equal(failure.error.retryable, false);
});

test("batch and pipe execute sequential JSON commands with safe failure control", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE batch_items (id INTEGER PRIMARY KEY, name TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO batch_items (name) VALUES ('one')"),
  );

  const responses = await collect(
    fixture.stateql.batch([
      {
        command: "query",
        sql: "SELECT * FROM batch_items ORDER BY id",
        as: "batch_rows",
      },
      {
        command: "filter",
        handle: "batch_rows",
        where: "name LIKE ?",
        params: ["o%"],
        as: "filtered_rows",
      },
      { command: "rows", handle: "filtered_rows", limit: 1 },
    ]),
  );
  assert.equal(responses.length, 3);
  assert.equal(responses.every((response) => response.ok), true);
  assert.equal(
    (
      (responses[0] as Extract<Response<unknown>, { ok: true }>)
        .data as Record<string, unknown>
    ).alias,
    "batch_rows",
  );
  assert.equal(
    (
      (responses[1] as Extract<Response<unknown>, { ok: true }>)
        .data as Record<string, unknown>
    ).alias,
    "filtered_rows",
  );

  const invalid = { command: "query" } as BatchCommand;
  assert.equal(
    (await collect(
      fixture.stateql.batch([invalid, { command: "status" }]),
    )).length,
    1,
  );
  assert.equal(
    (await collect(
      fixture.stateql.batch([invalid, { command: "status" }], {
        continueOnError: true,
      }),
    )).length,
    2,
  );
  fixture.stateql.close();

  const pipe = spawnSync(process.execPath, ["dist/src/cli.js", "pipe"], {
    cwd: process.cwd(),
    env: { ...process.env, STQL_HOME: fixture.home },
    input: [
      JSON.stringify({
        command: "query",
        sql: "SELECT * FROM batch_items ORDER BY id",
        as: "pipe_rows",
      }),
      JSON.stringify({ command: "count", handle: "pipe_rows" }),
      "",
    ].join("\n"),
    encoding: "utf8",
  });
  assert.equal(pipe.status, 0, pipe.stderr);
  const output = pipe.stdout
    .trim()
    .split(/\r?\n/)
    .map((line) => JSON.parse(line) as Record<string, any>);
  assert.equal(output.length, 2);
  assert.equal(output.every((response) => response.ok), true);
  assert.equal("data" in output[0]!, false);
  assert.equal(output[0]!.handle, "q_1");
  assert.equal(output[1]!.total, 1);

  const batchFile = join(fixture.home, "commands.json");
  writeFileSync(
    batchFile,
    JSON.stringify([{ command: "count", handle: "pipe_rows" }]),
  );
  const batch = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "batch", batchFile],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(batch.status, 0, batch.stderr);
  assert.equal(
    (JSON.parse(batch.stdout) as Response<unknown>).ok,
    true,
  );
});

test("history keeps the latest 10,000 entries per session", () => {
  const root = mkdtempSync(join(tmpdir(), "stateql-test-"));
  temporaryDirectories.push(root);
  const store = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  const session = store.ensureSession();

  store.db.exec(`
    WITH RECURSIVE sequence(value) AS (
      SELECT 1
      UNION ALL
      SELECT value + 1 FROM sequence WHERE value < 10001
    )
    INSERT INTO history
      (id, timestamp, session_id, command, executed, cached, success)
    SELECT
      'seed_' || value,
      '2026-01-01T00:00:00Z',
      '${session.id}',
      'seed',
      0,
      0,
      1
    FROM sequence
  `);

  const latest = store.addHistory({
    sessionId: session.id,
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

async function createFixture(now?: () => Date): Promise<{
  home: string;
  database: string;
  stateql: StateQL;
}> {
  const root = mkdtempSync(join(tmpdir(), "stateql-test-"));
  temporaryDirectories.push(root);
  const home = join(root, "state");
  const database = join(root, "target.sqlite");
  const stateql = new StateQL({ home, ...(now ? { now } : {}) });
  await succeed(stateql.connect(database, { readOnly: false }));
  return { home, database, stateql };
}

async function succeed(
  responseOrPromise: Response<unknown> | Promise<Response<unknown>>,
): Promise<Record<string, any>> {
  const response = await responseOrPromise;
  assert.equal(
    response.ok,
    true,
    response.ok ? undefined : JSON.stringify(response.error),
  );
  return response.data as Record<string, any>;
}

function assertFailure(response: Response<unknown>, code: string): void {
  assert.equal(response.ok, false);
  if (response.ok) return;
  assert.equal(response.error.code, code);
  assert.equal(response.error.executed, false);
}

function assertOutcomeUnknown(response: Response<unknown>): void {
  assert.equal(response.ok, false);
  if (response.ok) return;
  assert.equal(response.error.code, "OUTCOME_UNKNOWN");
  assert.equal(response.error.executed, true);
  assert.equal(response.error.retryable, false);
}

async function collect(
  responses: AsyncIterable<Response<unknown>>,
): Promise<Response<unknown>[]> {
  const collected: Response<unknown>[] = [];
  for await (const response of responses) collected.push(response);
  return collected;
}
