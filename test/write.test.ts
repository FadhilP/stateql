import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { StateQLError } from "../src/errors.js";
import { analyzeSql } from "../src/sql.js";
import { StateQL } from "../src/stateql.js";
import { StateStore } from "../src/store.js";
import {
  assertFailure,
  assertOutcomeUnknown,
  createFixture,
  succeed,
} from "./helpers.js";

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
      params: ["keyed"],
      idempotencyKey: "insert-keyed",
    }),
  );
  assert.equal(keyedAgain.operation_id, keyed.operation_id);
  assert.equal(keyedAgain.duplicate, true);
  assertFailure(
    await fixture.stateql.exec("INSERT INTO users (status) VALUES (?)", {
      params: ["changed-but-same-key"],
      idempotencyKey: "insert-keyed",
    }),
    "IDEMPOTENCY_CONFLICT",
  );

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


test("corrupt plan parameters fail before a durable claim", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE planned_rows (id INTEGER)"));
  const plan = await succeed(fixture.stateql.plan("INSERT INTO planned_rows (id) VALUES (?)", {
    params: [1],
  }));
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db.prepare("UPDATE plans SET parameters = ? WHERE id = ?")
    .run("not-json", plan.plan_id);

  assertFailure(await fixture.stateql.apply(String(plan.plan_id)), "STATE_CORRUPTED");
  assert.equal(
    (store.db.prepare("SELECT claim_token FROM plans WHERE id = ?").get(plan.plan_id) as { claim_token: string | null }).claim_token,
    null,
  );
  fixture.stateql.close();
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
    await fixture.stateql.exec(
      "INSERT OR REPLACE INTO policy_rows (id) VALUES (1)",
    ),
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

test("SQL analysis rejects multi-statement and hidden-write forms and classifies dialect mutations", () => {
  for (const [sql, driver] of [
    ["SELECT 1; SELECT 2", "sqlite"],
    ["SELECT 1; DELETE FROM policy_rows", "postgres"],
    ["/* a comment */ SELECT 1; /* another */ SELECT 2", "mysql"],
    ["SELECT * INTO copied_rows FROM policy_rows", "postgres"],
    ["WITH changed AS (UPDATE policy_rows SET id = 2 RETURNING *) SELECT * FROM changed", "postgres"],
  ] as const) {
    assert.throws(
      () => analyzeSql(sql, driver),
      (error: unknown) =>
        error instanceof StateQLError && error.details.code === "INVALID_SQL",
      sql,
    );
  }

  assert.equal(
    analyzeSql("/* harmless */ SELECT value FROM items -- trailing\n", "sqlite").read,
    true,
  );
  const unboundedUpdate = analyzeSql("UPDATE items SET value = 1", "sqlite");
  assert.equal(unboundedUpdate.unboundedMutation, true);
  assert.equal(unboundedUpdate.destructive, false);
  const boundedDelete = analyzeSql("DELETE FROM items WHERE id = 1", "postgres");
  assert.equal(boundedDelete.unboundedMutation, false);
  assert.equal(boundedDelete.destructive, true);
  assert.equal(analyzeSql("TRUNCATE TABLE items", "mysql").unboundedMutation, true);
  assert.equal(analyzeSql("REPLACE INTO items (id) VALUES (1)", "mysql").destructive, true);
  assert.equal(
    analyzeSql("INSERT OR REPLACE INTO items (id) VALUES (1)", "sqlite").destructive,
    true,
  );
});

test("idempotency conflicts are rejected while the first write is pending", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE keyed_rows (value TEXT)"));
  await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec("INSERT INTO keyed_rows (value) VALUES (?)", {
      params: ["first"],
      idempotencyKey: "pending-key",
    }),
  );
  assertFailure(
    await fixture.stateql.exec("INSERT INTO keyed_rows (value) VALUES (?)", {
      params: ["different"],
      idempotencyKey: "pending-key",
    }),
    "IDEMPOTENCY_CONFLICT",
  );
  await succeed(fixture.stateql.rollbackTransaction());
  fixture.stateql.close();
});

test("known SQLite write failures do not poison duplicate protection", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE unique_rows (id INTEGER PRIMARY KEY)"),
  );
  await succeed(fixture.stateql.exec("INSERT INTO unique_rows (id) VALUES (1)"));

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const failed = await fixture.stateql.exec(
      "INSERT INTO unique_rows (id) VALUES (?)",
      { params: [1] },
    );
    assert.equal(failed.ok, false);
    if (!failed.ok) {
      assert.equal(failed.error.code, "QUERY_FAILED");
      assert.equal(failed.error.executed, true);
    }
  }
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM unique_rows"))).rows,
    1,
  );
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


test("timed-out SQLite writes retain unknown-outcome protection", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE deadline_rows (value TEXT)"));
  const blocker = new DatabaseSync(fixture.database);
  try {
    blocker.exec("BEGIN EXCLUSIVE");
    assertOutcomeUnknown(
      await fixture.stateql.exec(
        "INSERT INTO deadline_rows (value) VALUES ('maybe')",
        { timeoutMs: 1_000 },
      ),
    );
  } finally {
    blocker.exec("ROLLBACK");
    blocker.close();
  }
  assertOutcomeUnknown(
    await fixture.stateql.exec(
      "INSERT INTO deadline_rows (value) VALUES ('maybe')",
    ),
  );
  fixture.stateql.close();
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

