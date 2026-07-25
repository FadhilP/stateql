import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { test } from "node:test";
import { StateQL } from "../src/stateql.js";
import { StateStore } from "../src/store.js";
import type { Response } from "../src/types.js";
import {
  assertFailure,
  createFixture,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

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


test("query materialization stops at the configured row bound", async () => {
  const root = createTemporaryDirectory("stateql-bound-test-");
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

test("SQLite deadlines and AbortSignal cancellation stay off the event loop", async () => {
  const fixture = await createFixture();
  let eventLoopTicked = false;
  setTimeout(() => {
    eventLoopTicked = true;
  }, 25);

  const slowSql = `
    WITH RECURSIVE count_up(value) AS (
      SELECT 0
      UNION ALL
      SELECT value + 1 FROM count_up WHERE value < 1000000000
    )
    SELECT sum(value) AS total FROM count_up
  `;
  const started = Date.now();
  const timedOut = await fixture.stateql.query(slowSql, {
    cache: "bypass",
    timeoutMs: 300,
  });
  assert.equal(timedOut.ok, false);
  if (!timedOut.ok) {
    assert.equal(timedOut.error.code, "DEADLINE_EXCEEDED");
    assert.equal(timedOut.error.executed, true);
  }
  assert.equal(eventLoopTicked, true);
  assert.ok(Date.now() - started < 3_000);

  const controller = new AbortController();
  setTimeout(() => controller.abort(), 100);
  const cancelled = await fixture.stateql.query(slowSql, {
    cache: "bypass",
    signal: controller.signal,
    timeoutMs: 5_000,
  });
  assert.equal(cancelled.ok, false);
  if (!cancelled.ok) {
    assert.equal(cancelled.error.code, "OPERATION_CANCELLED");
    assert.equal(cancelled.error.executed, true);
  }

  assert.equal(
    (await succeed(fixture.stateql.query("SELECT 1 AS healthy"))).preview[0]
      .healthy,
    1,
  );
  fixture.stateql.close();

  const cli = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "query",
      slowSql,
      "--cache",
      "bypass",
      "--timeout-ms",
      "200",
      "--output",
      "json",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
      timeout: 3_000,
    },
  );
  assert.equal(cli.status, 4, cli.stderr);
  assert.equal(
    (JSON.parse(cli.stdout) as Response<unknown> & { error?: { code: string } })
      .error?.code,
    "DEADLINE_EXCEEDED",
  );
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

