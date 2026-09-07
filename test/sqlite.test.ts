import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { detectDriver } from "../src/connection.js";
import { analyzeSql } from "../src/sql.js";
import type { Response } from "../src/types.js";
import { createFixture, createTemporaryDirectory, succeed } from "./helpers.js";
import { StateQL } from "../src/stateql.js";

test("SQLite detection and SQL parsing stay explicit", () => {
  assert.equal(detectDriver("target.sqlite"), "sqlite");
  assert.equal(
    analyzeSql("SELECT value FROM items WHERE id = ?", "sqlite").statementType,
    "select",
  );
});

test("SQLite catalog discovery filters and pages in the adapter", async () => {
  const fixture = await createFixture();
  try {
    await succeed(fixture.stateql.exec("CREATE TABLE alpha_table (id INTEGER PRIMARY KEY, value TEXT)"));
    await succeed(fixture.stateql.exec("CREATE TABLE beta_table (id INTEGER PRIMARY KEY)"));
    await succeed(fixture.stateql.exec("CREATE VIEW alpha_view AS SELECT id FROM alpha_table"));
    await succeed(fixture.stateql.exec("CREATE TRIGGER alpha_trigger AFTER INSERT ON alpha_table BEGIN UPDATE alpha_table SET value = value WHERE id = NEW.id; END"));
    const first = await succeed(fixture.stateql.listObjects({ limit: 2 }));
    assert.equal(first.objects.length, 2);
    assert.equal(typeof first.next_offset, "number");
    const second = await succeed(fixture.stateql.listObjects({ offset: first.next_offset, limit: 2 }));
    assert.ok(second.objects.length >= 1);
    const views = await succeed(fixture.stateql.listObjects({ kind: "view", search: "alpha", limit: 10 }));
    assert.deepEqual(views.supported_kinds, ["table", "view", "trigger"]);
    assert.deepEqual(views.objects.map((object: { name: string }) => object.name), ["alpha_view"]);
    const described = await succeed(fixture.stateql.describeObject(views.objects[0]));
    assert.match(described.definition, /CREATE VIEW/i);
    const unsupported = await fixture.stateql.listObjects({ kind: "function" });
    assert.equal(unsupported.ok, false);
  } finally { fixture.stateql.close(); }
});


test("SQLite in-memory targets are rejected instead of losing committed writes", async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  for (const target of [":memory:", "sqlite::memory:"]) {
    const response = await stateql.connect(target, { readOnly: false });
    assert.equal(response.ok, false);
    if (!response.ok) assert.equal(response.error.code, "INVALID_COMMAND");
  }
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
  const cancelledPromise = fixture.stateql.executeCommand(
    {
      command: "query",
      sql: slowSql,
      cache: "bypass",
      timeout_ms: 5_000,
    },
    { signal: controller.signal, origin: "user" },
  );
  const isolatedPromise = fixture.stateql.executeCommand(
    { command: "query", sql: "SELECT 2 AS healthy", cache: "bypass" },
    { origin: "model" },
  );
  setTimeout(() => controller.abort(), 100);
  const [cancelled, isolated] = await Promise.all([
    cancelledPromise,
    isolatedPromise,
  ]);
  assert.equal(cancelled.ok, false);
  if (!cancelled.ok) {
    assert.equal(cancelled.error.code, "OPERATION_CANCELLED");
    assert.equal(cancelled.error.executed, true);
  }
  assert.equal(isolated.ok, true);
  if (isolated.ok) {
    assert.equal(
      (isolated.data as { preview: Array<{ healthy: number }> }).preview[0]
        ?.healthy,
      2,
    );
  }

  const userHistory = await succeed(
    fixture.stateql.history(20, { origin: "user" }),
  );
  assert.ok(
    userHistory.history.some(
      (entry: { command: string; success: boolean }) =>
        entry.command === "query" && !entry.success,
    ),
  );
  const modelHistory = await succeed(
    fixture.stateql.history(20, { origin: "model" }),
  );
  assert.ok(
    modelHistory.history.some(
      (entry: { sql: string | null; success: boolean }) =>
        entry.sql === "SELECT 2 AS healthy" && entry.success,
    ),
  );
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
