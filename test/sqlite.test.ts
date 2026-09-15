import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  BatchWriteError,
  createAdapter,
  createAdapterContext,
} from "../src/adapters.js";
import { detectDriver } from "../src/connection.js";
import { StateQLError } from "../src/errors.js";
import { analyzeSql } from "../src/sql.js";
import type { Response } from "../src/types.js";
import {
  assertFailure,
  assertOutcomeUnknown,
  createFixture,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";
import { StateQL } from "../src/stateql.js";

test("SQLite detection and SQL parsing stay explicit", () => {
  assert.equal(detectDriver("target.sqlite"), "sqlite");
  assert.equal(
    analyzeSql("SELECT value FROM items WHERE id = ?", "sqlite").statementType,
    "select",
  );
});

test("SQLite EXPLAIN QUERY PLAN is a non-cacheable read diagnostic", async () => {
  const analysis = analyzeSql(
    "/* lead */ EXPLAIN QUERY PLAN SELECT value FROM items WHERE id = ?; -- trailing",
    "sqlite",
  );
  assert.equal(analysis.statementType, "explain");
  assert.equal(analysis.read, true);
  assert.equal(analysis.wrapForLimit, false);
  assert.equal(analysis.cacheable, false);

  for (const sql of [
    "EXPLAIN SELECT 1",
    "EXPLAIN QUERY PLAN",
    "EXPLAIN QUERY PLAN UPDATE items SET value = 'changed'",
    "EXPLAIN QUERY PLAN SELECT 1; DELETE FROM items",
    "EXPLAIN QUERY PLAN WITH changed AS (DELETE FROM items RETURNING *) SELECT * FROM changed",
  ]) {
    assert.throws(
      () => analyzeSql(sql, "sqlite"),
      (error: unknown) =>
        error instanceof StateQLError && error.details.code === "INVALID_SQL",
      sql,
    );
  }

  const fixture = await createFixture();
  try {
    await succeed(
      fixture.stateql.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"),
    );
    await succeed(
      fixture.stateql.exec("INSERT INTO items (value) VALUES ('alpha'), ('beta')"),
    );
    await succeed(fixture.stateql.exec("CREATE INDEX items_value_idx ON items(value)"));
    const sql = "EXPLAIN QUERY PLAN SELECT value FROM items WHERE value = ?";
    const first = await succeed(
      fixture.stateql.query(sql, { params: ["alpha"] }),
    );
    const second = await succeed(
      fixture.stateql.query(sql, { params: ["alpha"] }),
    );
    assert.equal(first.cached, false);
    assert.equal(second.cached, false);
    assert.notEqual(first.result_id, second.result_id);
    assert.match(String(first.preview[0]?.detail), /items_value_idx/i);
    assertFailure(
      await fixture.stateql.query(sql, { params: ["alpha"], cache: "require" }),
      "CACHE_MISS",
    );
    assertFailure(await fixture.stateql.exec(sql), "INVALID_SQL");
    await succeed(fixture.stateql.disconnect());
    await succeed(fixture.stateql.connect(fixture.database, { readOnly: true }));
    await succeed(
      fixture.stateql.query(sql, { params: ["alpha"], cache: "bypass" }),
    );
  } finally {
    fixture.stateql.close();
  }
});

test("SQLite command comments cannot disguise writes or additional statements", async () => {
  const fixture = await createFixture();
  try {
    await succeed(fixture.stateql.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)"));
    await succeed(fixture.stateql.exec("INSERT INTO items VALUES (1), (2)"));
    const operationCount = fixture.stateql.snapshot().recent_operations.length;

    for (const sql of [
      "/* /* */ DELETE FROM items; -- */ EXPLAIN QUERY PLAN SELECT 1",
      "EXPLAIN /* /* */ DELETE FROM items; -- */ QUERY PLAN SELECT 1",
      "EXPLAIN QUERY /* /* */ DELETE FROM items; -- */ PLAN SELECT 1",
    ]) {
      assertFailure(await fixture.stateql.query(sql), "INVALID_SQL");
    }
    for (const sql of [
      "ANALYZE /* /* */; DELETE FROM items; -- */",
      "ANALYZE items /* /* */; DELETE FROM items; -- */",
      "REINDEX /* /* */; DROP TABLE items; -- */",
      "VACUUM /* /* */; DELETE FROM items; -- */",
    ]) {
      assertFailure(
        await fixture.stateql.exec(sql, { allowDestructive: true }),
        "INVALID_SQL",
      );
      assertFailure(await fixture.stateql.plan(sql), "INVALID_SQL");
    }
    assert.equal(fixture.stateql.snapshot().recent_operations.length, operationCount);
    assert.deepEqual(
      (await succeed(
        fixture.stateql.query("SELECT id FROM items ORDER BY id", { cache: "bypass" }),
      )).preview,
      [{ id: 1 }, { id: 2 }],
    );

    // SQLite closes at the first */ even when the comment contains another /*.
    await succeed(fixture.stateql.query(
      "/* outer /* still a comment */ EXPLAIN QUERY PLAN SELECT '/* literal */' AS value",
    ));
    await succeed(fixture.stateql.exec(
      "ANALYZE /* outer /* still a comment */ items; -- trailing",
      { allowDestructive: true },
    ));
  } finally {
    fixture.stateql.close();
  }
});

test("SQLite maintenance uses guarded durable autocommit execution", async () => {
  for (const sql of [
    "VACUUM",
    "VACUUM; -- trailing",
    "ANALYZE",
    "ANALYZE maintenance_items",
    'ANALYZE "maintenance items"',
    "REINDEX",
    "REINDEX maintenance_items_idx",
  ]) {
    const analysis = analyzeSql(sql, "sqlite");
    assert.equal(analysis.statementType, sql.trim().split(/[\s;]/u, 1)[0]!.toLowerCase());
    assert.equal(analysis.destructive, true);
    assert.equal(analysis.requiresAutocommit, true);
  }
  for (const sql of [
    "VACUUM main",
    "VACUUM INTO 'copy.sqlite'",
    "ANALYZE main.maintenance_items",
    "ANALYZE 'maintenance_items'",
    "REINDEX main.maintenance_items_idx",
    "REINDEX maintenance_items_idx; DROP TABLE maintenance_items",
    "PRAGMA optimize",
    "ATTACH DATABASE 'other.sqlite' AS other",
  ]) {
    assert.throws(
      () => analyzeSql(sql, "sqlite"),
      (error: unknown) =>
        error instanceof StateQLError && error.details.code === "INVALID_SQL",
      sql,
    );
  }

  const fixture = await createFixture();
  try {
    await succeed(
      fixture.stateql.exec(
        "CREATE TABLE maintenance_items (id INTEGER PRIMARY KEY, value TEXT)",
      ),
    );
    await succeed(
      fixture.stateql.exec(
        "CREATE INDEX maintenance_items_idx ON maintenance_items(value)",
      ),
    );
    await succeed(
      fixture.stateql.exec(
        "CREATE INDEX maintenance_plan_idx ON maintenance_items(id, value)",
      ),
    );

    for (const sql of [
      "ANALYZE maintenance_items",
      "REINDEX maintenance_items_idx",
      "VACUUM",
    ]) {
      assertFailure(
        await fixture.stateql.exec(sql),
        "DESTRUCTIVE_OPERATION_BLOCKED",
      );
      const executed = await succeed(
        fixture.stateql.exec(sql, { allowDestructive: true }),
      );
      assert.equal(executed.statement_type, sql.split(/\s/u, 1)[0]!.toLowerCase());
      assert.equal(executed.status, "committed");
      assert.equal(executed.affected_rows, 0);
    }

    const operationCount = fixture.stateql.snapshot().recent_operations.length;
    assertFailure(
      await fixture.stateql.exec("ANALYZE maintenance_items", {
        allowDestructive: true,
        params: ["unused"],
      }),
      "INVALID_SQL",
    );
    assert.equal(
      fixture.stateql.snapshot().recent_operations.length,
      operationCount,
    );

    const pendingPlan = await succeed(fixture.stateql.plan("ANALYZE"));
    assert.equal(pendingPlan.requires_confirmation, true);
    assert.deepEqual(pendingPlan.required_overrides, ["--allow-destructive"]);
    assertFailure(
      await fixture.stateql.apply(String(pendingPlan.plan_id)),
      "DESTRUCTIVE_OPERATION_BLOCKED",
    );
    const approvedPlan = await succeed(
      fixture.stateql.plan("REINDEX maintenance_plan_idx", {
        allowDestructive: true,
      }),
    );
    assert.equal(
      (await succeed(
        fixture.stateql.apply(String(approvedPlan.plan_id)),
      )).statement_type,
      "reindex",
    );

    await succeed(fixture.stateql.beginTransaction());
    const stagedCount = fixture.stateql.snapshot().recent_operations.length;
    assertFailure(
      await fixture.stateql.exec("VACUUM", { allowDestructive: true }),
      "TRANSACTION_FAILED",
    );
    assert.equal(
      fixture.stateql.snapshot().recent_operations.length,
      stagedCount,
    );
    await succeed(fixture.stateql.rollbackTransaction());

    const adapter = await createAdapter(
      { driver: "sqlite", read_only: 0 } as unknown as Parameters<
        typeof createAdapter
      >[0],
      createAdapterContext(1_000),
      { source: fixture.database },
    );
    await assert.rejects(
      adapter.writeBatch(
        [{ statement_type: "vacuum" }] as unknown as Parameters<
          typeof adapter.writeBatch
        >[0],
        "serializable",
      ),
      (error: unknown) =>
        error instanceof BatchWriteError && !error.outcomeUnknown,
    );
    await adapter.close();

    await succeed(fixture.stateql.disconnect());
    await succeed(
      fixture.stateql.connect(fixture.database, { readOnly: true }),
    );
    assertFailure(
      await fixture.stateql.exec("ANALYZE", { allowDestructive: true }),
      "READ_ONLY_CONNECTION",
    );
  } finally {
    fixture.stateql.close();
  }
});

test("stopped SQLite maintenance retains unknown-outcome protection", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE stopped_maintenance (id INTEGER PRIMARY KEY, value TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      "CREATE INDEX stopped_maintenance_timeout_idx ON stopped_maintenance(value)",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      "CREATE INDEX stopped_maintenance_cancel_idx ON stopped_maintenance(id, value)",
    ),
  );

  const stopped = new AbortController();
  stopped.abort();
  const predispatch = await fixture.stateql.exec(
    "ANALYZE stopped_maintenance",
    { allowDestructive: true, signal: stopped.signal },
  );
  assert.equal(predispatch.ok, false);
  if (!predispatch.ok) {
    assert.equal(predispatch.error.code, "OPERATION_CANCELLED");
    assert.equal(predispatch.error.executed, false);
  }

  const blocker = new DatabaseSync(fixture.database);
  try {
    blocker.exec("BEGIN EXCLUSIVE");
    assertOutcomeUnknown(
      await fixture.stateql.exec(
        "REINDEX stopped_maintenance_timeout_idx",
        { allowDestructive: true, timeoutMs: 200 },
      ),
    );

    const controller = new AbortController();
    setTimeout(() => controller.abort(), 100).unref();
    assertOutcomeUnknown(
      await fixture.stateql.exec(
        "REINDEX stopped_maintenance_cancel_idx",
        { allowDestructive: true, signal: controller.signal },
      ),
    );
  } finally {
    blocker.exec("ROLLBACK");
    blocker.close();
  }
  assert.equal(
    (await succeed(
      fixture.stateql.query("SELECT 1 AS healthy", { cache: "bypass" }),
    )).preview[0]?.healthy,
    1,
  );
  fixture.stateql.close();
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
