import assert from "node:assert/strict";
import { test } from "node:test";
import { Client } from "pg";
import {
  BatchWriteError,
  createAdapter,
  createAdapterContext,
  normalizePostgresConnectionString,
} from "../src/adapters.js";
import {
  credentialSource,
  databaseUrlHasSecret,
  detectDriver,
} from "../src/connection.js";
import { StateQLError } from "../src/errors.js";
import { analyzeSql } from "../src/sql.js";
import { StateQL } from "../src/stateql.js";
import {
  assertFailure,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

test("PostgreSQL strict SSL aliases normalize without opting out of libpq compatibility", () => {
  for (const mode of ["prefer", "require", "verify-ca"]) {
    const source = `postgresql://user:p%25ss@localhost/app?application_name=stateql&sslmode=${mode}`;
    const normalized = normalizePostgresConnectionString(source);
    const url = new URL(normalized);
    assert.equal(url.username, "user");
    assert.equal(url.password, "p%25ss");
    assert.equal(url.searchParams.get("application_name"), "stateql");
    assert.deepEqual(url.searchParams.getAll("sslmode"), ["verify-full"]);
  }
  assert.match(
    normalizePostgresConnectionString("postgres://localhost/app?sslmode=%72equire"),
    /sslmode=verify-full$/,
  );
  assert.match(
    normalizePostgresConnectionString("postgres://localhost/app?sslmode=disable&sslmode=require"),
    /sslmode=verify-full$/,
  );
  for (const source of [
    "postgres://localhost/app",
    "postgres://localhost/app?sslmode=verify-full",
    "postgres://localhost/app?sslmode=no-verify",
    "postgres://localhost/app?sslmode=require&uselibpqcompat=true",
    "postgres://localhost/app?sslmode=require&sslmode=disable",
    "postgres://localhost/app?SSLMODE=require",
    "not a database URL",
  ]) {
    assert.equal(normalizePostgresConnectionString(source), source);
  }
});

test("PostgreSQL analysis accepts explicit null ordering without changing its fingerprint", () => {
  const first = analyzeSql(
    "SELECT value FROM items ORDER BY value NULLS FIRST",
    "postgres",
  );
  const last = analyzeSql(
    "SELECT value FROM items ORDER BY value NULLS /* keep */ LAST",
    "postgres",
  );
  const quoted = analyzeSql(
    "SELECT 'NULLS FIRST' AS value ORDER BY value nulls last",
    "postgres",
  );
  const multiple = analyzeSql(
    "SELECT value FROM items ORDER /* clause */ BY value DESC NULLS FIRST, id NULLS LAST",
    "postgres",
  );

  assert.equal(first.read, true);
  assert.equal(first.ordered, true);
  assert.match(first.normalized, /NULLS FIRST/);
  assert.match(last.normalized, /NULLS \/\* keep \*\/ LAST/);
  assert.match(quoted.normalized, /'NULLS FIRST'/);
  assert.match(multiple.normalized, /NULLS FIRST, id NULLS LAST/);
  assert.notEqual(first.normalized, last.normalized);
  assert.throws(
    () => analyzeSql("SELECT 1 NULLS FIRST", "postgres"),
    (error: unknown) => error instanceof StateQLError && error.details.code === "INVALID_SQL",
  );
});

test("PostgreSQL detection and SQL parsing stay explicit", () => {
  assert.equal(detectDriver("postgres://localhost/app"), "postgres");
  assert.equal(credentialSource("postgres:///app").driver, "postgres");
  assert.equal(
    databaseUrlHasSecret("postgres://user:password@localhost/app"),
    true,
  );
  assert.equal(
    analyzeSql('SELECT "value" FROM "items" WHERE "id" = $1', "postgres")
      .statementType,
    "select",
  );
  assert.throws(
    () => detectDriver("cockroachdb://localhost/app"),
    (error: unknown) =>
      error instanceof StateQLError &&
      error.details.code === "UNSUPPORTED_DRIVER",
  );
});

test("PostgreSQL command analysis is narrow and fail-closed", () => {
  for (const sql of [
    "EXPLAIN SELECT 1",
    "EXPLAIN VERBOSE ANALYZE SELECT 1",
    "EXPLAIN (ANALYZE FALSE, FORMAT JSON) UPDATE items SET value = 1",
    "/* lead */ EXPLAIN (ANALYZE OFF) UPDATE \"items;archive\" SET value = 1; -- trailing",
    "EXPLAIN SELECT * FROM \"items;archive\"; /* trailing */",
  ]) {
    const analysis = analyzeSql(sql, "postgres");
    assert.equal(analysis.statementType, "explain");
    assert.equal(analysis.read, true);
    assert.equal(analysis.wrapForLimit, false);
  }
  for (const sql of [
    "VACUUM",
    "/* lead */ VACUUM \"items;archive\"; -- trailing",
    "VACUUM (ANALYZE, SKIP_LOCKED TRUE) public.items(id)",
    "ANALYZE VERBOSE public.items",
    "REINDEX (VERBOSE) TABLE public.items",
    "REINDEX (TABLESPACE fastspace) INDEX public.items_idx",
    "REINDEX TABLE CONCURRENTLY public.items",
    "REINDEX DATABASE CONCURRENTLY app",
    "CLUSTER (VERBOSE) public.items USING items_id_idx",
  ]) {
    const analysis = analyzeSql(sql, "postgres");
    assert.equal(analysis.destructive, true);
    assert.equal(analysis.requiresAutocommit, true);
  }
  for (const sql of [
    "EXPLAIN ANALYZE DELETE FROM items",
    "EXPLAIN (ANALYZE, UNKNOWN) SELECT 1",
    "EXPLAIN VACUUM items",
    "EXPLAIN SELECT 1; VACUUM",
    "EXPLAIN SELECT 1; /* gap */ DELETE FROM items",
    "EXPLAIN SELECT 1 /* unterminated",
    "EXPLAIN SELECT $tag$unterminated",
    "EXPLAIN SELECT * FROM \"unterminated",
    "VACUUM items; DROP TABLE items",
    "VACUUM 'items'",
    "VACUUM \"unterminated",
    "VACUUM \"escaped\"\"",
    "VACUUM /* outer /* inner */",
    "VACUUM \"items\" trailing",
    "REINDEX items",
    "REINDEX SYSTEM CONCURRENTLY app",
    "REINDEX (CONCURRENTLY) SYSTEM app",
    "REINDEX (CONCURRENTLY) TABLE public.items",
    "REINDEX (VERBOSE, CONCURRENTLY) TABLE public.items",
    "REINDEX DATABASE app.extra",
    "REINDEX SCHEMA public.extra",
    "CLUSTER public.items USING public.items_idx",
    "BEGIN",
    "START TRANSACTION",
    "SAVEPOINT guarded",
    "RELEASE SAVEPOINT guarded",
    "COMMIT",
    "ROLLBACK",
  ]) {
    assert.throws(
      () => analyzeSql(sql, "postgres"),
      (error: unknown) =>
        error instanceof StateQLError && error.details.code === "INVALID_SQL",
      sql,
    );
  }
  assert.throws(
    () => analyzeSql("VACUUM items", "mysql"),
    (error: unknown) =>
      error instanceof StateQLError && error.details.code === "INVALID_SQL",
  );
});

test("PostgreSQL diagnostics and maintenance use guarded runtime routes", async (t) => {
  const calls: string[] = [];
  let hangConnect = false;
  let hangMaintenance = false;
  let closeCount = 0;
  const prototype = Client.prototype as unknown as {
    connect(): Promise<void>;
    end(): Promise<void>;
    query(sql: string, params?: unknown[]): Promise<unknown>;
  };
  t.mock.method(prototype, "connect", async () => {
    if (hangConnect) await new Promise<void>(() => undefined);
  });
  t.mock.method(prototype, "end", async () => {
    closeCount += 1;
  });
  t.mock.method(prototype, "query", async (sql: string) => {
    calls.push(sql);
    if (hangMaintenance && /^(?:VACUUM|ANALYZE)\b/i.test(sql)) {
      return new Promise(() => undefined);
    }
    if (/^EXPLAIN\b/i.test(sql)) {
      return {
        rows: [{ "QUERY PLAN": [{ Plan: { "Node Type": "Result" } }] }],
        fields: [{ name: "QUERY PLAN", dataTypeID: 114 }],
        rowCount: 1,
      };
    }
    return {
      rows: /^SELECT 1$/i.test(sql) ? [{ value: 1 }] : [],
      fields: /^SELECT 1$/i.test(sql)
        ? [{ name: "value", dataTypeID: 23 }]
        : [],
      rowCount: 0,
    };
  });

  const home = createTemporaryDirectory("stateql-postgres-routing-test-");
  const stateql = new StateQL({ home });
  await succeed(
    stateql.connect("postgres://localhost/stateql_test", { readOnly: false }),
  );

  const diagnosticStart = calls.length;
  const explained = await succeed(
    stateql.query(
      "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT 1 AS value",
      { cache: "bypass" },
    ),
  );
  assert.equal(explained.rows, 1);
  const diagnosticCalls = calls.slice(diagnosticStart);
  assert.ok(
    diagnosticCalls.includes(
      "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT 1 AS value",
    ),
  );
  assert.equal(
    diagnosticCalls.some((sql) => /SELECT \* FROM \(EXPLAIN/i.test(sql)),
    false,
  );
  const explainIndex = diagnosticCalls.findIndex((sql) => /^EXPLAIN\b/i.test(sql));
  assert.ok(explainIndex > diagnosticCalls.lastIndexOf("BEGIN READ ONLY"));
  const plannedMutationStart = calls.length;
  await succeed(
    stateql.query("EXPLAIN UPDATE guarded_table SET value = 1", {
      cache: "bypass",
    }),
  );
  assert.deepEqual(
    calls.slice(plannedMutationStart).filter((sql) => /^EXPLAIN\b/i.test(sql)),
    ["EXPLAIN UPDATE guarded_table SET value = 1"],
  );

  const repeatedSql = "EXPLAIN (FORMAT JSON) SELECT 2 AS value";
  const repeatedStart = calls.length;
  const firstDiagnostic = await succeed(stateql.query(repeatedSql));
  const secondDiagnostic = await succeed(stateql.query(repeatedSql));
  assert.equal(firstDiagnostic.cached, false);
  assert.equal(secondDiagnostic.cached, false);
  assert.notEqual(firstDiagnostic.result_id, secondDiagnostic.result_id);
  assert.equal(
    calls.slice(repeatedStart).filter((sql) => sql === repeatedSql).length,
    2,
  );
  const requireStart = calls.length;
  const requireCloseCount = closeCount;
  assertFailure(
    await stateql.query(repeatedSql, { cache: "require" }),
    "CACHE_MISS",
  );
  assert.equal(calls.length, requireStart);
  assert.equal(closeCount, requireCloseCount);

  for (const sql of [
    "VACUUM guarded_table",
    "ANALYZE guarded_table",
    "REINDEX TABLE CONCURRENTLY guarded_table",
    "CLUSTER guarded_table",
  ]) {
    assertFailure(
      await stateql.exec(sql),
      "DESTRUCTIVE_OPERATION_BLOCKED",
    );
    const start = calls.length;
    const executed = await succeed(
      stateql.exec(sql, { allowDestructive: true }),
    );
    assert.equal(executed.statement_type, sql.split(/\s/u, 1)[0]!.toLowerCase());
    assert.deepEqual(calls.slice(start), [sql]);
  }
  const invalidReindexStart = calls.length;
  assertFailure(
    await stateql.exec("REINDEX (CONCURRENTLY) TABLE guarded_table", {
      allowDestructive: true,
    }),
    "INVALID_SQL",
  );
  assert.equal(calls.length, invalidReindexStart);
  assertFailure(
    await stateql.exec("VACUUM parameter_table", {
      allowDestructive: true,
      params: ["unused"],
    }),
    "INVALID_SQL",
  );

  const batchAdapter = await createAdapter(
    { driver: "postgres", read_only: 0 } as unknown as Parameters<
      typeof createAdapter
    >[0],
    createAdapterContext(1_000),
    { source: "postgres://localhost/stateql_test" },
  );
  const batchStart = calls.length;
  await assert.rejects(
    batchAdapter.writeBatch(
      [{ statement_type: "vacuum" }] as unknown as Parameters<
        typeof batchAdapter.writeBatch
      >[0],
      "read committed",
    ),
    (error: unknown) =>
      error instanceof BatchWriteError && !error.outcomeUnknown,
  );
  assert.equal(calls.length, batchStart);
  await batchAdapter.close();

  const pendingPlan = await succeed(stateql.plan("CLUSTER planned_table"));
  assert.equal(pendingPlan.requires_confirmation, true);
  assert.deepEqual(pendingPlan.required_overrides, ["--allow-destructive"]);
  assertFailure(
    await stateql.apply(String(pendingPlan.plan_id)),
    "DESTRUCTIVE_OPERATION_BLOCKED",
  );
  const approvedPlan = await succeed(
    stateql.plan("CLUSTER approved_table", { allowDestructive: true }),
  );
  const applyStart = calls.length;
  const applied = await succeed(stateql.apply(String(approvedPlan.plan_id)));
  assert.equal(applied.statement_type, "cluster");
  assert.deepEqual(calls.slice(applyStart), ["CLUSTER approved_table"]);

  for (const sql of [
    "BEGIN",
    "START TRANSACTION",
    "SAVEPOINT guarded",
    "RELEASE SAVEPOINT guarded",
    "COMMIT",
    "ROLLBACK",
  ]) {
    assertFailure(await stateql.exec(sql), "INVALID_SQL");
  }
  for (const sql of [
    "EXPLAIN ANALYZE UPDATE guarded_table SET value = 1",
    "EXPLAIN ANALYZE SELECT * INTO copied_table FROM guarded_table",
    "EXPLAIN ANALYZE WITH changed AS (UPDATE guarded_table SET value = 1 RETURNING *) SELECT * FROM changed",
  ]) {
    assertFailure(await stateql.query(sql, { cache: "bypass" }), "INVALID_SQL");
  }

  hangConnect = true;
  const preDispatchStart = calls.length;
  const preDispatchCloseCount = closeCount;
  const stoppedBeforeDispatch = await stateql.exec("VACUUM predispatch_table", {
    allowDestructive: true,
    timeoutMs: 10,
  });
  assert.equal(stoppedBeforeDispatch.ok, false);
  if (!stoppedBeforeDispatch.ok) {
    assert.equal(stoppedBeforeDispatch.error.code, "DEADLINE_EXCEEDED");
    assert.equal(stoppedBeforeDispatch.error.executed, false);
  }
  assert.equal(calls.length, preDispatchStart);
  assert.ok(closeCount > preDispatchCloseCount);
  assert.equal(stateql.snapshot().recent_operations[0]?.status, "failed");
  hangConnect = false;

  const postDispatchCloseCount = closeCount;
  hangMaintenance = true;
  const timedOut = await stateql.exec("VACUUM timeout_table", {
    allowDestructive: true,
    timeoutMs: 10,
  });
  assert.equal(timedOut.ok, false);
  if (!timedOut.ok) {
    assert.equal(timedOut.error.code, "OUTCOME_UNKNOWN");
    assert.equal(timedOut.error.executed, true);
  }
  assert.equal(stateql.snapshot().recent_operations[0]?.status, "outcome_unknown");

  const controller = new AbortController();
  const cancelledCloseCount = closeCount;
  setTimeout(() => controller.abort(), 10).unref();
  const cancelled = await stateql.exec("ANALYZE cancelled_table", {
    allowDestructive: true,
    signal: controller.signal,
  });
  assert.equal(cancelled.ok, false);
  if (!cancelled.ok) {
    assert.equal(cancelled.error.code, "OUTCOME_UNKNOWN");
    assert.equal(cancelled.error.executed, true);
  }
  assert.equal(stateql.snapshot().recent_operations[0]?.status, "outcome_unknown");
  assert.ok(closeCount > cancelledCloseCount);
  assert.ok(closeCount > postDispatchCloseCount);
  hangMaintenance = false;

  await succeed(stateql.beginTransaction());
  const before = stateql.snapshot().recent_operations.length;
  assertFailure(
    await stateql.exec("VACUUM staged_table", { allowDestructive: true }),
    "TRANSACTION_FAILED",
  );
  assert.equal(stateql.snapshot().recent_operations.length, before);
  await succeed(stateql.rollbackTransaction());

  await succeed(stateql.disconnect());
  await succeed(
    stateql.connect("postgres://localhost/stateql_test", { readOnly: true }),
  );
  assertFailure(
    await stateql.exec("ANALYZE guarded_table", { allowDestructive: true }),
    "READ_ONLY_CONNECTION",
  );
  stateql.close();
});

const postgresUrl = process.env.STQL_POSTGRES_URL;

test(
  "PostgreSQL adapter reads, writes, inspects, transacts, and cancels",
  {
    skip: postgresUrl
      ? false
      : "Set STQL_POSTGRES_URL to run PostgreSQL integration tests.",
  },
  async () => {
    const home = createTemporaryDirectory("stateql-postgres-test-");
    const stateql = new StateQL({ home });
    const table = `stateql_test_${process.pid}_${Date.now()}`;
    const stagedTable = `${table}_staged`;
    const identifier = `"${table}"`;
    const stagedIdentifier = `"${stagedTable}"`;
    const index = `${table}_name_id_idx`;
    const indexIdentifier = `"${index}"`;

    await succeed(
      stateql.connect(undefined, {
        secretEnv: "STQL_POSTGRES_URL",
        readOnly: false,
      }),
    );

    try {
      await stateql.exec(`DROP TABLE IF EXISTS ${identifier}`, {
        allowDestructive: true,
      });
      await succeed(
        stateql.exec(
          `CREATE TABLE ${identifier} (` +
            "id BIGSERIAL PRIMARY KEY, " +
            "name VARCHAR(100) NOT NULL UNIQUE)",
        ),
      );
      await succeed(
        stateql.exec(
          `CREATE INDEX ${indexIdentifier} ON ${identifier} (name, id)`,
        ),
      );
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES ($1)`, {
          params: ["first"],
        }),
      );
      const queried = await succeed(
        stateql.query(
          `SELECT id, name FROM ${identifier} WHERE name = $1 ORDER BY id`,
          { params: ["first"], cache: "bypass" },
        ),
      );
      assert.equal(queried.rows, 1);
      assert.equal(queried.preview[0].name, "first");

      const explained = await succeed(
        stateql.query(
          `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id, name FROM ${identifier} WHERE name = $1`,
          { params: ["first"], cache: "bypass" },
        ),
      );
      assert.equal(explained.rows, 1);
      await succeed(
        stateql.exec(`VACUUM (ANALYZE) ${identifier}`, {
          allowDestructive: true,
        }),
      );
      await succeed(
        stateql.exec(`ANALYZE ${identifier}`, { allowDestructive: true }),
      );
      await succeed(
        stateql.exec(`REINDEX TABLE ${identifier}`, { allowDestructive: true }),
      );
      await succeed(
        stateql.exec(`CLUSTER ${identifier} USING ${indexIdentifier}`, {
          allowDestructive: true,
        }),
      );

      const inspected = await succeed(stateql.inspect("table", table));
      assert.equal(inspected.table, table);
      assert.equal(inspected.columns.length, 2);
      assert.ok(inspected.indexes >= 3);
      assert.ok(inspected.constraints >= 2);
      const inspectedIndexes = await succeed(stateql.inspect("indexes", table));
      const inspectedIndex = inspectedIndexes.indexes.find(
        (item: { name: string }) => item.name === index,
      );
      assert.ok(inspectedIndex);
      assert.match(inspectedIndex.definition, /\(name, id\)$/);

      await succeed(stateql.beginTransaction("serializable"));
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES ($1)`, {
          params: ["second"],
        }),
      );
      const committed = await succeed(stateql.commitTransaction());
      assert.equal(committed.statements_executed, 1);

      await succeed(stateql.beginTransaction());
      await succeed(
        stateql.exec(
          `INSERT INTO ${identifier} (name) VALUES ('rolled-back')`,
        ),
      );
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES ('first')`),
      );
      assertFailure(await stateql.commitTransaction(), "TRANSACTION_FAILED");
      assert.equal(
        (
          await succeed(
            stateql.query(
              `SELECT id FROM ${identifier} WHERE name = 'rolled-back'`,
              { cache: "bypass" },
            ),
          )
        ).rows,
        0,
      );

      await succeed(stateql.beginTransaction());
      await succeed(stateql.exec(`CREATE TABLE ${stagedIdentifier} (id INT)`));
      await succeed(stateql.commitTransaction());
      assert.equal(
        (await succeed(stateql.inspect("table", stagedTable))).table,
        stagedTable,
      );

      await succeed(stateql.disconnect());
      await succeed(
        stateql.connect(undefined, {
          secretEnv: "STQL_POSTGRES_URL",
          readOnly: true,
        }),
      );
      assertFailure(
        await stateql.exec(
          `INSERT INTO ${identifier} (name) VALUES ('blocked')`,
        ),
        "READ_ONLY_CONNECTION",
      );
      const invalidParameters = await stateql.query("SELECT $1 AS value", {
        params: { value: 1 },
        cache: "bypass",
      });
      assert.equal(invalidParameters.ok, false);
      if (!invalidParameters.ok) {
        assert.equal(invalidParameters.error.code, "QUERY_FAILED");
      }
      const timedOut = await stateql.query("SELECT pg_sleep(1)", {
        cache: "bypass",
        timeoutMs: 50,
      });
      assert.equal(timedOut.ok, false);
      if (!timedOut.ok) assert.equal(timedOut.error.code, "DEADLINE_EXCEEDED");

      const controller = new AbortController();
      setTimeout(() => controller.abort(), 50).unref();
      const cancelled = await stateql.executeCommand(
        { command: "query", sql: "SELECT pg_sleep(1)", cache: "bypass" },
        { signal: controller.signal, origin: "api" },
      );
      assert.equal(cancelled.ok, false);
      if (!cancelled.ok) {
        assert.equal(cancelled.error.code, "OPERATION_CANCELLED");
      }
      assert.equal(
        (await succeed(stateql.query("SELECT 1 AS healthy"))).preview[0]
          .healthy,
        1,
      );
    } finally {
      await stateql.disconnect();
      const reconnected = await stateql.connect(undefined, {
        secretEnv: "STQL_POSTGRES_URL",
        readOnly: false,
      });
      if (reconnected.ok) {
        await stateql.exec(`DROP TABLE IF EXISTS ${identifier}`, {
          allowDestructive: true,
        });
        await stateql.exec(`DROP TABLE IF EXISTS ${stagedIdentifier}`, {
          allowDestructive: true,
        });
      }
      stateql.close();
    }
  },
);
