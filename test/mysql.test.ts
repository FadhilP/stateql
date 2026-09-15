import assert from "node:assert/strict";
import { test } from "node:test";
import { createConnection as createMySqlConnection } from "mysql2/promise";
import { databaseUrlHasSecret, detectDriver } from "../src/connection.js";
import { StateQLError } from "../src/errors.js";
import { analyzeSql } from "../src/sql.js";
import { StateQL } from "../src/stateql.js";
import {
  assertFailure,
  assertOutcomeUnknown,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

test("MySQL detection and SQL parsing stay explicit", () => {
  assert.equal(detectDriver("mysql://localhost/app"), "mysql");
  assert.equal(databaseUrlHasSecret("mysql://user:password@localhost/app"), true);
  assert.equal(
    analyzeSql("SELECT `value` FROM `items` WHERE `id` = ?", "mysql")
      .statementType,
    "select",
  );
  for (const sql of [
    "EXPLAIN SELECT * FROM items WHERE id = ?",
    "SHOW TABLES",
    "SHOW COLUMNS FROM items",
    "SHOW INDEX FROM items",
    "SHOW INDEXES FROM items",
  ]) {
    const analysis = analyzeSql(sql, "mysql");
    assert.equal(analysis.read, true);
    assert.equal(analysis.wrapForLimit, false);
    assert.equal(analysis.cacheable, false);
  }
  for (const sql of [
    "ANALYZE TABLE items",
    "/* lead */ OPTIMIZE TABLE app.items; -- trailing",
    "OPTIMIZE TABLE `app`.`items`",
  ]) {
    const analysis = analyzeSql(sql, "mysql");
    assert.equal(analysis.read, false);
    assert.equal(analysis.destructive, true);
    assert.equal(analysis.requiresAutocommit, true);
  }
  for (const sql of [
    "CHECK TABLE items",
    "CHECK # comment\n TABLE `app`.`items`; -- trailing",
  ]) {
    const analysis = analyzeSql(sql, "mysql");
    assert.equal(analysis.statementType, "check");
    assert.equal(analysis.read, true);
    assert.equal(analysis.destructive, false);
    assert.equal(analysis.cacheable, false);
    assert.equal(analysis.wrapForLimit, false);
    assert.equal(analysis.requiresAutocommit, true);
  }
  for (const sql of [
    "EXPLAIN ANALYZE SELECT * FROM items",
    "EXPLAIN UPDATE items SET value = 1",
    "SHOW STATUS",
    "SHOW PROCESSLIST",
    "SHOW GRANTS",
    "SHOW CREATE TABLE items",
    "SHOW TABLES; DELETE FROM items",
    "SHOW COLUMNS FROM items LIKE 'id'",
    "ANALYZE items",
    "ANALYZE TABLE",
    "ANALYZE TABLE first, second",
    "ANALYZE TABLE items UPDATE HISTOGRAM ON value",
    "OPTIMIZE TABLE items; DROP TABLE items",
    "CHECK TABLE items QUICK",
    "CHECK TABLE items; DELETE FROM items",
    "CHECK TABLE 'items'",
    "CHECK TABLE `unterminated",
    "/*!40101 ANALYZE TABLE items */",

  ]) {
    assert.throws(
      () => analyzeSql(sql, "mysql"),
      (error: unknown) =>
        error instanceof StateQLError && error.details.code === "INVALID_SQL",
      sql,
    );
  }
  assert.throws(
    () => detectDriver("mariadb://localhost/app"),
    (error: unknown) =>
      error instanceof StateQLError &&
      error.details.code === "UNSUPPORTED_DRIVER",
  );
});

test("MySQL rejects executable comments throughout SQL without rejecting quoted markers", () => {
  for (const sql of [
    "/*!80000 SELECT 1 */",
    "EXPLAIN /*!80018 ANALYZE */ SELECT SLEEP(10)",
    "SHOW TABLES /*!50000 FROM information_schema */",
    "SELECT 'safe' /*!80000 UNION SELECT 2 */",
    "INSERT INTO items (id) SELECT id FROM source_items /*!80000 ON DUPLICATE KEY UPDATE id = VALUES(id) */",
    "ANALYZE TABLE items /*!80000 UPDATE HISTOGRAM ON id */",
    "CHECK TABLE items /*!80000 QUICK */",
    "SELECT 1--\u00a0/*!80000 + 1 */",
    // NO_BACKSLASH_ESCAPES exposes the comment after the first string.
    String.raw`EXPLAIN SELECT '\' /*!80000 + 1 */ -- ' AS value`,
    // ANSI_QUOTES without NO_BACKSLASH_ESCAPES exposes the double-quoted marker.
    String.raw`EXPLAIN SELECT '\' -- ' AS a, "\" /*!80000 + 1 */ -- " AS b`,
    "SHOW COLUMNS FROM `items\\` /*!80000 FROM other_db */",
  ]) {
    assert.throws(
      () => analyzeSql(sql, "mysql"),
      (error: unknown) =>
        error instanceof StateQLError &&
        error.details.code === "INVALID_SQL" &&
        /executable comments/i.test(error.message),
      sql,
    );
  }
  for (const sql of [
    "SELECT '/*! literal */' AS value",
    'SELECT "/*! literal */" AS value',
    "SELECT 'it''s /*! literal */' AS value",
    'SELECT "a""/*! literal */" AS value',
    String.raw`SELECT '\\/*! literal */' AS value`,
    String.raw`SELECT 'it\'s ordinary text' AS value`,
    "SHOW COLUMNS FROM `items``/*! literal */`",
    "CHECK TABLE `items/*! literal */`",
    "SELECT 1 /* /*! ordinary comment */",
    "SELECT 1 -- /*! ordinary comment */",
    "SELECT 1 # /*! ordinary comment */",
  ]) {
    assert.equal(analyzeSql(sql, "mysql").read, true, sql);
  }
});

const mysqlUrl = process.env.STQL_MYSQL_URL;

test(
  "MySQL adapter reads, writes, inspects, transacts, and cancels",
  { skip: mysqlUrl ? false : "Set STQL_MYSQL_URL to run MySQL integration tests." },
  async () => {
    const home = createTemporaryDirectory("stateql-mysql-test-");
    const stateql = new StateQL({ home });
    const table = `stateql_test_${process.pid}_${Date.now()}`;
    const stagedTable = `${table}_staged`;
    const identifier = `\`${table}\``;
    const stagedIdentifier = `\`${stagedTable}\``;

    await succeed(
      stateql.connect(undefined, {
        secretEnv: "STQL_MYSQL_URL",
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
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
            "name VARCHAR(100) NOT NULL UNIQUE, " +
            "INDEX idx_name_id (name, id))",
        ),
      );
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES (?)`, {
          params: ["first"],
        }),
      );
      const upserted = await succeed(
        stateql.exec(
          `INSERT INTO ${identifier} (name) VALUES (?) ON DUPLICATE KEY UPDATE name = VALUES(name)`,
          { params: ["first"] },
        ),
      );
      assert.equal(upserted.statement_type, "upsert");
      const assignmentUpsert = await succeed(
        stateql.exec(
          `INSERT INTO ${identifier} SET name = ? ON DUPLICATE KEY UPDATE name = VALUES(name)`,
          { params: ["first"] },
        ),
      );
      assert.equal(assignmentUpsert.statement_type, "upsert");
      const selectUpsert =
        `INSERT INTO ${identifier} (name) SELECT name FROM ${identifier} ` +
        "ON DUPLICATE KEY UPDATE name = VALUES(name)";
      assertFailure(await stateql.exec(selectUpsert), "UNBOUNDED_MUTATION");
      assert.equal(
        (await succeed(
          stateql.exec(selectUpsert, { allowUnbounded: true }),
        )).statement_type,
        "upsert",
      );
      const queried = await succeed(
        stateql.query(
          `SELECT id, name FROM ${identifier} WHERE name = ? ORDER BY id`,
          { params: ["first"], cache: "bypass" },
        ),
      );
      assert.equal(queried.rows, 1);
      assert.equal(queried.preview[0].name, "first");
      const explained = await succeed(
        stateql.query(`EXPLAIN SELECT id, name FROM ${identifier} WHERE name = ?`, {
          params: ["first"],
          cache: "bypass",
        }),
      );
      assert.ok(explained.rows >= 1);
      const shownTables = await succeed(
        stateql.query("SHOW TABLES", { cache: "bypass" }),
      );
      assert.ok(
        shownTables.preview.some((row: Record<string, unknown>) =>
          Object.values(row).includes(table)
        ),
      );
      const shownColumns = await succeed(
        stateql.query(`SHOW COLUMNS FROM ${identifier}`, { cache: "bypass" }),
      );
      assert.equal(shownColumns.rows, 2);
      const shownIndexes = await succeed(
        stateql.query(`SHOW INDEX FROM ${identifier}`, { cache: "bypass" }),
      );
      assert.ok(shownIndexes.rows >= 2);
      const checkSql = `CHECK TABLE ${identifier}`;
      const firstCheck = await succeed(stateql.query(checkSql));
      const secondCheck = await succeed(stateql.query(checkSql));
      assert.ok(firstCheck.rows >= 1);
      assert.ok(
        firstCheck.preview.some((row: Record<string, unknown>) =>
          Object.values(row).some(
            (value) => String(value).toLowerCase() === "status",
          )
        ),
      );
      assert.equal(firstCheck.cached, false);
      assert.equal(secondCheck.cached, false);
      assert.notEqual(firstCheck.result_id, secondCheck.result_id);
      assertFailure(
        await stateql.query(checkSql, { cache: "require" }),
        "CACHE_MISS",
      );
      assertFailure(
        await stateql.query(checkSql, { params: ["unused"] }),
        "INVALID_SQL",
      );

      const analyzeSqlText = `ANALYZE TABLE ${identifier}`;
      assertFailure(
        await stateql.exec(analyzeSqlText),
        "DESTRUCTIVE_OPERATION_BLOCKED",
      );
      assert.equal(
        (await succeed(
          stateql.exec(analyzeSqlText, { allowDestructive: true }),
        )).statement_type,
        "analyze",
      );
      assertFailure(
        await stateql.exec(analyzeSqlText, {
          allowDestructive: true,
          params: ["unused"],
        }),
        "INVALID_SQL",
      );

      const missingMaintenance = await stateql.exec(
        `ANALYZE TABLE \`${table}_missing\``,
        { allowDestructive: true },
      );
      assert.equal(missingMaintenance.ok, false);
      if (!missingMaintenance.ok) {
        assert.equal(missingMaintenance.error.code, "QUERY_FAILED");
        assert.equal(missingMaintenance.error.executed, true);
      }
      assert.equal(stateql.snapshot().recent_operations[0]?.status, "failed");

      const pendingOptimize = await succeed(
        stateql.plan(`OPTIMIZE TABLE ${identifier}`),
      );
      assert.equal(pendingOptimize.requires_confirmation, true);
      assert.deepEqual(
        pendingOptimize.required_overrides,
        ["--allow-destructive"],
      );
      assertFailure(
        await stateql.apply(String(pendingOptimize.plan_id)),
        "DESTRUCTIVE_OPERATION_BLOCKED",
      );
      const approvedOptimize = await succeed(
        stateql.plan(`OPTIMIZE TABLE ${identifier}`, {
          allowDestructive: true,
        }),
      );
      assert.equal(
        (await succeed(
          stateql.apply(String(approvedOptimize.plan_id)),
        )).statement_type,
        "optimize",
      );

      const blocker = await createMySqlConnection(mysqlUrl!);
      try {
        await blocker.query(`LOCK TABLES ${identifier} WRITE`);
        assertOutcomeUnknown(
          await stateql.exec(
            `ANALYZE /* timeout */ TABLE ${identifier}`,
            { allowDestructive: true, timeoutMs: 100 },
          ),
        );
        const controller = new AbortController();
        setTimeout(() => controller.abort(), 100).unref();
        assertOutcomeUnknown(
          await stateql.exec(
            `OPTIMIZE /* cancelled */ TABLE ${identifier}`,
            { allowDestructive: true, signal: controller.signal },
          ),
        );
      } finally {
        await blocker.query("UNLOCK TABLES");
        await blocker.end();
      }
      const firstExplain = await succeed(
        stateql.query(`EXPLAIN SELECT * FROM ${identifier}`),
      );
      const secondExplain = await succeed(
        stateql.query(`EXPLAIN SELECT * FROM ${identifier}`),
      );
      assert.equal(firstExplain.cached, false);
      assert.equal(secondExplain.cached, false);
      assert.notEqual(firstExplain.result_id, secondExplain.result_id);
      assertFailure(
        await stateql.query(`EXPLAIN SELECT * FROM ${identifier}`, {
          cache: "require",
        }),
        "CACHE_MISS",
      );

      const inspected = await succeed(stateql.inspect("table", table));
      assert.equal(inspected.table, table);
      assert.equal(inspected.columns.length, 2);
      assert.ok(inspected.indexes >= 3);
      assert.ok(inspected.constraints >= 2);
      const inspectedIndexes = await succeed(stateql.inspect("indexes", table));
      assert.match(
        inspectedIndexes.indexes.find(
          (index: { name: string }) => index.name === "idx_name_id",
        ).definition,
        /\(name, id\)$/,
      );

      await succeed(stateql.beginTransaction());
      const maintenanceOperationCount =
        stateql.snapshot().recent_operations.length;
      assertFailure(await stateql.query(checkSql), "TRANSACTION_FAILED");
      assertFailure(
        await stateql.exec(analyzeSqlText, { allowDestructive: true }),
        "TRANSACTION_FAILED",
      );
      assert.equal(
        stateql.snapshot().recent_operations.length,
        maintenanceOperationCount,
      );
      await succeed(stateql.rollbackTransaction());


      await succeed(stateql.beginTransaction("serializable"));
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES (?)`, {
          params: ["second"],
        }),
      );
      await succeed(
        stateql.exec(
          `INSERT INTO ${identifier} (name) VALUES (?) ON DUPLICATE KEY UPDATE name = VALUES(name)`,
          { params: ["second"] },
        ),
      );
      await succeed(
        stateql.exec(
          `INSERT INTO ${identifier} SET name = ? ON DUPLICATE KEY UPDATE name = VALUES(name)`,
          { params: ["second"] },
        ),
      );
      const committed = await succeed(stateql.commitTransaction());
      assert.equal(committed.statements_executed, 3);

      await succeed(stateql.beginTransaction());
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES ('rolled-back')`),
      );
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES ('first')`),
      );
      assertFailure(await stateql.commitTransaction(), "TRANSACTION_FAILED");
      assert.equal(
        (await succeed(
          stateql.query(
            `SELECT id FROM ${identifier} WHERE name = 'rolled-back'`,
            { cache: "bypass" },
          ),
        )).rows,
        0,
      );

      await succeed(stateql.beginTransaction());
      await succeed(stateql.exec(`CREATE TABLE ${stagedIdentifier} (id INT)`));
      const rejectedDdl = await stateql.commitTransaction();
      assertFailure(rejectedDdl, "TRANSACTION_FAILED");
      assert.equal(
        rejectedDdl.ok ? "" : rejectedDdl.error.message.includes("atomically"),
        true,
      );

      await succeed(stateql.disconnect());
      await succeed(
        stateql.connect(undefined, {
          secretEnv: "STQL_MYSQL_URL",
          readOnly: true,
        }),
      );
      assertFailure(
        await stateql.exec(`INSERT INTO ${identifier} (name) VALUES ('blocked')`),
        "READ_ONLY_CONNECTION",
      );
      const checkedReadOnly = await succeed(
        stateql.query(`CHECK TABLE ${identifier}`, { cache: "bypass" }),
      );
      assert.ok(checkedReadOnly.rows >= 1);
      assertFailure(
        await stateql.exec(`ANALYZE TABLE ${identifier}`, {
          allowDestructive: true,
        }),
        "READ_ONLY_CONNECTION",
      );
      await succeed(
        stateql.query(`EXPLAIN SELECT * FROM ${identifier}`, { cache: "bypass" }),
      );
      await succeed(stateql.query("SHOW TABLES", { cache: "bypass" }));
      const invalidParameters = await stateql.query("SELECT ? AS value", {
        params: [[1]],
        cache: "bypass",
      });
      assert.equal(invalidParameters.ok, false);
      if (!invalidParameters.ok) {
        assert.equal(invalidParameters.error.code, "QUERY_FAILED");
      }
      const timedOut = await stateql.query("SELECT SLEEP(1)", {
        cache: "bypass",
        timeoutMs: 50,
      });
      assert.equal(timedOut.ok, false);
      if (!timedOut.ok) assert.equal(timedOut.error.code, "DEADLINE_EXCEEDED");

      const controller = new AbortController();
      setTimeout(() => controller.abort(), 50).unref();
      const cancelled = await stateql.executeCommand(
        { command: "query", sql: "SELECT SLEEP(1)", cache: "bypass" },
        { signal: controller.signal, origin: "api" },
      );
      assert.equal(cancelled.ok, false);
      if (!cancelled.ok) {
        assert.equal(cancelled.error.code, "OPERATION_CANCELLED");
      }
      assert.equal(
        (await succeed(stateql.query("SELECT 1 AS healthy"))).preview[0].healthy,
        1,
      );
    } finally {
      await stateql.disconnect();
      const reconnected = await stateql.connect(undefined, {
        secretEnv: "STQL_MYSQL_URL",
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
