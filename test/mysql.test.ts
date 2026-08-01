import assert from "node:assert/strict";
import { test } from "node:test";
import { databaseUrlHasSecret, detectDriver } from "../src/connection.js";
import { StateQLError } from "../src/errors.js";
import { analyzeSql } from "../src/sql.js";
import { StateQL } from "../src/stateql.js";
import {
  assertFailure,
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
  assert.throws(
    () => detectDriver("mariadb://localhost/app"),
    (error: unknown) =>
      error instanceof StateQLError &&
      error.details.code === "UNSUPPORTED_DRIVER",
  );
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
      const queried = await succeed(
        stateql.query(
          `SELECT id, name FROM ${identifier} WHERE name = ? ORDER BY id`,
          { params: ["first"], cache: "bypass" },
        ),
      );
      assert.equal(queried.rows, 1);
      assert.equal(queried.preview[0].name, "first");

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

      await succeed(stateql.beginTransaction("serializable"));
      await succeed(
        stateql.exec(`INSERT INTO ${identifier} (name) VALUES (?)`, {
          params: ["second"],
        }),
      );
      const committed = await succeed(stateql.commitTransaction());
      assert.equal(committed.statements_executed, 1);

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
      const cancelled = await stateql.query("SELECT SLEEP(1)", {
        cache: "bypass",
        signal: controller.signal,
      });
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
