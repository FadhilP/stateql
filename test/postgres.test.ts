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

test("PostgreSQL detection and SQL parsing stay explicit", () => {
  assert.equal(detectDriver("postgres://localhost/app"), "postgres");
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
      const cancelled = await stateql.query("SELECT pg_sleep(1)", {
        cache: "bypass",
        signal: controller.signal,
      });
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
