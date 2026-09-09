import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { BSON, MongoClient } from "mongodb";
import {
  credentialSource,
  databaseUrlHasSecret,
  detectDriver,
  mongoDatabaseName,
} from "../src/connection.js";
import { StateQLError } from "../src/errors.js";
import {
  analyzeMongoWriteSafety,
  serializeMongoCommand,
  validateMongoReadCommand,
  validateMongoWriteCommand,
} from "../src/mongodb.js";
import { StateQL } from "../src/stateql.js";
import { StateStore, type ConnectionRecord } from "../src/store.js";
import type { MongoReadCommand, Response } from "../src/types.js";
import { hash } from "../src/util.js";

const temporaryDirectories: string[] = [];

afterEach(() => {
  while (temporaryDirectories.length) {
    rmSync(temporaryDirectories.pop()!, { recursive: true, force: true });
  }
});

function temporaryHome(): string {
  const root = mkdtempSync(join(tmpdir(), "stateql-mongodb-test-"));
  temporaryDirectories.push(root);
  return join(root, "state");
}

function storedMongo(readOnly = false): {
  home: string;
  stateql: StateQL;
  store: StateStore;
  connection: ConnectionRecord;
} {
  const home = temporaryHome();
  const stateql = new StateQL({ home });
  const store = (stateql as unknown as { store: StateStore }).store;
  const snapshot = stateql.snapshot();
  const connection = store.addConnection({
    sessionId: snapshot.session.session_id,
    actorId: snapshot.actor_id,
    name: "mongo-test",
    driver: "mongodb",
    databaseName: "stateql_test",
    source: "mongodb://localhost:27017/stateql_test",
    readOnly,
  });
  assert.ok(connection);
  return { home, stateql, store, connection };
}

function assertFailure(response: Response<unknown>, code: string): void {
  assert.equal(response.ok, false);
  if (!response.ok) assert.equal(response.error.code, code);
}

test("MongoDB URL detection requires a database and detects secret userinfo", () => {
  assert.equal(detectDriver("mongodb://localhost/app"), "mongodb");
  assert.equal(detectDriver("mongodb+srv://cluster.example/app"), "mongodb");
  assert.equal(mongoDatabaseName("mongodb://localhost/my%2Dapp?retryWrites=true"), "my-app");
  assert.equal(credentialSource("mongodb://localhost/app").driver, "mongodb");
  assert.equal(databaseUrlHasSecret("mongodb://user:secret@localhost/app"), true);
  assert.equal(databaseUrlHasSecret("mongodb://localhost/app"), false);
  for (const url of ["mongodb://localhost", "mongodb+srv://cluster.example/"]) {
    assert.throws(
      () => mongoDatabaseName(url),
      (error: unknown) =>
        error instanceof StateQLError &&
        error.details.code === "INVALID_COMMAND" &&
        /explicit database/.test(error.message),
    );
  }
});

test("native MongoDB commands validate allowlisted shapes and operators", () => {
  assert.equal(
    validateMongoReadCommand({
      operation: "find",
      collection: "items",
      filter: { status: "ready" },
      options: { sort: { created_at: -1 }, limit: 10 },
    }).operation,
    "find",
  );
  assert.equal(
    validateMongoWriteCommand({
      operation: "updateOne",
      collection: "items",
      filter: { id: 1 },
      update: { $set: { status: "done" } },
    }).operation,
    "updateOne",
  );
  assert.throws(
    () => validateMongoReadCommand({ operation: "count", collection: "items" }),
    /only support find and aggregate/,
  );
  assert.throws(
    () => validateMongoWriteCommand({
      operation: "insertOne",
      collection: "items",
      document: { value: 1 },
      bypassDocumentValidation: true,
    }),
    /Unknown MongoDB write command field/,
  );
  assert.throws(
    () => validateMongoReadCommand({
      operation: "aggregate",
      collection: "items",
      pipeline: [{ $merge: "archive" }],
    }),
    /operator "\$merge" is forbidden/,
  );
  assert.throws(
    () => validateMongoWriteCommand({
      operation: "updateOne",
      collection: "items",
      filter: { $where: "true" },
      update: { $set: { value: 1 } },
    }),
    /operator "\$where" is forbidden/,
  );
});

test("MongoDB empty-filter safety keeps unbounded and destructive flags independent", () => {
  assert.deepEqual(
    analyzeMongoWriteSafety({
      operation: "updateMany",
      collection: "items",
      filter: {},
      update: { $set: { active: false } },
    }),
    { unbounded: true, destructive: false },
  );
  assert.deepEqual(
    analyzeMongoWriteSafety({
      operation: "deleteMany",
      collection: "items",
      filter: {},
    }),
    { unbounded: true, destructive: true },
  );
  assert.deepEqual(
    analyzeMongoWriteSafety({
      operation: "insertOne",
      collection: "items",
      document: { value: 1 },
    }),
    { unbounded: false, destructive: false },
  );
});

test("order-preserving EJSON round-trips BSON and keeps fingerprint property order", () => {
  const command = BSON.EJSON.parse(
    '{"operation":"find","collection":"items","filter":{"_id":{"$oid":"507f1f77bcf86cd799439011"},"created_at":{"$date":{"$numberLong":"1704067200000"}}}}',
    { relaxed: false },
  ) as MongoReadCommand;
  const serialized = serializeMongoCommand(command);
  const restored = BSON.EJSON.parse(serialized, { relaxed: false }) as MongoReadCommand & {
    filter: { _id: { _bsontype: string }; created_at: Date };
  };

  assert.equal(restored.filter._id._bsontype, "ObjectId");
  assert.equal(restored.filter.created_at.toISOString(), "2024-01-01T00:00:00.000Z");
  assert.notEqual(
    serializeMongoCommand({
      operation: "find",
      collection: "items",
      filter: { a: 1, b: 2 },
    }),
    serializeMongoCommand({
      operation: "find",
      collection: "items",
      filter: { b: 2, a: 1 },
    }),
  );
});

test("MongoDB capabilities are explicit", async () => {
  const stateql = new StateQL({ home: temporaryHome() });
  const response = await stateql.capabilities();
  assert.equal(response.ok, true);
  if (response.ok) {
    assert.deepEqual(response.data.drivers, ["mongodb", "mysql", "postgres", "redis", "sqlite"]);
    assert.deepEqual(response.data.driver_features?.mongodb, {
      sql: false,
      native_read: true,
      native_write: true,
      plans: true,
      transactions: true,
      transactions_require_replica_set: true,
      inspection: true,
    });
  }
  stateql.close();
});

test("stored MongoDB connections reject SQL and enforce native write safety without network", async () => {
  const { stateql } = storedMongo();
  assertFailure(await stateql.query("SELECT 1"), "INVALID_COMMAND");
  assertFailure(await stateql.exec("DELETE FROM items"), "INVALID_COMMAND");
  assertFailure(await stateql.plan("DELETE FROM items"), "INVALID_COMMAND");
  assertFailure(
    await stateql.mongoExec({
      operation: "updateMany",
      collection: "items",
      filter: {},
      update: { $set: { active: false } },
    }),
    "UNBOUNDED_MUTATION",
  );
  assertFailure(
    await stateql.mongoExec({
      operation: "deleteOne",
      collection: "items",
      filter: { id: 1 },
    }),
    "DESTRUCTIVE_OPERATION_BLOCKED",
  );
  const plan = await stateql.mongoPlan({
    operation: "deleteMany",
    collection: "items",
    filter: {},
  });
  assert.equal(plan.ok, true);
  if (plan.ok) {
    assert.deepEqual(plan.data.required_overrides, [
      "--allow-unbounded",
      "--allow-destructive",
    ]);
  }
  stateql.close();
});

test("CLI parses positional EJSON and maps native result, operation, and plan handles", async () => {
  const { home, stateql, store, connection } = storedMongo();
  const payload = '{"operation":"find","collection":"items","filter":{"_id":{"$oid":"507f1f77bcf86cd799439011"},"created_at":{"$date":{"$numberLong":"1704067200000"}}}}';
  const command = BSON.EJSON.parse(payload, { relaxed: false }) as MongoReadCommand;
  const snapshot = stateql.snapshot();
  store.saveResult({
    sessionId: snapshot.session.session_id,
    connectionId: connection.id,
    fingerprint: hash({
      command: serializeMongoCommand(command),
      driver: connection.driver,
      connection: connection.id,
      database: connection.database_name,
      transaction: null,
      stateVersion: "sv_0",
    }),
    sql: "MONGO find",
    parameters: [serializeMongoCommand(command)],
    rows: [{
      _id: { $oid: "507f1f77bcf86cd799439011" },
      created_at: { $date: { $numberLong: "1704067200000" } },
    }],
    columns: [{ name: "_id", type: "objectId" }],
    stateVersion: "sv_0",
    stateSignature: "mongodb:ttl",
    stateConfidence: "ttl_based",
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
  });
  stateql.close();

  const run = (args: string[]) => spawnSync(
    process.execPath,
    ["dist/src/cli.js", ...args],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: home },
      encoding: "utf8",
    },
  );

  const query = run(["mongo", "query", payload]);
  assert.equal(query.status, 0, query.stderr || query.stdout);
  const queried = JSON.parse(query.stdout) as Record<string, any>;
  assert.match(queried.handle, /^q_[a-z2-7]{26}$/);
  assert.equal(queried.total, 1);
  assert.equal(queried.rows[0]._id.$oid, "507f1f77bcf86cd799439011");
  assert.equal("result_id" in queried, false);

  const plan = run([
    "mongo",
    "plan",
    '{"operation":"insertOne","collection":"items","document":{"value":1}}',
  ]);
  assert.equal(plan.status, 0, plan.stderr || plan.stdout);
  const planned = JSON.parse(plan.stdout) as Record<string, unknown>;
  assert.match(String(planned.handle), /^p_/);
  assert.equal("plan_id" in planned, false);

  const begin = run(["transaction", "begin"]);
  assert.equal(begin.status, 0, begin.stderr || begin.stdout);
  const exec = run([
    "mongo",
    "exec",
    '{"operation":"insertOne","collection":"items","document":{"created_at":{"$date":{"$numberLong":"1704067200000"}}}}',
    "--idempotency-key",
    "cli-mongo-insert",
  ]);
  assert.equal(exec.status, 0, exec.stderr || exec.stdout);
  const executed = JSON.parse(exec.stdout) as Record<string, unknown>;
  assert.match(String(executed.handle), /^op_/);
  assert.equal(executed.statement_type, "mongo.insertOne");
  assert.equal("operation_id" in executed, false);
  assert.equal(run(["transaction", "rollback"]).status, 0);

  const invalid = run(["mongo", "query", "{not-ejson}"]);
  assert.notEqual(invalid.status, 0);
  assert.match(invalid.stdout, /Invalid MongoDB EJSON command/);
});

test("operation outcome migration repairs old state and receipts parse outcomes", async () => {
  const { home, stateql } = storedMongo();
  stateql.close();
  const database = new DatabaseSync(join(home, "state.sqlite"));
  database.exec("DELETE FROM schema_migrations WHERE name = 'operation_outcomes_v1'");
  database.exec("ALTER TABLE operations DROP COLUMN outcome_json");
  database.close();

  const reopened = new StateQL({ home });
  const store = (reopened as unknown as { store: StateStore }).store;
  assert.ok(store.db.prepare(
    "SELECT 1 FROM schema_migrations WHERE name = 'operation_outcomes_v1'",
  ).get());
  assert.ok(store.db.prepare(
    "SELECT 1 FROM pragma_table_info('operations') WHERE name = 'outcome_json'",
  ).get());

  const snapshot = reopened.snapshot();
  assert.ok(snapshot.connection);
  const operation = store.saveOperation({
    sessionId: snapshot.session.session_id,
    actorId: snapshot.actor_id,
    connectionId: snapshot.connection.connection_id,
    fingerprint: "outcome-test",
    sql: "MONGO insertOne",
    parameters: [],
    statementType: "mongo.insertOne",
    status: "executing",
    stateVersionBefore: "sv_0",
  });
  store.finishOperation(operation.id, 1, "sv_1", {
    acknowledged: true,
    inserted_id: { $oid: "507f1f77bcf86cd799439011" },
  });
  const receipt = await reopened.receipt(operation.id);
  assert.equal(receipt.ok, true);
  if (receipt.ok) {
    assert.deepEqual(receipt.data.outcome, {
      acknowledged: true,
      inserted_id: { $oid: "507f1f77bcf86cd799439011" },
    });
  }

  store.db.prepare("UPDATE operations SET outcome_json = ? WHERE id = ?")
    .run('{"acknowledged":"yes"}', operation.id);
  assertFailure(await reopened.receipt(operation.id), "STATE_CORRUPTED");
  reopened.close();
});

const mongodbUrl = process.env.STQL_MONGODB_URL;

test(
  "MongoDB adapter reads, writes, inspects, plans, and conditionally transacts",
  {
    skip: mongodbUrl
      ? false
      : "Set STQL_MONGODB_URL to run MongoDB integration tests.",
  },
  async () => {
    const stateql = new StateQL({ home: temporaryHome() });
    const collection = "stateql_integration";
    const run = `stateql_${process.pid}_${Date.now()}`;
    let connected = false;
    await stateql.connect(undefined, {
      secretEnv: "STQL_MONGODB_URL",
      readOnly: false,
    }).then((response) => {
      assert.equal(response.ok, true, response.ok ? undefined : JSON.stringify(response.error));
      connected = response.ok;
    });

    try {
      assertFailure(
        await stateql.mongoExec({
          operation: "deleteMany",
          collection,
          filter: {},
        }),
        "UNBOUNDED_MUTATION",
      );
      const inserted = await stateql.mongoExec({
        operation: "insertOne",
        collection,
        document: { test_run: run, name: "first" },
      });
      assert.equal(inserted.ok, true);
      if (inserted.ok) assert.ok(inserted.data.outcome?.inserted_id);

      const found = await stateql.mongoQuery({
        operation: "find",
        collection,
        filter: { test_run: run },
        options: { sort: { name: 1 } },
      }, { cache: "bypass" });
      assert.equal(found.ok, true);
      if (found.ok) {
        assert.equal(found.data.rows, 1);
        assert.ok((found.data.preview[0]?._id as { $oid?: string }).$oid);
      }

      const controller = new AbortController();
      controller.abort();
      const cancelled = await stateql.executeCommand(
        {
          command: "mongo.query",
          mongo: {
            operation: "find",
            collection,
            filter: { test_run: run },
          },
          cache: "bypass",
        },
        { signal: controller.signal, origin: "api" },
      );
      assertFailure(cancelled, "OPERATION_CANCELLED");
      const healthyAfterCancellation = await stateql.mongoQuery(
        {
          operation: "find",
          collection,
          filter: { test_run: run },
        },
        { cache: "bypass" },
      );
      assert.equal(healthyAfterCancellation.ok, true);

      const updated = await stateql.mongoExec({
        operation: "updateOne",
        collection,
        filter: { test_run: run, name: "first" },
        update: { $set: { name: "updated" } },
      });
      assert.equal(updated.ok, true);
      if (updated.ok) assert.equal(updated.data.outcome?.matched_count, 1);

      const inspected = await stateql.inspect("collection", collection);
      assert.equal(inspected.ok, true);
      if (inspected.ok) assert.equal((inspected.data as { collection: string }).collection, collection);
      const collections = await stateql.inspect("collections");
      assert.equal(collections.ok, true);

      const plan = await stateql.mongoPlan({
        operation: "insertOne",
        collection,
        document: { test_run: run, name: "planned" },
      });
      assert.equal(plan.ok, true);
      if (plan.ok) {
        const applied = await stateql.apply(plan.data.plan_id);
        assert.equal(applied.ok, true);
        if (applied.ok) assert.ok(applied.data.outcome?.inserted_id);
      }

      const deleted = await stateql.mongoExec({
        operation: "deleteOne",
        collection,
        filter: { test_run: run, name: "updated" },
      }, { allowDestructive: true });
      assert.equal(deleted.ok, true);
      if (deleted.ok) assert.equal(deleted.data.outcome?.deleted_count, 1);

      if (await deploymentSupportsTransactions(mongodbUrl!)) {
        const transaction = await stateql.beginTransaction("snapshot");
        assert.equal(transaction.ok, true);
        const staged = await stateql.mongoExec({
          operation: "insertOne",
          collection,
          document: { test_run: run, name: "transaction" },
        });
        assert.equal(staged.ok, true);
        const committed = await stateql.commitTransaction();
        assert.equal(committed.ok, true);
        if (committed.ok) assert.equal(committed.data.statements_executed, 1);
      }
    } finally {
      if (connected) {
        if (stateql.snapshot().transaction?.state === "active") {
          await stateql.rollbackTransaction();
        }
        await stateql.mongoExec({
          operation: "deleteMany",
          collection,
          filter: { test_run: run },
        }, { allowDestructive: true, replay: true });
      }
      stateql.close();
    }
  },
);

async function deploymentSupportsTransactions(url: string): Promise<boolean> {
  const client = new MongoClient(url);
  try {
    await client.connect();
    const hello = await client.db(mongoDatabaseName(url)).admin().command({ hello: 1 });
    return Boolean(
      hello.logicalSessionTimeoutMinutes &&
      (hello.setName || hello.msg === "isdbgrid"),
    );
  } finally {
    await client.close();
  }
}
