import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { StateQL } from "../src/stateql.js";
import { createAdapter, createAdapterContext } from "../src/adapters.js";
import { StateStore } from "../src/store.js";
import { createFixture, succeed } from "./helpers.js";
import { compileTableUpdate, editableRow, type EditableTable } from "../src/table-editor.js";
import { analyzeSql } from "../src/sql.js";
import { BSON, MongoClient } from "mongodb";
import { createTemporaryDirectory } from "./helpers.js";
import { validateMongoWriteCommand } from "../src/mongodb.js";

test("conditional updates preserve native parameter identity and reject unsafe changes on every driver", () => {
  for (const driver of ["sqlite", "postgres", "mysql"] as const) {
    const metadata: EditableTable = { driver, table: { name: "items" }, writable: true, columns: [
      { name: "id", type: "integer", key: 1, generated: false, nullable: false },
      { name: "value", type: "text", key: 0, generated: false, nullable: true },
    ] };
    const update = { metadata, original: { id: 1, value: "before" }, changes: { set: { value: "after' ; DROP TABLE items; --" } } };
    const compiled = compileTableUpdate(update);
    const analysis = analyzeSql(compiled.sql, driver);
    assert.equal(analysis.statementType, "update");
    assert.equal(analysis.unboundedMutation, false);
    assert.ok(!compiled.sql.includes("DROP"));
    assert.throws(() => compileTableUpdate({ ...update, changes: { set: { id: 2 } } }));
  }
  assert.equal(editableRow({ driver: "mongodb", table: { name: "items" }, columns: [], writable: true }, { _id: "id", nested: { x: 1 } }), false);
  const id = new BSON.ObjectId();
  const original = BSON.EJSON.serialize({ _id: id, value: "before" }, { relaxed: false });
  const compiled = compileTableUpdate({ metadata: { driver: "mongodb", table: { name: "items" }, columns: [], writable: true }, original,
    changes: { set: { value: "after" } } });
  const command = validateMongoWriteCommand(compiled.mongo);
  assert.equal(command.operation, "updateOne");
  if (command.operation === "updateOne") assert.equal((command.filter._id as BSON.ObjectId).toHexString(), id.toHexString());
});

test("full-value panel pages preserve long cells without displacing history and export stored values safely", async () => {
  const { stateql } = await createFixture();
  try {
    const value = "=" + "x".repeat(500);
    const result = await succeed(stateql.query("SELECT ? AS value", { params: [value] }));
    assert.equal(result.preview[0].value.preview.length, 200);
    const history = stateql.snapshot({ historyLimit: 100 }).history;
    const page = stateql.readMaterialized(result.result_id);
    assert.equal(page.rows[0]!.value, value);
    page.rows[0]!.value = "tampered";
    assert.equal(stateql.readMaterialized(result.result_id).rows[0]!.value, value);
    assert.deepEqual(stateql.snapshot({ historyLimit: 100 }).history, history);
    const exported = await succeed(stateql.serializeResult(result.result_id, "csv"));
    assert.ok(exported.content.includes("'="));
    const csvResult = await succeed(stateql.query("SELECT ? AS value", { params: [" \t=SUM(1,2)\nsecond"] }));
    const csv = await succeed(stateql.serializeResult(csvResult.result_id, "csv"));
    assert.equal(csv.content, "value\n\"' \t=SUM(1,2)\nsecond\"\n");
    const json = await succeed(stateql.serializeResult(result.result_id, "json"));
    assert.deepEqual(JSON.parse(json.content), [{ value }]);
    const abort = new AbortController();
    abort.abort();
    assert.throws(() => stateql.readMaterialized(result.result_id, { signal: abort.signal }));
    assert.equal((await stateql.serializeResult(result.result_id, "json", abort.signal)).ok, false);
  } finally { stateql.close(); }
});

test("table plans bind original values, preserve composite identity, and apply one row only", async () => {
  const { stateql, database } = await createFixture();
  const external = new DatabaseSync(database);
  try {
    await succeed(stateql.exec("CREATE TABLE items (a INTEGER, b INTEGER, value TEXT, PRIMARY KEY (a, b))"));
    await succeed(stateql.exec("INSERT INTO items VALUES (1, 1, 'first'), (1, 2, 'second')"));
    const table = await succeed(stateql.readTable({ name: "items" }));
    const rows = stateql.readMaterialized(table.result_id);
    assert.ok(rows.row_tokens[0]);
    const plan = await succeed(stateql.planTableUpdate(rows.row_tokens[0]!, { set: { value: "updated" } }));
    await succeed(stateql.apply(plan.plan_id));
    assert.deepEqual(external.prepare("SELECT value FROM items ORDER BY b").all().map(row => row.value), ["updated", "second"]);
    assert.equal((await stateql.apply(plan.plan_id)).ok, false);

    const fresh = await succeed(stateql.readTable({ name: "items" }, 1000, { cache: "bypass" }));
    const token = stateql.readMaterialized(fresh.result_id).row_tokens[0]!;
    external.exec("UPDATE items SET value = 'external' WHERE b = 1");
    const stale = await succeed(stateql.planTableUpdate(token, { set: { value: "overwrite" } }));
    const response = await stateql.apply(stale.plan_id);
    assert.equal(response.ok, false);
    if (!response.ok) assert.equal(response.error.code, "ROW_CONFLICT");
    assert.equal(external.prepare("SELECT value FROM items WHERE b = 1").get()!.value, "external");
    assert.equal((await stateql.planTableUpdate(token, { set: { a: 9 } })).ok, false);
    await succeed(stateql.connect(database, { readOnly: true }));
    const readonly = await succeed(stateql.readTable({ name: "items" }));
    assert.ok(stateql.readMaterialized(readonly.result_id).row_tokens.every(token => token === null));
  } finally { external.close(); stateql.close(); }
});

test("conditional SQLite cardinality failure rolls back every changed row before commit", async () => {
  const { stateql, home, database } = await createFixture();
  const store = new StateStore(home, () => new Date());
  const connection = store.activeConnection(store.ensureSession("default"))!;
  const adapter = await createAdapter(connection, createAdapterContext(5000), { source: database });
  try {
    await succeed(stateql.exec("CREATE TABLE cardinality (id INTEGER, value TEXT)"));
    await succeed(stateql.exec("INSERT INTO cardinality VALUES (1, 'one'), (2, 'two')"));
    await assert.rejects(adapter.write("UPDATE cardinality SET value = ?", ["bad"], 1), /ROW_CONFLICT/);
    const external = new DatabaseSync(database);
    try { assert.deepEqual(external.prepare("SELECT value FROM cardinality ORDER BY id").all().map(row => row.value), ["one", "two"]); }
    finally { external.close(); }
  } finally { await adapter.close(); store.close(); stateql.close(); }
});

test("multi-row table plans apply atomically and roll back a middle-row conflict", async () => {
  const { stateql, database } = await createFixture();
  const external = new DatabaseSync(database);
  try {
    await succeed(stateql.exec("CREATE TABLE batch_items (id INTEGER PRIMARY KEY, value TEXT)"));
    await succeed(stateql.exec("INSERT INTO batch_items VALUES (1, 'one'), (2, 'two'), (3, 'three')"));
    await succeed(stateql.exec("CREATE TRIGGER batch_conflict AFTER UPDATE ON batch_items WHEN NEW.id = 1 BEGIN UPDATE batch_items SET value = 'triggered' WHERE id = 2; END"));
    const table = await succeed(stateql.readTable({ name: "batch_items" }, 100, { cache: "bypass" }));
    const page = stateql.readMaterialized(table.result_id);
    const plan = await succeed(stateql.planTableUpdates([
      { row_token: page.row_tokens[0]!, changes: { set: { value: "first" } } },
      { row_token: page.row_tokens[1]!, changes: { set: { value: "second" } } },
    ]));
    const conflicted = await stateql.apply(plan.plan_id);
    assert.equal(conflicted.ok, false);
    if (!conflicted.ok) assert.equal(conflicted.error.code, "ROW_CONFLICT");
    assert.deepEqual(external.prepare("SELECT value FROM batch_items ORDER BY id").all().map((row) => row.value), ["one", "two", "three"]);
    external.exec("DROP TRIGGER batch_conflict");

    const fresh = await succeed(stateql.readTable({ name: "batch_items" }, 100, { cache: "bypass" }));
    const freshPage = stateql.readMaterialized(fresh.result_id);
    const appliedPlan = await succeed(stateql.planTableUpdates([
      { row_token: freshPage.row_tokens[0]!, changes: { set: { value: "first" } } },
      { row_token: freshPage.row_tokens[1]!, changes: { set: { value: "second" } } },
    ]));
    const applied = await succeed(stateql.apply(appliedPlan.plan_id));
    assert.equal(applied.affected_rows, 2);
    assert.deepEqual(external.prepare("SELECT value FROM batch_items ORDER BY id").all().map((row) => row.value), ["first", "second", "three"]);
    assert.equal((await stateql.apply(appliedPlan.plan_id)).ok, false);
    const duplicate = await stateql.planTableUpdates([
      { row_token: freshPage.row_tokens[0]!, changes: { set: { value: "x" } } },
      { row_token: freshPage.row_tokens[0]!, changes: { set: { value: "y" } } },
    ]);
    assert.equal(duplicate.ok, false);
  } finally { external.close(); stateql.close(); }
});


for (const driver of ["postgres", "mysql"] as const) {
  const url = process.env[driver === "postgres" ? "STQL_POSTGRES_URL" : "STQL_MYSQL_URL"];
  test(`conditional ${driver} edits detect external changes and roll back cardinality failures`, { skip: !url }, async () => {
    const home = createTemporaryDirectory();
    const stateql = new StateQL({ home });
    const name = "panel_" + Date.now();
    let adapter: Awaited<ReturnType<typeof createAdapter>> | undefined;
    let store: StateStore | undefined;
    try {
      await succeed(stateql.connect(undefined, { secretEnv: driver === "postgres" ? "STQL_POSTGRES_URL" : "STQL_MYSQL_URL", readOnly: false }));
      await succeed(stateql.exec(`CREATE TABLE ${name} (a INTEGER, b INTEGER, value VARCHAR(100), PRIMARY KEY(a,b))`));
      await succeed(stateql.exec(`INSERT INTO ${name} VALUES (1,1,'before'),(1,2,'second')`));
      store = new StateStore(home, () => new Date());
      const connection = store.activeConnection(store.ensureSession("default"))!;
      adapter = await createAdapter(connection, createAdapterContext(10000), { source: url! });
      const table = await succeed(stateql.readTable({ name }));
      const page = stateql.readMaterialized(table.result_id);
      const index = page.rows.findIndex(row => row.b === 1);
      assert.ok(page.row_tokens[index], JSON.stringify(page));
      const plan = await succeed(stateql.planTableUpdate(page.row_tokens[index]!, { set: { value: "after" } }));
      await succeed(stateql.apply(plan.plan_id));
      assert.equal((await stateql.apply(plan.plan_id)).ok, false);
      const fresh = await succeed(stateql.readTable({ name }, 1000, { cache: "bypass" }));
      const freshPage = stateql.readMaterialized(fresh.result_id);
      const token = freshPage.row_tokens[freshPage.rows.findIndex(row => row.b === 1)]!;
      await adapter.write(`UPDATE ${name} SET value = 'external' WHERE b = 1`, []);
      const stale = await succeed(stateql.planTableUpdate(token, { set: { value: "bad" } }));
      const response = await stateql.apply(stale.plan_id);
      assert.equal(response.ok, false);
      if (!response.ok) assert.equal(response.error.code, "ROW_CONFLICT");
      await assert.rejects(adapter.write(`UPDATE ${name} SET value = 'bad'`, [], 1), /ROW_CONFLICT/);
      const final = await succeed(stateql.query(`SELECT value FROM ${name} ORDER BY b`, { cache: "bypass" }));
      assert.deepEqual(stateql.readMaterialized(final.result_id).rows.map(row => row.value), ["external", "second"]);
    } finally {
      if (adapter) { await adapter.write(`DROP TABLE IF EXISTS ${name}`, []); await adapter.close(); }
      store?.close();
      stateql.close();
    }
  });
}

test("conditional MongoDB edits preserve BSON identity, detect conflicts, and permit no-op matches", { skip: !process.env.STQL_MONGODB_URL }, async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  const client = await MongoClient.connect(process.env.STQL_MONGODB_URL!);
  const collection = client.db().collection("panel_" + Date.now());
  try {
    const id = new BSON.ObjectId();
    await collection.insertOne({ _id: id, value: "before", count: new BSON.Long("9007199254740993") });
    await succeed(stateql.connect(process.env.STQL_MONGODB_URL!, { readOnly: false }));
    const table = await succeed(stateql.readTable({ name: collection.collectionName }));
    const token = stateql.readMaterialized(table.result_id).row_tokens[0]!;
    assert.ok(token);
    const plan = await succeed(stateql.planTableUpdate(token, { set: { value: "after" } }));
    await succeed(stateql.apply(plan.plan_id));
    assert.equal((await collection.findOne({ _id: id }))!.value, "after");
    assert.equal((await stateql.apply(plan.plan_id)).ok, false);
    const fresh = await succeed(stateql.readTable({ name: collection.collectionName }, 1000, { cache: "bypass" }));
    const nextToken = stateql.readMaterialized(fresh.result_id).row_tokens[0]!;
    const noOp = await succeed(stateql.planTableUpdate(nextToken, { set: { value: "after" } }));
    await succeed(stateql.apply(noOp.plan_id));
    const afterNoOp = await succeed(stateql.readTable({ name: collection.collectionName }, 1000, { cache: "bypass" }));
    const staleToken = stateql.readMaterialized(afterNoOp.result_id).row_tokens[0]!;
    await collection.updateOne({ _id: id }, { $set: { count: new BSON.Decimal128("9007199254740993") } });
    const stale = await succeed(stateql.planTableUpdate(staleToken, { set: { value: "bad" } }));
    const response = await stateql.apply(stale.plan_id);
    assert.equal(response.ok, false);
    if (!response.ok) assert.equal(response.error.code, "ROW_CONFLICT");
    assert.equal((await collection.findOne({ _id: id }))!.value, "after");
  } finally { await collection.drop(); await client.close(); stateql.close(); }
});

test("inspection history retains the requested table across restart, including failed inspections", async () => {
  const { stateql, home } = await createFixture();
  await succeed(stateql.exec("CREATE TABLE history_target (id INTEGER PRIMARY KEY)"));
  await succeed(stateql.inspect("columns", "history_target"));
  await stateql.inspect("columns", "missing_table");
  stateql.close();
  const reopened = new StateQL({ home });
  try {
    const history = reopened.snapshot({ historyLimit: 20 }).history;
    assert.equal(history.find(entry => entry.command === "inspect.columns" && entry.success)?.target, "history_target");
    assert.equal(history.find(entry => entry.command === "inspect.columns" && !entry.success)?.target, "missing_table");
    assert.equal(history.find(entry => entry.command === "exec")?.target, undefined);
  } finally { reopened.close(); }
});

test("raw result access rejects another actor and expiry", async () => {
  let now = new Date("2026-09-01T00:00:00Z");
  const { stateql, home } = await createFixture(() => now);
  const other = StateQL.forActor({ home, actor: "other", now: () => now });
  try {
    const result = await succeed(stateql.query("SELECT 1 AS value"));
    assert.throws(() => other.readMaterialized(result.result_id), /not found/);
    now = new Date("2026-09-03T00:00:00Z");
    assert.throws(() => stateql.readMaterialized(result.result_id), /expired/);
  } finally { other.close(); stateql.close(); }
});
