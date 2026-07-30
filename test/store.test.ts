import assert from "node:assert/strict";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  StateQL,
  type HistoryEntry,
  type StateQLSnapshot,
} from "../src/index.js";
import { StateStore } from "../src/store.js";
import {
  assertFailure,
  createFixture,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

test("state persists across restarts, expires, and detects external SQLite writes", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE values_table (id INTEGER PRIMARY KEY, value TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO values_table (value) VALUES ('one')"),
  );
  const initial = await succeed(
    fixture.stateql.query("SELECT * FROM values_table ORDER BY id"),
  );
  fixture.stateql.close();

  const restarted = new StateQL({
    home: fixture.home,
    now: () => clock,
    cacheTtlSeconds: 300,
  });
  assert.equal(
    (await succeed(restarted.show(String(initial.result_id)))).result_id,
    initial.result_id,
  );

  const external = new DatabaseSync(fixture.database);
  external
    .prepare("INSERT INTO values_table (value) VALUES ('external')")
    .run();
  external.close();
  const refreshed = await succeed(
    restarted.query("SELECT * FROM values_table ORDER BY id"),
  );
  assert.notEqual(refreshed.result_id, initial.result_id);
  assert.equal(refreshed.rows, 2);

  clock = new Date(clock.getTime() + 86_401_000);
  assertFailure(
    await restarted.show(String(refreshed.result_id)),
    "RESULT_EXPIRED",
  );
  assertFailure(
    await restarted.filter(String(refreshed.result_id), "value IS NOT NULL"),
    "RESULT_EXPIRED",
  );
  restarted.close();
});

test("session names do not collide with another session ID", async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  const initial = await succeed(stateql.status());
  const named = await succeed(
    stateql.startSession(String(initial.session_id)),
  );
  assert.notEqual(named.session_id, initial.session_id);
  assert.equal(named.name, initial.session_id);
  stateql.close();
});

test("closed sessions reactivate without breaking later startup", async () => {
  const root = createTemporaryDirectory();
  const stateql = new StateQL({ home: root });
  const initial = await succeed(stateql.status());
  await succeed(stateql.closeSession());
  stateql.close();

  const reopened = new StateQL({ home: root });
  assert.equal(
    (await succeed(reopened.status())).session_id,
    initial.session_id,
  );
  reopened.close();
});

test("reactivation preserves an unresolved transaction lease", async () => {
  const fixture = await createFixture();
  const transaction = await succeed(fixture.stateql.beginTransaction());
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db
    .prepare("UPDATE sessions SET status = 'closed' WHERE active_transaction_id = ?")
    .run(String(transaction.transaction_id));
  fixture.stateql.close();

  const reopened = new StateQL({ home: fixture.home });
  assert.equal(
    (await succeed(reopened.transactionStatus(String(transaction.transaction_id))))
      .state,
    "active",
  );
  await succeed(reopened.rollbackTransaction(String(transaction.transaction_id)));
  reopened.close();
});

test("startup deletes expired results, aliases, and plans", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  await succeed(fixture.stateql.exec("CREATE TABLE cleanup_rows (id INTEGER)"));
  const result = await succeed(
    fixture.stateql.query("SELECT id FROM cleanup_rows"),
  );
  await succeed(
    fixture.stateql.setAlias("cleanup", String(result.result_id)),
  );
  await succeed(
    fixture.stateql.plan("INSERT INTO cleanup_rows (id) VALUES (1)"),
  );
  fixture.stateql.close();

  clock = new Date(clock.getTime() + 86_401_000);
  const reopened = new StateQL({ home: fixture.home, now: () => clock });
  const store = (reopened as unknown as { store: StateStore }).store;
  for (const table of ["results", "aliases", "plans"]) {
    const row = store.db
      .prepare(`SELECT COUNT(*) AS count FROM ${table}`)
      .get() as { count: number };
    assert.equal(row.count, 0, table);
  }
  reopened.close();
});

test("snapshot is typed, bounded, safe, and does not record history", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE private_values (value TEXT)"),
  );
  await succeed(
    fixture.stateql.query("SELECT ? AS value", {
      params: ["snapshot-secret"],
      cache: "bypass",
    }),
  );
  await succeed(fixture.stateql.beginTransaction());

  const snapshot: StateQLSnapshot = fixture.stateql.snapshot({ historyLimit: 2 });
  const historyEntry: HistoryEntry | undefined = snapshot.history[0];

  assert.equal(snapshot.session.status, "active");
  assert.equal(snapshot.connection?.status, "connected");
  assert.equal(snapshot.transaction?.state, "active");
  assert.equal(snapshot.state_confidence, "database_reported");
  assert.ok(snapshot.state_version);
  assert.ok(snapshot.recent_results.length <= 10);
  assert.ok(snapshot.recent_operations.length <= 10);
  assert.equal(snapshot.history.length, 2);
  assert.ok(historyEntry);
  assert.deepEqual(
    Object.keys(snapshot.recent_results[0] ?? {}).sort(),
    ["alias", "handle", "rows"],
  );
  assert.deepEqual(
    Object.keys(snapshot.recent_operations[0] ?? {}).sort(),
    ["actor_id", "affected_rows", "handle", "status", "type"],
  );
  assert.equal(JSON.stringify(snapshot).includes("snapshot-secret"), false);
  assert.equal(JSON.stringify(snapshot).includes("SELECT ? AS value"), false);
  assert.deepEqual(fixture.stateql.snapshot({ historyLimit: 2 }), snapshot);

  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  for (let index = 0; index < 55; index += 1) {
    store.addHistory({
      sessionId: snapshot.session.session_id,
      actorId: snapshot.actor_id,
      command: "seed",
      executed: false,
      cached: false,
      success: true,
    });
  }
  assert.equal(fixture.stateql.snapshot().history.length, 50);
  assert.throws(() => fixture.stateql.snapshot({ historyLimit: 0 }), /positive integer/);
  assert.throws(() => fixture.stateql.snapshot({ historyLimit: 1.5 }), /positive integer/);
  assert.throws(() => fixture.stateql.snapshot({ historyLimit: 101 }), /cannot exceed 100/);

  const before = await fixture.stateql.status();
  fixture.stateql.snapshot();
  const after = await fixture.stateql.status();
  assert.equal(commandNumber(after.command_id), commandNumber(before.command_id) + 1);

  await succeed(fixture.stateql.rollbackTransaction());
  fixture.stateql.close();
});

function commandNumber(commandId: string): number {
  return Number(commandId.slice(commandId.indexOf("_") + 1));
}

test("history keeps the latest 10,000 entries per session", () => {
  const root = createTemporaryDirectory();
  const store = new StateStore(root, () => new Date("2026-01-01T00:00:00Z"));
  const session = store.ensureSession();

  store.db.exec(`
    WITH RECURSIVE sequence(value) AS (
      SELECT 1
      UNION ALL
      SELECT value + 1 FROM sequence WHERE value < 10001
    )
    INSERT INTO history
      (id, timestamp, session_id, actor_id, command, executed, cached, success)
    SELECT
      'seed_' || value,
      '2026-01-01T00:00:00Z',
      '${session.id}',
      'default',
      'seed',
      0,
      0,
      1
    FROM sequence
  `);

  const latest = store.addHistory({
    sessionId: session.id,
    actorId: "default",
    command: "latest",
    executed: false,
    cached: false,
    success: true,
  });

  assert.equal(store.history(session.id, 20_000).length, 10_000);
  assert.equal(store.history(session.id, 1)[0]?.id, latest.id);
  const removed = store.db
    .prepare("SELECT COUNT(*) AS count FROM history WHERE id IN (?, ?)")
    .get("seed_1", "seed_2") as { count: number };
  assert.equal(removed.count, 0);
  store.close();
});

test("actors share workspace handles, aliases, history, and restarts", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "shared.sqlite");
  const owner = new StateQL({ home, session: "workspace" });
  await succeed(owner.connect(database, { readOnly: false }));
  await succeed(owner.exec("CREATE TABLE shared_rows (value TEXT)"));
  const result = await succeed(owner.query("SELECT * FROM shared_rows"));
  await succeed(owner.setAlias("shared", String(result.result_id)));

  const stranger = new StateQL({
    home,
    session: "workspace",
    actor: "actor-b",
  });
  assertFailure(await stranger.status(), "PERMISSION_DENIED");
  await succeed(owner.linkActor("workspace", "actor-b"));

  assert.equal(
    (await succeed(stranger.show(String(result.result_id)))).result_id,
    result.result_id,
  );
  assert.equal(
    (await succeed(stranger.show("shared"))).result_id,
    result.result_id,
  );
  const operation = await succeed(
    stranger.exec("INSERT INTO shared_rows (value) VALUES ('actor-b')"),
  );
  assert.equal(operation.actor_id, "actor-b");
  assert.equal(
    (await succeed(stranger.receipt(String(operation.operation_id)))).actor_id,
    "actor-b",
  );
  assert.equal((await succeed(owner.query("SELECT * FROM shared_rows"))).rows, 1);

  const actors = await succeed(owner.listActors("workspace"));
  assert.deepEqual(
    actors.actors.map((actor: { actor_id: string }) => actor.actor_id).sort(),
    ["actor-b", "workspace"],
  );
  const resolved = await succeed(owner.resolveActor("actor-b"));
  assert.equal(resolved.session.name, "workspace");
  const history = await succeed(owner.history(50));
  assert.ok(
    history.history.some(
      (entry: HistoryEntry) =>
        entry.actor_id === "actor-b" && entry.command === "exec",
    ),
  );
  assert.equal(stranger.snapshot().actor_id, "actor-b");

  stranger.close();
  const restarted = new StateQL({
    home,
    session: "workspace",
    actor: "actor-b",
  });
  assert.equal(
    (await succeed(restarted.show("shared"))).result_id,
    result.result_id,
  );
  restarted.close();
  owner.close();
});

test("plans and transactions remain actor-owned across concurrent clients", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "owned.sqlite");
  const owner = new StateQL({ home, session: "workspace" });
  await succeed(owner.connect(database, { readOnly: false }));
  await succeed(owner.exec("CREATE TABLE owned_rows (value TEXT)"));
  await succeed(owner.linkActor("workspace", "actor-b"));
  const actor = new StateQL({ home, session: "workspace", actor: "actor-b" });

  const plan = await succeed(
    owner.plan("INSERT INTO owned_rows (value) VALUES ('planned')"),
  );
  assertFailure(await actor.apply(String(plan.plan_id)), "PERMISSION_DENIED");

  const transaction = await succeed(owner.beginTransaction());
  const inspected = await succeed(
    actor.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(inspected.owner_actor_id, "workspace");
  assertFailure(
    await actor.exec("INSERT INTO owned_rows (value) VALUES ('blocked')"),
    "PERMISSION_DENIED",
  );
  assertFailure(
    await actor.commitTransaction(String(transaction.transaction_id)),
    "PERMISSION_DENIED",
  );
  assertFailure(
    await actor.rollbackTransaction(String(transaction.transaction_id)),
    "PERMISSION_DENIED",
  );
  assertFailure(
    await actor.unlinkActor("workspace", "workspace"),
    "TRANSACTION_FAILED",
  );
  await succeed(owner.rollbackTransaction(String(transaction.transaction_id)));
  await succeed(owner.apply(String(plan.plan_id)));

  const acquisitions = await Promise.all([
    owner.beginTransaction(),
    actor.beginTransaction(),
  ]);
  assert.equal(acquisitions.filter((response) => response.ok).length, 1);
  const winner = acquisitions[0]!.ok ? owner : actor;
  await succeed(winner.rollbackTransaction());

  const concurrentPlan = await succeed(
    owner.plan("INSERT INTO owned_rows (value) VALUES ('once')"),
  );
  const ownerClone = new StateQL({
    home,
    session: "workspace",
    actor: "workspace",
  });
  const applications = await Promise.all([
    owner.apply(String(concurrentPlan.plan_id)),
    ownerClone.apply(String(concurrentPlan.plan_id)),
  ]);
  assert.equal(applications.filter((response) => response.ok).length, 1);

  const connectionRace = await Promise.all([
    actor.connect(join(root, "replacement.sqlite"), { readOnly: false }),
    owner.beginTransaction(),
  ]);
  assert.equal(connectionRace.filter((response) => response.ok).length, 1);
  if (connectionRace[1]!.ok) await succeed(owner.rollbackTransaction());

  ownerClone.close();
  actor.close();
  owner.close();
});

test("legacy sessions migrate actor attribution without losing artifacts", async () => {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "legacy.sqlite");
  const legacy = new StateQL({ home });
  await succeed(legacy.connect(database, { readOnly: false }));
  const operation = await succeed(
    legacy.exec("CREATE TABLE legacy_rows (value TEXT)"),
  );
  const result = await succeed(legacy.query("SELECT * FROM legacy_rows"));
  await succeed(legacy.setAlias("legacy", String(result.result_id)));
  const plan = await succeed(
    legacy.plan("INSERT INTO legacy_rows (value) VALUES ('planned')"),
  );
  const transaction = await succeed(legacy.beginTransaction());
  await succeed(
    legacy.exec("INSERT INTO legacy_rows (value) VALUES ('staged')"),
  );
  legacy.close();

  const state = new DatabaseSync(join(home, "state.sqlite"));
  state.exec(`
    DROP TABLE session_members;
    DROP TABLE schema_migrations;
    ALTER TABLE operations DROP COLUMN actor_id;
    ALTER TABLE transactions DROP COLUMN owner_actor_id;
    ALTER TABLE plans DROP COLUMN owner_actor_id;
    ALTER TABLE plans DROP COLUMN claim_token;
    ALTER TABLE history DROP COLUMN actor_id;
  `);
  state.close();

  const migrated = new StateQL({ home });
  assert.equal(
    (await succeed(migrated.show("legacy"))).result_id,
    result.result_id,
  );
  assert.equal(
    (await succeed(migrated.receipt(String(operation.operation_id)))).actor_id,
    "default",
  );
  const transactionStatus = await succeed(
    migrated.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(transactionStatus.owner_actor_id, "default");
  await succeed(
    migrated.rollbackTransaction(String(transaction.transaction_id)),
  );
  await succeed(migrated.apply(String(plan.plan_id)));
  assert.ok(
    (await succeed(migrated.history(100))).history.every(
      (entry: HistoryEntry) => entry.actor_id === "default",
    ),
  );
  migrated.close();

  const reopened = new StateQL({ home });
  assert.equal((await succeed(reopened.query("SELECT * FROM legacy_rows"))).rows, 1);
  reopened.close();
});
