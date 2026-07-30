import assert from "node:assert/strict";
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
    ["affected_rows", "handle", "status", "type"],
  );
  assert.equal(JSON.stringify(snapshot).includes("snapshot-secret"), false);
  assert.equal(JSON.stringify(snapshot).includes("SELECT ? AS value"), false);
  assert.deepEqual(fixture.stateql.snapshot({ historyLimit: 2 }), snapshot);

  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  for (let index = 0; index < 55; index += 1) {
    store.addHistory({
      sessionId: snapshot.session.session_id,
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
      (id, timestamp, session_id, command, executed, cached, success)
    SELECT
      'seed_' || value,
      '2026-01-01T00:00:00Z',
      '${session.id}',
      'seed',
      0,
      0,
      1
    FROM sequence
  `);

  const latest = store.addHistory({
    sessionId: session.id,
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
