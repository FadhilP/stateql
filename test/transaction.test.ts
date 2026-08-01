import assert from "node:assert/strict";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { StateQL } from "../src/stateql.js";
import { StateStore } from "../src/store.js";
import {
  assertFailure,
  assertOutcomeUnknown,
  createFixture,
  succeed,
} from "./helpers.js";

test("transactions survive staging, commit atomically, and rollback clears duplicates", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)",
    ),
  );

  const rolledBack = await succeed(fixture.stateql.beginTransaction());
  const pending = await succeed(
    fixture.stateql.exec("INSERT INTO events (label) VALUES (?)", {
      params: ["discard"],
    }),
  );
  assert.equal(pending.status, "pending");
  await succeed(
    fixture.stateql.rollbackTransaction(String(rolledBack.transaction_id)),
  );

  await succeed(
    fixture.stateql.exec("INSERT INTO events (label) VALUES (?)", {
      params: ["discard"],
    }),
  );

  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec("INSERT INTO events (label) VALUES (?)", {
      params: ["keep"],
    }),
  );
  assertFailure(
    await fixture.stateql.query("SELECT * FROM events"),
    "TRANSACTION_FAILED",
  );
  assertFailure(
    await fixture.stateql.inspect("schema"),
    "TRANSACTION_FAILED",
  );
  assertFailure(
    await fixture.stateql.plan("INSERT INTO events (label) VALUES ('later')"),
    "TRANSACTION_FAILED",
  );
  const committed = await succeed(
    fixture.stateql.commitTransaction(String(transaction.transaction_id)),
  );
  assert.equal(committed.state, "committed");
  assert.equal(committed.statements_executed, 1);
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM events ORDER BY id")))
      .rows,
    2,
  );
  fixture.stateql.close();
});


test("transactions stay bound to one connection and enforce isolation", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE items (value TEXT)"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  assertFailure(
    await fixture.stateql.connect(join(fixture.home, "other.sqlite"), {
      readOnly: false,
    }),
    "TRANSACTION_FAILED",
  );
  assertFailure(await fixture.stateql.disconnect(), "TRANSACTION_FAILED");
  await succeed(
    fixture.stateql.rollbackTransaction(String(transaction.transaction_id)),
  );
  assertFailure(
    await fixture.stateql.beginTransaction("read committed"),
    "INVALID_COMMAND",
  );
  fixture.stateql.close();
});


test("timed-out transaction commits retain unknown-outcome protection", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE deadline_batch (value TEXT)"));
  await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec("INSERT INTO deadline_batch (value) VALUES ('maybe')"),
  );

  const blocker = new DatabaseSync(fixture.database);
  try {
    blocker.exec("BEGIN EXCLUSIVE");
    assertOutcomeUnknown(
      await fixture.stateql.commitTransaction(undefined, { timeoutMs: 200 }),
    );
  } finally {
    blocker.exec("ROLLBACK");
    blocker.close();
  }
  fixture.stateql.close();
});


test("transaction statement failures are known rollbacks", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE atomic_rows (id INTEGER PRIMARY KEY)"),
  );
  await succeed(fixture.stateql.exec("INSERT INTO atomic_rows (id) VALUES (1)"));
  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(fixture.stateql.exec("INSERT INTO atomic_rows (id) VALUES (2)"));
  await succeed(
    fixture.stateql.exec("INSERT INTO atomic_rows (id) VALUES (?)", {
      params: [1],
    }),
  );
  assertFailure(
    await fixture.stateql.commitTransaction(String(transaction.transaction_id)),
    "TRANSACTION_FAILED",
  );
  assert.equal(
    (await succeed(fixture.stateql.query("SELECT * FROM atomic_rows"))).rows,
    1,
  );
  fixture.stateql.close();
});

test("corrupt staged transaction parameters fail while the transaction remains active", async () => {
  const fixture = await createFixture();
  await succeed(fixture.stateql.exec("CREATE TABLE staged_rows (id INTEGER)"));
  const transaction = await succeed(fixture.stateql.beginTransaction());
  const operation = await succeed(
    fixture.stateql.exec("INSERT INTO staged_rows (id) VALUES (?)", { params: [1] }),
  );
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  store.db.prepare("UPDATE operations SET parameters = ? WHERE id = ?")
    .run("[] trailing-corruption", operation.operation_id);

  assertFailure(
    await fixture.stateql.commitTransaction(String(transaction.transaction_id)),
    "STATE_CORRUPTED",
  );
  assert.equal(
    (await succeed(fixture.stateql.transactionStatus(String(transaction.transaction_id)))).state,
    "active",
  );
  fixture.stateql.close();
});

test("stale committing transactions recover as unknown", async () => {
  let clock = new Date("2026-07-25T00:00:00.000Z");
  const fixture = await createFixture(() => clock);
  await succeed(
    fixture.stateql.exec("CREATE TABLE recovered_transaction (value TEXT)"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO recovered_transaction (value) VALUES ('pending')",
    ),
  );
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  const transactionRecord = store.getTransaction(String(transaction.transaction_id))!;
  assert.equal(
    store.markTransactionCommitting(
      transactionRecord.id,
      transactionRecord.session_id,
      transactionRecord.owner_actor_id,
    ),
    true,
  );
  fixture.stateql.close();

  clock = new Date(clock.getTime() + 5 * 60_000 + 1);
  const recovered = new StateQL({ home: fixture.home, now: () => clock });
  const status = await succeed(
    recovered.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(status.state, "outcome_unknown");
  assert.equal((await succeed(recovered.status())).transaction, null);
  recovered.close();
});

test("transaction metadata failures preserve an unknown outcome", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec("CREATE TABLE uncertain_transaction (value TEXT)"),
  );
  const transaction = await succeed(fixture.stateql.beginTransaction());
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO uncertain_transaction (value) VALUES ('committed')",
    ),
  );
  const store = (fixture.stateql as unknown as { store: StateStore }).store;
  const commitMetadata = store.commitTransactionMetadata.bind(store);
  store.commitTransactionMetadata = () => {
    throw new Error("injected transaction metadata failure");
  };
  assertOutcomeUnknown(
    await fixture.stateql.commitTransaction(String(transaction.transaction_id)),
  );
  store.commitTransactionMetadata = commitMetadata;

  const status = await succeed(
    fixture.stateql.transactionStatus(String(transaction.transaction_id)),
  );
  assert.equal(status.state, "outcome_unknown");
  assert.equal(
    (
      await succeed(
        fixture.stateql.query("SELECT * FROM uncertain_transaction"),
      )
    ).rows,
    1,
  );
  fixture.stateql.close();
});

