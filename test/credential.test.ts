import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import {
  CredentialResolutionError,
  StateQL,
  type CredentialRequest,
} from "../src/index.js";
import {
  assertFailure,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

function credentialFixture() {
  const root = createTemporaryDirectory("stateql-credential-test-");
  const home = join(root, "state");
  const database = join(root, "credentials.sqlite");
  const requests: CredentialRequest[] = [];
  let available = true;
  const stateql = new StateQL({
    home,
    credentialResolver(request) {
      requests.push(structuredClone(request));
      return available ? database : undefined;
    },
  });
  return {
    home,
    database,
    requests,
    stateql,
    setAvailable(value: boolean) { available = value; },
  };
}

test("custom credential resolvers receive bounded operation context and resolve once per adapter operation", async () => {
  const fixture = credentialFixture();
  const { stateql, requests } = fixture;
  try {
    await succeed(stateql.connect(undefined, {
      secretEnv: "APP_DATABASE_URL",
      name: "app",
      readOnly: false,
    }));
    const beforeRejectedWrite = requests.length;
    assertFailure(
      await stateql.exec("DELETE FROM credentials_test"),
      "UNBOUNDED_MUTATION",
    );
    assert.equal(requests.length, beforeRejectedWrite);

    await succeed(stateql.exec("CREATE TABLE credentials_test (id INTEGER PRIMARY KEY, name TEXT)"));
    await succeed(stateql.exec(
      "INSERT INTO credentials_test (id, name) VALUES (?, ?)",
      { params: [1, "Ada"] },
    ));
    await succeed(stateql.query(
      "SELECT id, name FROM credentials_test ORDER BY id LIMIT 10",
    ));
    await succeed(stateql.inspect("columns", "credentials_test"));

    const plan = await succeed(stateql.plan(
      "UPDATE credentials_test SET name = ? WHERE id = ?",
      { params: ["Grace", 1] },
    ));
    await succeed(stateql.apply(String(plan.plan_id)));

    await succeed(stateql.beginTransaction());
    const beforeStaging = requests.length;
    await succeed(stateql.exec(
      "UPDATE credentials_test SET name = ? WHERE id = ?",
      { params: ["Linus", 1] },
    ));
    assert.equal(requests.length, beforeStaging);
    await succeed(stateql.commitTransaction());

    assert.deepEqual(
      requests.map(({ operation, access }) => [operation, access]),
      [
        ["connect", "write"],
        ["exec", "write"],
        ["exec", "write"],
        ["query", "read"],
        ["inspect", "read"],
        ["plan", "read"],
        ["apply", "write"],
        ["transaction.commit", "write"],
      ],
    );
    const initial = requests[0]!;
    assert.equal(initial.reference, "APP_DATABASE_URL");
    assert.equal(initial.actorId, "default");
    assert.equal(initial.session.name, "default");
    assert.equal(initial.requestedReadOnly, false);
    assert.equal(initial.connection, undefined);

    for (const request of requests.slice(1)) {
      assert.equal(request.connection?.name, "app");
      assert.equal(request.connection?.driver, "sqlite");
      assert.equal(request.connection?.database, "credentials.sqlite");
      assert.equal(request.connection?.readOnly, false);
      assert.equal(JSON.stringify(request).includes(fixture.database), false);
    }
  } finally {
    stateql.close();
  }
});

test("custom resolvers fail closed without environment fallback and can retry safely", async () => {
  const fixture = credentialFixture();
  const previous = process.env.APP_DATABASE_URL;
  process.env.APP_DATABASE_URL = fixture.database;
  fixture.setAvailable(false);
  try {
    assertFailure(
      await fixture.stateql.connect(undefined, {
        secretEnv: "APP_DATABASE_URL",
        readOnly: false,
      }),
      "CREDENTIAL_UNAVAILABLE",
    );
    fixture.setAvailable(true);
    await succeed(fixture.stateql.connect(undefined, {
      secretEnv: "APP_DATABASE_URL",
      readOnly: false,
    }));
    await succeed(fixture.stateql.exec("CREATE TABLE retry_test (id INTEGER PRIMARY KEY)"));

    const plan = await succeed(fixture.stateql.plan(
      "UPDATE retry_test SET id = ? WHERE id = ?",
      { params: [2, 1] },
    ));
    fixture.setAvailable(false);
    assertFailure(
      await fixture.stateql.apply(String(plan.plan_id)),
      "CREDENTIAL_UNAVAILABLE",
    );
    fixture.setAvailable(true);
    await succeed(fixture.stateql.apply(String(plan.plan_id)));

    fixture.setAvailable(false);
    assertFailure(
      await fixture.stateql.exec("INSERT INTO retry_test (id) VALUES (?)", {
        params: [1],
        idempotencyKey: "credential-retry",
      }),
      "CREDENTIAL_UNAVAILABLE",
    );
    const failedSnapshot = fixture.stateql.snapshot();
    assert.equal(failedSnapshot.recent_operations[0]?.status, "failed");
    assert.equal(failedSnapshot.recent_operations[0]?.affected_rows, null);
    fixture.setAvailable(true);
    await succeed(fixture.stateql.exec("INSERT INTO retry_test (id) VALUES (?)", {
      params: [1],
      idempotencyKey: "credential-retry",
    }));
    const beforeDuplicate = fixture.requests.length;
    const duplicate = await succeed(fixture.stateql.exec(
      "INSERT INTO retry_test (id) VALUES (?)",
      { params: [1], idempotencyKey: "credential-retry" },
    ));
    assert.equal(duplicate.duplicate, true);
    assert.equal(fixture.requests.length, beforeDuplicate);
    const rows = await succeed(fixture.stateql.query(
      "SELECT id FROM retry_test ORDER BY id LIMIT 10",
    ));
    assert.equal(rows.rows, 1);
  } finally {
    fixture.stateql.close();
    if (previous === undefined) delete process.env.APP_DATABASE_URL;
    else process.env.APP_DATABASE_URL = previous;
  }
});

test("the default resolver remains compatible with process environment credentials", async () => {
  const root = createTemporaryDirectory("stateql-default-credential-test-");
  const home = join(root, "state");
  const database = join(root, "default.sqlite");
  const previous = process.env.STQL_TEST_DATABASE_URL;
  process.env.STQL_TEST_DATABASE_URL = database;
  const stateql = new StateQL({ home });
  try {
    await succeed(stateql.connect(undefined, {
      secretEnv: "STQL_TEST_DATABASE_URL",
      readOnly: false,
    }));
    await succeed(stateql.exec("CREATE TABLE default_resolver_test (id INTEGER PRIMARY KEY)"));
    const rows = await succeed(stateql.query(
      "SELECT id FROM default_resolver_test ORDER BY id LIMIT 10",
    ));
    assert.equal(rows.rows, 0);
  } finally {
    stateql.close();
    if (previous === undefined) delete process.env.STQL_TEST_DATABASE_URL;
    else process.env.STQL_TEST_DATABASE_URL = previous;
  }
});

test("credential failures are controlled, cancellable, and do not expose resolver messages", async () => {
  const root = createTemporaryDirectory("stateql-credential-failure-test-");
  const secret = "postgres://user:hunter2@example.com/private";

  const rejected = new StateQL({
    home: join(root, "rejected"),
    credentialResolver() {
      throw new Error(`resolver leaked ${secret}`);
    },
  });
  const rejectedResponse = await rejected.connect(undefined, {
    secretEnv: "REJECTED_DATABASE_URL",
  });
  assertFailure(rejectedResponse, "CREDENTIAL_RESOLUTION_FAILED");
  assert.equal(JSON.stringify(rejectedResponse).includes(secret), false);
  rejected.close();
  assert.equal(
    readFileSync(join(root, "rejected", "state.sqlite")).includes(secret),
    false,
  );

  const denied = new StateQL({
    home: join(root, "denied"),
    credentialResolver() {
      throw new CredentialResolutionError("denied");
    },
  });
  assertFailure(
    await denied.connect(undefined, { secretEnv: "DENIED_DATABASE_URL" }),
    "PERMISSION_DENIED",
  );
  denied.close();

  let resolverCalled = false;
  const controller = new AbortController();
  controller.abort();
  const cancelled = new StateQL({
    home: join(root, "cancelled"),
    signal: controller.signal,
    credentialResolver() {
      resolverCalled = true;
      return secret;
    },
  });
  assertFailure(
    await cancelled.connect(undefined, { secretEnv: "CANCELLED_DATABASE_URL" }),
    "OPERATION_CANCELLED",
  );
  assert.equal(resolverCalled, false);
  cancelled.close();

  const timedOut = new StateQL({
    home: join(root, "timeout"),
    timeoutMs: 5,
    credentialResolver: () => new Promise(() => undefined),
  });
  assertFailure(
    await timedOut.connect(undefined, { secretEnv: "TIMEOUT_DATABASE_URL" }),
    "DEADLINE_EXCEEDED",
  );
  timedOut.close();
});

test("resolved sources stay out of responses, snapshots, and history", async () => {
  const fixture = credentialFixture();
  const { stateql } = fixture;
  try {
    const connected = await stateql.connect(undefined, {
      secretEnv: "PRIVATE_DATABASE_URL",
      readOnly: false,
    });
    assert.equal(connected.ok, true);
    await succeed(stateql.exec("CREATE TABLE leak_test (id INTEGER PRIMARY KEY)"));
    await succeed(stateql.query("SELECT id FROM leak_test ORDER BY id LIMIT 10"));

    const visible = JSON.stringify({
      connected,
      snapshot: stateql.snapshot(),
      history: await stateql.history(20),
    });
    assert.equal(visible.includes(fixture.database), false);
  } finally {
    stateql.close();
  }
});
