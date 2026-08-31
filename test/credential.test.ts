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
  collect,
  succeed,
} from "./helpers.js";

function credentialFixture() {
  const root = createTemporaryDirectory("stateql-credential-test-");
  const home = join(root, "state");
  const database = join(root, "credentials.sqlite");
  const requests: CredentialRequest[] = [];
  let available = true;
  let source = `sqlite:${database}`;
  const stateql = new StateQL({
    home,
    credentialResolver(request) {
      requests.push(structuredClone(request));
      return available ? source : undefined;
    },
  });
  return {
    home,
    database,
    requests,
    stateql,
    setAvailable(value: boolean) { available = value; },
    setSource(value: string) { source = value; },
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
  process.env.STQL_TEST_DATABASE_URL = `sqlite:${database}`;
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

test("opaque credential refs require a trusted resolver and flow through profiles and batch commands", async () => {
  const root = createTemporaryDirectory("stateql-opaque-credential-test-");
  const home = join(root, "state");
  const database = join(root, "opaque.sqlite");
  const envReference = "STQL_OPAQUE_CREDENTIAL_REF";
  const previous = process.env[envReference];
  process.env[envReference] = `sqlite:${database}`;

  const withoutResolver = new StateQL({ home });
  try {
    assertFailure(
      await withoutResolver.connect(undefined, { credentialRef: envReference }),
      "CREDENTIAL_UNAVAILABLE",
    );
  } finally {
    withoutResolver.close();
  }

  const reference = "  vault://team/app database  ";
  const requests: CredentialRequest[] = [];
  const stateql = new StateQL({
    home,
    credentialResolver(request) {
      requests.push(structuredClone(request));
      return `sqlite:${database}`;
    },
  });
  try {
    const responses = await collect(stateql.batch([
      {
        command: "profile.add",
        name: "hosted",
        credential_ref: reference,
        read_only: false,
      },
      { command: "connect", profile: "hosted" },
    ]));
    assert.equal(responses.length, 2);
    assert.ok(responses.every((response) => response.ok));

    const profile = await succeed(stateql.showProfile("hosted"));
    assert.equal(profile.target, null);
    assert.equal(profile.secret_env, null);
    assert.equal(profile.credential_ref, reference);
    assert.equal(JSON.stringify(profile).includes(database), false);

    await succeed(stateql.exec("CREATE TABLE opaque_ref_test (id INTEGER PRIMARY KEY)"));
    assert.deepEqual(requests.map((request) => request.source), [
      "credential_ref",
      "credential_ref",
    ]);
    assert.ok(requests.every((request) => request.reference === reference));
    assert.ok(requests.every((request) => !("origin" in request)));
  } finally {
    stateql.close();
    if (previous === undefined) delete process.env[envReference];
    else process.env[envReference] = previous;
  }
});

test("opaque credential refs are bounded but otherwise preserve their syntax", async () => {
  const root = createTemporaryDirectory("stateql-credential-ref-validation-test-");
  const stateql = new StateQL({ home: join(root, "state") });
  try {
    assertFailure(
      await stateql.addProfile("blank", undefined, { credentialRef: " \t " }),
      "INVALID_COMMAND",
    );
    assertFailure(
      await stateql.addProfile("long", undefined, { credentialRef: "x".repeat(1_025) }),
      "INVALID_COMMAND",
    );
    const reference = `not an ENV name/${"x".repeat(1_008)}`;
    assert.equal(reference.length, 1_024);
    const profile = await succeed(
      stateql.addProfile("bounded", undefined, { credentialRef: reference }),
    );
    assert.equal(profile.credential_ref, reference);
  } finally {
    stateql.close();
  }
});

test("connect rejects ambiguous sources before resolving credentials", async () => {
  const root = createTemporaryDirectory("stateql-credential-source-test-");
  let resolverCalled = false;
  const stateql = new StateQL({
    home: join(root, "state"),
    credentialResolver() {
      resolverCalled = true;
      return "postgres://localhost/app";
    },
  });
  try {
    assertFailure(
      await stateql.connect("postgres://localhost/app", {
        secretEnv: "APP_DATABASE_URL",
      }),
      "INVALID_COMMAND",
    );
    assertFailure(
      await stateql.connect(undefined, {
        profile: "app",
        secretEnv: "APP_DATABASE_URL",
      }),
      "INVALID_COMMAND",
    );
    assertFailure(
      await stateql.connect("postgres://localhost/app", {
        credentialRef: "vault://app",
      }),
      "INVALID_COMMAND",
    );
    assertFailure(
      await stateql.connect(undefined, {
        secretEnv: "APP_DATABASE_URL",
        credentialRef: "vault://app",
      }),
      "INVALID_COMMAND",
    );
    assertFailure(
      await stateql.connect(undefined, {
        profile: "app",
        credentialRef: "vault://app",
      }),
      "INVALID_COMMAND",
    );
    assertFailure(
      await stateql.addProfile("ambiguous", "./app.sqlite", {
        credentialRef: "vault://app",
      }),
      "INVALID_COMMAND",
    );
    assert.equal(resolverCalled, false);
  } finally {
    stateql.close();
  }
});

test("credential sources require an explicit driver and retain stored-driver identity", async () => {
  const fixture = credentialFixture();
  try {
    fixture.setSource("password-only");
    const invalid = await fixture.stateql.connect(undefined, {
      secretEnv: "APP_DATABASE_URL",
      readOnly: false,
    });
    assertFailure(invalid, "INVALID_COMMAND");
    if (!invalid.ok) {
      assert.equal(
        invalid.error.message,
        "Secret environment variable must contain a complete PostgreSQL/MySQL URL or an explicit sqlite: source; MongoDB URLs are also supported.",
      );
      assert.equal(JSON.stringify(invalid).includes("password-only"), false);
      assert.equal(
        readFileSync(join(fixture.home, "state.sqlite")).includes("password-only"),
        false,
      );
    }

    fixture.setSource("postgres://");
    assertFailure(
      await fixture.stateql.connect(undefined, {
        secretEnv: "APP_DATABASE_URL",
        readOnly: false,
      }),
      "INVALID_COMMAND",
    );

    fixture.setSource("sqlite://unsupported/path");
    assertFailure(
      await fixture.stateql.connect(undefined, {
        secretEnv: "APP_DATABASE_URL",
        readOnly: false,
      }),
      "UNSUPPORTED_DRIVER",
    );

    fixture.setSource(`sqlite:${fixture.database}`);
    await succeed(fixture.stateql.connect(undefined, {
      secretEnv: "APP_DATABASE_URL",
      readOnly: false,
    }));
    await succeed(fixture.stateql.exec("CREATE TABLE driver_test (id INTEGER PRIMARY KEY)"));
    fixture.setSource("postgres://localhost/app");
    const mismatch = await fixture.stateql.query(
      "SELECT id FROM driver_test ORDER BY id LIMIT 1",
    );
    assertFailure(mismatch, "INVALID_COMMAND");
    if (!mismatch.ok) assert.match(mismatch.error.message, /driver does not match/);
  } finally {
    fixture.stateql.close();
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
