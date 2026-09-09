import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type AddressInfo, type Server, type Socket } from "node:net";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import {
  CredentialResolutionError,
  StateQL,
  type CredentialRequest,
} from "../src/index.js";
import { injectPassword, validatePasswordReferenceTarget } from "../src/connection.js";
import { StateStore } from "../src/store.js";
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

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

interface FakeRedisServer {
  server: Server;
  sockets: Set<Socket>;
  commands: string[][];
  url: string;
  close(): Promise<void>;
}

function parseRespCommand(buffer: Buffer): { args: string[]; bytes: number } | undefined {
  const lineEnd = buffer.indexOf("\r\n");
  if (lineEnd < 0 || buffer[0] !== 42) return undefined;
  const count = Number(buffer.subarray(1, lineEnd).toString());
  if (!Number.isInteger(count) || count < 0) return undefined;
  const args: string[] = [];
  let offset = lineEnd + 2;
  for (let index = 0; index < count; index++) {
    const lengthEnd = buffer.indexOf("\r\n", offset);
    if (lengthEnd < 0 || buffer[offset] !== 36) return undefined;
    const length = Number(buffer.subarray(offset + 1, lengthEnd).toString());
    const valueStart = lengthEnd + 2;
    const valueEnd = valueStart + length;
    if (!Number.isInteger(length) || length < 0 || buffer.length < valueEnd + 2) return undefined;
    args.push(buffer.subarray(valueStart, valueEnd).toString());
    offset = valueEnd + 2;
  }
  return { args, bytes: offset };
}

async function fakeRedisServer(failure?: string): Promise<FakeRedisServer> {
  const sockets = new Set<Socket>();
  const commands: string[][] = [];
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      while (true) {
        const parsed = parseRespCommand(pending);
        if (!parsed) break;
        pending = pending.subarray(parsed.bytes);
        commands.push(parsed.args);
        const command = parsed.args[0]?.toUpperCase();
        socket.write(
          failure ? `-ERR ${failure}\r\n`
            : command === "PING" ? "+PONG\r\n"
            : command === "GET" ? "$-1\r\n"
            : "+OK\r\n",
        );
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address() as AddressInfo;
  return {
    server,
    sockets,
    commands,
    url: `redis://127.0.0.1:${address.port}/0`,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
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
        "Secret environment variable must contain a complete PostgreSQL/MySQL/Redis URL or an explicit sqlite: source; MongoDB URLs are also supported.",
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

  let observedAbort = false;
  const activeController = new AbortController();
  const activelyCancelled = new StateQL({
    home: join(root, "actively-cancelled"),
    signal: activeController.signal,
    credentialResolver(request) {
      return new Promise(() => {
        request.signal?.addEventListener("abort", () => {
          observedAbort = true;
        }, { once: true });
      });
    },
  });
  const pending = activelyCancelled.connect(undefined, {
    secretEnv: "ACTIVELY_CANCELLED_DATABASE_URL",
  });
  setTimeout(() => activeController.abort(), 10);
  assertFailure(await pending, "OPERATION_CANCELLED");
  assert.equal(observedAbort, true);
  activelyCancelled.close();

  const timedOut = new StateQL({
    home: join(root, "timeout"),
    timeoutMs: 1_000,
    credentialTimeoutMs: 5,
    credentialResolver: () => new Promise(() => undefined),
  });
  assertFailure(
    await timedOut.connect(undefined, { secretEnv: "TIMEOUT_DATABASE_URL" }),
    "DEADLINE_EXCEEDED",
  );
  timedOut.close();
});

test("credential resolution has a separate deadline before the database deadline starts", async () => {
  const root = createTemporaryDirectory("stateql-credential-deadline-test-");
  const database = join(root, "deadline.sqlite");
  const stateql = new StateQL({
    home: join(root, "state"),
    credentialTimeoutMs: 500,
    credentialResolver: async (request) => {
      if (request.operation === "query") await delay(150);
      return `sqlite:${database}`;
    },
  });
  const slowSql = `
    WITH RECURSIVE count_up(value) AS (
      SELECT 0
      UNION ALL
      SELECT value + 1 FROM count_up WHERE value < 1000000000
    )
    SELECT sum(value) AS total FROM count_up
  `;
  try {
    await succeed(stateql.connect(undefined, {
      secretEnv: "DEADLINE_DATABASE_URL",
      readOnly: false,
      timeoutMs: 1_000,
    }));
    const started = Date.now();
    const response = await stateql.query(slowSql, {
      cache: "bypass",
      timeoutMs: 100,
    });
    assert.equal(response.ok, false);
    if (!response.ok) {
      assert.equal(response.error.code, "DEADLINE_EXCEEDED");
      assert.equal(response.error.executed, true);
    }
    assert.ok(Date.now() - started >= 200);
  } finally {
    stateql.close();
  }
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


test("password references inject only password bytes and preserve remote target fidelity", () => {
  const password = "p:@/% ü";
  const encoded = "p%3A%40%2F%25%20%C3%BC";
  const targets = [
    "postgresql://user@[::1]:5432/app?sslmode=verify-full&sslrootcert=%2Ftmp%2Fca.pem&application_name=a%20b",
    "mysql://user@db.example/app?ssl=%7B%22ca%22%3A%22cert.pem%22%7D",
    "mongodb://user@db-a.example:27017,db-b.example:27018/app?tls=true&tlsCAFile=%2Ftmp%2Fca.pem",
    "rediss://cache.example/2?keepAlive=1",
  ];
  for (const target of targets) {
    const { source } = injectPassword(target, password);
    const authorityStart = target.indexOf("//") + 2;
    const relativeAuthorityEnd = target.slice(authorityStart).search(/[/?#]/u);
    const authorityEnd = relativeAuthorityEnd < 0 ? -1 : authorityStart + relativeAuthorityEnd;
    const suffix = authorityEnd < 0 ? "" : target.slice(authorityEnd);
    assert.equal(source.endsWith(suffix), true);
    assert.equal(source.includes(`:${encoded}@`), true);
    assert.equal(validatePasswordReferenceTarget(target),
      target.startsWith("postgres") ? "postgres" :
      target.startsWith("mysql") ? "mysql" :
      target.startsWith("mongodb") ? "mongodb" : "redis");
  }
  assert.equal(
    injectPassword("redis://default@cache.example/0?tls=true", "").source,
    "redis://default:@cache.example/0?tls=true",
  );
});

test("password_ref resolves once, persists no password, and reconnects with the same target and reference", async () => {
  const fake = await fakeRedisServer();
  const root = createTemporaryDirectory("stateql-password-ref-test-");
  const home = join(root, "state");
  const reference = "vault://database/password";
  const password = "p:@/% ü";
  const target = fake.url.replace("redis://", "redis://default@") + "?keepAlive=1";
  const requests: CredentialRequest[] = [];
  let stateql = new StateQL({
    home,
    credentialResolver(request) {
      requests.push(structuredClone(request));
      return password;
    },
  });
  try {
    const connected = await stateql.executeCommand({
      command: "connect",
      target,
      password_ref: reference,
      read_only: true,
    });
    assert.equal(connected.ok, true);
    assert.equal(requests.length, 1);
    const initial = requests[0]!;
    assert.equal(initial.source, "password_ref");
    if (initial.source === "password_ref") assert.equal(initial.target, target);
    assert.equal(initial.reference, reference);
    assert.ok(fake.commands.some((command) => command.includes(password)));

    const store = (stateql as unknown as { store: StateStore }).store;
    const connection = store.activeConnection(store.getSessionByName("default")!)!;
    assert.equal(connection.source, target);
    assert.equal(connection.password_ref, reference);
    assert.equal(connection.secret_env, null);
    assert.equal(connection.credential_ref, null);
    const visible = JSON.stringify({ connected, snapshot: stateql.snapshot(), history: await stateql.history(20) });
    assert.equal(visible.includes(password), false);
    assert.equal(readFileSync(join(home, "state.sqlite")).includes(password), false);

    stateql.close();
    const reconnectRequests: CredentialRequest[] = [];
    stateql = new StateQL({
      home,
      credentialResolver(request) {
        reconnectRequests.push(structuredClone(request));
        return password;
      },
    });
    await succeed(stateql.redisQuery({ command: "GET", args: ["missing"] }, { cache: "bypass" }));
    assert.equal(reconnectRequests.length, 1);
    const reconnect = reconnectRequests[0]!;
    assert.equal(reconnect.source, "password_ref");
    if (reconnect.source === "password_ref") assert.equal(reconnect.target, target);
    assert.equal(reconnect.reference, reference);
    assert.equal(reconnect.connection?.driver, "redis");
  } finally {
    stateql.close();
    await fake.close();
  }
});

test("password_ref accepts an explicitly resolved empty password and undefined fails closed", async () => {
  const fake = await fakeRedisServer();
  const root = createTemporaryDirectory("stateql-empty-password-ref-test-");
  const requests: CredentialRequest[] = [];
  const stateql = new StateQL({
    home: join(root, "state"),
    credentialResolver(request) {
      requests.push(structuredClone(request));
      return "";
    },
  });
  try {
    await succeed(stateql.connect(fake.url, { passwordRef: "vault://empty" }));
    assert.equal(requests.length, 1);
    assert.equal(requests[0]?.source, "password_ref");
  } finally {
    stateql.close();
  }
  const unavailable = new StateQL({
    home: join(root, "unavailable"),
    credentialResolver: () => undefined,
  });
  try {
    assertFailure(
      await unavailable.connect(fake.url, { passwordRef: "vault://missing" }),
      "CREDENTIAL_UNAVAILABLE",
    );
  } finally {
    unavailable.close();
    await fake.close();
  }
});

test("password_ref profile updates preserve, clear, and replace adjunct state explicitly", async () => {
  const root = createTemporaryDirectory("stateql-password-profile-test-");
  const stateql = new StateQL({ home: join(root, "state") });
  const firstTarget = "postgres://app@db.example/app?sslmode=verify-full&sslrootcert=%2Fca.pem";
  const nextTarget = "postgres://app@new.example/app?sslmode=verify-full&sslrootcert=%2Fca.pem";
  try {
    const added = await collect(stateql.batch([{
      command: "profile.add",
      name: "app",
      target: firstTarget,
      password_ref: "vault://app/password",
      read_only: true,
    }]));
    assert.equal(added[0]?.ok, true);
    let profile = await succeed(stateql.showProfile("app"));
    assert.equal(profile.password_ref, "vault://app/password");
    assert.equal(profile.target, firstTarget);
    assert.equal("password" in profile, false);

    profile = await succeed(stateql.updateProfile("app", { readOnly: false }));
    assert.equal(profile.password_ref, "vault://app/password");
    profile = await succeed(stateql.updateProfile("app", { target: nextTarget }));
    assert.equal(profile.password_ref, null);
    assert.equal(profile.target, nextTarget);
    profile = await succeed(stateql.updateProfile("app", { passwordRef: "vault://new" }));
    assert.equal(profile.password_ref, "vault://new");
    profile = await succeed(stateql.updateProfile("app", { target: nextTarget }));
    assert.equal(profile.password_ref, "vault://new");
    profile = await succeed(stateql.executeCommand({ command: "profile.update", name: "app", password_ref: null })) as typeof profile;
    assert.equal(profile.password_ref, null);
    profile = await succeed(stateql.updateProfile("app", { passwordRef: "vault://new" }));
    assert.equal(profile.password_ref, "vault://new");
    profile = await succeed(stateql.updateProfile("app", { secretEnv: "APP_DATABASE_URL" }));
    assert.equal(profile.target, null);
    assert.equal(profile.secret_env, "APP_DATABASE_URL");
    assert.equal(profile.password_ref, null);

    assertFailure(
      await stateql.updateProfile("app", { credentialRef: "vault://full-url", passwordRef: "vault://password" }),
      "INVALID_COMMAND",
    );
    const legacy = await succeed(stateql.addProfile("legacy", undefined, { credentialRef: "vault://full-url" }));
    assert.equal(legacy.credential_ref, "vault://full-url");
    assert.equal(legacy.password_ref, null);
  } finally {
    stateql.close();
  }
});

test("invalid password_ref mixes are rejected before resolver and remote driver access", async () => {
  const fake = await fakeRedisServer();
  let resolverCalls = 0;
  const stateql = new StateQL({
    home: createTemporaryDirectory("stateql-password-ref-invalid-test-"),
    credentialResolver() {
      resolverCalls++;
      return "secret";
    },
  });
  try {
    for (const response of [
      await stateql.connect("./local.sqlite", { passwordRef: "vault://password" }),
      await stateql.connect(`${fake.url}?host=other.example`, { passwordRef: "vault://password" }),
      await stateql.connect(fake.url.replace("redis://", "redis://default:embedded@"), { passwordRef: "vault://password" }),
      await stateql.connect(undefined, { secretEnv: "APP_DATABASE_URL", passwordRef: "vault://password" }),
      await stateql.connect(fake.url, { credentialRef: "vault://full", passwordRef: "vault://password" }),
      await stateql.connect("postgres://alice%ZZ@db.example/app", { passwordRef: "vault://password" }),
      await stateql.addProfile("sqlite", "./local.sqlite", { passwordRef: "vault://password" }),
      await stateql.addProfile("query", `${fake.url}?%70assword=embedded`, { passwordRef: "vault://password" }),
    ]) assertFailure(response, response.ok ? "" : response.error.code);
    assert.equal(resolverCalls, 0);
    assert.equal(fake.commands.length, 0);
  } finally {
    stateql.close();
    await fake.close();
  }
});

test("password_ref adapter failures redact decoded and encoded passwords", async () => {
  const password = "hunter2!@";
  const fake = await fakeRedisServer(`authentication rejected ${password}`);
  const home = createTemporaryDirectory("stateql-password-ref-redaction-test-");
  const stateql = new StateQL({
    home,
    credentialResolver: () => password,
  });
  try {
    const response = await stateql.connect(fake.url, { passwordRef: "vault://failure" });
    assertFailure(response, "CONNECTION_FAILED");
    const serialized = JSON.stringify(response);
    assert.equal(serialized.includes(password), false);
    assert.equal(serialized.includes(encodeURIComponent(password)), false);
    assert.equal(readFileSync(join(home, "state.sqlite")).includes(password), false);
  } finally {
    stateql.close();
    await fake.close();
  }
});


test("password reference capability gates setup before legacy runtimes execute it", () => {
  let setupCalls = 0;
  const connectionSetup = (runtime: { passwordReferenceVersion?: number }): void => {
    if ((runtime.passwordReferenceVersion ?? 0) < 1) {
      throw new Error("Installed StateQL does not support password references.");
    }
    setupCalls++;
  };

  assert.throws(() => connectionSetup({}), /does not support password references/);
  assert.equal(setupCalls, 0);
  connectionSetup(StateQL);
  assert.equal(setupCalls, 1);
});
