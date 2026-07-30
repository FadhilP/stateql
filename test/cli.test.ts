import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { StateQL } from "../src/stateql.js";
import type { BatchCommand, Response } from "../src/types.js";
import {
  assertFailure,
  collect,
  createFixture,
  createTemporaryDirectory,
  succeed,
} from "./helpers.js";

test("CLI version matches the published package version", () => {
  const version = spawnSync(process.execPath, ["dist/src/cli.js", "--version"], {
    cwd: process.cwd(),
    encoding: "utf8",
  });
  const packageJson = JSON.parse(readFileSync("package.json", "utf8")) as {
    version: string;
  };
  assert.equal(version.status, 0, version.stderr);
  assert.equal(version.stdout.trim(), packageJson.version);
});

test("local profiles persist and connect by bare or explicit name", async () => {
  const root = createTemporaryDirectory("stateql-profile-test-");
  const home = join(root, "state");
  const database = join(root, "profile.sqlite");
  const stateql = new StateQL({ home });

  const added = await succeed(
    stateql.addProfile("local", database, { readOnly: false }),
  );
  assert.equal(added.profile, "local");
  assert.equal(added.read_only, false);
  assert.equal(JSON.stringify(added).includes("password"), false);
  assertFailure(
    await stateql.addProfile(
      "unsafe",
      "postgres://user:password@example.com/app",
    ),
    "PERMISSION_DENIED",
  );

  const connected = await succeed(stateql.connect("local"));
  assert.equal(connected.profile, "local");
  assert.equal(connected.read_only, false);
  await succeed(stateql.exec("CREATE TABLE profile_test (id INTEGER)"));
  stateql.close();

  const restarted = new StateQL({ home });
  assert.equal(
    (await succeed(restarted.listProfiles())).profiles.length,
    1,
  );
  assert.equal(
    (await succeed(restarted.connect(undefined, { profile: "local" }))).profile,
    "local",
  );
  await succeed(restarted.removeProfile("local"));
  assertFailure(await restarted.showProfile("local"), "CONNECTION_NOT_FOUND");
  restarted.close();

  const add = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "profile",
      "add",
      "cli",
      database,
      "--read-write",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: home },
      encoding: "utf8",
    },
  );
  assert.equal(add.status, 0, add.stderr);
  const cliConnect = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "connect", "cli"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: home },
      encoding: "utf8",
    },
  );
  assert.equal(cliConnect.status, 0, cliConnect.stderr);
  assert.equal(
    (JSON.parse(cliConnect.stdout) as { profile: string }).profile,
    "cli",
  );
});

test("CLI accepts shell-safe parameters and applies destructive plans", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE cli_params (id INTEGER PRIMARY KEY, name TEXT)",
    ),
  );
  fixture.stateql.close();

  const insert = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "exec",
      "INSERT INTO cli_params (id, name) VALUES (?, ?)",
      "--param",
      "7",
      "--param",
      "Ada",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(insert.status, 0, insert.stdout);

  const paramsFile = join(fixture.home, "params.json");
  writeFileSync(paramsFile, '[8,"Bob"]');
  const insertFromFile = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "exec",
      "INSERT INTO cli_params (id, name) VALUES (?, ?)",
      "--params-file",
      paramsFile,
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(insertFromFile.status, 0, insertFromFile.stdout);

  const plan = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "plan",
      "DELETE FROM cli_params WHERE id = 7",
      "--allow-destructive",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(plan.status, 0, plan.stdout);
  const planId = (JSON.parse(plan.stdout) as { handle: string }).handle;
  const apply = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "apply", planId],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(apply.status, 0, apply.stdout);
  const applied = JSON.parse(apply.stdout) as Record<string, unknown>;
  assert.match(String(applied.handle), /^op_/);
  assert.equal(applied.plan_id, planId);
  assert.equal("operation_id" in applied, false);

  const restarted = new StateQL({ home: fixture.home });
  const rows = await succeed(
    restarted.query("SELECT id, name FROM cli_params ORDER BY id"),
  );
  assert.equal(rows.rows, 1);
  assert.equal(rows.preview[0].name, "Bob");
  restarted.close();
});

test("CLI defaults to compact agent output and preserves verbose JSON", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE output_rows (id INTEGER PRIMARY KEY, name TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec(
      "INSERT INTO output_rows (name) VALUES ('Ada'), ('Grace'), ('Linus')",
    ),
  );
  fixture.stateql.close();

  const compactQuery = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "query",
      "SELECT id, name FROM output_rows ORDER BY id",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactQuery.status, 0, compactQuery.stderr);
  assert.equal(compactQuery.stdout.trim().split(/\r?\n/).length, 1);
  const compact = JSON.parse(compactQuery.stdout) as Record<string, any>;
  assert.equal(compact.ok, true);
  assert.equal(compact.handle, "q_1");
  assert.equal(compact.total, 3);
  assert.equal(compact.rows.length, 3);
  assert.equal(compact.next_offset, null);
  assert.equal("data" in compact, false);
  assert.equal("command_id" in compact, false);
  assert.equal("session_id" in compact, false);
  assert.equal("columns" in compact, false);
  assert.equal("meta" in compact, false);

  const compactRows = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "rows", "q_1", "--limit", "2"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactRows.status, 0, compactRows.stderr);
  const page = JSON.parse(compactRows.stdout) as Record<string, any>;
  assert.equal(page.handle, "q_1");
  assert.equal(page.rows.length, 2);
  assert.equal(page.total, 3);
  assert.equal(page.next_offset, 2);
  assert.equal("offset" in page, false);
  assert.equal("limit" in page, false);
  assert.equal("returned" in page, false);

  const compactCount = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "count", "q_1"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactCount.status, 0, compactCount.stderr);
  assert.deepEqual(JSON.parse(compactCount.stdout), {
    ok: true,
    handle: "q_1",
    total: 3,
  });

  const compactFilter = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "filter",
      "q_1",
      "name LIKE ?",
      "--param",
      "A%",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactFilter.status, 0, compactFilter.stderr);
  const filtered = JSON.parse(compactFilter.stdout) as Record<string, any>;
  assert.equal(filtered.handle, "q_2");
  assert.equal(filtered.total, 1);
  assert.equal(filtered.rows[0].name, "Ada");
  assert.equal(filtered.next_offset, null);

  const compactWarning = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "query", "SELECT id FROM output_rows"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(compactWarning.status, 0, compactWarning.stderr);
  const warning = JSON.parse(compactWarning.stdout) as Record<string, any>;
  assert.equal(warning.warnings[0].code, "NON_DETERMINISTIC_PAGINATION");

  const verboseQuery = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "query",
      "SELECT id, name FROM output_rows ORDER BY id",
      "--cache",
      "bypass",
      "--output",
      "json",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(verboseQuery.status, 0, verboseQuery.stderr);
  const verbose = JSON.parse(verboseQuery.stdout) as Record<string, any>;
  assert.equal(verbose.ok, true);
  assert.equal(typeof verbose.command_id, "string");
  assert.equal(typeof verbose.session_id, "string");
  assert.equal(verbose.data.rows, 3);
  assert.equal(verbose.data.columns.length, 2);
  assert.equal(typeof verbose.meta.duration_ms, "number");

  const compactError = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "exec", "UPDATE output_rows SET name = 'unsafe'"],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.notEqual(compactError.status, 0);
  const failure = JSON.parse(compactError.stdout) as Record<string, any>;
  assert.deepEqual(Object.keys(failure), ["ok", "error"]);
  assert.equal(failure.error.code, "UNBOUNDED_MUTATION");
  assert.equal(failure.error.executed, false);
  assert.equal(failure.error.retryable, false);
});

test("batch and pipe execute sequential JSON commands with safe failure control", async () => {
  const fixture = await createFixture();
  await succeed(
    fixture.stateql.exec(
      "CREATE TABLE batch_items (id INTEGER PRIMARY KEY, name TEXT)",
    ),
  );
  await succeed(
    fixture.stateql.exec("INSERT INTO batch_items (name) VALUES ('one')"),
  );

  const responses = await collect(
    fixture.stateql.batch([
      {
        command: "query",
        sql: "SELECT * FROM batch_items ORDER BY id",
        as: "batch_rows",
      },
      {
        command: "filter",
        handle: "batch_rows",
        where: "name LIKE ?",
        params: ["o%"],
        as: "filtered_rows",
      },
      { command: "rows", handle: "filtered_rows", limit: 1 },
    ]),
  );
  assert.equal(responses.length, 3);
  assert.equal(responses.every((response) => response.ok), true);
  assert.equal(
    (
      (responses[0] as Extract<Response<unknown>, { ok: true }>)
        .data as Record<string, unknown>
    ).alias,
    "batch_rows",
  );
  assert.equal(
    (
      (responses[1] as Extract<Response<unknown>, { ok: true }>)
        .data as Record<string, unknown>
    ).alias,
    "filtered_rows",
  );

  const invalid = { command: "query" } as BatchCommand;
  assert.equal(
    (await collect(
      fixture.stateql.batch([invalid, { command: "status" }]),
    )).length,
    1,
  );
  assert.equal(
    (await collect(
      fixture.stateql.batch([invalid, { command: "status" }], {
        continueOnError: true,
      }),
    )).length,
    2,
  );
  fixture.stateql.close();

  const pipe = spawnSync(process.execPath, ["dist/src/cli.js", "pipe"], {
    cwd: process.cwd(),
    env: { ...process.env, STQL_HOME: fixture.home },
    input: [
      JSON.stringify({
        command: "query",
        sql: "SELECT * FROM batch_items ORDER BY id",
        as: "pipe_rows",
      }),
      JSON.stringify({ command: "count", handle: "pipe_rows" }),
      "",
    ].join("\n"),
    encoding: "utf8",
  });
  assert.equal(pipe.status, 0, pipe.stderr);
  const output = pipe.stdout
    .trim()
    .split(/\r?\n/)
    .map((line) => JSON.parse(line) as Record<string, any>);
  assert.equal(output.length, 2);
  assert.equal(output.every((response) => response.ok), true);
  assert.equal("data" in output[0]!, false);
  assert.equal(output[0]!.handle, "q_1");
  assert.equal(output[1]!.total, 1);

  const batchFile = join(fixture.home, "commands.json");
  writeFileSync(
    batchFile,
    JSON.stringify([{ command: "count", handle: "pipe_rows" }]),
  );
  const batch = spawnSync(
    process.execPath,
    ["dist/src/cli.js", "batch", batchFile],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
    },
  );
  assert.equal(batch.status, 0, batch.stderr);
  assert.equal(
    (JSON.parse(batch.stdout) as Response<unknown>).ok,
    true,
  );
});

