import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { detectDriver } from "../src/connection.js";
import { analyzeSql } from "../src/sql.js";
import type { Response } from "../src/types.js";
import { createFixture, createTemporaryDirectory, succeed } from "./helpers.js";
import { StateQL } from "../src/stateql.js";

test("SQLite detection and SQL parsing stay explicit", () => {
  assert.equal(detectDriver("target.sqlite"), "sqlite");
  assert.equal(
    analyzeSql("SELECT value FROM items WHERE id = ?", "sqlite").statementType,
    "select",
  );
});

test("SQLite in-memory targets are rejected instead of losing committed writes", async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  for (const target of [":memory:", "sqlite::memory:"]) {
    const response = await stateql.connect(target, { readOnly: false });
    assert.equal(response.ok, false);
    if (!response.ok) assert.equal(response.error.code, "INVALID_COMMAND");
  }
  stateql.close();
});

test("SQLite deadlines and AbortSignal cancellation stay off the event loop", async () => {
  const fixture = await createFixture();
  let eventLoopTicked = false;
  setTimeout(() => {
    eventLoopTicked = true;
  }, 25);

  const slowSql = `
    WITH RECURSIVE count_up(value) AS (
      SELECT 0
      UNION ALL
      SELECT value + 1 FROM count_up WHERE value < 1000000000
    )
    SELECT sum(value) AS total FROM count_up
  `;
  const started = Date.now();
  const timedOut = await fixture.stateql.query(slowSql, {
    cache: "bypass",
    timeoutMs: 300,
  });
  assert.equal(timedOut.ok, false);
  if (!timedOut.ok) {
    assert.equal(timedOut.error.code, "DEADLINE_EXCEEDED");
    assert.equal(timedOut.error.executed, true);
  }
  assert.equal(eventLoopTicked, true);
  assert.ok(Date.now() - started < 3_000);

  const controller = new AbortController();
  setTimeout(() => controller.abort(), 100);
  const cancelled = await fixture.stateql.query(slowSql, {
    cache: "bypass",
    signal: controller.signal,
    timeoutMs: 5_000,
  });
  assert.equal(cancelled.ok, false);
  if (!cancelled.ok) {
    assert.equal(cancelled.error.code, "OPERATION_CANCELLED");
    assert.equal(cancelled.error.executed, true);
  }

  assert.equal(
    (await succeed(fixture.stateql.query("SELECT 1 AS healthy"))).preview[0]
      .healthy,
    1,
  );
  fixture.stateql.close();

  const cli = spawnSync(
    process.execPath,
    [
      "dist/src/cli.js",
      "query",
      slowSql,
      "--cache",
      "bypass",
      "--timeout-ms",
      "200",
      "--output",
      "json",
    ],
    {
      cwd: process.cwd(),
      env: { ...process.env, STQL_HOME: fixture.home },
      encoding: "utf8",
      timeout: 3_000,
    },
  );
  assert.equal(cli.status, 4, cli.stderr);
  assert.equal(
    (JSON.parse(cli.stdout) as Response<unknown> & { error?: { code: string } })
      .error?.code,
    "DEADLINE_EXCEEDED",
  );
});
