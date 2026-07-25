import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach } from "node:test";
import { StateQL } from "../src/stateql.js";
import type { Response } from "../src/types.js";

const temporaryDirectories: string[] = [];

afterEach(() => {
  while (temporaryDirectories.length) {
    rmSync(temporaryDirectories.pop()!, { recursive: true, force: true });
  }
});

export function createTemporaryDirectory(prefix = "stateql-test-"): string {
  const root = mkdtempSync(join(tmpdir(), prefix));
  temporaryDirectories.push(root);
  return root;
}

export async function createFixture(now?: () => Date): Promise<{
  home: string;
  database: string;
  stateql: StateQL;
}> {
  const root = createTemporaryDirectory();
  const home = join(root, "state");
  const database = join(root, "target.sqlite");
  const stateql = new StateQL({ home, ...(now ? { now } : {}) });
  await succeed(stateql.connect(database, { readOnly: false }));
  return { home, database, stateql };
}

export async function succeed(
  responseOrPromise: Response<unknown> | Promise<Response<unknown>>,
): Promise<Record<string, any>> {
  const response = await responseOrPromise;
  assert.equal(
    response.ok,
    true,
    response.ok ? undefined : JSON.stringify(response.error),
  );
  return response.data as Record<string, any>;
}

export function assertFailure(response: Response<unknown>, code: string): void {
  assert.equal(response.ok, false);
  if (response.ok) return;
  assert.equal(response.error.code, code);
  assert.equal(response.error.executed, false);
}

export function assertOutcomeUnknown(response: Response<unknown>): void {
  assert.equal(response.ok, false);
  if (response.ok) return;
  assert.equal(response.error.code, "OUTCOME_UNKNOWN");
  assert.equal(response.error.executed, true);
  assert.equal(response.error.retryable, false);
}

export async function collect(
  responses: AsyncIterable<Response<unknown>>,
): Promise<Response<unknown>[]> {
  const collected: Response<unknown>[] = [];
  for await (const response of responses) collected.push(response);
  return collected;
}
