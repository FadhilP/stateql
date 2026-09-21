import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";

const OPTIONAL_RUNTIME_MODULES = new Set([
  "@redis/client",
  "mongodb",
  "mysql2",
  "mysql2/promise",
  "node-sql-parser",
  "pg",
]);

test("library import defers optional database clients and SQL parser", async () => {
  const resolved: string[] = [];
  const hooks = registerHooks({
    resolve(specifier, context, nextResolve) {
      resolved.push(specifier);
      return nextResolve(specifier, context);
    },
  });
  try {
    await import("../src/index.js");
  } finally {
    hooks.deregister();
  }

  assert.deepEqual(
    resolved.filter(specifier => OPTIONAL_RUNTIME_MODULES.has(specifier)),
    [],
  );
});
