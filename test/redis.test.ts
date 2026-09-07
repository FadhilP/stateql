import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createClient } from "@redis/client";
import { detectDriver, redisDatabaseName } from "../src/connection.js";
import { validateRedisReadCommand, validateRedisWriteCommand } from "../src/redis.js";
import { StateQL } from "../src/stateql.js";
import { createTemporaryDirectory, succeed } from "./helpers.js";

test("Redis URLs and bounded command classification are explicit", () => {
  assert.equal(detectDriver("redis://localhost:6379/2"), "redis");
  assert.equal(detectDriver("rediss://localhost/0"), "redis");
  assert.equal(redisDatabaseName("redis://localhost:6379/12"), "db12");
  assert.equal(validateRedisReadCommand({ command: "get", args: ["key"] }).command, "GET");
  assert.equal(validateRedisWriteCommand({ command: "hset", args: ["key", "field", "value"] }).command, "HSET");
  assert.throws(() => validateRedisReadCommand({ command: "KEYS", args: ["*"] }), /Unsupported/);
  assert.throws(() => validateRedisReadCommand({ command: "EVAL", args: ["return 1", "0"] }), /Unsupported/);
  assert.throws(() => validateRedisReadCommand({ command: "LRANGE", args: ["key", "0", "1000"] }), /at most/);
  assert.throws(() => validateRedisWriteCommand({ command: "DEL", args: ["one", "two"] }), /requires 1/);
  assert.throws(() => validateRedisWriteCommand({ command: "FLUSHALL", args: [] }), /Unsupported/);
});

test("Redis query results and guarded plans detect pre-apply changes", { skip: !process.env.STQL_REDIS_URL }, async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  const external = createClient({ url: process.env.STQL_REDIS_URL });
  external.on("error", () => undefined);
  const key = `stateql:test:${randomUUID()}`;
  try {
    await external.connect();
    await succeed(stateql.connect(undefined, { secretEnv: "STQL_REDIS_URL", readOnly: false }));
    const plan = await succeed(stateql.redisPlan({ command: "SET", args: [key, "planned"] }));
    await external.set(key, "external");
    const conflict = await stateql.apply(plan.plan_id);
    assert.equal(conflict.ok, false);
    if (!conflict.ok) assert.equal(conflict.error.code, "ROW_CONFLICT");
    assert.equal(await external.get(key), "external");

    await succeed(stateql.redisExec({ command: "SET", args: [key, "applied"] }, { idempotencyKey: randomUUID() }));
    const queried = await succeed(stateql.redisQuery({ command: "GET", args: [key] }, { cache: "bypass" }));
    assert.match(queried.alias, /^[a-z2-7]{10}$/);
    assert.equal(queried.preview[0].value, "applied");
    const objects = await succeed(stateql.listObjects({ kind: "key", search: key, limit: 20 }));
    assert.ok(objects.objects.some((object: { name: string }) => object.name === key));
    const described = await succeed(stateql.describeObject({ kind: "key", name: key, identity: key }));
    assert.equal(described.definition.type, "string");
  } finally {
    if (external.isOpen) { await external.del(key); await external.quit(); }
    stateql.close();
  }
});
