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
  assert.equal(validateRedisWriteCommand({ command: "DEL", args: ["one", "two"] }).command, "DEL");
  assert.throws(() => validateRedisWriteCommand({ command: "DEL", args: [] }), /requires 1-100/);
  assert.throws(() => validateRedisWriteCommand({ command: "FLUSHALL", args: [] }), /Unsupported/);
  for (const args of [["k", "v", "EX", "10"], ["k", "v", "px", "5", "NX"], ["k", "v", "XX"]]) {
    assert.equal(validateRedisWriteCommand({ command: "SET", args }).command, "SET", args.join(" "));
  }
  for (const args of [["k", "v", "EX", "0"], ["k", "v", "EX", "1", "PX"], ["k", "v", "NX", "XX"], ["k", "v", "GET"], ["k", "v", "EX"]]) {
    assert.throws(() => validateRedisWriteCommand({ command: "SET", args }), /Redis SET/, args.join(" "));
  }
  assert.throws(() => validateRedisWriteCommand({ command: "HSET", args: ["k", "a", "1", "b"] }), /pairs/);
  assert.throws(() => validateRedisWriteCommand({ command: "INCRBY", args: ["k", "1.5"] }), /integer/);
  assert.throws(() => validateRedisWriteCommand({ command: "EXPIRE", args: ["k", "-1"] }), /positive integer/);
  assert.throws(() => validateRedisReadCommand({ command: "ZRANGE", args: ["k", "0", "1", "BYSCORE"] }), /index ranges/);
  assert.throws(() => validateRedisReadCommand({ command: "ZRANGE", args: ["k", "0", "-1"] }), /at most/);
  assert.throws(() => validateRedisReadCommand({ command: "INFO", args: ["a b"] }), /section/);
});

test("Redis common reads and writes stay bounded", { skip: !process.env.STQL_REDIS_URL }, async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  const external = createClient({ url: process.env.STQL_REDIS_URL });
  external.on("error", () => undefined);
  const prefix = `stateql:test:${randomUUID()}`;
  const [hashKey, setKey, zsetKey, counter, big, other] = ["hash", "set", "zset", "counter", "big", "other"].map((name) => `${prefix}:${name}`) as [string, string, string, string, string, string];
  const exec = (command: string, ...args: string[]) => succeed(stateql.redisExec({ command, args }, { idempotencyKey: randomUUID() }));
  const query = async (command: string, ...args: string[]) => (await succeed(stateql.redisQuery({ command, args }, { cache: "bypass" }))).preview;
  try {
    await external.connect();
    await succeed(stateql.connect(undefined, { secretEnv: "STQL_REDIS_URL", readOnly: false }));
    await exec("HSET", hashKey, "a", "1", "b", "2");
    assert.equal((await exec("HINCRBY", hashKey, "a", "5")).affected_rows, 1);
    assert.deepEqual(await query("HGETALL", hashKey), [{ field: "a", value: "6" }, { field: "b", value: "2" }]);
    assert.deepEqual(await query("HLEN", hashKey), [{ value: 2 }]);
    await exec("SADD", setKey, "x");
    assert.deepEqual(await query("SMEMBERS", setKey), [{ index: 0, value: "x" }]);
    assert.deepEqual(await query("SISMEMBER", setKey, "x"), [{ value: 1 }]);
    await exec("ZADD", zsetKey, "2", "two");
    await exec("ZADD", zsetKey, "1", "one");
    assert.deepEqual(await query("ZRANGE", zsetKey, "0", "9", "WITHSCORES"), [{ value: "one", score: "1" }, { value: "two", score: "2" }]);
    assert.deepEqual(await query("ZSCORE", zsetKey, "two"), [{ value: "2" }]);
    assert.equal((await exec("INCRBY", counter, "41")).affected_rows, 1);
    await exec("INCR", counter);
    assert.equal(await external.get(counter), "42");
    assert.equal((await exec("SET", counter, "7", "NX")).affected_rows, 0);
    await exec("SET", counter, "7", "XX", "EX", "100");
    assert.ok((await external.ttl(counter)) > 90);
    await exec("PERSIST", counter);
    assert.equal(await external.ttl(counter), -1);
    assert.ok((await query("DBSIZE"))[0]!.value as number >= 4);
    assert.ok((await query("INFO", "server")).some((row: { field: string }) => row.field === "redis_version"));

    await external.sAdd(big, Array.from({ length: 201 }, (_, index) => String(index)));
    const tooBig = await stateql.redisQuery({ command: "SMEMBERS", args: [big] }, { cache: "bypass" });
    assert.equal(tooBig.ok, false);
    if (!tooBig.ok) assert.match(tooBig.error.message, /limit is 200\. Use SSCAN/);

    await external.set(other, "o");
    const plan = await succeed(stateql.redisPlan({ command: "DEL", args: [counter, other] }));
    await external.set(other, "changed");
    const conflict = await stateql.apply(plan.plan_id);
    assert.equal(conflict.ok, false);
    if (!conflict.ok) assert.equal(conflict.error.code, "ROW_CONFLICT");
    assert.equal((await exec("UNLINK", counter, other)).affected_rows, 2);
  } finally {
    if (external.isOpen) { await external.del([hashKey, setKey, zsetKey, counter, big, other]); await external.quit(); }
    stateql.close();
  }
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

test("Redis writes share duplicate protection, plans, and read caching", { skip: !process.env.STQL_REDIS_URL }, async () => {
  const stateql = new StateQL({ home: createTemporaryDirectory() });
  const external = createClient({ url: process.env.STQL_REDIS_URL });
  external.on("error", () => undefined);
  const key = `stateql:test:${randomUUID()}`;
  try {
    await external.connect();
    await succeed(stateql.connect(undefined, { secretEnv: "STQL_REDIS_URL", readOnly: false }));
    const idempotencyKey = randomUUID();
    const first = await succeed(stateql.redisExec({ command: "SET", args: [key, "one"] }, { idempotencyKey }));
    const repeated = await succeed(stateql.redisExec({ command: "SET", args: [key, "one"] }, { idempotencyKey }));
    assert.equal(repeated.duplicate_of, first.operation_id);
    const duplicate = await stateql.redisExec({ command: "SET", args: [key, "one"] });
    assert.equal(duplicate.ok, false);
    if (!duplicate.ok) assert.equal(duplicate.error.code, "POTENTIAL_DUPLICATE_WRITE");

    const plan = await succeed(stateql.redisPlan({ command: "SET", args: [key, "planned"] }));
    const applied = await succeed(stateql.apply(plan.plan_id));
    assert.equal(applied.plan_id, plan.plan_id);
    assert.equal(await external.get(key), "planned");

    await succeed(stateql.redisQuery({ command: "GET", args: [key] }));
    const cached = await succeed(stateql.redisQuery({ command: "GET", args: [key] }));
    assert.equal(cached.cached, true);
    assert.equal(cached.preview[0].value, "planned");

    const transaction = await stateql.beginTransaction();
    assert.equal(transaction.ok, false);
    if (!transaction.ok) assert.equal(transaction.error.code, "UNSUPPORTED_DRIVER");

    await succeed(stateql.connect(undefined, { secretEnv: "STQL_REDIS_URL" }));
    const readOnly = await stateql.redisExec({ command: "DEL", args: [key] });
    assert.equal(readOnly.ok, false);
    if (!readOnly.ok) assert.equal(readOnly.error.code, "READ_ONLY_CONNECTION");
    assert.equal(await external.get(key), "planned");
  } finally {
    if (external.isOpen) { await external.del(key); await external.quit(); }
    stateql.close();
  }
});
