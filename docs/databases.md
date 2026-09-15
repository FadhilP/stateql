# Database support

[Home](../README.md) · [Usage](usage.md) · [Database support](databases.md) · [TypeScript library](library.md)

- [Driver notes](#driver-notes)
- [SQL statement support](#sql-statement-support)
- [Dialect upserts](#dialect-upserts)
- [PostgreSQL diagnostics and maintenance](#postgresql-diagnostics-and-maintenance)
- [SQLite and MySQL diagnostics and maintenance](#sqlite-and-mysql-diagnostics-and-maintenance)
- [Native MongoDB](#native-mongodb)
- [Native Redis](#native-redis)

Connect using the [usage guide](usage.md#connections-and-profiles). All writes
require a read-write connection; SQL/MongoDB approvals and retry rules are
covered under [write safety](usage.md#write-safety).

## Driver notes

- **SQLite:** use a filesystem path for direct connections or `sqlite:` for an
  environment-backed path.
- **PostgreSQL:** StateQL preserves strict TLS verification by normalizing
  `sslmode=prefer`, `require`, and `verify-ca` to `verify-full` before opening
  the adapter. Use `sslmode=verify-full` explicitly for clarity. Setting
  `uselibpqcompat=true` opts out and keeps libpq-compatible SSL semantics.
- **MySQL:** uses positional `?` parameters. MariaDB compatibility is not
  currently claimed.
- **MongoDB:** supports `mongodb://` and `mongodb+srv://` URLs with an explicit
  database path. SQL methods are rejected; use the native MongoDB methods below.
- **Redis:** supports `redis://` and TLS-backed `rediss://` URLs. SQL methods and
  legacy `inspect` commands are unavailable; use native commands and catalog
  inspection instead.

## SQL statement support

StateQL parses and classifies one `SELECT`, `INSERT`, `REPLACE`, `UPDATE`,
`DELETE`, `CREATE`, `ALTER`, `DROP`, or `TRUNCATE` statement through
`node-sql-parser`, subject to dialect support and safety validation. Reads use
`query`; mutations and DDL use durable `exec`/`plan`/`apply` operations. Raw
transaction-control SQL is unsupported: use
[staged transactions](usage.md#transactions), not `BEGIN` or `COMMIT` statements.
MongoDB and Redis use separate native command APIs.

Unrecognized or parser-unsupported forms remain blocked.

## Dialect upserts

PostgreSQL `INSERT ... ON CONFLICT DO NOTHING|UPDATE` and MySQL `INSERT ... ON
DUPLICATE KEY UPDATE` are structurally validated and recorded with statement
type `upsert`. Finite `VALUES` and MySQL `INSERT ... SET` sources use normal
write policy. An update-upsert fed by `SELECT` requires `--allow-unbounded`
because its candidate row count is not statically bounded. Upserts support direct
`exec`, `plan`/`apply`, and staged transactions; hidden additional writes are rejected.

MySQL `INSERT IGNORE` and SQLite `INSERT OR IGNORE` remain non-overwriting
inserts. SQLite `INSERT OR REPLACE` retains destructive-operation approval,
while SQLite modern `ON CONFLICT ... DO UPDATE` and every `MERGE` form remain
blocked until the parser can expose their complete mutation structure.

## PostgreSQL diagnostics and maintenance

Run PostgreSQL plans through `query`:

```bash
stql query "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM jobs WHERE id = 42"
```

Plain `EXPLAIN` may plan a structurally validated `SELECT`, `INSERT`, `UPDATE`,
or `DELETE`. Because `EXPLAIN ANALYZE` executes its inner statement, StateQL
accepts only a validated read-only `SELECT`; `SELECT INTO`, writing CTEs, and
mutations are rejected. Diagnostics execute inside PostgreSQL `BEGIN READ ONLY`
and are never reused from cache. `--cache require` therefore returns
`CACHE_MISS` without executing the diagnostic.

Legacy `EXPLAIN [ANALYZE] [VERBOSE] statement` is also supported. Option lists
allow PostgreSQL boolean diagnostic options plus `FORMAT` and `SERIALIZE`;
unknown, duplicate, or malformed options fail closed. PostgreSQL's read-only
transaction protects database writes, but cannot contain external effects from
user-defined or incorrectly labelled functions.

StateQL supports PostgreSQL 14–18. Top-level `VALUES` is a bounded read and
accepts normal PostgreSQL positional parameters. It is conservatively
non-cacheable because expressions may be volatile. The following narrow `SHOW`
allowlist is also available as non-cacheable diagnostics:
`server_version`, `server_version_num`, `transaction_read_only`,
`transaction_isolation`, and `default_transaction_isolation`. `SHOW ALL` and
other settings remain blocked. Syntax accepted by StateQL but introduced by a
newer PostgreSQL release may be rejected safely by an older server.

`VACUUM`, `ANALYZE`, `REINDEX`, and `CLUSTER` are PostgreSQL maintenance writes:

```bash
stql exec "VACUUM (ANALYZE) public.jobs" --allow-destructive
stql plan "REINDEX TABLE public.jobs" --allow-destructive
```

The PostgreSQL 14–18 grammar includes parenthesized `REINDEX CONCURRENTLY`,
PostgreSQL 16 `BUFFER_USAGE_LIMIT` for `VACUUM`/`ANALYZE`, optional
`DATABASE`/`SYSTEM` reindex names, and PostgreSQL 18 `ONLY table *` maintenance
targets. Memory sizes accept an integer number of kilobytes or a quoted
`B|kB|MB|GB|TB` value. Older servers may reject newer forms after dispatch, so
StateQL retains conservative unknown-outcome handling.

They require a read-write connection and `--allow-destructive`, reject StateQL
parameters and optimistic row-count preconditions, and run as individually
tracked autocommit operations. They cannot be staged in a StateQL transaction,
even where a PostgreSQL variant could run inside a database transaction. A
timeout, cancellation, or error after dispatch is reported as `OUTCOME_UNKNOWN`;
inspect database state before replaying it.

## SQLite and MySQL diagnostics and maintenance

SQLite supports `EXPLAIN QUERY PLAN` for structurally read-only `SELECT`
statements. MySQL supports `EXPLAIN SELECT` plus bare `SHOW TABLES`,
`SHOW COLUMNS FROM table`, and `SHOW INDEX|INDEXES FROM table`. Broader
`EXPLAIN`, `SHOW`, and write-bearing forms remain blocked.

MySQL executable comments (`/*! ... */`) are rejected throughout SQL. Because
StateQL does not assume a server `sql_mode`, quoting that could expose these
comments under `ANSI_QUOTES` or `NO_BACKSLASH_ESCAPES` is also rejected.

These diagnostics use `query`, work with read-only connections, preserve the
original statement instead of applying StateQL's limiting SQL wrapper, and are
never reused from cache. Materialized results still receive StateQL's row and
byte checks.

SQLite also supports bare `VACUUM`, plus `ANALYZE [target]` and
`REINDEX [target]` with at most one unqualified or double-quoted target:

```bash
stql exec "ANALYZE jobs" --allow-destructive
stql exec "REINDEX jobs_created_at_idx" --allow-destructive
stql exec "VACUUM" --allow-destructive
```

These commands require a read-write connection, reject parameters, run as
individually tracked autocommit operations, and cannot be staged. A timeout,
cancellation, or error after dispatch is reported as `OUTCOME_UNKNOWN`.
`VACUUM INTO`, schema-qualified targets, paths, `ATTACH`, and arbitrary `PRAGMA`
remain blocked.

MySQL supports one optionally qualified bare or backtick-quoted target for
`ANALYZE TABLE`, `OPTIMIZE TABLE`, and `CHECK TABLE`:

```bash
stql exec "ANALYZE TABLE jobs" --allow-destructive
stql plan "OPTIMIZE TABLE jobs" --allow-destructive
stql query "CHECK TABLE jobs"
```

`ANALYZE` and `OPTIMIZE` are durable autocommit writes requiring a read-write
connection and destructive approval; server-reported error rows become known
failed operations, while timeout or cancellation after dispatch remains
`OUTCOME_UNKNOWN`. `CHECK TABLE` is an unwrapped, non-cacheable autocommit read
that works on read-only connections and retains normal result limits. All three
reject StateQL parameters, options, multiple targets, and additional
statements, and none can run during a staged transaction.
## Native MongoDB

MongoDB commands use official Extended JSON (EJSON), so BSON values survive the
CLI boundary:

```bash
stql mongo query '{"operation":"find","collection":"users","filter":{"_id":{"$oid":"507f1f77bcf86cd799439011"}}}'
stql mongo exec '{"operation":"updateOne","collection":"users","filter":{"_id":{"$oid":"507f1f77bcf86cd799439011"}},"update":{"$set":{"seen_at":{"$date":"2026-01-01T00:00:00Z"}}}}'
stql mongo plan '{"operation":"deleteMany","collection":"users","filter":{"disabled":true}}' --allow-destructive
```

The TypeScript equivalents are `mongoQuery(command)`, `mongoExec(command)`, and
`mongoPlan(command)`. Supported reads are `find` and `aggregate`; writes are
`insertOne`, `insertMany`, `updateOne`, `updateMany`, `replaceOne`, `deleteOne`,
and `deleteMany`. Result documents are JSON-safe, order-preserving EJSON: for example,
ObjectIds and dates appear as `{ "$oid": "..." }` and
`{ "$date": { "$numberLong": "..." } }`.

Empty update, replacement, or delete filters require `--allow-unbounded`;
deletes and replacements also require `--allow-destructive`. Mongo inspection
accepts `collections`, `collection`, `columns`, `indexes`, and `constraints`
(`schema` and `table` remain aliases shared with SQL drivers). MongoDB cache
confidence is TTL-based: external writes are not detected, so use
`--cache bypass` for a fresh read.

```ts
const result = await stateql.mongoQuery({
  operation: "find",
  collection: "users",
  filter: { active: true },
  options: { sort: { _id: 1 }, limit: 50 },
});
```

## Native Redis

Redis/Rediss URLs support URL database selection, password or ACL username,
and TLS (`rediss`). Supply credential-bearing URLs through `--env` or a trusted
full-URL credential reference. Library/batch callers may instead use a
[password reference](library.md#password-references) with a password-free target.
Native commands accept `{command: string, args?: string[]}`:

```bash
stql redis query '{"command":"GET","args":["app:status"]}'
stql redis exec '{"command":"SET","args":["app:status","ready"]}' --idempotency-key status-ready
stql redis plan '{"command":"SET","args":["app:status","paused"]}'
stql objects key --search app --limit 50
stql object key app:status
```

Writes and plans require a read-write connection. The TypeScript methods are:

- `redisQuery`: `GET`, `MGET`, `TYPE`, `EXISTS`, `TTL`, `PTTL`, `HGET`, `HMGET`,
  bounded `LRANGE`, and bounded `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`.
- `redisExec` and `redisPlan`: one-key `SET`, `DEL`, `HSET`, `HDEL`, `LPUSH`,
  `RPUSH`, `SADD`, `SREM`, `ZADD`, or `ZREM` mutation.
- `describeObject({kind:"key",name})`: bounded string/hash/list/set/zset value
  inspection with TTL and continuation metadata where applicable.

Arguments are UTF-8 strings, at most 100 values/256 KiB; materialized replies are
at most 1 MiB. `KEYS`, scripts, modules, pub/sub, blocking commands, admin/flush,
and arbitrary commands are rejected. Key discovery always uses SCAN. A Redis
plan snapshots one bounded key and `apply` uses an isolated `WATCH` + one-command
`MULTI/EXEC`; a pre-apply content or expiry change returns `ROW_CONFLICT` and is
never retried automatically. Direct `redisExec` has Redis single-command
atomicity only. Redis has no SQL rollback or StateQL staged transaction support;
a lost write/EXEC reply is reported as `OUTCOME_UNKNOWN` and remains blocked.
