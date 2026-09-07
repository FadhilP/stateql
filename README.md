# StateQL

StateQL is a stateful database CLI and TypeScript library for AI agents and
automation. It provides a safe interface for querying, changing, and inspecting
SQLite, PostgreSQL, MySQL, MongoDB, and Redis databases while keeping results
reusable and operations traceable across commands.

StateQL is built around durable handles:

1. Run a query and receive a result handle such as `q_1`.
2. Reuse, filter, page, count, alias, or export that stored result without
   rerunning the original SQL.
3. Use operation, plan, and transaction handles to inspect and control writes.

Requires Node.js 22.16 or newer for the required `node:sqlite` APIs.

## Quick start

Install the CLI:

```bash
npm install -g @fadhilp/stateql
```

Connect to an existing SQLite database and run a bounded, parameterized query:

```bash
export STQL_SESSION=audit
stql profile add local ./app.sqlite
stql connect local

stql query \
  "SELECT id, name, email FROM users WHERE status = ? AND created_at >= ? ORDER BY id LIMIT 50" \
  --param active \
  --param 2026-01-01
```

Parameters keep values separate from SQL. `ORDER BY` makes paging stable, and
`LIMIT` bounds work at the database. The default `agent` output is compact,
one-line JSON:

```json
{"ok":true,"handle":"q_1","rows":[{"id":7,"name":"Ada","email":"ada@example.com"},{"id":12,"name":"Grace","email":"grace@example.com"},{"id":18,"name":"Linus","email":"linus@kernel.org"}],"truncated":false,"cached":false,"total":3,"next_offset":null}
```

`q_1` is a durable snapshot. Filter it locally without accessing the original
database:

```bash
stql filter q_1 "email LIKE ?" --param "%@example.com"
```

```json
{"ok":true,"handle":"q_2","rows":[{"id":7,"name":"Ada","email":"ada@example.com"},{"id":12,"name":"Grace","email":"grace@example.com"}],"truncated":false,"cached":false,"total":2,"next_offset":null}
```

The filtered snapshot receives its own handle. Give it a readable alias, page
through it, inspect its count, or export it without rerunning SQL:

```bash
stql alias set example-users q_2
stql rows example-users --offset 0 --limit 1
stql rows example-users --offset 1 --limit 1
stql count example-users
stql export example-users --output example-users.csv --format csv
```

Example first page:

```json
{"ok":true,"handle":"q_2","rows":[{"id":7,"name":"Ada","email":"ada@example.com"}],"total":2,"truncated":true,"next_offset":1}
```

Running the same normalized query with the same parameters reuses `q_1` while
its cache entry is valid. Use `--cache bypass` when a fresh read is required.

## Connections and profiles

A connection accepts exactly one source: a direct target, `--env`,
`--credential-ref`, or `--profile`.

```bash
stql connect <sqlite-path|postgres-url|mysql-url|mongodb-url> [--name NAME] [--read-write]
stql connect --env ENV [--name NAME] [--read-write]
stql connect --credential-ref REF [--name NAME] [--read-write]
stql connect --profile NAME
stql disconnect
stql status
```

### Environment-backed credentials

PostgreSQL, MySQL, and MongoDB credentials should come from environment
variables. The variable must contain the complete connection URL, not only its
password. Environment-backed SQLite paths require an explicit `sqlite:` prefix.

```bash
export APP_DATABASE_URL='postgres://user:password@host/app'
stql connect --env APP_DATABASE_URL --name app --read-only

export MYSQL_DATABASE_URL='mysql://user:password@host/app'
stql connect --env MYSQL_DATABASE_URL --name mysql-app --read-only

export MONGODB_URL='mongodb://user:password@host/app'
stql connect --env MONGODB_URL --name mongo-app --read-only

export SQLITE_DATABASE='sqlite:./app.sqlite'
stql connect --env SQLITE_DATABASE --name local --read-only
```

StateQL stores no PostgreSQL, MySQL, or MongoDB password. Credential-bearing
URLs must be supplied through `--env`. SQLite paths remain persisted as
connection metadata.

### Local profiles

Profiles store exactly one connection target, environment-variable name, or
opaque credential reference together with read-only policy. Credential values
are never stored. Profiles persist under `STQL_HOME` with other StateQL
metadata, and list/show responses include `credential_ref` when configured.

```bash
stql profile add local ./app.sqlite --read-write
stql profile add production --env PROD_DATABASE_URL --read-only
stql profile add hosted --credential-ref 'vault://team/app' --read-only
stql profile list
stql profile show production
stql connect local
stql connect --profile production
```

Credential references are bounded nonempty opaque strings; StateQL does not
apply environment-variable syntax or normalization to them. They can only be
resolved by a trusted host `CredentialResolver`, so the standalone CLI may
store them in profiles but cannot connect with them.

A bare connection target matching a profile name resolves to that profile;
otherwise it remains a path or database URL.

### Driver notes

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

## CLI reference

```text
stql connect <sqlite-path|postgres-url|mysql-url|mongodb-url> [--name NAME] [--read-write]
stql connect --env ENV [--name NAME] [--read-write]
stql connect --profile NAME
stql disconnect
stql status
stql profile add|list|show|remove
stql session start|list|show|summary|close
stql query <sql> [--params JSON | --param VALUE...] [--cache auto|bypass|require]
stql filter <result-handle> <predicate> [--params JSON | --param VALUE...]
stql exec <sql> [--params JSON | --param VALUE...] [--idempotency-key KEY] [--replay]
              [--allow-unbounded] [--allow-destructive]
stql mongo query|exec|plan '<EJSON command>' [--cache MODE] [--idempotency-key KEY]
                                  [--replay] [--allow-unbounded] [--allow-destructive]
stql show|count|columns <result-handle>
stql rows <result-handle> [--offset N] [--limit N]
stql alias set <name> <result-handle>
stql export <result-handle> --output FILE [--format json|jsonl|csv]
stql inspect schema|table|collection|collections|columns|indexes|constraints [name]
stql transaction begin|status|commit|rollback [--isolation LEVEL]
stql plan <sql> [--allow-unbounded] [--allow-destructive]
stql apply <plan-handle>
stql history [--limit N]
stql receipt <operation-handle>
stql doctor
stql purge [expired|results|history|all]
stql capabilities
stql batch [commands.json|commands.jsonl|-] [--continue-on-error]
stql pipe [--continue-on-error]
```

### SQL parameters

For shell-safe positional parameters, repeat `--param`. JSON scalars become
their native types; other values remain strings.

```powershell
stql exec "INSERT INTO users (name, status) VALUES (?, ?)" `
  --param Ada --param trial
```

Use `--params JSON` for a JSON array or named parameters. Use
`--params-file FILE` when JSON is awkward to quote; `--params-file -` reads
JSON from standard input.

### Native MongoDB

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
deletes and replacements also require `--allow-destructive`. Mongo inspection accepts `collections`,
`collection`, `columns`, `indexes`, and `constraints` (`schema` and `table`
remain aliases shared with SQL drivers). MongoDB cache confidence is TTL-based:
external writes are not detected, so use `--cache bypass` for a fresh read.

```ts
const result = await stateql.mongoQuery({
  operation: "find",
  collection: "users",
  filter: { active: true },
  options: { sort: { _id: 1 }, limit: 50 },
});
```
### Output modes

CLI output defaults to compact, one-line `agent` JSON. Successful responses
flatten useful data and expose the primary durable ID as `handle`. Errors retain
their complete error object. Empty warnings and tracing metadata are omitted.

```json
{"ok":false,"error":{"code":"UNBOUNDED_MUTATION","message":"Mutation has no WHERE clause.","retryable":false,"executed":false,"override_flag":"--allow-unbounded"}}
```

Other modes are:

- `--output json`: original pretty, verbose envelope.
- `--output jsonl`: verbose envelope on one line.
- `--output text`: short human-readable status.
- `--output silent`: only a successful handle.

Set `STQL_OUTPUT` to choose a mode globally. For `export`, `--output` names the
file, so use `STQL_OUTPUT` to choose the command's response mode. Library
responses always keep the full envelope.

### Deadlines and cancellation

Database commands accept `--timeout-ms N`; the default is 30,000 ms. `Ctrl+C`
cancels active work.

- SQLite runs in a killable child process so long synchronous statements cannot
  block StateQL's event loop.
- PostgreSQL combines server-side `statement_timeout` with client deadlines.
- MySQL deadlines destroy the active connection.
- MongoDB uses driver deadlines and closes stopped operations.

A timed-out or cancelled write may return `OUTCOME_UNKNOWN` when its commit
status cannot be proven. Cancellation stops that command's driver work; it does
not close the `StateQL` actor, and later commands remain usable.

## Durable state and result reuse

State metadata lives under `STQL_HOME`, or the platform data directory when
unset. StateQL keeps connections, sessions, handles, aliases, cache entries,
plans, transactions, history, and receipts available across CLI invocations.

### Sessions and actors

Set `STQL_SESSION` to select a named session and `STQL_ACTOR` to select an
attached actor. A session is a shared workspace: attached actors reuse its
connection, handles, aliases, cache, and state version. Plans and staged
transactions remain owned by the actor that created them.

Callers that omit `actor` keep the legacy behavior where the actor ID is the
session name.

### Result lifetime and limits

SQLite result rows are materialized locally for durable access. Read cache
entries expire after five minutes, and materialized handles expire after 24
hours. Expired results and plans are deleted the next time StateQL opens.

Queries exceeding 10,000 rows or 16 MiB of serialized row data fail before
persistence. Narrow the `WHERE` clause, add `LIMIT`, or select fewer columns.
These caps bound persisted materialization; the independent deadline bounds
execution time.

Command history keeps the latest 10,000 entries per session. SQLite cache reuse
also checks the database file signature. PostgreSQL, MySQL, and MongoDB cache
reuse is labeled `ttl_based` and is never authoritative.

StateQL limits persisted result payloads to 256 MiB by default. When that quota
is reached it removes the oldest unaliased results; aliases remain protected. A
single result that cannot fit fails with `STATE_QUOTA_EXCEEDED`. Configure the
limit with `maxStateBytes` in the library or `--max-state-bytes` in the CLI.
Cache and result retention can be configured with `cacheTtlSeconds` and
`resultTtlSeconds`, or their `--cache-ttl-seconds` and
`--result-ttl-seconds` CLI equivalents.

`stql doctor` checks SQLite integrity and stored payload shapes without printing
SQL, parameters, or result values. `stql purge` removes expired data by default;
use `results`, `history`, or `all` for explicit session cleanup. On POSIX
systems, StateQL removes group and world access from its state directory,
database, and SQLite sidecar files.

### Local filtering

`filter` evaluates one scalar SQLite predicate against a stored result. It
preserves source order, state metadata, and expiry, and never accesses the
original database.

Use parameters for values. Subqueries, query-shaping clauses, and
non-allowlisted functions are rejected. Common deterministic functions such as
`lower`, `upper`, `length`, and `coalesce` are supported.

## Write safety

Destructive and unbounded operations require `--allow-destructive` and
`--allow-unbounded`, respectively. The flags are independent.

`plan` validates and stores a write for later application. A plan persists only
the flags explicitly supplied when it is created; `apply` never adds
authorization.

Use an idempotency key to protect retryable writes from duplicate execution:

```bash
stql exec "UPDATE jobs SET claimed = 1 WHERE id = ?" \
  --param 42 \
  --idempotency-key claim-job-42
```

If a write starts but StateQL cannot safely record its final outcome, it returns
`OUTCOME_UNKNOWN` and blocks automatic replay. Inspect database state before
using `--replay`. Interrupted commits remain fail-closed; stale `committing`
records become `outcome_unknown` after five minutes.

### Transactions

Transactions are staged in local state so they survive CLI invocations, then
executed atomically on commit. While a transaction is active, StateQL rejects
database reads, plans, connection changes, and disconnects. Commit or roll back
first.

SQLite supports `serializable`. PostgreSQL and MySQL also support
`repeatable read`, `read committed`, and `read uncommitted`. Server reads run
inside database-enforced read-only transactions. MySQL staged transactions
reject DDL because MySQL implicitly commits those statements.
MongoDB transactions use `snapshot` isolation and require a replica set or
sharded deployment; standalone servers do not support them.

## Batch and pipes

`batch` reads a JSON array from a `.json` file or JSONL from a `.jsonl` file.
`pipe` reads JSONL from standard input. Commands run sequentially and stop on
the first error unless `--continue-on-error` is set. Output defaults to one
compact `agent` JSON object per line.

Pipe commands directly:

```bash
printf '%s\n' \
  '{"command":"query","sql":"SELECT id, email FROM users ORDER BY id","as":"users"}' \
  '{"command":"filter","handle":"users","where":"email LIKE ?","params":["%@example.com"],"as":"example_users"}' \
  '{"command":"rows","handle":"example_users","limit":10}' |
  stql pipe
```

Or save a JSON array as `commands.json`:

```json
[
  {
    "command": "exec",
    "sql": "UPDATE jobs SET claimed = 1 WHERE id = ?",
    "params": [42],
    "idempotency_key": "claim-job-42"
  },
  {
    "command": "query",
    "sql": "SELECT * FROM jobs WHERE id = ?",
    "params": [42]
  }
]
```

```bash
stql batch commands.json
```

Batch fields use snake case. Supported command names match CLI paths, such as
`filter`, `transaction.begin`, `session.summary`, `alias.set`, `plan`, and
`apply`. Native MongoDB batches use `mongo.query`, `mongo.exec`, or `mongo.plan`
with the command object in `mongo`; the same cache, replay, idempotency, safety,
and timeout fields apply. Database commands may set `timeout_ms`; otherwise they
use the 30-second default.

## TypeScript library

The package exports the same stateful operations for programmatic use. Library
responses retain the full response envelope regardless of the configured CLI
output mode.

```ts
import { StateQL } from "@fadhilp/stateql";

const stateql = StateQL.forActor({
  home: "./.stql",
  actor: "pi-session-id",
  timeoutMs: 30_000,
  credentialTimeoutMs: 120_000,
  maxResultBytes: 16 * 1024 * 1024,
  maxStateBytes: 256 * 1024 * 1024,
});

const controller = new AbortController();
const response = await stateql.query("SELECT * FROM users", {
  signal: controller.signal,
  timeoutMs: 5_000,
});

if (response.ok) {
  const handle = (response.data as { result_id: string }).result_id;
  await stateql.filter(handle, "email LIKE ?", {
    params: ["%@example.com"],
  });
}
```

Hosts that dispatch batch-shaped commands can attach trusted metadata out of
band. `origin` is audit/source metadata only; it never changes actor membership,
workspace access, or write authorization.

```ts
const controller = new AbortController();
await stateql.executeCommand(
  { command: "query", sql: "SELECT * FROM users", cache: "bypass" },
  { origin: "user", signal: controller.signal },
);

const userHistory = await stateql.history(50, { origin: "user" });
await stateql.executeCommand(
  { command: "history", limit: 50, history_origin: "user" },
  { origin: "model" },
);
```

Supported origins are `legacy`, `user`, `model`, `system`, and `api`. Existing
direct calls and `executeCommand(command)` calls are recorded as `legacy`.
`history_origin` is only a retrieval filter; putting an `origin` field in a
`BatchCommand` cannot attribute the command. `batch` accepts the same trusted
context as `options.executionContext` for all commands in that batch.

### Actor workspaces

`StateQL.forActor(...)` resolves the actor's attached session directly from
StateQL storage, avoiding a duplicate actor-to-session mapping in integrations.
On first use, it creates a legacy-compatible session named after the actor. Use
`new StateQL({ session, actor })` when the session is already known.

Membership is managed only through the library API, not batch commands:
`linkActor(session, actorId)`, `unlinkActor(session, actorId)`,
`listActors(session)`, and `resolveActor(actorId)`. An existing member must link
an actor before that actor opens an existing workspace. Integrations should ask
for user confirmation before changing membership or the shared connection.

### Harness credential resolution

Library integrations can resolve environment-variable names or opaque
credential references through a trusted approval or secret-storage layer
instead of mutating `process.env`:

```ts
import {
  CredentialResolutionError,
  StateQL,
  type CredentialRequest,
} from "@fadhilp/stateql";

async function resolveCredential(
  request: CredentialRequest,
): Promise<string | undefined> {
  const approved = await credentialBroker.request({
    reference: request.reference,
    source: request.source ?? "secret_env",
    actor: request.actorId,
    session: request.session.id,
    operation: request.operation,
    access: request.access,
    signal: request.signal,
  });

  if (approved.denied) throw new CredentialResolutionError("denied");
  return approved.value;
}

const stateql = StateQL.forActor({
  actor: "agent-session-id",
  credentialResolver: resolveCredential,
});
```

Credential resolution has its own two-minute default deadline
(`credentialTimeoutMs`) and remains cancellable through `request.signal`.
The database-operation timeout begins after a credential is resolved.

When no custom resolver is configured, StateQL reads only `secret_env`
references from `process.env`; `credential_ref` never falls back to the
environment. A configured resolver is authoritative for both sources: returning
`undefined` produces `CREDENTIAL_UNAVAILABLE` and never falls back to the
process environment. Resolver requests include `source` (`secret_env` or
`credential_ref`) while retaining `reference`; source may be omitted only on
legacy secret-environment request objects. Resolvers may throw
`CredentialResolutionError` with `denied`, `cancelled`, `timeout`, or
`unavailable` to produce controlled, secret-free failures. Unknown resolver
errors are replaced with a generic `CREDENTIAL_RESOLUTION_FAILED` response.

StateQL calls the resolver only immediately before database access, after SQL
safety and duplicate checks. Requests contain actor and session identity, the
operation's effective read/write access, an abort signal, and sanitized
connection metadata.

Returned values must be complete PostgreSQL, MySQL, MongoDB, or Redis URLs, or
explicit `sqlite:` sources. StateQL validates the source and its stored driver before
adapter construction and normalizes SQLite paths. Credential-bearing database
URLs are redacted before connection metadata is persisted and never enter
history, snapshots, cache keys, or responses. SQLite paths remain persisted
connection metadata, as they are for direct SQLite connections.

Harnesses remain responsible for approval policy, binding lifetime, revocation,
and keeping values out of their own logs and model-visible data.

For writes, credential resolution happens after StateQL atomically reserves the
operation for duplicate protection. A resolution failure keeps a non-executed
`failed` audit record, does not consume the idempotency key, and permits a safe
retry.

## Pylon database integration API (0.9.0)

### Result identities and aliases

Every materialized SQL, MongoDB, Redis, table, or derived result keeps its
immutable `q_*` `result_id` and receives a cryptographically random 10-character
lowercase base32 `display_alias`. `ResultData.alias` normally equals that alias.
When a batch command supplies `as`, `alias` remains the caller alias for backward
compatibility while `display_alias` remains canonical. Generated aliases are
session-scoped, allocated atomically with the result, stable on cache reuse, and
cannot be reassigned by `setAlias`; explicit aliases and all old handles continue
to resolve.

### Safe profile updates

```ts
updateProfile(name, {
  target?: string | null,
  secretEnv?: string | null,
  credentialRef?: string | null,
  readOnly?: boolean,
})
```

Omitting all source fields keeps the existing source. Supplying any source field
replaces the source atomically: exactly one non-null source is required and the
other source columns are cleared. Direct non-SQLite URLs containing credentials
or secret-like query parameters are rejected. `profile.list/show/update` return
only `{profile,target,secret_env,credential_ref,read_only}`. `target` is therefore
a normalized SQLite path or a secret-free URL; reference-backed profiles expose
only the environment-variable name or opaque credential reference, never a
resolved value. Profile changes affect subsequent `connect` calls and do not
silently mutate an already-open connection.

### Bounded catalog

```ts
listObjects(
  { kind?, schema?, search?, offset?, limit? },
  { timeoutMs?, signal? },
) -> { objects, next_offset, supported_kinds }

describeObject(
  { kind, schema?, name, identity? },
  { timeoutMs?, signal? },
) -> { object, definition? }
```

SQL/MongoDB offsets are non-negative numbers; limits default to 50 and are at
most 200. Redis `offset` and `next_offset` are opaque numeric SCAN cursor strings;
its limit is a SCAN `COUNT` hint with a hard 200-item response bound. Redis pages
are not snapshots and can be empty or contain duplicates while keys change.
Search is a case-insensitive name substring for SQL/MongoDB and escaped glob
substring matching for Redis. No exact counts are forced.

Supported kinds are returned on every page: SQLite `table,view,trigger`;
PostgreSQL `table,view,function,trigger,enum`; MySQL
`table,view,function,trigger`; MongoDB `collection,view`; Redis `key`.
PostgreSQL function identities include identity arguments, so overloads remain
distinct. `describeObject` is read-only and requires the structured identity;
legacy `inspect` behavior is unchanged (and intentionally unavailable for Redis).

### Reviewed multi-row table edits

```ts
planTableUpdates(
  Array<{ row_token: string; changes: { set?: object; unset?: string[] } }>,
  options?,
) -> PlanData
```

Batches contain 1-100 distinct row identities and at most 256 KiB. All tokens,
connection/state versions, expiries, metadata, editable columns, and values are
validated before one plan is stored; expiry is the earliest token expiry.
`apply(plan_id)` executes all conditional row updates in one SQLite/PostgreSQL/
MySQL transaction and requires every row predicate to match, otherwise all are
rolled back. MongoDB uses one snapshot transaction and rejects deployments that
do not support transactions. Redis and active staged StateQL transactions are
rejected. The existing `planTableUpdate` and `apply` APIs remain supported.
Plans are actor-owned, claimed once, and retained as non-replayable when the
remote commit outcome is uncertain.

### Redis native commands

Redis/Rediss URLs support URL database selection, password or ACL username,
and TLS (`rediss`). Credential-bearing URLs must come from `secretEnv` or
`credentialRef`. Native methods accept `{command: string, args?: string[]}`:

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

### Lean history

```ts
history(limit?, {
  origin?,
  category?: "statement" | "introspection" | "management",
  internal?: boolean,
  offset?: number,
})
```

`category` and trusted-host `internal` filters are applied in SQLite before
`ORDER BY`, `LIMIT`, and `OFFSET`, so introspection cannot starve statement
history. `CommandExecutionContext.internal` is trusted host metadata and cannot
be supplied inside a batch command. Existing calls and origin filtering remain
compatible; old rows are classified from their command name and migrate as `internal: false`.

The synchronous, non-mutating snapshot bridge accepts the same classification
filters without entering the command queue or writing a history row:

```ts
stateql.snapshot({
  historyLimit: 50,
  historyCategory: "statement",
  historyInternal: false,
});
```

Both snapshot filters are applied by the store before `historyLimit`. Calling
`snapshot()` with no options preserves the legacy 50-entry CLI snapshot.